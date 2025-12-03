import os
import asyncio
from datetime import date, datetime, timedelta
from decimal import Decimal

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command

from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from aiogram.enums.parse_mode import ParseMode

from sqlalchemy import (
    String, Integer, Numeric, Date, DateTime, ForeignKey, Boolean,
    select, func, delete, case
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, selectinload
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker


# ===================== Settings =====================
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

DB_URL = os.getenv("DB_URL", "sqlite+aiosqlite:////var/data/data.db")

ADMIN_USER_IDS = set(
    int(x) for x in os.getenv("ADMIN_USER_IDS", "").split(",")
    if x.strip().isdigit()
)

print("=== BOOT ===", flush=True)
print("TOKEN set:", bool(TOKEN), flush=True)
print("DB_URL:", DB_URL, flush=True)


# ===================== DB models =====================
class Base(DeclarativeBase):
    pass


class Warehouse(Base):
    __tablename__ = "warehouses"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, index=True)


class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(150), unique=True, index=True)


class Bank(Base):
    __tablename__ = "banks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, index=True)


class Stock(Base):
    __tablename__ = "stocks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"), index=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), index=True)
    qty_kg: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=Decimal("0"))

    warehouse: Mapped[Warehouse] = relationship()
    product: Mapped[Product] = relationship()


class MoneyLedger(Base):
    """
    direction:
      - in  (приход денег)
      - out (расход денег)

    method:
      - cash    (как оплатили)
      - noncash

    account_type (куда легло/откуда ушло):
      - cash (наличные)
      - bank (банк компании)
      - ip   (счет ИП)

    bank_id для bank/ip обязателен, для cash = None
    """
    __tablename__ = "money_ledger"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    entry_date: Mapped[date] = mapped_column(Date, index=True)

    direction: Mapped[str] = mapped_column(String(10))  # in / out
    method: Mapped[str] = mapped_column(String(10))      # cash / noncash

    account_type: Mapped[str] = mapped_column(String(10), default="cash")  # cash/bank/ip
    bank_id: Mapped[int | None] = mapped_column(ForeignKey("banks.id"), nullable=True)
    bank: Mapped[Bank | None] = relationship()

    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    note: Mapped[str] = mapped_column(String(300), default="")


class Sale(Base):
    __tablename__ = "sales"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    doc_date: Mapped[date] = mapped_column(Date, index=True)

    customer_name: Mapped[str] = mapped_column(String(150), default="")
    customer_phone: Mapped[str] = mapped_column(String(50), default="")

    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"), index=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), index=True)

    qty_kg: Mapped[Decimal] = mapped_column(Numeric(18, 3))
    price_per_kg: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    total_amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    delivery_cost: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"))

    is_paid: Mapped[bool] = mapped_column(Boolean, default=True)
    payment_method: Mapped[str] = mapped_column(String(10), default="")  # cash/noncash

    # NEW:
    account_type: Mapped[str] = mapped_column(String(10), default="cash")  # cash/bank/ip
    bank_id: Mapped[int | None] = mapped_column(ForeignKey("banks.id"), nullable=True)

    warehouse: Mapped[Warehouse] = relationship()
    product: Mapped[Product] = relationship()
    bank: Mapped[Bank | None] = relationship()


class Income(Base):
    __tablename__ = "incomes"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    doc_date: Mapped[date] = mapped_column(Date, index=True)

    supplier_name: Mapped[str] = mapped_column(String(150), default="")
    supplier_phone: Mapped[str] = mapped_column(String(50), default="")

    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"), index=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), index=True)

    qty_kg: Mapped[Decimal] = mapped_column(Numeric(18, 3))
    price_per_kg: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    total_amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    delivery_cost: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"))

    add_money_entry: Mapped[bool] = mapped_column(Boolean, default=False)
    payment_method: Mapped[str] = mapped_column(String(10), default="")  # cash/noncash for expense

    # NEW:
    account_type: Mapped[str] = mapped_column(String(10), default="cash")  # cash/bank/ip
    bank_id: Mapped[int | None] = mapped_column(ForeignKey("banks.id"), nullable=True)

    warehouse: Mapped[Warehouse] = relationship()
    product: Mapped[Product] = relationship()
    bank: Mapped[Bank | None] = relationship()


class Debtor(Base):
    __tablename__ = "debtors"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    doc_date: Mapped[date] = mapped_column(Date, index=True)

    customer_name: Mapped[str] = mapped_column(String(150), default="")
    customer_phone: Mapped[str] = mapped_column(String(50), default="")

    warehouse_name: Mapped[str] = mapped_column(String(120), default="")
    product_name: Mapped[str] = mapped_column(String(150), default="")

    qty_kg: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=Decimal("0"))
    price_per_kg: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0"))
    total_amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0"))
    delivery_cost: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0"))

    is_paid: Mapped[bool] = mapped_column(Boolean, default=False)


engine = create_async_engine(DB_URL, echo=False)
Session = async_sessionmaker(engine, expire_on_commit=False)


# ===================== Helpers =====================
def is_admin(user_id: int) -> bool:
    return (not ADMIN_USER_IDS) or (user_id in ADMIN_USER_IDS)


def dec(s: str) -> Decimal:
    s = (s or "").strip().replace(",", ".")
    return Decimal(s)


def fmt_money(x: Decimal) -> str:
    return f"{Decimal(x):.2f}"


def fmt_kg(x: Decimal) -> str:
    return f"{Decimal(x):.3f}".rstrip("0").rstrip(".")


def safe_text(s: str) -> str:
    return (s or "").strip()


def safe_phone(s: str) -> str:
    return (s or "").strip()


# ===================== Menus =====================
def main_menu_kb():
    kb = ReplyKeyboardBuilder()
    kb.button(text="📦 Остатки")
    kb.button(text="💰 Деньги")
    kb.adjust(2)

    kb.button(text="🟢 Приход")
    kb.button(text="🔴 Продажа")
    kb.adjust(2)

    kb.button(text="📄 Приходы")
    kb.button(text="📄 Продажи")
    kb.adjust(2)

    kb.button(text="📋 Должники")
    kb.button(text="➕ Добавить должн...")
    kb.adjust(2)

    kb.button(text="🏬 Склады")
    kb.button(text="🧺 Товары")
    kb.adjust(2)

    kb.button(text="🏦 Банки")
    kb.adjust(1)

    kb.button(text="❌ Отмена")
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True)


def warehouses_menu_kb():
    kb = ReplyKeyboardBuilder()
    kb.button(text="➕ Добавить склад")
    kb.button(text="📃 Список складов")
    kb.button(text="🗑 Удалить склад")
    kb.adjust(1)
    kb.button(text="⬅️ Назад в меню")
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True)


def products_menu_kb():
    kb = ReplyKeyboardBuilder()
    kb.button(text="➕ Добавить товар")
    kb.button(text="📃 Список товаров")
    kb.button(text="🗑 Удалить товар")
    kb.adjust(1)
    kb.button(text="⬅️ Назад в меню")
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True)


def banks_menu_kb():
    kb = ReplyKeyboardBuilder()
    kb.button(text="➕ Добавить банк")
    kb.button(text="📃 Список банков")
    kb.button(text="🗑 Удалить банк")
    kb.adjust(1)
    kb.button(text="⬅️ Назад в меню")
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True)


# ===================== Inline helpers =====================
def yes_no_kb(prefix: str):
    ikb = InlineKeyboardBuilder()
    ikb.button(text="✅ Да", callback_data=f"{prefix}:yes")
    ikb.button(text="❌ Нет", callback_data=f"{prefix}:no")
    ikb.adjust(2)
    return ikb.as_markup()


def nav_kb(prefix: str, allow_skip: bool):
    ikb = InlineKeyboardBuilder()
    ikb.button(text="⬅️ Назад", callback_data=f"{prefix}:back")
    if allow_skip:
        ikb.button(text="⏭ Пропустить", callback_data=f"{prefix}:skip")
    ikb.adjust(2)
    return ikb.as_markup()


def pay_method_kb(prefix: str):
    ikb = InlineKeyboardBuilder()
    ikb.button(text="💵 Нал", callback_data=f"{prefix}:cash")
    ikb.button(text="🏦 Безнал", callback_data=f"{prefix}:noncash")
    ikb.adjust(2)
    return ikb.as_markup()


def account_type_kb(prefix: str):
    ikb = InlineKeyboardBuilder()
    ikb.button(text="💵 Наличные", callback_data=f"{prefix}:cash")
    ikb.button(text="🏦 Банк", callback_data=f"{prefix}:bank")
    ikb.button(text="👤 Счёт ИП", callback_data=f"{prefix}:ip")
    ikb.adjust(1)
    return ikb.as_markup()


def sale_status_kb():
    ikb = InlineKeyboardBuilder()
    ikb.button(text="✅ Оплачено", callback_data="sale_status:paid")
    ikb.button(text="🧾 Не оплачено", callback_data="sale_status:unpaid")
    ikb.adjust(2)
    return ikb.as_markup()


# ===================== Simple Inline Calendar =====================
def cal_open_kb(scope: str, year: int, month: int):
    first = date(year, month, 1)
    start_weekday = first.weekday()  # Monday=0
    if month == 12:
        next_m = date(year + 1, 1, 1)
    else:
        next_m = date(year, month + 1, 1)
    days_in_month = (next_m - timedelta(days=1)).day

    ikb = InlineKeyboardBuilder()
    title = first.strftime("%B %Y")
    ikb.button(text=f"📅 {title}", callback_data=f"cal:{scope}:noop:{year:04d}-{month:02d}")

    for w in ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]:
        ikb.button(text=w, callback_data=f"cal:{scope}:noop:{year:04d}-{month:02d}")

    cells = []
    for _ in range(start_weekday):
        cells.append((" ", f"cal:{scope}:noop:{year:04d}-{month:02d}"))

    for day in range(1, days_in_month + 1):
        d = date(year, month, day)
        cells.append((str(day), f"cal:{scope}:pick:{d.isoformat()}"))

    while len(cells) % 7 != 0:
        cells.append((" ", f"cal:{scope}:noop:{year:04d}-{month:02d}"))

    for text, cb in cells:
        ikb.button(text=text, callback_data=cb)

    prev_y, prev_m = year, month - 1
    if prev_m == 0:
        prev_m = 12
        prev_y -= 1
    next_y, next_m = year, month + 1
    if next_m == 13:
        next_m = 1
        next_y += 1

    ikb.button(text="◀️", callback_data=f"cal:{scope}:prev:{prev_y:04d}-{prev_m:02d}")
    ikb.button(text="Сегодня", callback_data=f"cal:{scope}:pick:{date.today().isoformat()}")
    ikb.button(text="▶️", callback_data=f"cal:{scope}:next:{next_y:04d}-{next_m:02d}")

    rows = 1 + 1 + (len(cells) // 7) + 1
    ikb.adjust(1, 7, *([7] * (rows - 3)), 3)
    return ikb.as_markup()


def choose_date_kb(scope: str):
    ikb = InlineKeyboardBuilder()
    ikb.button(text="📅 Выбрать дату", callback_data=f"cal:{scope}:open:{date.today().strftime('%Y-%m')}")
    ikb.adjust(1)
    return ikb.as_markup()


# ===================== FSM =====================
class SaleWizard(StatesGroup):
    doc_date = State()
    customer_name = State()
    customer_phone = State()
    warehouse = State()
    product = State()
    qty = State()
    price = State()
    delivery = State()
    paid_status = State()
    pay_method = State()
    account_type = State()
    bank_pick = State()
    confirm = State()

    adding_warehouse = State()
    adding_product = State()
    adding_bank = State()


class IncomeWizard(StatesGroup):
    doc_date = State()
    supplier_name = State()
    supplier_phone = State()
    warehouse = State()
    product = State()
    qty = State()
    price = State()
    delivery = State()
    add_money = State()
    pay_method = State()
    account_type = State()
    bank_pick = State()
    confirm = State()

    adding_warehouse = State()
    adding_product = State()
    adding_bank = State()


class DebtorWizard(StatesGroup):
    doc_date = State()
    customer_name = State()
    customer_phone = State()
    warehouse_name = State()
    product_name = State()
    qty = State()
    price = State()
    delivery = State()
    confirm = State()


class WarehousesAdmin(StatesGroup):
    adding = State()
    deleting = State()


class ProductsAdmin(StatesGroup):
    adding = State()
    deleting = State()


class BanksAdmin(StatesGroup):
    adding = State()
    deleting = State()


# ===================== Router =====================
router = Router()

MENU_TEXTS = {
    "📦 Остатки", "💰 Деньги", "🟢 Приход", "🔴 Продажа",
    "📄 Приходы", "📄 Продажи", "📋 Должники", "➕ Добавить должн...",
    "🏬 Склады", "🧺 Товары", "🏦 Банки",
    "❌ Отмена",
    "➕ Добавить склад", "📃 Список складов", "🗑 Удалить склад",
    "➕ Добавить товар", "📃 Список товаров", "🗑 Удалить товар",
    "➕ Добавить банк", "📃 Список банков", "🗑 Удалить банк",
    "⬅️ Назад в меню",
}


# ===================== Core DB helper =====================
async def get_stock_row(session, warehouse_id: int, product_id: int) -> Stock:
    row = await session.scalar(
        select(Stock).where(
            Stock.warehouse_id == warehouse_id,
            Stock.product_id == product_id
        )
    )
    if row:
        return row
    row = Stock(warehouse_id=warehouse_id, product_id=product_id, qty_kg=Decimal("0"))
    session.add(row)
    await session.flush()
    return row


# ===================== Picklists (inline) =====================
async def pick_warehouse_kb(prefix: str):
    async with Session() as s:
        rows = (await s.execute(select(Warehouse).order_by(Warehouse.name))).scalars().all()
    ikb = InlineKeyboardBuilder()
    if not rows:
        ikb.button(text="➕ Добавить склад", callback_data=f"{prefix}:add_new")
        ikb.button(text="⬅️ Назад", callback_data=f"{prefix}:back")
        ikb.adjust(1)
        return ikb.as_markup()
    for w in rows:
        ikb.button(text=w.name, callback_data=f"{prefix}:id:{w.id}")
    ikb.button(text="➕ Добавить склад", callback_data=f"{prefix}:add_new")
    ikb.button(text="⬅️ Назад", callback_data=f"{prefix}:back")
    ikb.adjust(2)
    return ikb.as_markup()


async def pick_product_kb(prefix: str):
    async with Session() as s:
        rows = (await s.execute(select(Product).order_by(Product.name))).scalars().all()
    ikb = InlineKeyboardBuilder()
    if not rows:
        ikb.button(text="➕ Добавить товар", callback_data=f"{prefix}:add_new")
        ikb.button(text="⬅️ Назад", callback_data=f"{prefix}:back")
        ikb.adjust(1)
        return ikb.as_markup()
    for p in rows:
        ikb.button(text=p.name, callback_data=f"{prefix}:id:{p.id}")
    ikb.button(text="➕ Добавить товар", callback_data=f"{prefix}:add_new")
    ikb.button(text="⬅️ Назад", callback_data=f"{prefix}:back")
    ikb.adjust(2)
    return ikb.as_markup()


async def pick_bank_kb(prefix: str):
    async with Session() as s:
        rows = (await s.execute(select(Bank).order_by(Bank.name))).scalars().all()
    ikb = InlineKeyboardBuilder()
    if not rows:
        ikb.button(text="➕ Добавить банк", callback_data=f"{prefix}:add_new")
        ikb.button(text="⬅️ Назад", callback_data=f"{prefix}:back")
        ikb.adjust(1)
        return ikb.as_markup()
    for b in rows:
        ikb.button(text=b.name, callback_data=f"{prefix}:id:{b.id}")
    ikb.button(text="➕ Добавить банк", callback_data=f"{prefix}:add_new")
    ikb.button(text="⬅️ Назад", callback_data=f"{prefix}:back")
    ikb.adjust(2)
    return ikb.as_markup()


# ===================== Menu handler =====================
@router.message(F.text.in_(MENU_TEXTS))
async def menu_anywhere(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа.")

    text = message.text

    if text == "❌ Отмена":
        await state.clear()
        return await message.answer("Ок, отменил ✅", reply_markup=main_menu_kb())

    if text == "⬅️ Назад в меню":
        await state.clear()
        return await message.answer("Меню:", reply_markup=main_menu_kb())

    if text == "📦 Остатки":
        await state.clear()
        return await show_stocks_table(message)

    if text == "💰 Деньги":
        await state.clear()
        return await show_money(message)

    if text == "🟢 Приход":
        await state.clear()
        return await start_income(message, state)

    if text == "🔴 Продажа":
        await state.clear()
        return await start_sale(message, state)

    if text == "📄 Продажи":
        await state.clear()
        return await list_sales(message)

    if text == "📄 Приходы":
        await state.clear()
        return await list_incomes(message)

    if text == "📋 Должники":
        await state.clear()
        return await list_debtors(message)

    if text == "➕ Добавить должн...":
        await state.clear()
        return await start_debtor(message, state)

    if text == "🏬 Склады":
        await state.clear()
        return await message.answer("Управление складами:", reply_markup=warehouses_menu_kb())

    if text == "🧺 Товары":
        await state.clear()
        return await message.answer("Управление товарами:", reply_markup=products_menu_kb())

    if text == "🏦 Банки":
        await state.clear()
        return await message.answer("Управление банками:", reply_markup=banks_menu_kb())

    # warehouses admin actions
    if text == "➕ Добавить склад":
        await state.clear()
        await state.set_state(WarehousesAdmin.adding)
        return await message.answer("Напиши название склада:", reply_markup=warehouses_menu_kb())

    if text == "📃 Список складов":
        await state.clear()
        return await list_warehouses(message)

    if text == "🗑 Удалить склад":
        await state.clear()
        await state.set_state(WarehousesAdmin.deleting)
        return await message.answer("Напиши EXACT название склада для удаления:", reply_markup=warehouses_menu_kb())

    # products admin actions
    if text == "➕ Добавить товар":
        await state.clear()
        await state.set_state(ProductsAdmin.adding)
        return await message.answer("Напиши название товара:", reply_markup=products_menu_kb())

    if text == "📃 Список товаров":
        await state.clear()
        return await list_products(message)

    if text == "🗑 Удалить товар":
        await state.clear()
        await state.set_state(ProductsAdmin.deleting)
        return await message.answer("Напиши EXACT название товара для удаления:", reply_markup=products_menu_kb())

    # banks admin actions
    if text == "➕ Добавить банк":
        await state.clear()
        await state.set_state(BanksAdmin.adding)
        return await message.answer("Напиши название банка:", reply_markup=banks_menu_kb())

    if text == "📃 Список банков":
        await state.clear()
        return await list_banks(message)

    if text == "🗑 Удалить банк":
        await state.clear()
        await state.set_state(BanksAdmin.deleting)
        return await message.answer("Напиши EXACT название банка для удаления:", reply_markup=banks_menu_kb())


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа.")
    await state.clear()
    await message.answer("Привет! Выбери действие:", reply_markup=main_menu_kb())


# ===================== Warehouses Admin =====================
@router.message(WarehousesAdmin.adding)
async def wh_add(message: Message, state: FSMContext):
    name = safe_text(message.text)
    if not name:
        return await message.answer("Пусто. Напиши название склада.")
    async with Session() as s:
        exists = await s.scalar(select(Warehouse).where(Warehouse.name == name))
        if exists:
            await state.clear()
            return await message.answer("Такой склад уже есть ✅", reply_markup=warehouses_menu_kb())
        s.add(Warehouse(name=name))
        await s.commit()
    await state.clear()
    await message.answer(f"✅ Склад добавлен: {name}", reply_markup=warehouses_menu_kb())


@router.message(WarehousesAdmin.deleting)
async def wh_del(message: Message, state: FSMContext):
    name = safe_text(message.text)
    async with Session() as s:
        w = await s.scalar(select(Warehouse).where(Warehouse.name == name))
        if not w:
            await state.clear()
            return await message.answer("Склад не найден.", reply_markup=warehouses_menu_kb())

        cnt = await s.scalar(select(func.count()).select_from(Stock).where(Stock.warehouse_id == w.id))
        if int(cnt) > 0:
            await state.clear()
            return await message.answer("Нельзя удалить: есть остатки/движения по этому складу.", reply_markup=warehouses_menu_kb())

        await s.execute(delete(Warehouse).where(Warehouse.id == w.id))
        await s.commit()

    await state.clear()
    await message.answer(f"🗑 Склад удалён: {name}", reply_markup=warehouses_menu_kb())


async def list_warehouses(message: Message):
    async with Session() as s:
        rows = (await s.execute(select(Warehouse).order_by(Warehouse.name))).scalars().all()
    if not rows:
        return await message.answer("Складов пока нет. Добавь через ➕", reply_markup=warehouses_menu_kb())
    txt = "🏬 *Склады:*\n" + "\n".join([f"• {w.name}" for w in rows])
    await message.answer(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=warehouses_menu_kb())


# ===================== Products Admin =====================
@router.message(ProductsAdmin.adding)
async def prod_add(message: Message, state: FSMContext):
    name = safe_text(message.text)
    if not name:
        return await message.answer("Пусто. Напиши название товара.")
    async with Session() as s:
        exists = await s.scalar(select(Product).where(Product.name == name))
        if exists:
            await state.clear()
            return await message.answer("Такой товар уже есть ✅", reply_markup=products_menu_kb())
        s.add(Product(name=name))
        await s.commit()
    await state.clear()
    await message.answer(f"✅ Товар добавлен: {name}", reply_markup=products_menu_kb())


@router.message(ProductsAdmin.deleting)
async def prod_del(message: Message, state: FSMContext):
    name = safe_text(message.text)
    async with Session() as s:
        p = await s.scalar(select(Product).where(Product.name == name))
        if not p:
            await state.clear()
            return await message.answer("Товар не найден.", reply_markup=products_menu_kb())

        cnt = await s.scalar(select(func.count()).select_from(Stock).where(Stock.product_id == p.id))
        if int(cnt) > 0:
            await state.clear()
            return await message.answer("Нельзя удалить: есть остатки/движения по этому товару.", reply_markup=products_menu_kb())

        await s.execute(delete(Product).where(Product.id == p.id))
        await s.commit()

    await state.clear()
    await message.answer(f"🗑 Товар удалён: {name}", reply_markup=products_menu_kb())


async def list_products(message: Message):
    async with Session() as s:
        rows = (await s.execute(select(Product).order_by(Product.name))).scalars().all()
    if not rows:
        return await message.answer("Товаров пока нет. Добавь через ➕", reply_markup=products_menu_kb())
    txt = "🧺 *Товары:*\n" + "\n".join([f"• {p.name}" for p in rows])
    await message.answer(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=products_menu_kb())


# ===================== Banks Admin =====================
@router.message(BanksAdmin.adding)
async def bank_add(message: Message, state: FSMContext):
    name = safe_text(message.text)
    if not name:
        return await message.answer("Пусто. Напиши название банка.")
    async with Session() as s:
        exists = await s.scalar(select(Bank).where(Bank.name == name))
        if exists:
            await state.clear()
            return await message.answer("Такой банк уже есть ✅", reply_markup=banks_menu_kb())
        s.add(Bank(name=name))
        await s.commit()
    await state.clear()
    await message.answer(f"✅ Банк добавлен: {name}", reply_markup=banks_menu_kb())


@router.message(BanksAdmin.deleting)
async def bank_del(message: Message, state: FSMContext):
    name = safe_text(message.text)
    async with Session() as s:
        b = await s.scalar(select(Bank).where(Bank.name == name))
        if not b:
            await state.clear()
            return await message.answer("Банк не найден.", reply_markup=banks_menu_kb())

        cnt = await s.scalar(select(func.count()).select_from(MoneyLedger).where(MoneyLedger.bank_id == b.id))
        if int(cnt) > 0:
            await state.clear()
            return await message.answer("Нельзя удалить: есть операции по этому банку.", reply_markup=banks_menu_kb())

        await s.execute(delete(Bank).where(Bank.id == b.id))
        await s.commit()

    await state.clear()
    await message.answer(f"🗑 Банк удалён: {name}", reply_markup=banks_menu_kb())


async def list_banks(message: Message):
    async with Session() as s:
        rows = (await s.execute(select(Bank).order_by(Bank.name))).scalars().all()
    if not rows:
        return await message.answer("Банков пока нет. Добавь через ➕", reply_markup=banks_menu_kb())
    txt = "🏦 *Банки:*\n" + "\n".join([f"• {b.name}" for b in rows])
    await message.answer(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=banks_menu_kb())


# ===================== Stocks =====================
async def show_stocks_table(message: Message):
    async with Session() as s:
        rows = (await s.execute(
            select(Stock)
            .options(selectinload(Stock.warehouse), selectinload(Stock.product))
            .order_by(Stock.warehouse_id, Stock.product_id)
        )).scalars().all()

    if not rows:
        return await message.answer("Остатков пока нет.", reply_markup=main_menu_kb())

    data = [(r.warehouse.name, r.product.name, fmt_kg(r.qty_kg)) for r in rows if Decimal(r.qty_kg) != 0]
    if not data:
        return await message.answer("Пока везде 0.", reply_markup=main_menu_kb())

    w1 = max(len("Склад"), max(len(x[0]) for x in data))
    w2 = max(len("Товар"), max(len(x[1]) for x in data))
    w3 = max(len("Остаток(кг)"), max(len(x[2]) for x in data))

    lines = []
    lines.append(f"{'Склад'.ljust(w1)} | {'Товар'.lajust(w2)} | {'Остаток(кг)'.rjust(w3)}")
    lines.append(f"{'-'*w1}-+-{'-'*w2}-+-{'-'*w3}")
    for wh, pr, q in data:
        lines.append(f"{wh.lajust(w1)} | {pr.ljust(w2)} | {q.rjust(w3)}")

    txt = "📦 Остатки:\n<pre>" + "\n".join(lines) + "</pre>"
    await message.answer(txt, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb())


# ===================== Money =====================
async def show_money(message: Message):
    async with Session() as s:
        # balance = sum(in) - sum(out)
        rows = (await s.execute(
            select(
                MoneyLedger.account_type,
                MoneyLedger.bank_id,
                func.coalesce(
                    func.sum(
                        case(
                            (MoneyLedger.direction == "in", MoneyLedger.amount),
                            else_=-MoneyLedger.amount
                        )
                    ),
                    0
                ).label("bal")
            )
            .group_by(MoneyLedger.account_type, MoneyLedger.bank_id)
        )).all()

        bank_ids = [r.bank_id for r in rows if r.bank_id is not None]
        bank_map = {}
        if bank_ids:
            banks = (await s.execute(select(Bank).where(Bank.id.in_(bank_ids)))).scalars().all()
            bank_map = {b.id: b.name for b in banks}

    cash_balance = Decimal("0")
    bank_lines = []
    ip_lines = []

    for acc_type, bank_id, bal in rows:
        bal = Decimal(bal)
        if acc_type == "cash":
            cash_balance += bal
        elif acc_type == "bank":
            name = bank_map.get(bank_id, "Без названия")
            bank_lines.append((name, bal))
        elif acc_type == "ip":
            name = bank_map.get(bank_id, "Без названия")
            ip_lines.append((name, bal))

    bank_lines.sort(key=lambda x: x[0].lower())
    ip_lines.sort(key=lambda x: x[0].lower())

    txt = ["💰 *Деньги (балансы):*",
           f"\n💵 *Наличные:* *{fmt_money(cash_balance)}*"]

    txt.append("\n🏦 *Банки:*")
    if bank_lines:
        for name, bal in bank_lines:
            txt.append(f"• {name}: *{fmt_money(bal)}*")
    else:
        txt.append("• (пусто)")

    txt.append("\n👤 *Счёт ИП:*")
    if ip_lines:
        for name, bal in ip_lines:
            txt.append(f"• {name}: *{fmt_money(bal)}*")
    else:
        txt.append("• (пусто)")

    await message.answer("\n".join(txt), parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_kb())


# ===================== Lists (Sales/Incomes/Debtors) =====================
def sales_actions_kb(sale_id: int, paid: bool):
    ikb = InlineKeyboardBuilder()
    if not paid:
        ikb.button(text="✅ Отметить как оплачено", callback_data=f"sale_paid_id:{sale_id}")
    ikb.button(text="🗑 Удалить", callback_data=f"sale_del:{sale_id}")
    ikb.adjust(1)
    return ikb.as_markup()


@router.callback_query(F.data.startswith("sale_paid_id:"))
async def cb_sale_paid_id(cq: CallbackQuery):
    part = cq.data.split(":", 1)[1]
    if not part.isdigit():
        return await cq.answer("Ошибка кнопки. Обнови сообщение.", show_alert=True)

    sale_id = int(part)

    async with Session() as s:
        sale = await s.get(Sale, sale_id)
        if not sale:
            return await cq.answer("Не найдено", show_alert=True)
        if sale.is_paid:
            return await cq.answer("Уже оплачено", show_alert=True)

        sale.is_paid = True

        # Если продажа была неоплачена — считаем, что при оплате деньги попали туда же,
        # куда выбрали бы при оплате. Но если изначально нет данных — положим в cash.
        account_type = sale.account_type or "cash"
        bank_id = sale.bank_id if account_type in ("bank", "ip") else None

        s.add(MoneyLedger(
            entry_date=sale.doc_date,
            direction="in",
            method=sale.payment_method or "cash",
            account_type=account_type,
            bank_id=bank_id,
            amount=sale.total_amount,
            note=f"Оплата продажи #{sale.id} ({sale.customer_name})"
        ))

        # Если была запись в должниках - удаляем ее
        await s.execute(delete(Debtor).where(
            Debtor.customer_name == sale.customer_name,
            Debtor.customer_phone == sale.customer_phone,
            Debtor.warehouse_name.is_(None)
        ))

        await s.commit()

    await cq.answer("✅ Отмечено как оплачено. Запись в MoneyLedger добавлена.", show_alert=False)
    # Обновление сообщения со списком продаж
    await list_sales(cq.message)


# ===================== Income Wizard (Приход) =====================

# --- Добавлено: Функции для начала процесса и перехода к следующему шагу ---

async def next_income_step(message: Message, state: FSMContext):
    """Отправляет запрос следующего шага в мастере прихода."""
    current_state = await state.get_state()
    if current_state == IncomeWizard.doc_date:
        await state.set_state(IncomeWizard.supplier_name)
        return await message.answer("Напиши имя поставщика (можно пропустить):", reply_markup=nav_kb("inc", True))
    elif current_state == IncomeWizard.supplier_name:
        await state.set_state(IncomeWizard.supplier_phone)
        return await message.answer("Напиши телефон поставщика (можно пропустить):", reply_markup=nav_kb("inc", True))
    elif current_state == IncomeWizard.supplier_phone:
        await state.set_state(IncomeWizard.warehouse)
        return await message.answer("Выбери склад:", reply_markup=await pick_warehouse_kb("inc_wh"))
    elif current_state == IncomeWizard.warehouse:
        await state.set_state(IncomeWizard.product)
        return await message.answer("Выбери товар:", reply_markup=await pick_product_kb("inc_prod"))
    elif current_state == IncomeWizard.product:
        await state.set_state(IncomeWizard.qty)
        return await message.answer("Напиши количество товара в кг (напр. 100.5):", reply_markup=nav_kb("inc", False))
    elif current_state == IncomeWizard.qty:
        await state.set_state(IncomeWizard.price)
        return await message.answer("Напиши цену за кг (напр. 15.00):", reply_markup=nav_kb("inc", False))
    elif current_state == IncomeWizard.price:
        await state.set_state(IncomeWizard.delivery)
        return await message.answer("Напиши стоимость доставки (напр. 200.00 или 0):", reply_markup=nav_kb("inc", True))
    elif current_state == IncomeWizard.delivery:
        await state.set_state(IncomeWizard.add_money)
        return await message.answer("Добавить расход в MoneyLedger?", reply_markup=yes_no_kb("inc_money"))
    elif current_state == IncomeWizard.add_money:
        data = await state.get_data()
        if data.get("add_money_entry"):
            await state.set_state(IncomeWizard.pay_method)
            return await message.answer("Метод оплаты:", reply_markup=pay_method_kb("inc_pay"))
        else:
            await state.set_state(IncomeWizard.confirm)
            return await show_income_summary(message, state)
    elif current_state == IncomeWizard.pay_method:
        await state.set_state(IncomeWizard.account_type)
        return await message.answer("Куда записать расход:", reply_markup=account_type_kb("inc_acc"))
    elif current_state == IncomeWizard.account_type:
        data = await state.get_data()
        if data.get("account_type") in ("bank", "ip"):
            await state.set_state(IncomeWizard.bank_pick)
            return await message.answer("Выбери банк/счет:", reply_markup=await pick_bank_kb("inc_bank"))
        else: # cash
            await state.set_state(IncomeWizard.confirm)
            return await show_income_summary(message, state)
    elif current_state == IncomeWizard.bank_pick:
        await state.set_state(IncomeWizard.confirm)
        return await show_income_summary(message, state)


async def start_income(message: Message, state: FSMContext):
    await state.set_state(IncomeWizard.doc_date)
    return await message.answer("Выбери дату прихода:", reply_markup=choose_date_kb("inc_date"))


async def show_income_summary(message: Message, state: FSMContext):
    data = await state.get_data()

    total = data["qty_kg"] * data["price_per_kg"] + data["delivery_cost"]

    txt = [
        "🟢 *Подтверждение Прихода:*",
        f"📅 Дата: *{data['doc_date'].strftime('%Y-%m-%d')}*",
        f"👤 Поставщик: _{data.get('supplier_name') or 'Пропущено'}_",
        f"📞 Телефон: _{data.get('supplier_phone') or 'Пропущено'}_",
        f"🏬 Склад: *{data['warehouse_name']}*",
        f"🧺 Товар: *{data['product_name']}*",
        f"⚖️ Кол-во (кг): *{fmt_kg(data['qty_kg'])}*",
        f"💵 Цена/кг: *{fmt_money(data['price_per_kg'])}*",
        f"🚚 Доставка: *{fmt_money(data['delivery_cost'])}*",
        f"---",
        f"💰 *ИТОГО:* *{fmt_money(total)}*",
    ]

    if data.get("add_money_entry"):
        method = "Нал" if data.get("payment_method") == "cash" else "Безнал"
        acc_type = data.get("account_type")
        acc_name = "Наличные"
        if acc_type == "bank":
            acc_name = f"Банк: {data.get('bank_name')}"
        elif acc_type == "ip":
            acc_name = f"Счет ИП: {data.get('bank_name')}"

        txt.append(f"\n_💸 В Ledger (Расход):_")
        txt.append(f"• Метод: {method}")
        txt.append(f"• Счёт: {acc_name}")


    ikb = InlineKeyboardBuilder()
    ikb.button(text="✅ Подтвердить и сохранить", callback_data="inc_confirm:yes")
    ikb.button(text="❌ Отмена", callback_data="inc_confirm:no")
    ikb.adjust(1)

    await message.answer("\n".join(txt), parse_mode=ParseMode.MARKDOWN, reply_markup=ikb.as_markup())


# --- Date step handler ---
@router.callback_query(F.data.startswith("cal:inc_date:"))
async def cb_inc_date(cq: CallbackQuery, state: FSMContext):
    # Логика календаря
    parts = cq.data.split(":")
    scope, action, rest = parts[1], parts[2], parts[3]

    if action == "open":
        year, month = map(int, rest.split("-"))
        await cq.message.edit_reply_markup(reply_markup=cal_open_kb(scope, year, month))
        return await cq.answer()

    if action == "prev" or action == "next":
        year, month = map(int, rest.split("-"))
        await cq.message.edit_reply_markup(reply_markup=cal_open_kb(scope, year, month))
        return await cq.answer()

    if action == "pick":
        picked_date = date.fromisoformat(rest)
        await state.update_data(doc_date=picked_date)
        await cq.message.edit_text(f"📅 Дата прихода: *{picked_date.strftime('%Y-%m-%d')}*", parse_mode=ParseMode.MARKDOWN)
        await cq.answer()
        return await next_income_step(cq.message, state)

    await cq.answer()


# --- Supplier Name / Phone / Delivery handler ---
@router.message(IncomeWizard.supplier_name, F.text)
@router.message(IncomeWizard.supplier_phone, F.text)
@router.message(IncomeWizard.delivery, F.text)
async def inc_text_input(message: Message, state: FSMContext):
    current_state = await state.get_state()
    text = safe_text(message.text)
    
    # Skip
    if text == "⏭ Пропустить" and current_state in (IncomeWizard.supplier_name, IncomeWizard.supplier_phone, IncomeWizard.delivery):
        if current_state == IncomeWizard.supplier_name:
            await state.update_data(supplier_name=None)
        elif current_state == IncomeWizard.supplier_phone:
            await state.update_data(supplier_phone=None)
        elif current_state == IncomeWizard.delivery:
            await state.update_data(delivery_cost=Decimal("0.00"))
            
        await message.answer("Пропущено.")
        return await next_income_step(message, state)

    # Back
    if text == "⬅️ Назад":
        # Logic to go back one step (omitted for brevity, but needed in real app)
        await message.answer("Функция 'Назад' пока не реализована.")
        return # return to current state

    # Input validation
    if current_state == IncomeWizard.delivery:
        try:
            delivery_cost = dec(text)
            if delivery_cost < 0:
                 return await message.answer("Стоимость доставки не может быть отрицательной.")
            await state.update_data(delivery_cost=delivery_cost)
            await message.answer(f"Стоимость доставки: *{fmt_money(delivery_cost)}*.", parse_mode=ParseMode.MARKDOWN)
        except Exception:
            return await message.answer("Неверный формат числа для стоимости доставки.")
    elif current_state == IncomeWizard.supplier_name:
        await state.update_data(supplier_name=text)
        await message.answer(f"Имя поставщика: *{text}*", parse_mode=ParseMode.MARKDOWN)
    elif current_state == IncomeWizard.supplier_phone:
        await state.update_data(supplier_phone=safe_phone(text))
        await message.answer(f"Телефон поставщика: *{safe_phone(text)}*", parse_mode=ParseMode.MARKDOWN)


    return await next_income_step(message, state)


@router.callback_query(F.data.startswith("inc:back"), IncomeWizard.supplier_name)
@router.callback_query(F.data.startswith("inc:back"), IncomeWizard.supplier_phone)
@router.callback_query(F.data.startswith("inc:skip"), IncomeWizard.supplier_name)
@router.callback_query(F.data.startswith("inc:skip"), IncomeWizard.supplier_phone)
@router.callback_query(F.data.startswith("inc:skip"), IncomeWizard.delivery)
async def cb_inc_nav(cq: CallbackQuery, state: FSMContext):
    # This is a generic handler for 'back' and 'skip' in steps using nav_kb
    # The 'back' logic is complex and usually requires mapping FSM states explicitly,
    # but the 'skip' logic is straightforward:
    action = cq.data.split(":")[1]
    
    if action == "skip":
        current_state = await state.get_state()
        if current_state == IncomeWizard.supplier_name:
            await state.update_data(supplier_name=None)
        elif current_state == IncomeWizard.supplier_phone:
            await state.update_data(supplier_phone=None)
        elif current_state == IncomeWizard.delivery:
            await state.update_data(delivery_cost=Decimal("0.00"))
        
        await cq.message.edit_text(f"Пропущено.")
        return await next_income_step(cq.message, state)
    
    # For 'back', you'd implement the state transition here (omitted)
    await cq.answer("Функция 'Назад' пока не реализована.", show_alert=True)


# --- Warehouse step handler ---
@router.callback_query(F.data.startswith("inc_wh:"))
async def inc_choose_wh(cq: CallbackQuery, state: FSMContext):
    parts = cq.data.split(":", 2)
    prefix, action = parts[0], parts[1]
    rest = parts[2] if len(parts) == 3 else None # 🐛 ИСПРАВЛЕНИЕ: Безопасное получение rest

    await cq.answer()

    if action == "id" and rest is not None and rest.isdigit():
        warehouse_id = int(rest)
        async with Session() as s:
            w = await s.get(Warehouse, warehouse_id)
            if not w:
                await cq.message.answer("Склад не найден. Выбери из списка.")
                return # Stay in current state

            await state.update_data(warehouse_id=w.id, warehouse_name=w.name)
            await cq.message.edit_text(f"🏬 Склад выбран: *{w.name}*", parse_mode=ParseMode.MARKDOWN)
            return await next_income_step(cq.message, state)

    elif action == "add_new":
        await state.set_state(IncomeWizard.adding_warehouse)
        return await cq.message.edit_text("Напиши название нового склада:", reply_markup=nav_kb("inc_wh_add", False))

    elif action == "back":
        # Logic to go back (to supplier_phone, omitted)
        return await cq.message.answer("Функция 'Назад' пока не реализована.")
        
    # Default catch (e.g., if warehouse list is empty and user clicks back/add)
    await cq.message.answer("Пожалуйста, выбери склад или добавь новый.")
    await cq.message.edit_reply_markup(reply_markup=await pick_warehouse_kb("inc_wh"))


@router.callback_query(F.data == "inc_wh_add:back", IncomeWizard.adding_warehouse)
async def inc_add_warehouse_back(cq: CallbackQuery, state: FSMContext):
    await cq.answer()
    await state.set_state(IncomeWizard.warehouse) # Return to warehouse selection step
    await cq.message.edit_text("Выбери склад:", reply_markup=await pick_warehouse_kb("inc_wh"))


@router.message(IncomeWizard.adding_warehouse)
async def inc_add_warehouse_input(message: Message, state: FSMContext):
    name = safe_text(message.text)
    if not name:
        return await message.answer("Пусто. Напиши название склада.")
    
    async with Session() as s:
        exists = await s.scalar(select(Warehouse).where(Warehouse.name == name))
        if exists:
            await message.answer("Такой склад уже есть. Выбери его из списка или введи другое имя.")
            return # Stay in adding_warehouse state
            
        new_wh = Warehouse(name=name)
        s.add(new_wh)
        await s.commit()
        
        # Select the newly added warehouse
        await state.update_data(warehouse_id=new_wh.id, warehouse_name=new_wh.name)
        
    await message.answer(f"✅ Склад добавлен: *{name}*", parse_mode=ParseMode.MARKDOWN)
    return await next_income_step(message, state)


# ===================== Main Loop =====================
async def main():
    bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped.")
    except Exception as e:
        print(f"An error occurred: {e}")
