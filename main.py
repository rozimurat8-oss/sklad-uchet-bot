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
    select, func, delete, case, update, else_
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
    __tablename__ = "money_ledger"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    entry_date: Mapped[date] = mapped_column(Date, index=True)

    direction: Mapped[str] = mapped_column(String(10))  # in / out
    method: Mapped[str] = mapped_column(String(10))     # cash / noncash

    account_type: Mapped[str] = mapped_column(String(10), default="cash")  # cash/bank/ip
    bank_id: Mapped[int | None] = mapped_column(ForeignKey("banks.id"), nullable=True)
    bank: Mapped["Bank | None"] = relationship()

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

    account_type: Mapped[str] = mapped_column(String(10), default="cash")  # cash/bank/ip
    bank_id: Mapped[int | None] = mapped_column(ForeignKey("banks.id"), nullable=True)

    warehouse: Mapped[Warehouse] = relationship()
    product: Mapped[Product] = relationship()
    bank: Mapped["Bank | None"] = relationship()


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

    account_type: Mapped[str] = mapped_column(String(10), default="cash")  # cash/bank/ip
    bank_id: Mapped[int | None] = mapped_column(ForeignKey("banks.id"), nullable=True)

    warehouse: Mapped[Warehouse] = relationship()
    product: Mapped[Product] = relationship()
    bank: Mapped["Bank | None"] = relationship()


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


def parse_cb(data: str, prefix: str):
    """
    Безопасный парсер callback_data.
    Возвращает список частей ПОСЛЕ prefix.
    Например: data="sale_pr:id:12", prefix="sale_pr" -> ["id","12"]
    """
    if not data or not data.startswith(prefix + ":"):
        return []
    rest = data[len(prefix) + 1 :]
    return rest.split(":") if rest else []


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

    # NEW
    kb.button(text="📥 Выгрузка (таблица)")
    kb.adjust(1)

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
    "📄 Приходы", "📄 Продажи",
    "📥 Выгрузка (таблица)",  # NEW
    "📋 Должники", "➕ Добавить должн...",
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

    # NEW
    if text == "📥 Выгрузка (таблица)":
        await state.clear()
        return await export_menu(message)

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
    lines.append(f"{'Склад'.ljust(w1)} | {'Товар'.ljust(w2)} | {'Остаток(кг)'.rjust(w3)}")
    lines.append(f"{'-'*w1}-+-{'-'*w2}-+-{'-'*w3}")
    for wh, pr, q in data:
        lines.append(f"{wh.ljust(w1)} | {pr.ljust(w2)} | {q.rjust(w3)}")

    txt = "📦 Остатки:\n<pre>" + "\n".join(lines) + "</pre>"
    await message.answer(txt, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb())


# ===================== Money =====================
async def show_money(message: Message):
    async with Session() as s:
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


# ===================== EXPORT TABLES (CHAT) =====================
EXPORT_PAGE_SIZE = 20  # безопасно для Telegram, можно менять


def export_menu_kb():
    ikb = InlineKeyboardBuilder()
    ikb.button(text="📦 Остатки", callback_data="exp:stocks:0")
    ikb.button(text="🟢 Приходы", callback_data="exp:incomes:0")
    ikb.button(text="🔴 Продажи", callback_data="exp:sales:0")
    ikb.button(text="⬅️ Назад", callback_data="exp:back")
    ikb.adjust(2, 1, 1)
    return ikb.as_markup()


def export_pager_kb(kind: str, page: int, has_prev: bool, has_next: bool):
    ikb = InlineKeyboardBuilder()
    if has_prev:
        ikb.button(text="⬅️ Назад", callback_data=f"exp:{kind}:{page-1}")
    if has_next:
        ikb.button(text="➡️ Далее", callback_data=f"exp:{kind}:{page+1}")
    ikb.button(text="🏠 Меню выгрузки", callback_data="exp:menu")
    ikb.adjust(2, 1)
    return ikb.as_markup()


async def export_menu(message: Message):
    await message.answer("📥 Выгрузка таблиц (в чате):", reply_markup=export_menu_kb())


def _render_pre_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))

    line1 = " | ".join(headers[i].ljust(widths[i]) for i in range(len(headers)))
    line2 = "-+-".join("-" * widths[i] for i in range(len(headers)))

    body = []
    for r in rows:
        body.append(" | ".join(r[i].ljust(widths[i]) for i in range(len(headers))))

    return "<pre>" + "\n".join([line1, line2] + body) + "</pre>"


async def export_stocks_text(page: int):
    async with Session() as s:
        rows = (await s.execute(
            select(Stock)
            .options(selectinload(Stock.warehouse), selectinload(Stock.product))
            .order_by(Stock.warehouse_id, Stock.product_id)
        )).scalars().all()

    data = []
    for r in rows:
        q = Decimal(r.qty_kg or 0)
        if q == 0:
            continue
        data.append([r.warehouse.name, r.product.name, fmt_kg(q)])

    if not data:
        return "📦 Остатки: (везде 0)", None

    total = len(data)
    start = page * EXPORT_PAGE_SIZE
    end = start + EXPORT_PAGE_SIZE
    slice_rows = data[start:end]

    has_prev = page > 0
    has_next = end < total

    txt = "📦 Остатки:\n" + _render_pre_table(
        headers=["Склад", "Товар", "Остаток(кг)"],
        rows=slice_rows
    )
    kb = export_pager_kb("stocks", page, has_prev, has_next)
    return txt, kb


async def export_incomes_text(page: int):
    async with Session() as s:
        rows = (await s.execute(
            select(Income)
            .options(selectinload(Income.warehouse), selectinload(Income.product))
            .order_by(Income.id.desc())
            .limit(50)
        )).scalars().all()

    if not rows:
        return "🟢 Приходы: записей нет.", None

    data = []
    for r in rows:
        data.append([
            str(r.doc_date),
            r.warehouse.name if r.warehouse else "-",
            r.product.name if r.product else "-",
            fmt_kg(Decimal(r.qty_kg or 0)),
        ])

    total = len(data)
    start = page * EXPORT_PAGE_SIZE
    end = start + EXPORT_PAGE_SIZE
    slice_rows = data[start:end]

    has_prev = page > 0
    has_next = end < total

    txt = "🟢 Приходы (последние 50):\n" + _render_pre_table(
        headers=["Дата", "Склад", "Товар", "Кол-во(кг)"],
        rows=slice_rows
    )
    kb = export_pager_kb("incomes", page, has_prev, has_next)
    return txt, kb


async def export_sales_text(page: int):
    async with Session() as s:
        rows = (await s.execute(
            select(Sale)
            .options(selectinload(Sale.warehouse), selectinload(Sale.product))
            .order_by(Sale.id.desc())
            .limit(50)
        )).scalars().all()

    if not rows:
        return "🔴 Продажи: записей нет.", None

    data = []
    for r in rows:
        who = safe_text(r.customer_name) or "-"
        paid = "✅" if r.is_paid else "🧾"
        data.append([
            str(r.doc_date),
            who,
            r.warehouse.name if r.warehouse else "-",
            r.product.name if r.product else "-",
            fmt_kg(Decimal(r.qty_kg or 0)),
            fmt_money(Decimal(r.price_per_kg or 0)),
            fmt_money(Decimal(r.total_amount or 0)),
            paid
        ])

    total = len(data)
    start = page * EXPORT_PAGE_SIZE
    end = start + EXPORT_PAGE_SIZE
    slice_rows = data[start:end]

    has_prev = page > 0
    has_next = end < total

    txt = "🔴 Продажи (последние 50):\n" + _render_pre_table(
        headers=["Дата", "Кому", "Склад", "Товар", "Кол-во(кг)", "Цена/кг", "Сумма", "Опл"],
        rows=slice_rows
    )
    kb = export_pager_kb("sales", page, has_prev, has_next)
    return txt, kb


@router.callback_query(F.data.startswith("exp:"))
async def export_router(cq: CallbackQuery):
    """
    exp:menu
    exp:back
    exp:stocks:0
    exp:incomes:1
    exp:sales:2
    """
    parts = (cq.data or "").split(":")
    if len(parts) < 2:
        return await cq.answer()

    action = parts[1]

    if action == "menu":
        await cq.message.answer("📥 Выгрузка таблиц (в чате):", reply_markup=export_menu_kb())
        return await cq.answer()

    if action == "back":
        await cq.message.answer("Меню:", reply_markup=main_menu_kb())
        return await cq.answer()

    if len(parts) != 3:
        return await cq.answer("Ошибка кнопки", show_alert=True)

    kind = parts[1]
    page_s = parts[2]
    if not page_s.isdigit():
        return await cq.answer("Ошибка страницы", show_alert=True)
    page = int(page_s)

    if kind == "stocks":
        txt, kb = await export_stocks_text(page)
        await cq.message.answer(txt, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await cq.answer()

    if kind == "incomes":
        txt, kb = await export_incomes_text(page)
        await cq.message.answer(txt, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await cq.answer()

    if kind == "sales":
        txt, kb = await export_sales_text(page)
        await cq.message.answer(txt, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await cq.answer()

    return await cq.answer("Неизвестный раздел", show_alert=True)


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
    part = cq.data.split(":", 1)[1] if cq.data else ""
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

        account_type = sale.account_type or "cash"
        bank_id = sale.bank_id if account_type in ("bank", "ip") else None

        s.add(MoneyLedger(
            entry_date=sale.doc_date,
            direction="in",
            method=sale.payment_method or "cash",
            account_type=account_type,
            bank_id=bank_id,
            amount=Decimal(sale.total_amount),
            note=f"Оплата по продаже #{sale.id} ({sale.customer_name})"
        ))

        d = await s.scalar(
            select(Debtor).where(
                Debtor.customer_name == sale.customer_name,
                Debtor.customer_phone == sale.customer_phone,
                Debtor.total_amount == sale.total_amount,
                Debtor.is_paid == False
            )
        )
        if d:
            d.is_paid = True

        await s.commit()

    await cq.message.answer(f"✅ Продажа #{sale_id} отмечена как оплачено.")
    await cq.answer()


@router.callback_query(F.data.startswith("sale_del:"))
async def cb_sale_del(cq: CallbackQuery):
    part = cq.data.split(":", 1)[1] if cq.data else ""
    if not part.isdigit():
        return await cq.answer("Ошибка кнопки", show_alert=True)
    sale_id = int(part)
    async with Session() as s:
        sale = await s.get(Sale, sale_id)
        if not sale:
            return await cq.answer("Не найдено", show_alert=True)

        await s.execute(delete(Sale).where(Sale.id == sale_id))
        await s.commit()

    await cq.message.answer(f"🗑 Продажа #{sale_id} удалена.")
    await cq.answer()


async def list_sales(message: Message):
    async with Session() as s:
        rows = (await s.execute(
            select(Sale)
            .options(selectinload(Sale.warehouse), selectinload(Sale.product), selectinload(Sale.bank))
            .order_by(Sale.id.desc())
            .limit(30)
        )).scalars().all()

    if not rows:
        return await message.answer("Продаж пока нет.", reply_markup=main_menu_kb())

    lines = ["📄 *Последние продажи* (последние 30):"]
    for r in rows:
        paid = "✅" if r.is_paid else "🧾"
        acc = {"cash": "Нал", "bank": "Банк", "ip": "ИП"}.get(r.account_type, "-")
        bank_name = (r.bank.name if r.bank else "")
        where_txt = acc + (f" / {bank_name}" if bank_name else "")
        lines.append(
            f"\n*#{r.id}* {paid} {r.doc_date} — {r.customer_name} ({r.customer_phone})\n"
            f"{r.warehouse.name} / {r.product.name} — {fmt_kg(r.qty_kg)} кг × {fmt_money(r.price_per_kg)} = *{fmt_money(r.total_amount)}*\n"
            f"Куда: *{where_txt}*"
        )

    await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_kb())
    await message.answer("Чтобы управлять: напиши `продажа #ID` например: `продажа #12`",
                         reply_markup=main_menu_kb())


@router.message(F.text.regexp(r"(?i)^продажа\s+#\d+$"))
async def sale_by_id(message: Message):
    sale_id = int(message.text.split("#")[1])
    async with Session() as s:
        r = await s.scalar(
            select(Sale)
            .options(selectinload(Sale.warehouse), selectinload(Sale.product), selectinload(Sale.bank))
            .where(Sale.id == sale_id)
        )
    if not r:
        return await message.answer("Не найдено.", reply_markup=main_menu_kb())

    paid = "✅ Оплачено" if r.is_paid else "🧾 Не оплачено"
    acc = {"cash": "Наличные", "bank": "Банк", "ip": "Счёт ИП"}.get(r.account_type, "-")
    bank_name = r.bank.name if r.bank else "-"
    where_txt = f"{acc}" + (f" / {bank_name}" if r.account_type in ("bank", "ip") else "")

    txt = (
        f"🔴 *Продажа #{r.id}*\n"
        f"Дата: *{r.doc_date}*\n"
        f"Клиент: *{r.customer_name}* / {r.customer_phone}\n"
        f"Склад: *{r.warehouse.name}*\n"
        f"Товар: *{r.product.name}*\n"
        f"Кол-во: *{fmt_kg(r.qty_kg)} кг*\n"
        f"Цена: *{fmt_money(r.price_per_kg)}*\n"
        f"Сумма: *{fmt_money(r.total_amount)}*\n"
        f"Доставка: *{fmt_money(r.delivery_cost)}*\n"
        f"Статус: *{paid}*\n"
        f"Куда: *{where_txt}*\n"
    )
    await message.answer(txt, parse_mode=ParseMode.MARKDOWN,
                         reply_markup=sales_actions_kb(r.id, r.is_paid))


def income_actions_kb(income_id: int):
    ikb = InlineKeyboardBuilder()
    ikb.button(text="🗑 Удалить", callback_data=f"inc_del:{income_id}")
    ikb.adjust(1)
    return ikb.as_markup()


@router.callback_query(F.data.startswith("inc_del:"))
async def cb_inc_del(cq: CallbackQuery):
    part = cq.data.split(":", 1)[1] if cq.data else ""
    if not part.isdigit():
        return await cq.answer("Ошибка кнопки", show_alert=True)
    income_id = int(part)
    async with Session() as s:
        inc = await s.get(Income, income_id)
        if not inc:
            return await cq.answer("Не найдено", show_alert=True)
        await s.execute(delete(Income).where(Income.id == income_id))
        await s.commit()
    await cq.message.answer(f"🗑 Приход #{income_id} удалён.")
    await cq.answer()


async def list_incomes(message: Message):
    async with Session() as s:
        rows = (await s.execute(
            select(Income)
            .options(selectinload(Income.warehouse), selectinload(Income.product), selectinload(Income.bank))
            .order_by(Income.id.desc())
            .limit(30)
        )).scalars().all()

    if not rows:
        return await message.answer("Приходов пока нет.", reply_markup=main_menu_kb())

    lines = ["📄 *Последние приходы* (последние 30):"]
    for r in rows:
        acc = {"cash": "Нал", "bank": "Банк", "ip": "ИП"}.get(r.account_type, "-")
        bank_name = (r.bank.name if r.bank else "")
        where_txt = acc + (f" / {bank_name}" if bank_name else "")
        lines.append(
            f"\n*#{r.id}* {r.doc_date} — {r.supplier_name} ({r.supplier_phone})\n"
            f"{r.warehouse.name} / {r.product.name} — {fmt_kg(r.qty_kg)} кг × {fmt_money(r.price_per_kg)} = *{fmt_money(r.total_amount)}*\n"
            f"Расход денег: *{'✅' if r.add_money_entry else '❌'}* | Куда: *{where_txt}*"
        )

    await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_kb())
    await message.answer("Чтобы посмотреть: напиши `приход #ID` например: `приход #7`",
                         reply_markup=main_menu_kb())


@router.message(F.text.regexp(r"(?i)^приход\s+#\d+$"))
async def inc_by_id(message: Message):
    inc_id = int(message.text.split("#")[1])
    async with Session() as s:
        r = await s.scalar(
            select(Income)
            .options(selectinload(Income.warehouse), selectinload(Income.product), selectinload(Income.bank))
            .where(Income.id == inc_id)
        )
    if not r:
        return await message.answer("Не найдено.", reply_markup=main_menu_kb())

    acc = {"cash": "Наличные", "bank": "Банк", "ip": "Счёт ИП"}.get(r.account_type, "-")
    bank_name = r.bank.name if r.bank else "-"
    where_txt = f"{acc}" + (f" / {bank_name}" if r.account_type in ("bank", "ip") else "")

    txt = (
        f"🟢 *Приход #{r.id}*\n"
        f"Дата: *{r.doc_date}*\n"
        f"Поставщик: *{r.supplier_name}* / {r.supplier_phone}\n"
        f"Склад: *{r.warehouse.name}*\n"
        f"Товар: *{r.product.name}*\n"
        f"Кол-во: *{fmt_kg(r.qty_kg)} кг*\n"
        f"Цена: *{fmt_money(r.price_per_kg)}*\n"
        f"Сумма: *{fmt_money(r.total_amount)}*\n"
        f"Доставка: *{fmt_money(r.delivery_cost)}*\n"
        f"Расход денег по приходу: *{'✅' if r.add_money_entry else '❌'}*\n"
        f"Куда: *{where_txt}*\n"
    )
    await message.answer(txt, parse_mode=ParseMode.MARKDOWN,
                         reply_markup=income_actions_kb(r.id))


# ===================== Debtors =====================
def debtor_actions_kb(debtor_id: int, paid: bool):
    ikb = InlineKeyboardBuilder()
    if not paid:
        ikb.button(text="✅ Отметить как оплачено", callback_data=f"deb_paid:{debtor_id}")
    ikb.button(text="🗑 Удалить", callback_data=f"deb_del:{debtor_id}")
    ikb.adjust(1)
    return ikb.as_markup()


@router.callback_query(F.data.startswith("deb_paid:"))
async def cb_deb_paid(cq: CallbackQuery):
    part = cq.data.split(":", 1)[1] if cq.data else ""
    if not part.isdigit():
        return await cq.answer("Ошибка кнопки", show_alert=True)
    debtor_id = int(part)
    async with Session() as s:
        d = await s.get(Debtor, debtor_id)
        if not d:
            return await cq.answer("Не найдено", show_alert=True)
        d.is_paid = True
        await s.commit()
    await cq.message.answer(f"✅ Должник #{debtor_id} отмечен как оплачено.")
    await cq.answer()


@router.callback_query(F.data.startswith("deb_del:"))
async def cb_deb_del(cq: CallbackQuery):
    part = cq.data.split(":", 1)[1] if cq.data else ""
    if not part.isdigit():
        return await cq.answer("Ошибка кнопки", show_alert=True)
    debtor_id = int(part)
    async with Session() as s:
        await s.execute(delete(Debtor).where(Debtor.id == debtor_id))
        await s.commit()
    await cq.message.answer(f"🗑 Должник #{debtor_id} удалён.")
    await cq.answer()


async def list_debtors(message: Message):
    async with Session() as s:
        rows = (await s.execute(
            select(Debtor).order_by(Debtor.id.desc()).limit(50)
        )).scalars().all()

    if not rows:
        return await message.answer("Должников нет ✅", reply_markup=main_menu_kb())

    lines = ["📋 *Должники* (последние 50):"]
    for r in rows:
        status = "✅" if r.is_paid else "🧾"
        lines.append(
            f"\n*#{r.id}* {status} {r.doc_date} — {r.customer_name} ({r.customer_phone})\n"
            f"{r.warehouse_name} / {r.product_name} — {fmt_kg(r.qty_kg)} кг × {fmt_money(r.price_per_kg)} = *{fmt_money(r.total_amount)}*"
        )

    await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_kb())
    await message.answer("Чтобы управлять: напиши `должник #ID` например: `должник #3`",
                         reply_markup=main_menu_kb())


@router.message(F.text.regexp(r"(?i)^должник\s+#\d+$"))
async def debtor_by_id(message: Message):
    d_id = int(message.text.split("#")[1])
    async with Session() as s:
        r = await s.get(Debtor, d_id)
    if not r:
        return await message.answer("Не найдено.", reply_markup=main_menu_kb())

    status = "✅ Оплачено" if r.is_paid else "🧾 Не оплачено"
    txt = (
        f"📋 *Должник #{r.id}*\n"
        f"Дата: *{r.doc_date}*\n"
        f"Клиент: *{r.customer_name}* / {r.customer_phone}\n"
        f"Склад: *{r.warehouse_name}*\n"
        f"Товар: *{r.product_name}*\n"
        f"Кол-во: *{fmt_kg(r.qty_kg)} кг*\n"
        f"Цена: *{fmt_money(r.price_per_kg)}*\n"
        f"Сумма: *{fmt_money(r.total_amount)}*\n"
        f"Доставка: *{fmt_money(r.delivery_cost)}*\n"
        f"Статус: *{status}*\n"
    )
    await message.answer(txt, parse_mode=ParseMode.MARKDOWN,
                         reply_markup=debtor_actions_kb(r.id, r.is_paid))


# ===================== SALE wizard =====================
SALE_FLOW = [
    "doc_date", "customer_name", "customer_phone", "warehouse_id", "product_id",
    "qty", "price", "delivery", "paid_status", "pay_method", "account_type", "bank_pick", "confirm"
]


def sale_state_name(state: State) -> str:
    return str(state).split(":")[-1]


async def sale_go_to(state: FSMContext, step: str):
    mapping = {
        "doc_date": SaleWizard.doc_date,
        "customer_name": SaleWizard.customer_name,
        "customer_phone": SaleWizard.customer_phone,
        "warehouse_id": SaleWizard.warehouse,
        "product_id": SaleWizard.product,
        "qty": SaleWizard.qty,
        "price": SaleWizard.price,
        "delivery": SaleWizard.delivery,
        "paid_status": SaleWizard.paid_status,
        "pay_method": SaleWizard.pay_method,
        "account_type": SaleWizard.account_type,
        "bank_pick": SaleWizard.bank_pick,
        "confirm": SaleWizard.confirm,
    }
    await state.set_state(mapping[step])


async def sale_prompt(message: Message, state: FSMContext):
    cur = await state.get_state()
    step = sale_state_name(cur)

    if step == "doc_date":
        await message.answer("Дата продажи:", reply_markup=choose_date_kb("sale"))
        return

    if step == "customer_name":
        await message.answer("Имя клиента:", reply_markup=nav_kb("sale_nav:customer_name", allow_skip=True))
        return

    if step == "customer_phone":
        await message.answer("Телефон клиента:", reply_markup=nav_kb("sale_nav:customer_phone", allow_skip=True))
        return

    if step == "warehouse":
        await message.answer("Выбери склад:", reply_markup=await pick_warehouse_kb("sale_wh"))
        return

    if step == "product":
        await message.answer("Выбери товар:", reply_markup=await pick_product_kb("sale_pr"))
        return

    if step == "qty":
        await message.answer("Кол-во (кг), например 125.5:", reply_markup=nav_kb("sale_nav:qty", allow_skip=False))
        return

    if step == "price":
        await message.answer("Цена за 1 кг:", reply_markup=nav_kb("sale_nav:price", allow_skip=False))
        return

    if step == "delivery":
        await message.answer("Доставка (0 если нет):", reply_markup=nav_kb("sale_nav:delivery", allow_skip=True))
        return

    if step == "paid_status":
        await message.answer("Статус оплаты:", reply_markup=sale_status_kb())
        return

    if step == "pay_method":
        await message.answer("Как оплатили?", reply_markup=pay_method_kb("sale_pay"))
        return

    if step == "account_type":
        await message.answer("Куда поступили деньги?", reply_markup=account_type_kb("sale_acc"))
        return

    if step == "bank_pick":
        await message.answer("Выбери банк/счёт из списка:", reply_markup=await pick_bank_kb("sale_bank"))
        return

    if step == "confirm":
        data = await state.get_data()
        await message.answer(build_sale_summary(data) + "\n\nПодтвердить?",
                             parse_mode=ParseMode.MARKDOWN,
                             reply_markup=yes_no_kb("sale_confirm"))
        return


async def start_sale(message: Message, state: FSMContext):
    await state.clear()
    await sale_go_to(state, "doc_date")
    await sale_prompt(message, state)


@router.callback_query(F.data.startswith("cal:sale:"))
async def cal_sale_handler(cq: CallbackQuery, state: FSMContext):
    # cal:sale:action:payload
    parts = (cq.data or "").split(":", 3)
    if len(parts) < 4:
        return await cq.answer()
    _, scope, action, payload = parts

    if action in ("open", "prev", "next"):
        y, m = payload.split("-")
        kb = cal_open_kb("sale", int(y), int(m))
        await cq.message.edit_reply_markup(reply_markup=kb)
        return await cq.answer()

    if action == "pick":
        d = datetime.strptime(payload, "%Y-%m-%d").date()
        await state.update_data(doc_date=d.isoformat())
        await sale_go_to(state, "customer_name")
        await cq.message.answer(f"✅ Дата выбрана: {d.isoformat()}")
        await sale_prompt(cq.message, state)
        return await cq.answer()

    await cq.answer()


@router.callback_query(F.data.startswith("sale_nav:"))
async def sale_nav_handler(cq: CallbackQuery, state: FSMContext):
    # sale_nav:field:action
    parts = (cq.data or "").split(":", 2)
    if len(parts) < 3:
        return await cq.answer()
    _, field, action = parts

    cur = await state.get_state()
    step = sale_state_name(cur)

    step_map = {
        "doc_date": "doc_date",
        "customer_name": "customer_name",
        "customer_phone": "customer_phone",
        "warehouse": "warehouse_id",
        "product": "product_id",
        "qty": "qty",
        "price": "price",
        "delivery": "delivery",
        "paid_status": "paid_status",
        "pay_method": "pay_method",
        "account_type": "account_type",
        "bank_pick": "bank_pick",
        "confirm": "confirm",
    }
    key = step_map.get(step, "customer_name")
    idx = SALE_FLOW.index(key)

    if action == "back":
        if idx == 0:
            await state.clear()
            await cq.message.answer("Отменено ✅", reply_markup=main_menu_kb())
            return await cq.answer()
        prev_key = SALE_FLOW[idx - 1]
        await sale_go_to(state, prev_key)
        await sale_prompt(cq.message, state)
        return await cq.answer()

    if action == "skip":
        if key == "customer_name":
            await state.update_data(customer_name="-")
        if key == "customer_phone":
            await state.update_data(customer_phone="-")
        if key == "delivery":
            await state.update_data(delivery="0")

        next_key = SALE_FLOW[min(idx + 1, len(SALE_FLOW) - 1)]
        await sale_go_to(state, next_key)
        await sale_prompt(cq.message, state)
        return await cq.answer()

    await cq.answer()


@router.callback_query(F.data.startswith("sale_wh:"))
async def sale_choose_wh(cq: CallbackQuery, state: FSMContext):
    parts = parse_cb(cq.data, "sale_wh")
    if not parts:
        return await cq.answer()

    action = parts[0]

    if action == "back":
        await sale_go_to(state, "customer_phone")
        await sale_prompt(cq.message, state)
        return await cq.answer()

    if action == "add_new":
        await state.set_state(SaleWizard.adding_warehouse)
        await cq.message.answer("Напиши название нового склада:")
        return await cq.answer()

    if action == "id" and len(parts) >= 2 and parts[1].isdigit():
        await state.update_data(warehouse_id=int(parts[1]))
        await sale_go_to(state, "product_id")
        await sale_prompt(cq.message, state)
        return await cq.answer()

    return await cq.answer("Ошибка склада", show_alert=True)


@router.message(SaleWizard.adding_warehouse)
async def sale_add_warehouse_inline(message: Message, state: FSMContext):
    name = safe_text(message.text)
    if not name:
        return await message.answer("Пусто. Напиши название склада:")

    async with Session() as s:
        exists = await s.scalar(select(Warehouse).where(Warehouse.name == name))
        if not exists:
            s.add(Warehouse(name=name))
            await s.commit()

    await sale_go_to(state, "warehouse_id")
    await message.answer("✅ Склад добавлен. Теперь выбери склад:", reply_markup=await pick_warehouse_kb("sale_wh"))


@router.callback_query(F.data.startswith("sale_pr:"))
async def sale_choose_pr(cq: CallbackQuery, state: FSMContext):
    parts = parse_cb(cq.data, "sale_pr")
    if not parts:
        return await cq.answer()

    action = parts[0]

    if action == "back":
        await sale_go_to(state, "warehouse_id")
        await sale_prompt(cq.message, state)
        return await cq.answer()

    if action == "add_new":
        await state.set_state(SaleWizard.adding_product)
        await cq.message.answer("Напиши название нового товара:")
        return await cq.answer()

    if action == "id" and len(parts) >= 2 and parts[1].isdigit():
        await state.update_data(product_id=int(parts[1]))
        await sale_go_to(state, "qty")
        await sale_prompt(cq.message, state)
        return await cq.answer()

    return await cq.answer("Ошибка товара", show_alert=True)


@router.message(SaleWizard.adding_product)
async def sale_add_product_inline(message: Message, state: FSMContext):
    name = safe_text(message.text)
    if not name:
        return await message.answer("Пусто. Напиши название товара:")

    async with Session() as s:
        exists = await s.scalar(select(Product).where(Product.name == name))
        if not exists:
            s.add(Product(name=name))
            await s.commit()

    await sale_go_to(state, "product_id")
    await message.answer("✅ Товар добавлен. Теперь выбери товар:", reply_markup=await pick_product_kb("sale_pr"))


@router.message(SaleWizard.customer_name)
async def sale_customer_name(message: Message, state: FSMContext):
    txt = safe_text(message.text) or "-"
    await state.update_data(customer_name=txt)
    await sale_go_to(state, "customer_phone")
    await sale_prompt(message, state)


@router.message(SaleWizard.customer_phone)
async def sale_customer_phone(message: Message, state: FSMContext):
    txt = safe_phone(message.text) or "-"
    await state.update_data(customer_phone=txt)
    await sale_go_to(state, "warehouse_id")
    await sale_prompt(message, state)


@router.message(SaleWizard.qty)
async def sale_qty(message: Message, state: FSMContext):
    try:
        q = dec(message.text)
        if q <= 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число > 0, например 10 или 10.5")
    await state.update_data(qty=str(q))
    await sale_go_to(state, "price")
    await sale_prompt(message, state)


@router.message(SaleWizard.price)
async def sale_price(message: Message, state: FSMContext):
    try:
        p = dec(message.text)
        if p < 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число, например 250 или 250.5")
    await state.update_data(price=str(p))
    await sale_go_to(state, "delivery")
    await sale_prompt(message, state)


@router.message(SaleWizard.delivery)
async def sale_delivery(message: Message, state: FSMContext):
    txt = safe_text(message.text)
    if txt == "":
        txt = "0"
    try:
        d = dec(txt)
        if d < 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число, например 0 или 1500")
    await state.update_data(delivery=str(d))
    await sale_go_to(state, "paid_status")
    await sale_prompt(message, state)


@router.callback_query(F.data.startswith("sale_status:"))
async def sale_status_chosen(cq: CallbackQuery, state: FSMContext):
    status = cq.data.split(":", 1)[1] if cq.data else ""
    if status == "paid":
        await state.update_data(is_paid=True)
        await sale_go_to(state, "pay_method")
        await sale_prompt(cq.message, state)
    else:
        await state.update_data(is_paid=False, payment_method="", account_type="cash", bank_id=None)
        await sale_go_to(state, "confirm")
        await sale_prompt(cq.message, state)
    await cq.answer()


@router.callback_query(F.data.startswith("sale_pay:"))
async def sale_pay_method(cq: CallbackQuery, state: FSMContext):
    method = cq.data.split(":", 1)[1] if cq.data else "cash"
    await state.update_data(payment_method=method)
    await sale_go_to(state, "account_type")
    await sale_prompt(cq.message, state)
    await cq.answer()


@router.callback_query(F.data.startswith("sale_acc:"))
async def sale_account_type_pick(cq: CallbackQuery, state: FSMContext):
    acc = cq.data.split(":", 1)[1] if cq.data else "cash"
    await state.update_data(account_type=acc)

    if acc == "cash":
        await state.update_data(bank_id=None)
        await sale_go_to(state, "confirm")
        await sale_prompt(cq.message, state)
    else:
        await sale_go_to(state, "bank_pick")
        await sale_prompt(cq.message, state)

    await cq.answer()


@router.callback_query(F.data.startswith("sale_bank:"))
async def sale_bank_pick(cq: CallbackQuery, state: FSMContext):
    parts = parse_cb(cq.data, "sale_bank")
    if not parts:
        return await cq.answer()

    action = parts[0]

    if action == "back":
        await sale_go_to(state, "account_type")
        await sale_prompt(cq.message, state)
        return await cq.answer()

    if action == "add_new":
        await state.set_state(SaleWizard.adding_bank)
        await cq.message.answer("Напиши название нового банка (для Банка/ИП):")
        return await cq.answer()

    if action == "id" and len(parts) >= 2 and parts[1].isdigit():
        await state.update_data(bank_id=int(parts[1]))
        await sale_go_to(state, "confirm")
        await sale_prompt(cq.message, state)
        return await cq.answer()

    return await cq.answer("Ошибка банка", show_alert=True)


@router.message(SaleWizard.adding_bank)
async def sale_add_bank_inline(message: Message, state: FSMContext):
    name = safe_text(message.text)
    if not name:
        return await message.answer("Пусто. Напиши название банка:")

    async with Session() as s:
        exists = await s.scalar(select(Bank).where(Bank.name == name))
        if not exists:
            s.add(Bank(name=name))
            await s.commit()

    await sale_go_to(state, "bank_pick")
    await message.answer("✅ Банк добавлен. Теперь выбери банк:", reply_markup=await pick_bank_kb("sale_bank"))


def build_sale_summary(data: dict) -> str:
    qty = Decimal(data["qty"])
    price = Decimal(data["price"])
    total = qty * price
    delivery = Decimal(data.get("delivery", "0"))
    paid = "✅ Оплачено" if data.get("is_paid") else "🧾 Не оплачено"
    pay_method = data.get("payment_method") or "-"

    acc = {"cash": "Наличные", "bank": "Банк", "ip": "Счёт ИП"}.get(data.get("account_type"), "-")
    bank_id = data.get("bank_id")

    wh_id = data.get("warehouse_id")
    pr_id = data.get("product_id")
    wh_name = f"#{wh_id}" if wh_id else "-"
    pr_name = f"#{pr_id}" if pr_id else "-"

    bank_txt = "-"
    if data.get("account_type") in ("bank", "ip"):
        bank_txt = f"#{bank_id}" if bank_id else "-"

    return (
        "🔴 *ПРОДАЖА (проверка):*\n"
        f"Дата: *{data.get('doc_date','-')}*\n"
        f"Клиент: *{data.get('customer_name','-')}* / {data.get('customer_phone','-')}\n"
        f"Склад: *{wh_name}*\n"
        f"Товар: *{pr_name}*\n"
        f"Кол-во: *{fmt_kg(qty)} кг*\n"
        f"Цена: *{fmt_money(price)}*\n"
        f"Сумма: *{fmt_money(total)}*\n"
        f"Доставка: *{fmt_money(delivery)}*\n"
        f"Оплата: *{paid}*\n"
        f"Метод: *{pay_method}*\n"
        f"Куда: *{acc}*\n"
        f"Банк/ИП: *{bank_txt}*"
    )


@router.callback_query(F.data.startswith("sale_confirm:"))
async def sale_confirm(cq: CallbackQuery, state: FSMContext):
    ch = cq.data.split(":", 1)[1] if cq.data else "no"
    if ch == "no":
        await state.clear()
        await cq.message.answer("Отменено ✅", reply_markup=main_menu_kb())
        return await cq.answer()

    data = await state.get_data()

    doc_date = datetime.strptime(data["doc_date"], "%Y-%m-%d").date()
    customer_name = data.get("customer_name", "-")
    customer_phone = data.get("customer_phone", "-")

    warehouse_id = int(data["warehouse_id"])
    product_id = int(data["product_id"])
    qty = Decimal(data["qty"])
    price = Decimal(data["price"])
    total = qty * price
    delivery = Decimal(data.get("delivery", "0"))

    is_paid = bool(data.get("is_paid"))
    payment_method = data.get("payment_method", "")

    account_type = data.get("account_type", "cash")
    bank_id = data.get("bank_id")

    if account_type not in ("cash", "bank", "ip"):
        account_type = "cash"

    if account_type == "cash":
        bank_id = None
    else:
        if not bank_id:
            await cq.answer("Выбери банк/счёт", show_alert=True)
            return
        bank_id = int(bank_id)

    async with Session() as s:
        w = await s.get(Warehouse, warehouse_id)
        p = await s.get(Product, product_id)
        if not w or not p:
            await state.clear()
            await cq.message.answer("Ошибка: склад/товар не найден. Проверь справочники.", reply_markup=main_menu_kb())
            return await cq.answer()

        if account_type in ("bank", "ip"):
            b = await s.get(Bank, bank_id)
            if not b:
                await cq.answer("Банк не найден", show_alert=True)
                return

        await get_stock_row(s, w.id, p.id)

        res = await s.execute(
            update(Stock)
            .where(
                Stock.warehouse_id == w.id,
                Stock.product_id == p.id,
                Stock.qty_kg >= qty
            )
            .values(qty_kg=Stock.qty_kg - qty)
        )

        if res.rowcount == 0:
            cur_qty = await s.scalar(
                select(Stock.qty_kg).where(
                    Stock.warehouse_id == w.id,
                    Stock.product_id == p.id
                )
            )
            cur_qty = Decimal(cur_qty or 0)

            await state.clear()
            await cq.message.answer(
                f"❗ Недостаточно товара.\nЕсть: {fmt_kg(cur_qty)} кг, нужно: {fmt_kg(qty)} кг",
                reply_markup=main_menu_kb()
            )
            return await cq.answer()

        sale = Sale(
            doc_date=doc_date,
            customer_name=customer_name,
            customer_phone=customer_phone,
            warehouse_id=w.id,
            product_id=p.id,
            qty_kg=qty,
            price_per_kg=price,
            total_amount=total,
            delivery_cost=delivery,
            is_paid=is_paid,
            payment_method=payment_method if is_paid else "",
            account_type=account_type if is_paid else "cash",
            bank_id=bank_id if (is_paid and account_type in ("bank", "ip")) else None
        )
        s.add(sale)
        await s.flush()

        if is_paid:
            s.add(MoneyLedger(
                entry_date=doc_date,
                direction="in",
                method=payment_method or "cash",
                account_type=account_type,
                bank_id=bank_id if account_type in ("bank", "ip") else None,
                amount=total,
                note=f"Продажа #{sale.id} ({customer_name})"
            ))
        else:
            s.add(Debtor(
                doc_date=doc_date,
                customer_name=customer_name,
                customer_phone=customer_phone,
                warehouse_name=w.name,
                product_name=p.name,
                qty_kg=qty,
                price_per_kg=price,
                total_amount=total,
                delivery_cost=delivery,
                is_paid=False
            ))

        await s.commit()

    await state.clear()
    await cq.message.answer("✅ Продажа сохранена.", reply_markup=main_menu_kb())
    await cq.answer()


# ===================== INCOME wizard =====================
INCOME_FLOW = [
    "doc_date", "supplier_name", "supplier_phone", "warehouse_id", "product_id",
    "qty", "price", "delivery", "add_money", "pay_method", "account_type", "bank_pick", "confirm"
]


def income_state_name(state: State) -> str:
    return str(state).split(":")[-1]


async def income_go_to(state: FSMContext, step: str):
    mapping = {
        "doc_date": IncomeWizard.doc_date,
        "supplier_name": IncomeWizard.supplier_name,
        "supplier_phone": IncomeWizard.supplier_phone,
        "warehouse_id": IncomeWizard.warehouse,
        "product_id": IncomeWizard.product,
        "qty": IncomeWizard.qty,
        "price": IncomeWizard.price,
        "delivery": IncomeWizard.delivery,
        "add_money": IncomeWizard.add_money,
        "pay_method": IncomeWizard.pay_method,
        "account_type": IncomeWizard.account_type,
        "bank_pick": IncomeWizard.bank_pick,
        "confirm": IncomeWizard.confirm,
    }
    await state.set_state(mapping[step])


async def income_prompt(message: Message, state: FSMContext):
    cur = await state.get_state()
    step = income_state_name(cur)

    if step == "doc_date":
        await message.answer("Дата прихода:", reply_markup=choose_date_kb("inc"))
        return

    if step == "supplier_name":
        await message.answer("Имя поставщика:", reply_markup=nav_kb("inc_nav:supplier_name", allow_skip=True))
        return

    if step == "supplier_phone":
        await message.answer("Телефон поставщика:", reply_markup=nav_kb("inc_nav:supplier_phone", allow_skip=True))
        return

    if step == "warehouse":
        await message.answer("Выбери склад прихода:", reply_markup=await pick_warehouse_kb("inc_wh"))
        return

    if step == "product":
        await message.answer("Выбери товар:", reply_markup=await pick_product_kb("inc_pr"))
        return

    if step == "qty":
        await message.answer("Кол-во (кг):", reply_markup=nav_kb("inc_nav:qty", allow_skip=False))
        return

    if step == "price":
        await message.answer("Цена за 1 кг:", reply_markup=nav_kb("inc_nav:price", allow_skip=False))
        return

    if step == "delivery":
        await message.answer("Доставка (0 если нет):", reply_markup=nav_kb("inc_nav:delivery", allow_skip=True))
        return

    if step == "add_money":
        await message.answer("Добавить запись денег (расход) по этому приходу?", reply_markup=yes_no_kb("inc_money"))
        return

    if step == "pay_method":
        await message.answer("Как оплатили поставщику?", reply_markup=pay_method_kb("inc_pay"))
        return

    if step == "account_type":
        await message.answer("С какого счёта ушли деньги?", reply_markup=account_type_kb("inc_acc"))
        return

    if step == "bank_pick":
        await message.answer("Выбери банк/счёт из списка:", reply_markup=await pick_bank_kb("inc_bank"))
        return

    if step == "confirm":
        data = await state.get_data()
        await message.answer(build_income_summary(data) + "\n\nПодтвердить?",
                             parse_mode=ParseMode.MARKDOWN,
                             reply_markup=yes_no_kb("inc_confirm"))
        return


async def start_income(message: Message, state: FSMContext):
    await state.clear()
    await income_go_to(state, "doc_date")
    await income_prompt(message, state)


@router.callback_query(F.data.startswith("cal:inc:"))
async def cal_inc_handler(cq: CallbackQuery, state: FSMContext):
    parts = (cq.data or "").split(":", 3)
    if len(parts) < 4:
        return await cq.answer()
    _, scope, action, payload = parts

    if action in ("open", "prev", "next"):
        y, m = payload.split("-")
        kb = cal_open_kb("inc", int(y), int(m))
        await cq.message.edit_reply_markup(reply_markup=kb)
        return await cq.answer()

    if action == "pick":
        d = datetime.strptime(payload, "%Y-%m-%d").date()
        await state.update_data(doc_date=d.isoformat())
        await income_go_to(state, "supplier_name")
        await cq.message.answer(f"✅ Дата выбрана: {d.isoformat()}")
        await income_prompt(cq.message, state)
        return await cq.answer()

    await cq.answer()


@router.callback_query(F.data.startswith("inc_nav:"))
async def inc_nav_handler(cq: CallbackQuery, state: FSMContext):
    parts = (cq.data or "").split(":", 2)
    if len(parts) < 3:
        return await cq.answer()
    _, field, action = parts

    cur = await state.get_state()
    step = income_state_name(cur)

    step_map = {
        "doc_date": "doc_date",
        "supplier_name": "supplier_name",
        "supplier_phone": "supplier_phone",
        "warehouse": "warehouse_id",
        "product": "product_id",
        "qty": "qty",
        "price": "price",
        "delivery": "delivery",
        "add_money": "add_money",
        "pay_method": "pay_method",
        "account_type": "account_type",
        "bank_pick": "bank_pick",
        "confirm": "confirm",
    }
    key = step_map.get(step, "supplier_name")
    idx = INCOME_FLOW.index(key)

    if action == "back":
        if idx == 0:
            await state.clear()
            await cq.message.answer("Отменено ✅", reply_markup=main_menu_kb())
            return await cq.answer()
        prev_key = INCOME_FLOW[idx - 1]
        await income_go_to(state, prev_key)
        await income_prompt(cq.message, state)
        return await cq.answer()

    if action == "skip":
        if key == "supplier_name":
            await state.update_data(supplier_name="-")
        if key == "supplier_phone":
            await state.update_data(supplier_phone="-")
        if key == "delivery":
            await state.update_data(delivery="0")

        next_key = INCOME_FLOW[min(idx + 1, len(INCOME_FLOW) - 1)]
        await income_go_to(state, next_key)
        await income_prompt(cq.message, state)
        return await cq.answer()

    await cq.answer()


@router.callback_query(F.data.startswith("inc_wh:"))
async def inc_choose_wh(cq: CallbackQuery, state: FSMContext):
    parts = parse_cb(cq.data, "inc_wh")
    if not parts:
        return await cq.answer()

    action = parts[0]

    if action == "back":
        await income_go_to(state, "supplier_phone")
        await income_prompt(cq.message, state)
        return await cq.answer()

    if action == "add_new":
        await state.set_state(IncomeWizard.adding_warehouse)
        await cq.message.answer("Напиши название нового склада:")
        return await cq.answer()

    if action == "id" and len(parts) >= 2 and parts[1].isdigit():
        await state.update_data(warehouse_id=int(parts[1]))
        await income_go_to(state, "product_id")
        await income_prompt(cq.message, state)
        return await cq.answer()

    return await cq.answer("Ошибка склада", show_alert=True)


@router.message(IncomeWizard.adding_warehouse)
async def inc_add_warehouse_inline(message: Message, state: FSMContext):
    name = safe_text(message.text)
    if not name:
        return await message.answer("Пусто. Напиши название склада:")

    async with Session() as s:
        exists = await s.scalar(select(Warehouse).where(Warehouse.name == name))
        if not exists:
            s.add(Warehouse(name=name))
            await s.commit()

    await income_go_to(state, "warehouse_id")
    await message.answer("✅ Склад добавлен. Теперь выбери склад:", reply_markup=await pick_warehouse_kb("inc_wh"))


@router.callback_query(F.data.startswith("inc_pr:"))
async def inc_choose_pr(cq: CallbackQuery, state: FSMContext):
    parts = parse_cb(cq.data, "inc_pr")
    if not parts:
        return await cq.answer()

    action = parts[0]

    if action == "back":
        await income_go_to(state, "warehouse_id")
        await income_prompt(cq.message, state)
        return await cq.answer()

    if action == "add_new":
        await state.set_state(IncomeWizard.adding_product)
        await cq.message.answer("Напиши название нового товара:")
        return await cq.answer()

    if action == "id" and len(parts) >= 2 and parts[1].isdigit():
        await state.update_data(product_id=int(parts[1]))
        await income_go_to(state, "qty")
        await income_prompt(cq.message, state)
        return await cq.answer()

    return await cq.answer("Ошибка товара", show_alert=True)


@router.message(IncomeWizard.adding_product)
async def inc_add_product_inline(message: Message, state: FSMContext):
    name = safe_text(message.text)
    if not name:
        return await message.answer("Пусто. Напиши название товара:")

    async with Session() as s:
        exists = await s.scalar(select(Product).where(Product.name == name))
        if not exists:
            s.add(Product(name=name))
            await s.commit()

    await income_go_to(state, "product_id")
    await message.answer("✅ Товар добавлен. Теперь выбери товар:", reply_markup=await pick_product_kb("inc_pr"))


@router.message(IncomeWizard.supplier_name)
async def inc_supplier_name(message: Message, state: FSMContext):
    txt = safe_text(message.text) or "-"
    await state.update_data(supplier_name=txt)
    await income_go_to(state, "supplier_phone")
    await income_prompt(message, state)


@router.message(IncomeWizard.supplier_phone)
async def inc_supplier_phone(message: Message, state: FSMContext):
    txt = safe_phone(message.text) or "-"
    await state.update_data(supplier_phone=txt)
    await income_go_to(state, "warehouse_id")
    await income_prompt(message, state)


@router.message(IncomeWizard.qty)
async def inc_qty(message: Message, state: FSMContext):
    try:
        q = dec(message.text)
        if q <= 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число > 0, например 10 или 10.5")
    await state.update_data(qty=str(q))
    await income_go_to(state, "price")
    await income_prompt(message, state)


@router.message(IncomeWizard.price)
async def inc_price(message: Message, state: FSMContext):
    try:
        p = dec(message.text)
        if p < 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число, например 250 или 250.5")
    await state.update_data(price=str(p))
    await income_go_to(state, "delivery")
    await income_prompt(message, state)


@router.message(IncomeWizard.delivery)
async def inc_delivery(message: Message, state: FSMContext):
    txt = safe_text(message.text)
    if txt == "":
        txt = "0"
    try:
        d = dec(txt)
        if d < 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число, например 0 или 1500")
    await state.update_data(delivery=str(d))
    await income_go_to(state, "add_money")
    await income_prompt(message, state)


@router.callback_query(F.data.startswith("inc_money:"))
async def inc_money_choice(cq: CallbackQuery, state: FSMContext):
    ch = cq.data.split(":", 1)[1] if cq.data else "no"
    if ch == "yes":
        await state.update_data(add_money_entry=True)
        await income_go_to(state, "pay_method")
        await income_prompt(cq.message, state)
    else:
        await state.update_data(add_money_entry=False, payment_method="", account_type="cash", bank_id=None)
        await income_go_to(state, "confirm")
        await income_prompt(cq.message, state)
    await cq.answer()


@router.callback_query(F.data.startswith("inc_pay:"))
async def inc_pay_choice(cq: CallbackQuery, state: FSMContext):
    method = cq.data.split(":", 1)[1] if cq.data else "cash"
    await state.update_data(payment_method=method)
    await income_go_to(state, "account_type")
    await income_prompt(cq.message, state)
    await cq.answer()


@router.callback_query(F.data.startswith("inc_acc:"))
async def inc_account_type_pick(cq: CallbackQuery, state: FSMContext):
    acc = cq.data.split(":", 1)[1] if cq.data else "cash"
    await state.update_data(account_type=acc)

    if acc == "cash":
        await state.update_data(bank_id=None)
        await income_go_to(state, "confirm")
        await income_prompt(cq.message, state)
    else:
        await income_go_to(state, "bank_pick")
        await income_prompt(cq.message, state)

    await cq.answer()


@router.callback_query(F.data.startswith("inc_bank:"))
async def inc_bank_pick(cq: CallbackQuery, state: FSMContext):
    parts = parse_cb(cq.data, "inc_bank")
    if not parts:
        return await cq.answer()

    action = parts[0]

    if action == "back":
        await income_go_to(state, "account_type")
        await income_prompt(cq.message, state)
        return await cq.answer()

    if action == "add_new":
        await state.set_state(IncomeWizard.adding_bank)
        await cq.message.answer("Напиши название нового банка (для Банка/ИП):")
        return await cq.answer()

    if action == "id" and len(parts) >= 2 and parts[1].isdigit():
        await state.update_data(bank_id=int(parts[1]))
        await income_go_to(state, "confirm")
        await income_prompt(cq.message, state)
        return await cq.answer()

    return await cq.answer("Ошибка банка", show_alert=True)


@router.message(IncomeWizard.adding_bank)
async def inc_add_bank_inline(message: Message, state: FSMContext):
    name = safe_text(message.text)
    if not name:
        return await message.answer("Пусто. Напиши название банка:")

    async with Session() as s:
        exists = await s.scalar(select(Bank).where(Bank.name == name))
        if not exists:
            s.add(Bank(name=name))
            await s.commit()

    await income_go_to(state, "bank_pick")
    await message.answer("✅ Банк добавлен. Теперь выбери банк:", reply_markup=await pick_bank_kb("inc_bank"))


def build_income_summary(data: dict) -> str:
    qty = Decimal(data["qty"])
    price = Decimal(data["price"])
    total = qty * price
    delivery = Decimal(data.get("delivery", "0"))
    add_money = "✅ Да" if data.get("add_money_entry") else "❌ Нет"
    method = data.get("payment_method") or "-"

    acc = {"cash": "Наличные", "bank": "Банк", "ip": "Счёт ИП"}.get(data.get("account_type"), "-")
    bank_id = data.get("bank_id")
    bank_txt = "-"
    if data.get("account_type") in ("bank", "ip"):
        bank_txt = f"#{bank_id}" if bank_id else "-"

    wh_id = data.get("warehouse_id")
    pr_id = data.get("product_id")
    wh_name = f"#{wh_id}" if wh_id else "-"
    pr_name = f"#{pr_id}" if pr_id else "-"

    return (
        "🟢 *ПРИХОД (проверка):*\n"
        f"Дата: *{data.get('doc_date','-')}*\n"
        f"Поставщик: *{data.get('supplier_name','-')}* / {data.get('supplier_phone','-')}\n"
        f"Склад: *{wh_name}*\n"
        f"Товар: *{pr_name}*\n"
        f"Кол-во: *{fmt_kg(qty)} кг*\n"
        f"Цена: *{fmt_money(price)}*\n"
        f"Сумма: *{fmt_money(total)}*\n"
        f"Доставка: *{fmt_money(delivery)}*\n"
        f"Запись денег (расход): *{add_money}*\n"
        f"Метод оплаты: *{method}*\n"
        f"С какого счёта: *{acc}*\n"
        f"Банк/ИП: *{bank_txt}*"
    )


@router.callback_query(F.data.startswith("inc_confirm:"))
async def inc_confirm(cq: CallbackQuery, state: FSMContext):
    ch = cq.data.split(":", 1)[1] if cq.data else "no"
    if ch == "no":
        await state.clear()
        await cq.message.answer("Отменено ✅", reply_markup=main_menu_kb())
        return await cq.answer()

    data = await state.get_data()

    doc_date = datetime.strptime(data["doc_date"], "%Y-%m-%d").date()
    supplier_name = data.get("supplier_name", "-")
    supplier_phone = data.get("supplier_phone", "-")

    warehouse_id = int(data["warehouse_id"])
    product_id = int(data["product_id"])
    qty = Decimal(data["qty"])
    price = Decimal(data["price"])
    total = qty * price
    delivery = Decimal(data.get("delivery", "0"))

    add_money_entry = bool(data.get("add_money_entry"))
    payment_method = data.get("payment_method", "")

    account_type = data.get("account_type", "cash")
    bank_id = data.get("bank_id")

    if account_type not in ("cash", "bank", "ip"):
        account_type = "cash"

    if account_type == "cash":
        bank_id = None
    else:
        if not bank_id:
            await cq.answer("Выбери банк/счёт", show_alert=True)
            return
        bank_id = int(bank_id)

    async with Session() as s:
        w = await s.get(Warehouse, warehouse_id)
        p = await s.get(Product, product_id)
        if not w or not p:
            await state.clear()
            await cq.message.answer("Ошибка: склад/товар не найден. Проверь справочники.", reply_markup=main_menu_kb())
            return await cq.answer()

        if account_type in ("bank", "ip"):
            b = await s.get(Bank, bank_id)
            if not b:
                await cq.answer("Банк не найден", show_alert=True)
                return

        stock = await get_stock_row(s, w.id, p.id)
        stock.qty_kg = Decimal(stock.qty_kg) + qty

        inc = Income(
            doc_date=doc_date,
            supplier_name=supplier_name,
            supplier_phone=supplier_phone,
            warehouse_id=w.id,
            product_id=p.id,
            qty_kg=qty,
            price_per_kg=price,
            total_amount=total,
            delivery_cost=delivery,
            add_money_entry=add_money_entry,
            payment_method=payment_method if add_money_entry else "",
            account_type=account_type if add_money_entry else "cash",
            bank_id=bank_id if (add_money_entry and account_type in ("bank", "ip")) else None
        )
        s.add(inc)
        await s.flush()

        if add_money_entry:
            s.add(MoneyLedger(
                entry_date=doc_date,
                direction="out",
                method=payment_method or "cash",
                account_type=account_type,
                bank_id=bank_id if account_type in ("bank", "ip") else None,
                amount=total,
                note=f"Приход #{inc.id} (поставщик {supplier_name})"
            ))

        await s.commit()

    await state.clear()
    await cq.message.answer("✅ Приход сохранён.", reply_markup=main_menu_kb())
    await cq.answer()


# ===================== Debtor manual wizard =====================
async def start_debtor(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(DebtorWizard.doc_date)
    await message.answer("Дата (для должника):", reply_markup=choose_date_kb("deb"))


@router.callback_query(F.data.startswith("cal:deb:"))
async def cal_deb_handler(cq: CallbackQuery, state: FSMContext):
    parts = (cq.data or "").split(":", 3)
    if len(parts) < 4:
        return await cq.answer()
    _, scope, action, payload = parts

    if action in ("open", "prev", "next"):
        y, m = payload.split("-")
        await cq.message.edit_reply_markup(reply_markup=cal_open_kb("deb", int(y), int(m)))
        return await cq.answer()

    if action == "pick":
        d = datetime.strptime(payload, "%Y-%m-%d").date()
        await state.update_data(doc_date=d.isoformat())
        await state.set_state(DebtorWizard.customer_name)
        await cq.message.answer("Имя клиента:", reply_markup=nav_kb("deb_nav:customer_name", allow_skip=False))
        return await cq.answer()

    await cq.answer()


@router.callback_query(F.data.startswith("deb_nav:"))
async def deb_nav_handler(cq: CallbackQuery, state: FSMContext):
    parts = (cq.data or "").split(":", 2)
    if len(parts) < 3:
        return await cq.answer()
    _, field, action = parts

    if action == "back":
        cur = await state.get_state()
        step = str(cur).split(":")[-1]
        if step == "customer_name":
            await state.set_state(DebtorWizard.doc_date)
            await cq.message.answer("Дата (для должника):", reply_markup=choose_date_kb("deb"))
        elif step == "customer_phone":
            await state.set_state(DebtorWizard.customer_name)
            await cq.message.answer("Имя клиента:", reply_markup=nav_kb("deb_nav:customer_name", allow_skip=False))
        elif step == "warehouse_name":
            await state.set_state(DebtorWizard.customer_phone)
            await cq.message.answer("Телефон клиента:", reply_markup=nav_kb("deb_nav:customer_phone", allow_skip=True))
        elif step == "product_name":
            await state.set_state(DebtorWizard.warehouse_name)
            await cq.message.answer("Склад (текст):", reply_markup=nav_kb("deb_nav:warehouse_name", allow_skip=False))
        elif step == "qty":
            await state.set_state(DebtorWizard.product_name)
            await cq.message.answer("Товар (текст):", reply_markup=nav_kb("deb_nav:product_name", allow_skip=False))
        elif step == "price":
            await state.set_state(DebtorWizard.qty)
            await cq.message.answer("Кол-во (кг):", reply_markup=nav_kb("deb_nav:qty", allow_skip=False))
        elif step == "delivery":
            await state.set_state(DebtorWizard.price)
            await cq.message.answer("Цена за 1 кг:", reply_markup=nav_kb("deb_nav:price", allow_skip=False))
        elif step == "confirm":
            await state.set_state(DebtorWizard.delivery)
            await cq.message.answer("Доставка (0 если нет):", reply_markup=nav_kb("deb_nav:delivery", allow_skip=True))
        else:
            await state.clear()
            await cq.message.answer("Меню:", reply_markup=main_menu_kb())
        return await cq.answer()

    if action == "skip":
        cur = await state.get_state()
        step = str(cur).split(":")[-1]
        if step == "customer_phone":
            await state.update_data(customer_phone="-")
            await state.set_state(DebtorWizard.warehouse_name)
            await cq.message.answer("Склад (текст):", reply_markup=nav_kb("deb_nav:warehouse_name", allow_skip=False))
        elif step == "delivery":
            await state.update_data(delivery="0")
            await state.set_state(DebtorWizard.confirm)
            data = await state.get_data()
            await cq.message.answer(build_debtor_summary(data) + "\n\nПодтвердить?",
                                   parse_mode=ParseMode.MARKDOWN,
                                   reply_markup=yes_no_kb("deb_confirm"))
        return await cq.answer()

    await cq.answer()


@router.message(DebtorWizard.customer_name)
async def deb_name(message: Message, state: FSMContext):
    await state.update_data(customer_name=safe_text(message.text))
    await state.set_state(DebtorWizard.customer_phone)
    await message.answer("Телефон клиента:", reply_markup=nav_kb("deb_nav:customer_phone", allow_skip=True))


@router.message(DebtorWizard.customer_phone)
async def deb_phone(message: Message, state: FSMContext):
    await state.update_data(customer_phone=safe_phone(message.text) or "-")
    await state.set_state(DebtorWizard.warehouse_name)
    await message.answer("Склад (текст):", reply_markup=nav_kb("deb_nav:warehouse_name", allow_skip=False))


@router.message(DebtorWizard.warehouse_name)
async def deb_wh(message: Message, state: FSMContext):
    await state.update_data(warehouse_name=safe_text(message.text))
    await state.set_state(DebtorWizard.product_name)
    await message.answer("Товар (текст):", reply_markup=nav_kb("deb_nav:product_name", allow_skip=False))


@router.message(DebtorWizard.product_name)
async def deb_pr(message: Message, state: FSMContext):
    await state.update_data(product_name=safe_text(message.text))
    await state.set_state(DebtorWizard.qty)
    await message.answer("Кол-во (кг):", reply_markup=nav_kb("deb_nav:qty", allow_skip=False))


@router.message(DebtorWizard.qty)
async def deb_qty(message: Message, state: FSMContext):
    try:
        q = dec(message.text)
        if q < 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число, например 10 или 10.5")
    await state.update_data(qty=str(q))
    await state.set_state(DebtorWizard.price)
    await message.answer("Цена за 1 кг:", reply_markup=nav_kb("deb_nav:price", allow_skip=False))


@router.message(DebtorWizard.price)
async def deb_price(message: Message, state: FSMContext):
    try:
        p = dec(message.text)
        if p < 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число, например 250")
    await state.update_data(price=str(p))
    await state.set_state(DebtorWizard.delivery)
    await message.answer("Доставка (0 если нет):", reply_markup=nav_kb("deb_nav:delivery", allow_skip=True))


@router.message(DebtorWizard.delivery)
async def deb_delivery(message: Message, state: FSMContext):
    txt = safe_text(message.text)
    if txt == "":
        txt = "0"
    try:
        d = dec(txt)
        if d < 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число, например 0")
    await state.update_data(delivery=str(d))
    await state.set_state(DebtorWizard.confirm)
    data = await state.get_data()
    await message.answer(build_debtor_summary(data) + "\n\nПодтвердить?",
                         parse_mode=ParseMode.MARKDOWN,
                         reply_markup=yes_no_kb("deb_confirm"))


def build_debtor_summary(data: dict) -> str:
    qty = Decimal(data["qty"])
    price = Decimal(data["price"])
    total = qty * price
    delivery = Decimal(data.get("delivery", "0"))
    return (
        "📋 *ДОЛЖНИК (проверка):*\n"
        f"Дата: *{data['doc_date']}*\n"
        f"Клиент: *{data.get('customer_name','')}* / {data.get('customer_phone','-')}\n"
        f"Склад: *{data['warehouse_name']}*\n"
        f"Товар: *{data['product_name']}*\n"
        f"Кол-во: *{fmt_kg(qty)} кг*\n"
        f"Цена: *{fmt_money(price)}*\n"
        f"Сумма: *{fmt_money(total)}*\n"
        f"Доставка: *{fmt_money(delivery)}*"
    )


@router.callback_query(F.data.startswith("deb_confirm:"))
async def deb_confirm(cq: CallbackQuery, state: FSMContext):
    ch = cq.data.split(":", 1)[1] if cq.data else "no"
    if ch == "no":
        await state.clear()
        await cq.message.answer("Отменено ✅", reply_markup=main_menu_kb())
        return await cq.answer()

    data = await state.get_data()
    d = datetime.strptime(data["doc_date"], "%Y-%m-%d").date()

    qty = Decimal(data["qty"])
    price = Decimal(data["price"])
    total = qty * price
    delivery = Decimal(data.get("delivery", "0"))

    async with Session() as s:
        s.add(Debtor(
            doc_date=d,
            customer_name=data.get("customer_name", ""),
            customer_phone=data.get("customer_phone", "-"),
            warehouse_name=data["warehouse_name"],
            product_name=data["product_name"],
            qty_kg=qty,
            price_per_kg=price,
            total_amount=total,
            delivery_cost=delivery,
            is_paid=False
        ))
        await s.commit()

    await state.clear()
    await cq.message.answer("✅ Должник добавлен.", reply_markup=main_menu_kb())
    await cq.answer()


# ===================== main =====================
async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    bot = Bot(TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    print("=== BOT STARTED OK ===", flush=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
