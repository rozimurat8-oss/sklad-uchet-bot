import os
import asyncio
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

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
    select, func, delete, update
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, selectinload
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker


# ===================== Settings =====================
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

# Render persistent disk path:
# IMPORTANT: 4 slashes after sqlite+aiosqlite:
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
    direction: Mapped[str] = mapped_column(String(10))  # "in" / "out"
    method: Mapped[str] = mapped_column(String(10))     # "cash" / "noncash"
    bank: Mapped[str] = mapped_column(String(120), default="")
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
    bank: Mapped[str] = mapped_column(String(120), default="")

    warehouse: Mapped[Warehouse] = relationship()
    product: Mapped[Product] = relationship()


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

    # money (optional)
    add_money_entry: Mapped[bool] = mapped_column(Boolean, default=False)
    payment_method: Mapped[str] = mapped_column(String(10), default="")
    bank: Mapped[str] = mapped_column(String(120), default="")

    warehouse: Mapped[Warehouse] = relationship()
    product: Mapped[Product] = relationship()


class Debtor(Base):
    """
    Должник: может появляться из продажи (не оплачено) или вручную.
    Можно отметить как оплачено.
    """
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


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# ===================== Helpers =====================
def is_admin(user_id: int) -> bool:
    return (not ADMIN_USER_IDS) or (user_id in ADMIN_USER_IDS)


def dec(s: str) -> Decimal:
    s = s.strip().replace(",", ".")
    return Decimal(s)


def fmt_money(x: Decimal) -> str:
    return f"{x:.2f}"


def fmt_kg(x: Decimal) -> str:
    return f"{x:.3f}".rstrip("0").rstrip(".")


def safe_phone(s: str) -> str:
    return s.strip()


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
    kb.button(text="❌ Отмена")
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True)


def date_choice_kb(prefix: str):
    ikb = InlineKeyboardBuilder()
    ikb.button(text="📅 Сегодня", callback_data=f"{prefix}:today")
    ikb.button(text="📅 Вчера", callback_data=f"{prefix}:yesterday")
    ikb.button(text="✍️ Ввести вручную (YYYY-MM-DD)", callback_data=f"{prefix}:manual")
    ikb.adjust(1)
    return ikb.as_markup()


def yes_no_kb(prefix: str):
    ikb = InlineKeyboardBuilder()
    ikb.button(text="✅ Да", callback_data=f"{prefix}:yes")
    ikb.button(text="❌ Нет", callback_data=f"{prefix}:no")
    ikb.adjust(2)
    return ikb.as_markup()


def paid_kb(prefix: str):
    ikb = InlineKeyboardBuilder()
    ikb.button(text="✅ Оплачено", callback_data=f"{prefix}:paid")
    ikb.button(text="🧾 Не оплачено", callback_data=f"{prefix}:unpaid")
    ikb.adjust(2)
    return ikb.as_markup()


def pay_method_kb(prefix: str):
    ikb = InlineKeyboardBuilder()
    ikb.button(text="💵 Нал", callback_data=f"{prefix}:cash")
    ikb.button(text="🏦 Безнал", callback_data=f"{prefix}:noncash")
    ikb.adjust(2)
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
    bank = State()
    confirm = State()


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
    bank = State()
    confirm = State()


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


# ===================== Router & Bot =====================
router = Router()

MENU_TEXTS = {
    "📦 Остатки", "💰 Деньги", "🟢 Приход", "🔴 Продажа",
    "📄 Приходы", "📄 Продажи", "📋 Должники", "➕ Добавить должн...",
    "❌ Отмена"
}


@router.message(F.text.in_(MENU_TEXTS))
async def menu_anywhere(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа.")

    text = message.text
    await state.clear()

    if text == "📦 Остатки":
        return await show_stocks(message)
    if text == "💰 Деньги":
        return await show_money(message)
    if text == "🟢 Приход":
        return await start_income(message, state)
    if text == "🔴 Продажа":
        return await start_sale(message, state)
    if text == "📄 Продажи":
        return await list_sales(message)
    if text == "📄 Приходы":
        return await list_incomes(message)
    if text == "📋 Должники":
        return await list_debtors(message)
    if text == "➕ Добавить должн...":
        return await start_debtor(message, state)
    if text == "❌ Отмена":
        return await message.answer("Ок, отменил ✅", reply_markup=main_menu_kb())


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа.")
    await state.clear()
    await message.answer("Привет! Выбери действие:", reply_markup=main_menu_kb())


# ===================== Core actions =====================
async def get_or_create_warehouse(name: str) -> Warehouse:
    name = name.strip()
    async with Session() as s:
        w = await s.scalar(select(Warehouse).where(Warehouse.name == name))
        if w:
            return w
        w = Warehouse(name=name)
        s.add(w)
        await s.commit()
        return w


async def get_or_create_product(name: str) -> Product:
    name = name.strip()
    async with Session() as s:
        p = await s.scalar(select(Product).where(Product.name == name))
        if p:
            return p
        p = Product(name=name)
        s.add(p)
        await s.commit()
        return p


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


async def show_stocks(message: Message):
    async with Session() as s:
        rows = (await s.execute(
            select(Stock)
            .options(selectinload(Stock.warehouse), selectinload(Stock.product))
            .order_by(Stock.warehouse_id, Stock.product_id)
        )).scalars().all()

    if not rows:
        return await message.answer("Остатков пока нет.", reply_markup=main_menu_kb())

    lines = ["📦 *Остатки:*"]
    for r in rows:
        if r.qty_kg and r.qty_kg != 0:
            lines.append(f"• {r.warehouse.name} — {r.product.name}: *{fmt_kg(r.qty_kg)} кг*")
    if len(lines) == 1:
        lines.append("Пока везде 0.")
    await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_kb())


async def show_money(message: Message):
    async with Session() as s:
        total_in = await s.scalar(select(func.coalesce(func.sum(MoneyLedger.amount), 0)).where(MoneyLedger.direction == "in"))
        total_out = await s.scalar(select(func.coalesce(func.sum(MoneyLedger.amount), 0)).where(MoneyLedger.direction == "out"))
    balance = Decimal(total_in) - Decimal(total_out)

    txt = (
        "💰 *Деньги:*\n"
        f"Приход: *{fmt_money(Decimal(total_in))}*\n"
        f"Расход: *{fmt_money(Decimal(total_out))}*\n"
        f"Баланс: *{fmt_money(balance)}*"
    )
    await message.answer(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_kb())


# ===================== Sales list / View / Actions =====================
def sales_actions_kb(sale_id: int, paid: bool):
    ikb = InlineKeyboardBuilder()
    if not paid:
        ikb.button(text="✅ Отметить как оплачено", callback_data=f"sale_paid:{sale_id}")
    ikb.button(text="🗑 Удалить", callback_data=f"sale_del:{sale_id}")
    ikb.adjust(1)
    return ikb.as_markup()


@router.callback_query(F.data.startswith("sale_paid:"))
async def cb_sale_paid(cq: CallbackQuery):
    sale_id = int(cq.data.split(":")[1])
    async with Session() as s:
        sale = await s.get(Sale, sale_id)
        if not sale:
            return await cq.answer("Не найдено", show_alert=True)
        if sale.is_paid:
            return await cq.answer("Уже оплачено", show_alert=True)

        sale.is_paid = True
        # добавить поступление денег, если раньше не добавляли
        if sale.payment_method:
            s.add(MoneyLedger(
                entry_date=sale.doc_date,
                direction="in",
                method=sale.payment_method,
                bank=sale.bank or "",
                amount=Decimal(sale.total_amount),
                note=f"Оплата по продаже #{sale.id} ({sale.customer_name})"
            ))

        # найти должника и закрыть (если был)
        d = await s.scalar(select(Debtor).where(Debtor.customer_name == sale.customer_name,
                                               Debtor.customer_phone == sale.customer_phone,
                                               Debtor.total_amount == sale.total_amount,
                                               Debtor.is_paid == False))
        if d:
            d.is_paid = True

        await s.commit()

    await cq.message.answer(f"✅ Продажа #{sale_id} отмечена как оплачено.")
    await cq.answer()


@router.callback_query(F.data.startswith("sale_del:"))
async def cb_sale_del(cq: CallbackQuery):
    sale_id = int(cq.data.split(":")[1])
    async with Session() as s:
        sale = await s.get(Sale, sale_id)
        if not sale:
            return await cq.answer("Не найдено", show_alert=True)

        # ВНИМАНИЕ: при удалении продажи мы НЕ возвращаем товар назад.
        # Если нужно — можно добавить отдельную кнопку "Отменить продажу" с обратным движением.
        await s.execute(delete(Sale).where(Sale.id == sale_id))
        await s.commit()

    await cq.message.answer(f"🗑 Продажа #{sale_id} удалена.")
    await cq.answer()


async def list_sales(message: Message):
    async with Session() as s:
        rows = (await s.execute(
            select(Sale)
            .options(selectinload(Sale.warehouse), selectinload(Sale.product))
            .order_by(Sale.id.desc())
            .limit(30)
        )).scalars().all()

    if not rows:
        return await message.answer("Продаж пока нет.", reply_markup=main_menu_kb())

    lines = ["📄 *Последние продажи* (последние 30):"]
    for r in rows:
        paid = "✅" if r.is_paid else "🧾"
        lines.append(
            f"\n*#{r.id}* {paid} {r.doc_date} — {r.customer_name} ({r.customer_phone})\n"
            f"{r.warehouse.name} / {r.product.name} — {fmt_kg(r.qty_kg)} кг × {fmt_money(r.price_per_kg)} = *{fmt_money(r.total_amount)}*"
        )
    await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_kb())
    await message.answer("Чтобы управлять записью: напиши `продажа #ID` например: `продажа #12`",
                         reply_markup=main_menu_kb())


@router.message(F.text.regexp(r"(?i)^продажа\s+#\d+$"))
async def sale_by_id(message: Message):
    sale_id = int(message.text.split("#")[1])
    async with Session() as s:
        r = await s.scalar(
            select(Sale)
            .options(selectinload(Sale.warehouse), selectinload(Sale.product))
            .where(Sale.id == sale_id)
        )
    if not r:
        return await message.answer("Не найдено.", reply_markup=main_menu_kb())

    paid = "✅ Оплачено" if r.is_paid else "🧾 Не оплачено"
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
    )
    await message.answer(txt, parse_mode=ParseMode.MARKDOWN,
                         reply_markup=sales_actions_kb(r.id, r.is_paid))


# ===================== Incomes list / View / delete =====================
def income_actions_kb(income_id: int):
    ikb = InlineKeyboardBuilder()
    ikb.button(text="🗑 Удалить", callback_data=f"inc_del:{income_id}")
    ikb.adjust(1)
    return ikb.as_markup()


@router.callback_query(F.data.startswith("inc_del:"))
async def cb_inc_del(cq: CallbackQuery):
    income_id = int(cq.data.split(":")[1])
    async with Session() as s:
        inc = await s.get(Income, income_id)
        if not inc:
            return await cq.answer("Не найдено", show_alert=True)
        # ВНИМАНИЕ: при удалении прихода мы НЕ уменьшаем склад назад.
        await s.execute(delete(Income).where(Income.id == income_id))
        await s.commit()
    await cq.message.answer(f"🗑 Приход #{income_id} удалён.")
    await cq.answer()


async def list_incomes(message: Message):
    async with Session() as s:
        rows = (await s.execute(
            select(Income)
            .options(selectinload(Income.warehouse), selectinload(Income.product))
            .order_by(Income.id.desc())
            .limit(30)
        )).scalars().all()

    if not rows:
        return await message.answer("Приходов пока нет.", reply_markup=main_menu_kb())

    lines = ["📄 *Последние приходы* (последние 30):"]
    for r in rows:
        lines.append(
            f"\n*#{r.id}* {r.doc_date} — {r.supplier_name} ({r.supplier_phone})\n"
            f"{r.warehouse.name} / {r.product.name} — {fmt_kg(r.qty_kg)} кг × {fmt_money(r.price_per_kg)} = *{fmt_money(r.total_amount)}*"
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
            .options(selectinload(Income.warehouse), selectinload(Income.product))
            .where(Income.id == inc_id)
        )
    if not r:
        return await message.answer("Не найдено.", reply_markup=main_menu_kb())

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
    debtor_id = int(cq.data.split(":")[1])
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
    debtor_id = int(cq.data.split(":")[1])
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
async def start_sale(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(SaleWizard.doc_date)
    await message.answer("Дата продажи:", reply_markup=date_choice_kb("sale_date"))


@router.callback_query(F.data.startswith("sale_date:"))
async def cb_sale_date(cq: CallbackQuery, state: FSMContext):
    choice = cq.data.split(":")[1]
    if choice == "today":
        d = date.today()
        await state.update_data(doc_date=d.isoformat())
        await state.set_state(SaleWizard.customer_name)
        await cq.message.answer("Имя клиента (можно '-'):")
    elif choice == "yesterday":
        d = date.today() - timedelta(days=1)
        await state.update_data(doc_date=d.isoformat())
        await state.set_state(SaleWizard.customer_name)
        await cq.message.answer("Имя клиента (можно '-'):")
    else:
        await state.update_data(doc_date="manual")
        await cq.message.answer("Введи дату в формате YYYY-MM-DD:")
    await cq.answer()


@router.message(SaleWizard.doc_date)
async def sale_date_manual(message: Message, state: FSMContext):
    try:
        d = datetime.strptime(message.text.strip(), "%Y-%m-%d").date()
    except Exception:
        return await message.answer("Неверный формат. Пример: 2025-12-03")
    await state.update_data(doc_date=d.isoformat())
    await state.set_state(SaleWizard.customer_name)
    await message.answer("Имя клиента (можно '-'):")


@router.message(SaleWizard.customer_name)
async def sale_customer_name(message: Message, state: FSMContext):
    await state.update_data(customer_name=message.text.strip())
    await state.set_state(SaleWizard.customer_phone)
    await message.answer("Телефон клиента (можно '-' чтобы пропустить):")


@router.message(SaleWizard.customer_phone)
async def sale_customer_phone(message: Message, state: FSMContext):
    await state.update_data(customer_phone=safe_phone(message.text))
    await state.set_state(SaleWizard.warehouse)
    await message.answer("С какого склада? (напиши название склада):")


@router.message(SaleWizard.warehouse)
async def sale_warehouse(message: Message, state: FSMContext):
    await state.update_data(warehouse_name=message.text.strip())
    await state.set_state(SaleWizard.product)
    await message.answer("Какой товар? (название):")


@router.message(SaleWizard.product)
async def sale_product(message: Message, state: FSMContext):
    await state.update_data(product_name=message.text.strip())
    await state.set_state(SaleWizard.qty)
    await message.answer("Кол-во (кг), например 125.5 :")


@router.message(SaleWizard.qty)
async def sale_qty(message: Message, state: FSMContext):
    try:
        q = dec(message.text)
        if q <= 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число > 0, например 10 или 10.5")
    await state.update_data(qty=str(q))
    await state.set_state(SaleWizard.price)
    await message.answer("Цена за 1 кг:")


@router.message(SaleWizard.price)
async def sale_price(message: Message, state: FSMContext):
    try:
        p = dec(message.text)
        if p < 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число, например 250 или 250.5")
    await state.update_data(price=str(p))
    await state.set_state(SaleWizard.delivery)
    await message.answer("Расходы на доставку (0 если нет):")


@router.message(SaleWizard.delivery)
async def sale_delivery(message: Message, state: FSMContext):
    try:
        d = dec(message.text)
        if d < 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число, например 0 или 1500")
    await state.update_data(delivery=str(d))
    await state.set_state(SaleWizard.paid_status)
    await message.answer("Статус оплаты:", reply_markup=paid_kb("sale_paid"))


@router.callback_query(F.data.startswith("sale_paid:"))
async def cb_sale_paid_status(cq: CallbackQuery, state: FSMContext):
    ch = cq.data.split(":")[1]
    if ch == "paid":
        await state.update_data(is_paid=True)
        await state.set_state(SaleWizard.pay_method)
        await cq.message.answer("Как оплатили?", reply_markup=pay_method_kb("sale_pay"))
    else:
        # НЕ ОПЛАЧЕНО: не спрашиваем банк/метод, сразу подтверждение
        await state.update_data(is_paid=False, payment_method="", bank="")
        await state.set_state(SaleWizard.confirm)
        data = await state.get_data()
        await cq.message.answer(build_sale_summary(data) + "\n\nПодтвердить?", reply_markup=yes_no_kb("sale_confirm"))
    await cq.answer()


@router.callback_query(F.data.startswith("sale_pay:"))
async def cb_sale_pay_method(cq: CallbackQuery, state: FSMContext):
    method = cq.data.split(":")[1]  # cash/noncash
    await state.update_data(payment_method=method)
    if method == "cash":
        await state.update_data(bank="")
        await state.set_state(SaleWizard.confirm)
        data = await state.get_data()
        await cq.message.answer(build_sale_summary(data) + "\n\nПодтвердить?", reply_markup=yes_no_kb("sale_confirm"))
    else:
        await state.set_state(SaleWizard.bank)
        await cq.message.answer("Название банка (например Kaspi / Halyk / ...):")
    await cq.answer()


@router.message(SaleWizard.bank)
async def sale_bank(message: Message, state: FSMContext):
    await state.update_data(bank=message.text.strip())
    await state.set_state(SaleWizard.confirm)
    data = await state.get_data()
    await message.answer(build_sale_summary(data) + "\n\nПодтвердить?", reply_markup=yes_no_kb("sale_confirm"))


def build_sale_summary(data: dict) -> str:
    qty = Decimal(data["qty"])
    price = Decimal(data["price"])
    total = qty * price
    delivery = Decimal(data["delivery"])
    paid = "✅ Оплачено" if data.get("is_paid") else "🧾 Не оплачено"
    pay_method = data.get("payment_method") or "-"
    bank = data.get("bank") or "-"

    return (
        "🔴 *ПРОДАЖА (проверка):*\n"
        f"Дата: *{data['doc_date']}*\n"
        f"Клиент: *{data.get('customer_name','')}* / {data.get('customer_phone','')}\n"
        f"Склад: *{data['warehouse_name']}*\n"
        f"Товар: *{data['product_name']}*\n"
        f"Кол-во: *{fmt_kg(qty)} кг*\n"
        f"Цена: *{fmt_money(price)}*\n"
        f"Сумма: *{fmt_money(total)}*\n"
        f"Доставка: *{fmt_money(delivery)}*\n"
        f"Оплата: *{paid}*\n"
        f"Метод: *{pay_method}*\n"
        f"Банк: *{bank}*"
    )


@router.callback_query(F.data.startswith("sale_confirm:"))
async def cb_sale_confirm(cq: CallbackQuery, state: FSMContext):
    ch = cq.data.split(":")[1]
    if ch == "no":
        await state.clear()
        await cq.message.answer("Отменено ✅", reply_markup=main_menu_kb())
        return await cq.answer()

    data = await state.get_data()

    d = datetime.strptime(data["doc_date"], "%Y-%m-%d").date()
    customer_name = data.get("customer_name", "")
    customer_phone = data.get("customer_phone", "")

    warehouse_name = data["warehouse_name"]
    product_name = data["product_name"]
    qty = Decimal(data["qty"])
    price = Decimal(data["price"])
    total = qty * price
    delivery = Decimal(data["delivery"])

    is_paid = bool(data.get("is_paid"))
    payment_method = data.get("payment_method", "")
    bank = data.get("bank", "")

    async with Session() as s:
        w = await s.scalar(select(Warehouse).where(Warehouse.name == warehouse_name))
        if not w:
            w = Warehouse(name=warehouse_name)
            s.add(w)
            await s.flush()

        p = await s.scalar(select(Product).where(Product.name == product_name))
        if not p:
            p = Product(name=product_name)
            s.add(p)
            await s.flush()

        stock = await get_stock_row(s, w.id, p.id)
        if stock.qty_kg < qty:
            await state.clear()
            await cq.message.answer(
                f"❗ Недостаточно товара на складе.\n"
                f"Есть: {fmt_kg(stock.qty_kg)} кг, нужно: {fmt_kg(qty)} кг",
                reply_markup=main_menu_kb()
            )
            return await cq.answer()

        stock.qty_kg = Decimal(stock.qty_kg) - qty

        sale = Sale(
            doc_date=d,
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
            bank=bank if is_paid else ""
        )
        s.add(sale)
        await s.flush()

        # деньги добавляем ТОЛЬКО если оплачено
        if is_paid:
            s.add(MoneyLedger(
                entry_date=d,
                direction="in",
                method=payment_method,
                bank=bank if payment_method == "noncash" else "",
                amount=total,
                note=f"Продажа #{sale.id} ({customer_name})"
            ))
        else:
            # в должники
            s.add(Debtor(
                doc_date=d,
                customer_name=customer_name,
                customer_phone=customer_phone,
                warehouse_name=warehouse_name,
                product_name=product_name,
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
async def start_income(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(IncomeWizard.doc_date)
    await message.answer("Дата прихода:", reply_markup=date_choice_kb("inc_date"))


@router.callback_query(F.data.startswith("inc_date:"))
async def cb_income_date(cq: CallbackQuery, state: FSMContext):
    choice = cq.data.split(":")[1]
    if choice == "today":
        d = date.today()
        await state.update_data(doc_date=d.isoformat())
        await state.set_state(IncomeWizard.supplier_name)
        await cq.message.answer("Имя поставщика (можно '-'):")
    elif choice == "yesterday":
        d = date.today() - timedelta(days=1)
        await state.update_data(doc_date=d.isoformat())
        await state.set_state(IncomeWizard.supplier_name)
        await cq.message.answer("Имя поставщика (можно '-'):")
    else:
        await state.update_data(doc_date="manual")
        await cq.message.answer("Введи дату в формате YYYY-MM-DD:")
    await cq.answer()


@router.message(IncomeWizard.doc_date)
async def income_date_manual(message: Message, state: FSMContext):
    try:
        d = datetime.strptime(message.text.strip(), "%Y-%m-%d").date()
    except Exception:
        return await message.answer("Неверный формат. Пример: 2025-12-03")
    await state.update_data(doc_date=d.isoformat())
    await state.set_state(IncomeWizard.supplier_name)
    await message.answer("Имя поставщика (можно '-'):")


@router.message(IncomeWizard.supplier_name)
async def income_supplier_name(message: Message, state: FSMContext):
    await state.update_data(supplier_name=message.text.strip())
    await state.set_state(IncomeWizard.supplier_phone)
    await message.answer("Телефон поставщика (можно '-' чтобы пропустить):")


@router.message(IncomeWizard.supplier_phone)
async def income_supplier_phone(message: Message, state: FSMContext):
    await state.update_data(supplier_phone=safe_phone(message.text))
    await state.set_state(IncomeWizard.warehouse)
    await message.answer("На какой склад? (название склада):")


@router.message(IncomeWizard.warehouse)
async def income_warehouse(message: Message, state: FSMContext):
    await state.update_data(warehouse_name=message.text.strip())
    await state.set_state(IncomeWizard.product)
    await message.answer("Какой товар? (название):")


@router.message(IncomeWizard.product)
async def income_product(message: Message, state: FSMContext):
    await state.update_data(product_name=message.text.strip())
    await state.set_state(IncomeWizard.qty)
    await message.answer("Кол-во (кг), например 125.5 :")


@router.message(IncomeWizard.qty)
async def income_qty(message: Message, state: FSMContext):
    try:
        q = dec(message.text)
        if q <= 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число > 0, например 10 или 10.5")
    await state.update_data(qty=str(q))
    await state.set_state(IncomeWizard.price)
    await message.answer("Цена за 1 кг:")


@router.message(IncomeWizard.price)
async def income_price(message: Message, state: FSMContext):
    try:
        p = dec(message.text)
        if p < 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число, например 250 или 250.5")
    await state.update_data(price=str(p))
    await state.set_state(IncomeWizard.delivery)
    await message.answer("Расходы на доставку (0 если нет):")


@router.message(IncomeWizard.delivery)
async def income_delivery(message: Message, state: FSMContext):
    try:
        d = dec(message.text)
        if d < 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число, например 0 или 1500")
    await state.update_data(delivery=str(d))
    await state.set_state(IncomeWizard.add_money)
    await message.answer("Добавить запись денег (расход) по этому приходу?", reply_markup=yes_no_kb("inc_money"))


@router.callback_query(F.data.startswith("inc_money:"))
async def cb_inc_money(cq: CallbackQuery, state: FSMContext):
    ch = cq.data.split(":")[1]
    if ch == "yes":
        await state.update_data(add_money_entry=True)
        await state.set_state(IncomeWizard.pay_method)
        await cq.message.answer("Как оплатили поставщику?", reply_markup=pay_method_kb("inc_pay"))
    else:
        await state.update_data(add_money_entry=False, payment_method="", bank="")
        await state.set_state(IncomeWizard.confirm)
        data = await state.get_data()
        await cq.message.answer(build_income_summary(data) + "\n\nПодтвердить?", reply_markup=yes_no_kb("inc_confirm"))
    await cq.answer()


@router.callback_query(F.data.startswith("inc_pay:"))
async def cb_inc_pay_method(cq: CallbackQuery, state: FSMContext):
    method = cq.data.split(":")[1]
    await state.update_data(payment_method=method)
    if method == "cash":
        await state.update_data(bank="")
        await state.set_state(IncomeWizard.confirm)
        data = await state.get_data()
        await cq.message.answer(build_income_summary(data) + "\n\nПодтвердить?", reply_markup=yes_no_kb("inc_confirm"))
    else:
        await state.set_state(IncomeWizard.bank)
        await cq.message.answer("Название банка (Kaspi / Halyk / ...):")
    await cq.answer()


@router.message(IncomeWizard.bank)
async def income_bank(message: Message, state: FSMContext):
    await state.update_data(bank=message.text.strip())
    await state.set_state(IncomeWizard.confirm)
    data = await state.get_data()
    await message.answer(build_income_summary(data) + "\n\nПодтвердить?", reply_markup=yes_no_kb("inc_confirm"))


def build_income_summary(data: dict) -> str:
    qty = Decimal(data["qty"])
    price = Decimal(data["price"])
    total = qty * price
    delivery = Decimal(data["delivery"])
    add_money = "✅ Да" if data.get("add_money_entry") else "❌ Нет"
    method = data.get("payment_method") or "-"
    bank = data.get("bank") or "-"

    return (
        "🟢 *ПРИХОД (проверка):*\n"
        f"Дата: *{data['doc_date']}*\n"
        f"Поставщик: *{data.get('supplier_name','')}* / {data.get('supplier_phone','')}\n"
        f"Склад: *{data['warehouse_name']}*\n"
        f"Товар: *{data['product_name']}*\n"
        f"Кол-во: *{fmt_kg(qty)} кг*\n"
        f"Цена: *{fmt_money(price)}*\n"
        f"Сумма: *{fmt_money(total)}*\n"
        f"Доставка: *{fmt_money(delivery)}*\n"
        f"Запись денег: *{add_money}*\n"
        f"Метод: *{method}*\n"
        f"Банк: *{bank}*"
    )


@router.callback_query(F.data.startswith("inc_confirm:"))
async def cb_income_confirm(cq: CallbackQuery, state: FSMContext):
    ch = cq.data.split(":")[1]
    if ch == "no":
        await state.clear()
        await cq.message.answer("Отменено ✅", reply_markup=main_menu_kb())
        return await cq.answer()

    data = await state.get_data()

    d = datetime.strptime(data["doc_date"], "%Y-%m-%d").date()
    supplier_name = data.get("supplier_name", "")
    supplier_phone = data.get("supplier_phone", "")

    warehouse_name = data["warehouse_name"]
    product_name = data["product_name"]
    qty = Decimal(data["qty"])
    price = Decimal(data["price"])
    total = qty * price
    delivery = Decimal(data["delivery"])

    add_money_entry = bool(data.get("add_money_entry"))
    payment_method = data.get("payment_method", "")
    bank = data.get("bank", "")

    async with Session() as s:
        w = await s.scalar(select(Warehouse).where(Warehouse.name == warehouse_name))
        if not w:
            w = Warehouse(name=warehouse_name)
            s.add(w)
            await s.flush()

        p = await s.scalar(select(Product).where(Product.name == product_name))
        if not p:
            p = Product(name=product_name)
            s.add(p)
            await s.flush()

        stock = await get_stock_row(s, w.id, p.id)
        stock.qty_kg = Decimal(stock.qty_kg) + qty

        inc = Income(
            doc_date=d,
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
            bank=bank if add_money_entry else ""
        )
        s.add(inc)
        await s.flush()

        # Деньги: если включили — это расход (покупка товара)
        if add_money_entry:
            s.add(MoneyLedger(
                entry_date=d,
                direction="out",
                method=payment_method,
                bank=bank if payment_method == "noncash" else "",
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
    await message.answer("Дата (для должника):", reply_markup=date_choice_kb("deb_date"))


@router.callback_query(F.data.startswith("deb_date:"))
async def cb_deb_date(cq: CallbackQuery, state: FSMContext):
    choice = cq.data.split(":")[1]
    if choice == "today":
        d = date.today()
        await state.update_data(doc_date=d.isoformat())
        await state.set_state(DebtorWizard.customer_name)
        await cq.message.answer("Имя клиента:")
    elif choice == "yesterday":
        d = date.today() - timedelta(days=1)
        await state.update_data(doc_date=d.isoformat())
        await state.set_state(DebtorWizard.customer_name)
        await cq.message.answer("Имя клиента:")
    else:
        await cq.message.answer("Введи дату YYYY-MM-DD:")
    await cq.answer()


@router.message(DebtorWizard.doc_date)
async def deb_date_manual(message: Message, state: FSMContext):
    try:
        d = datetime.strptime(message.text.strip(), "%Y-%m-%d").date()
    except Exception:
        return await message.answer("Неверный формат. Пример: 2025-12-03")
    await state.update_data(doc_date=d.isoformat())
    await state.set_state(DebtorWizard.customer_name)
    await message.answer("Имя клиента:")


@router.message(DebtorWizard.customer_name)
async def deb_name(message: Message, state: FSMContext):
    await state.update_data(customer_name=message.text.strip())
    await state.set_state(DebtorWizard.customer_phone)
    await message.answer("Телефон клиента (можно '-' чтобы пропустить):")


@router.message(DebtorWizard.customer_phone)
async def deb_phone(message: Message, state: FSMContext):
    await state.update_data(customer_phone=safe_phone(message.text))
    await state.set_state(DebtorWizard.warehouse_name)
    await message.answer("Склад (текст):")


@router.message(DebtorWizard.warehouse_name)
async def deb_w(message: Message, state: FSMContext):
    await state.update_data(warehouse_name=message.text.strip())
    await state.set_state(DebtorWizard.product_name)
    await message.answer("Товар (текст):")


@router.message(DebtorWizard.product_name)
async def deb_p(message: Message, state: FSMContext):
    await state.update_data(product_name=message.text.strip())
    await state.set_state(DebtorWizard.qty)
    await message.answer("Кол-во (кг):")


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
    await message.answer("Цена за 1 кг:")


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
    await message.answer("Доставка (0 если нет):")


@router.message(DebtorWizard.delivery)
async def deb_delivery(message: Message, state: FSMContext):
    try:
        d = dec(message.text)
        if d < 0:
            raise ValueError
    except Exception:
        return await message.answer("Ошибка. Введи число, например 0")
    await state.update_data(delivery=str(d))
    await state.set_state(DebtorWizard.confirm)
    data = await state.get_data()
    await message.answer(build_debtor_summary(data) + "\n\nПодтвердить?", reply_markup=yes_no_kb("deb_confirm"))


def build_debtor_summary(data: dict) -> str:
    qty = Decimal(data["qty"])
    price = Decimal(data["price"])
    total = qty * price
    delivery = Decimal(data["delivery"])
    return (
        "📋 *ДОЛЖНИК (проверка):*\n"
        f"Дата: *{data['doc_date']}*\n"
        f"Клиент: *{data.get('customer_name','')}* / {data.get('customer_phone','')}\n"
        f"Склад: *{data['warehouse_name']}*\n"
        f"Товар: *{data['product_name']}*\n"
        f"Кол-во: *{fmt_kg(qty)} кг*\n"
        f"Цена: *{fmt_money(price)}*\n"
        f"Сумма: *{fmt_money(total)}*\n"
        f"Доставка: *{fmt_money(delivery)}*"
    )


@router.callback_query(F.data.startswith("deb_confirm:"))
async def cb_deb_confirm(cq: CallbackQuery, state: FSMContext):
    ch = cq.data.split(":")[1]
    if ch == "no":
        await state.clear()
        await cq.message.answer("Отменено ✅", reply_markup=main_menu_kb())
        return await cq.answer()

    data = await state.get_data()
    d = datetime.strptime(data["doc_date"], "%Y-%m-%d").date()
    customer_name = data.get("customer_name", "")
    customer_phone = data.get("customer_phone", "")
    warehouse_name = data["warehouse_name"]
    product_name = data["product_name"]
    qty = Decimal(data["qty"])
    price = Decimal(data["price"])
    total = qty * price
    delivery = Decimal(data["delivery"])

    async with Session() as s:
        s.add(Debtor(
            doc_date=d,
            customer_name=customer_name,
            customer_phone=customer_phone,
            warehouse_name=warehouse_name,
            product_name=product_name,
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
    await init_db()

    bot = Bot(TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    print("=== BOT STARTED OK ===", flush=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
