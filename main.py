import os
import asyncio
import html
from datetime import date, datetime, timedelta
from decimal import Decimal

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from aiogram.enums.parse_mode import ParseMode

from sqlalchemy import (
    String, Integer, Numeric, Date, DateTime, ForeignKey, Boolean,
    select, func, delete, case, update, text
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, selectinload
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker


load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

DB_URL = os.getenv("DB_URL", "sqlite+aiosqlite:////var/data/data.db")
engine = create_async_engine(DB_URL, echo=False)
Session = async_sessionmaker(engine, expire_on_commit=False)

OWNER_ID = int(os.getenv("OWNER_ID", "139099578") or 0)

print("=== BOOT ===", flush=True)
print("TOKEN set:", bool(TOKEN), flush=True)
print("DB_URL:", DB_URL, flush=True)
print("OWNER_ID:", OWNER_ID, flush=True)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    user_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    full_name: Mapped[str] = mapped_column(String(200), default="")
    username: Mapped[str] = mapped_column(String(100), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    name: Mapped[str] = mapped_column(String(120), default="")


class Warehouse(Base):
    __tablename__ = "warehouses"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, index=True)


class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(150), unique=True, index=True)



class StockMovement(Base):
    __tablename__ = "stock_movements"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    entry_date: Mapped[date] = mapped_column(Date, index=True)

    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"), index=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), index=True)
    qty_kg: Mapped[Decimal] = mapped_column(Numeric(18, 3))  # +income, -sale

    doc_type: Mapped[str] = mapped_column(String(20), index=True)  # "sale"/"income"/"adjust"
    doc_id: Mapped[int] = mapped_column(Integer, index=True)

    warehouse: Mapped[Warehouse] = relationship()
    product: Mapped[Product] = relationship()


class MoneyMovement(Base):
    __tablename__ = "money_movements"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    entry_date: Mapped[date] = mapped_column(Date, index=True)

    direction: Mapped[str] = mapped_column(String(10))  # in/out (informational)
    method: Mapped[str] = mapped_column(String(10), default="")

    account_type: Mapped[str] = mapped_column(String(10), default="cash")  # cash/bank/ip
    bank_id: Mapped[int | None] = mapped_column(ForeignKey("banks.id"), nullable=True)
    bank: Mapped["Bank | None"] = relationship()

    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))  # +in, -out

    doc_type: Mapped[str] = mapped_column(String(20), index=True)  # "sale"/"income"/"adjust"
    doc_id: Mapped[int] = mapped_column(Integer, index=True)

    note: Mapped[str] = mapped_column(String(300), default="")

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

    direction: Mapped[str] = mapped_column(String(10))
    method: Mapped[str] = mapped_column(String(10))

    account_type: Mapped[str] = mapped_column(String(10), default="cash")
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
    payment_method: Mapped[str] = mapped_column(String(10), default="")

    account_type: Mapped[str] = mapped_column(String(10), default="cash")
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
    payment_method: Mapped[str] = mapped_column(String(10), default="")

    account_type: Mapped[str] = mapped_column(String(10), default="cash")
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


class AllowedUser(Base):
    __tablename__ = "allowed_users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    added_by: Mapped[int] = mapped_column(Integer, default=0)
    note: Mapped[str] = mapped_column(String(300), default="")


async def ensure_allowed_users_schema(conn):
    await conn.execute(text("PRAGMA foreign_keys=ON"))
    await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS allowed_users (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL UNIQUE,
            created_at DATETIME NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            added_by INTEGER,
            note TEXT
        )
    """))

    cols = (await conn.execute(text("PRAGMA table_info(allowed_users)"))).fetchall()
    colnames = {c[1] for c in cols}

    if "added_at" in colnames and "created_at" not in colnames:
        try:
            await conn.execute(text("ALTER TABLE allowed_users RENAME COLUMN added_at TO created_at"))
        except Exception:
            pass

    cols = (await conn.execute(text("PRAGMA table_info(allowed_users)"))).fetchall()
    colnames = {c[1] for c in cols}

    if "added_by" not in colnames:
        await conn.execute(text("ALTER TABLE allowed_users ADD COLUMN added_by INTEGER"))
    if "note" not in colnames:
        await conn.execute(text("ALTER TABLE allowed_users ADD COLUMN note TEXT"))

    if "created_at" not in colnames:
        await conn.execute(text("ALTER TABLE allowed_users ADD COLUMN created_at DATETIME"))
        await conn.execute(text("UPDATE allowed_users SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP)"))
    try:
        await conn.execute(text("UPDATE allowed_users SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP) WHERE created_at IS NULL"))
    except Exception:
        pass


async def ensure_users_schema(conn):
    await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            full_name TEXT,
            username TEXT,
            created_at DATETIME NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            name TEXT
        )
    """))

    cols = (await conn.execute(text("PRAGMA table_info(users)"))).fetchall()
    colnames = {c[1] for c in cols}

    if "full_name" not in colnames:
        await conn.execute(text("ALTER TABLE users ADD COLUMN full_name TEXT"))
    if "username" not in colnames:
        await conn.execute(text("ALTER TABLE users ADD COLUMN username TEXT"))
    if "name" not in colnames:
        await conn.execute(text("ALTER TABLE users ADD COLUMN name TEXT"))
    if "created_at" not in colnames:
        await conn.execute(text("ALTER TABLE users ADD COLUMN created_at DATETIME"))
        await conn.execute(text("UPDATE users SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP)"))
    try:
        await conn.execute(text("UPDATE users SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP) WHERE created_at IS NULL"))
    except Exception:
        pass


def dec(s: str) -> Decimal:
    s = (s or "").strip().replace(",", ".")
    return Decimal(s)


def fmt_money(x: Decimal) -> str:
    return f"{Decimal(x):.2f}"


def fmt_kg(x: Decimal) -> str:
    return f"{Decimal(x):.3f}".rstrip("0").rstrip(".")


def render_pre_table(headers: list[str], rows: list[list[str]]) -> str:
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

def safe_text(s: str) -> str:
    return (s or "").strip()


def safe_phone(s: str) -> str:
    return (s or "").strip()




def h(s: str) -> str:
    return html.escape((s or "").strip(), quote=False)
def parse_cb(data: str, prefix: str):
    if not data or not data.startswith(prefix + ":"):
        return []
    rest = data[len(prefix) + 1:]
    return rest.split(":") if rest else []


def is_owner(user_id: int) -> bool:
    return int(user_id) == int(OWNER_ID)


async def is_allowed(user_id: int) -> bool:
    if is_owner(user_id):
        return True
    async with Session() as s:
        return bool(await s.scalar(select(AllowedUser.id).where(AllowedUser.user_id == int(user_id))))


async def upsert_user_from_tg(tg_user) -> User:
    uid = int(tg_user.id)
    full_name = safe_text(getattr(tg_user, "full_name", "") or "")
    username = safe_text(getattr(tg_user, "username", "") or "")

    async with Session() as s:
        u = await s.get(User, uid)
        if not u:
            u = User(user_id=uid, full_name=full_name, username=username, created_at=datetime.utcnow(), name="")
            s.add(u)
            await s.commit()
            await s.refresh(u)
            return u

        changed = False
        if full_name and u.full_name != full_name:
            u.full_name = full_name
            changed = True
        if username != u.username:
            u.username = username
            changed = True
        if changed:
            await s.commit()
            await s.refresh(u)
        return u


async def get_ui_ctx(state: FSMContext) -> dict:
    data = await state.get_data()
    cur_menu = data.get("cur_menu") or "main"
    return {"cur_menu": cur_menu}


async def set_menu(state: FSMContext, menu: str):
    await state.update_data(cur_menu=menu)


def main_menu_kb(is_admin: bool):
    kb = ReplyKeyboardBuilder()
    # Стабильные 2 колонки: короткие тексты и adjust(2) без пересборки сетки
    kb.button(text="🟢 Приход")
    kb.button(text="🔴 Продажа")
    kb.button(text="📦 Остатки")
    kb.button(text="💰 Деньги")
    kb.button(text="📊 Отчеты")
    kb.button(text="❌ Отмена")
    kb.adjust(2, 2, 2)
    return kb.as_markup(resize_keyboard=True)

def reports_menu_kb(is_admin: bool):
    kb = ReplyKeyboardBuilder()
    # Стабильные 2 колонки (где возможно)
    kb.button(text="📄 Приходы")
    kb.button(text="📄 Продажи")
    kb.button(text="📥 Выгрузка")
    kb.button(text="📋 Должники")
    kb.button(text="➕ Должник")
    kb.button(text="🏬 Склады")
    kb.button(text="🧺 Товары")
    kb.button(text="🏦 Банки")
    if is_admin:
        kb.button(text="👥 Users")
    kb.button(text="⬅️ Назад")

    # Если есть Users — 5 рядов по 2 + Назад отдельной строкой
    if is_admin:
        kb.adjust(2, 2, 2, 2, 1)
    else:
        kb.adjust(2, 2, 2, 2)
    return kb.as_markup(resize_keyboard=True)

def warehouses_menu_kb():
    kb = ReplyKeyboardBuilder()
    kb.button(text="➕ Добавить склад")
    kb.button(text="📃 Список складов")
    kb.button(text="🗑 Удалить склад")
    kb.button(text="⬅️ Назад в отчеты")
    kb.adjust(2, 2)
    return kb.as_markup(resize_keyboard=True)

def products_menu_kb():
    kb = ReplyKeyboardBuilder()
    kb.button(text="➕ Добавить товар")
    kb.button(text="📃 Список товаров")
    kb.button(text="🗑 Удалить товар")
    kb.button(text="⬅️ Назад в отчеты")
    kb.adjust(2, 2)
    return kb.as_markup(resize_keyboard=True)

def banks_menu_kb():
    kb = ReplyKeyboardBuilder()
    kb.button(text="➕ Добавить банк")
    kb.button(text="📃 Список банков")
    kb.button(text="🗑 Удалить банк")
    kb.button(text="⬅️ Назад в отчеты")
    kb.adjust(2, 2)
    return kb.as_markup(resize_keyboard=True)

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


def cal_open_kb(scope: str, year: int, month: int):
    first = date(year, month, 1)
    start_weekday = first.weekday()
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

    for text_, cb in cells:
        ikb.button(text=text_, callback_data=cb)

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


class AuthWizard(StatesGroup):
    ask_name = State()


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

def income_state_name(st):
    m = {
        IncomeWizard.doc_date: "doc_date",
        IncomeWizard.supplier_name: "supplier_name",
        IncomeWizard.supplier_phone: "supplier_phone",
        IncomeWizard.warehouse: "warehouse",
        IncomeWizard.product: "product",
        IncomeWizard.qty: "qty",
        IncomeWizard.price: "price",
        IncomeWizard.delivery: "delivery",
        IncomeWizard.add_money: "add_money",
        IncomeWizard.pay_method: "pay_method",
        IncomeWizard.account_type: "account_type",
        IncomeWizard.bank_pick: "bank_pick",
        IncomeWizard.confirm: "confirm",
    }
    return m.get(st, "unknown")


class WarehousesAdmin(StatesGroup):
    adding = State()
    deleting = State()


class ProductsAdmin(StatesGroup):
    adding = State()
    deleting = State()


class BanksAdmin(StatesGroup):
    adding = State()
    deleting = State()


router = Router()

BTN = {
    "cancel": "❌ Отмена",
    "main_reports": "📊 Отчеты",
    "main_stocks": "📦 Остатки",
    "main_money": "💰 Деньги",
    "main_income": "🟢 Приход",
    "main_sale": "🔴 Продажа",
    "rep_incomes": "📄 Приходы",
    "rep_sales": "📄 Продажи",
    "rep_export": "📥 Выгрузка",
    "rep_debtors": "📋 Должники",
    "rep_deb_add": "➕ Должник",
    "rep_wh": "🏬 Склады",
    "rep_pr": "🧺 Товары",
    "rep_bk": "🏦 Банки",
    "rep_users": "👥 Users",
    "back": "⬅️ Назад",
    "back_reports": "⬅️ Назад в отчеты",
    "wh_add": "➕ Добавить склад",
    "wh_list": "📃 Список складов",
    "wh_del": "🗑 Удалить склад",
    "pr_add": "➕ Добавить товар",
    "pr_list": "📃 Список товаров",
    "pr_del": "🗑 Удалить товар",
    "bk_add": "➕ Добавить банк",
    "bk_list": "📃 Список банков",
    "bk_del": "🗑 Удалить банк",
}


MAIN_BTNS = {
    BTN["main_income"], BTN["main_sale"], BTN["main_stocks"], BTN["main_money"], BTN["main_reports"], BTN["cancel"]
}

REPORTS_BTNS = {
    BTN["rep_incomes"], BTN["rep_sales"], BTN["rep_export"], BTN["rep_debtors"], BTN["rep_deb_add"],
    BTN["rep_wh"], BTN["rep_pr"], BTN["rep_bk"], BTN["rep_users"], BTN["back"]
}

WH_BTNS = {BTN["wh_add"], BTN["wh_list"], BTN["wh_del"], BTN["back_reports"]}
PR_BTNS = {BTN["pr_add"], BTN["pr_list"], BTN["pr_del"], BTN["back_reports"]}
BK_BTNS = {BTN["bk_add"], BTN["bk_list"], BTN["bk_del"], BTN["back_reports"]}


async def allow_user(user_id: int, added_by: int, note: str = "approved"):
    async with Session() as s:
        exists = await s.scalar(select(AllowedUser).where(AllowedUser.user_id == int(user_id)))
        if not exists:
            s.add(AllowedUser(user_id=int(user_id), created_at=datetime.utcnow(), added_by=int(added_by), note=note))
            await s.commit()


async def deny_user(user_id: int):
    async with Session() as s:
        await s.execute(delete(AllowedUser).where(AllowedUser.user_id == int(user_id)))
        await s.commit()


async def rm_user(user_id: int):
    async with Session() as s:
        await s.execute(delete(User).where(User.user_id == int(user_id)))
        await s.commit()


USERS_PAGE_SIZE = 10


def users_pager_kb(page: int, has_prev: bool, has_next: bool):
    ikb = InlineKeyboardBuilder()
    if has_prev:
        ikb.button(text="⬅️ Назад", callback_data=f"users:page:{page-1}")
    if has_next:
        ikb.button(text="➡️ Далее", callback_data=f"users:page:{page+1}")
    ikb.button(text="🔄 Обновить", callback_data=f"users:page:{page}")
    ikb.adjust(2, 1)
    return ikb.as_markup()


def users_list_kb(page: int, users: list[User], allowed_ids: set[int], has_prev: bool, has_next: bool):
    ikb = InlineKeyboardBuilder()
    for u in users:
        ikb.button(text=f"⚙️ {u.user_id}", callback_data=f"users:manage:{u.user_id}:{page}")
    if has_prev:
        ikb.button(text="⬅️ Назад", callback_data=f"users:page:{page-1}")
    if has_next:
        ikb.button(text="➡️ Далее", callback_data=f"users:page:{page+1}")
    ikb.button(text="🔄 Обновить", callback_data=f"users:page:{page}")
    ikb.adjust(2, 2, 1)
    return ikb.as_markup()


def user_manage_kb(uid: int, allowed: bool, back_page: int):
    ikb = InlineKeyboardBuilder()
    if allowed:
        ikb.button(text="❌ Deny", callback_data=f"users:deny:{uid}:{back_page}")
    else:
        ikb.button(text="✅ Allow", callback_data=f"users:allow:{uid}:{back_page}")
    ikb.button(text="🗑 Удалить", callback_data=f"users:rm:{uid}:{back_page}")
    ikb.button(text="⬅️ К списку", callback_data=f"users:page:{back_page}")
    ikb.adjust(2, 1, 1)
    return ikb.as_markup()


async def render_users_page(page: int) -> tuple[str, list[User], set[int], bool, bool, int, int]:
    async with Session() as s:
        total = int(await s.scalar(select(func.count()).select_from(User)) or 0)
        if total <= 0:
            return "👥 <b>Users</b>: пусто.", [], set(), False, False, 0, 0

        page = max(int(page), 0)
        max_page = max(0, (total - 1) // USERS_PAGE_SIZE)
        real_page = min(page, max_page)

        users = (await s.execute(
            select(User)
            .order_by(User.created_at.desc())
            .offset(real_page * USERS_PAGE_SIZE)
            .limit(USERS_PAGE_SIZE)
        )).scalars().all()

        allowed_ids = set((await s.execute(select(AllowedUser.user_id))).scalars().all())

    has_prev = real_page > 0
    has_next = real_page < max_page

    lines = [f"👥 <b>Users</b> (всего: <b>{total}</b>), стр <b>{real_page+1}</b> из <b>{max_page+1}</b>:\n"]
    for u in users:
        st = "✅" if (u.user_id in allowed_ids or is_owner(u.user_id)) else "⛔"
        uname = f"@{h(u.username)}" if u.username else "-"
        nm = h(u.name) if u.name else "-"
        fn = h(u.full_name) if u.full_name else "-"
        lines.append(f"\n{st} <b>{u.user_id}</b> | {uname} | имя: <b>{nm}</b>\n└ {fn}")

    return "\n".join(lines), users, allowed_ids, has_prev, has_next, real_page, total
async def render_user_card(uid: int) -> tuple[str, bool]:
    async with Session() as s:
        u = await s.get(User, int(uid))
        if not u:
            return "User не найден.", False
        allowed = bool(await s.scalar(select(AllowedUser.id).where(AllowedUser.user_id == int(uid))))

    uname = f"@{h(u.username)}" if u.username else "-"
    nm = h(u.name) if u.name else "-"
    fn = h(u.full_name) if u.full_name else "-"
    status = "✅ ДОСТУП ЕСТЬ" if (allowed or is_owner(u.user_id)) else "⛔ ДОСТУП НЕТ"
    txt = (
        f"👤 <b>User {u.user_id}</b>\n"
        f"Статус: <b>{status}</b>\n"
        f"Username: <b>{uname}</b>\n"
        f"Имя в системе: <b>{nm}</b>\n"
        f"Имя TG: {fn}\n"
        f"Создан: <code>{h(str(u.created_at))}</code>"
    )
    return txt, (allowed or is_owner(u.user_id))

async def reply_in_menu(message: Message, state: FSMContext, text_: str, kb=None, parse_mode=None):
    ui = await get_ui_ctx(state)
    is_admin = is_owner(message.from_user.id)
    if kb is None:
        kb = reports_menu_kb(is_admin) if ui["cur_menu"] == "reports" else main_menu_kb(is_admin)
    await message.answer(text_, reply_markup=kb, parse_mode=parse_mode)


async def get_stock_row(session, warehouse_id: int, product_id: int) -> Stock:
    row = await session.scalar(
        select(Stock).where(
            Stock.warehouse_id == warehouse_id,
            Stock.product_id == product_id
        )
    )
    if row:
        return row



async def recalc_stocks(session):
    # Recompute `stocks` table from `stock_movements` (cache/live view).
    await session.execute(delete(Stock))
    await session.flush()

    rows = (await session.execute(
        select(StockMovement.warehouse_id, StockMovement.product_id, func.coalesce(func.sum(StockMovement.qty_kg), 0))
        .group_by(StockMovement.warehouse_id, StockMovement.product_id)
    )).all()

    for wid, pid, qty in rows:
        q = Decimal(qty or 0)
        session.add(Stock(warehouse_id=wid, product_id=pid, qty_kg=q))
    await session.flush()


async def recalc_money_ledger(session):
    # Recompute `money_ledger` from `money_movements` (keeps existing UI compatible).
    await session.execute(delete(MoneyLedger))
    await session.flush()

    mvs = (await session.execute(select(MoneyMovement).order_by(MoneyMovement.id))).scalars().all()
    for mv in mvs:
        amt = Decimal(mv.amount or 0)
        direction = "in" if amt >= 0 else "out"
        session.add(MoneyLedger(
            entry_date=mv.entry_date,
            direction=direction,
            method=mv.method or ("cash" if mv.account_type == "cash" else "noncash"),
            account_type=mv.account_type,
            bank_id=mv.bank_id,
            amount=abs(amt),
            note=mv.note or f"{mv.doc_type}#{mv.doc_id}"
        ))
    await session.flush()



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



async def show_stocks_table(message: Message, state: FSMContext):
    async with Session() as s:
        rows = (await s.execute(
            select(
                Warehouse.name,
                Product.name,
                func.coalesce(func.sum(StockMovement.qty_kg), 0).label("qty")
            )
            .join(Warehouse, Warehouse.id == StockMovement.warehouse_id)
            .join(Product, Product.id == StockMovement.product_id)
            .group_by(Warehouse.name, Product.name)
            .order_by(Warehouse.name, Product.name)
        )).all()

    if not rows:
        return await reply_in_menu(message, state, "Остатков пока нет.")

    data = [(wh, pr, fmt_kg(Decimal(qty))) for (wh, pr, qty) in rows if Decimal(qty) != 0]
    if not data:
        return await reply_in_menu(message, state, "Пока везде 0.")

    w1 = max(len("Склад"), max(len(x[0]) for x in data))
    w2 = max(len("Товар"), max(len(x[1]) for x in data))
    w3 = max(len("Остаток(кг)"), max(len(x[2]) for x in data))

    lines = []
    lines.append(f"{'Склад'.ljust(w1)} | {'Товар'.ljust(w2)} | {'Остаток(кг)'.rjust(w3)}")
    lines.append(f"{'-' * w1}-+-{'-' * w2}-+-{'-' * w3}")
    for wh, pr, q in data:
        lines.append(f"{wh.ljust(w1)} | {pr.ljust(w2)} | {q.rjust(w3)}")

    txt = "📦 <b>Остатки</b>:\n<pre>" + "\n".join(lines) + "</pre>"
    await message.answer(txt, parse_mode=ParseMode.HTML)
    await reply_in_menu(message, state, "Готово ✅")



async def show_money(message: Message, state: FSMContext):
    async with Session() as s:
        rows = (await s.execute(
            select(
                MoneyMovement.account_type,
                MoneyMovement.bank_id,
                func.coalesce(func.sum(MoneyMovement.amount), 0).label("bal")
            )
            .group_by(MoneyMovement.account_type, MoneyMovement.bank_id)
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

    lines = [
        "💰 <b>Деньги (балансы)</b>",
        f"\n💵 <b>Наличные:</b> <b>{h(fmt_money(cash_balance))}</b>",
        "\n🏦 <b>Банки:</b>",
    ]

    if bank_lines:
        for name, bal in bank_lines:
            lines.append(f"• {h(name)}: <b>{h(fmt_money(bal))}</b>")
    else:
        lines.append("• (пусто)")

    lines.append("\n👤 <b>Счёт ИП:</b>")
    if ip_lines:
        for name, bal in ip_lines:
            lines.append(f"• {h(name)}: <b>{h(fmt_money(bal))}</b>")
    else:
        lines.append("• (пусто)")

    await message.answer("\n".join(lines), parse_mode=ParseMode.HTML)
    await reply_in_menu(message, state, "Готово ✅")


EXPORT_PAGE_SIZE = 20


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


async def export_menu(message: Message, state: FSMContext):
    await set_menu(state, "reports")
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
    start = max(0, page) * EXPORT_PAGE_SIZE
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
    start = max(0, page) * EXPORT_PAGE_SIZE
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
    start = max(0, page) * EXPORT_PAGE_SIZE
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
async def export_router(cq: CallbackQuery, state: FSMContext):
    parts = (cq.data or "").split(":")
    if len(parts) < 2:
        return await cq.answer()

    action = parts[1]

    if action == "menu":
        await cq.message.answer("📥 Выгрузка таблиц (в чате):", reply_markup=export_menu_kb())
        return await cq.answer()

    if action == "back":
        await set_menu(state, "reports")
        await cq.message.answer("Отчеты:", reply_markup=reports_menu_kb(is_owner(cq.from_user.id)))
        return await cq.answer()

    if len(parts) != 3:
        return await cq.answer("Ошибка кнопки", show_alert=True)

    kind = parts[1]
    page_s = parts[2]
    if not page_s.lstrip("-").isdigit():
        return await cq.answer("Ошибка страницы", show_alert=True)
    page = int(page_s)
    if page < 0:
        page = 0

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


def sales_actions_kb(sale_id: int, paid: bool):
    ikb = InlineKeyboardBuilder()
    if not paid:
        ikb.button(text="✅ Оплачено", callback_data=f"sale_paid_id:{sale_id}")
    ikb.button(text="🗑 Удалить", callback_data=f"sale_del:{sale_id}")
    ikb.adjust(2)
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
        async with s.begin():
            sale = await s.get(Sale, sale_id)
            if not sale:
                return await cq.answer("Не найдено", show_alert=True)

            await s.execute(delete(StockMovement).where(StockMovement.doc_type == "sale", StockMovement.doc_id == sale_id))
            await s.execute(delete(MoneyMovement).where(MoneyMovement.doc_type == "sale", MoneyMovement.doc_id == sale_id))
            await s.execute(delete(Sale).where(Sale.id == sale_id))

            await recalc_stocks(s)
            await recalc_money_ledger(s)

    await cq.message.answer(f"🗑 Продажа <b>#{sale_id}</b> удалена (с откатом движений).", parse_mode=ParseMode.HTML)
    await cq.answer()



@router.message(F.text.regexp(r"(?i)^продажа\s+#\d+$"))
async def sale_by_id(message: Message, state: FSMContext):
    sale_id = int(message.text.split("#")[1])
    async with Session() as s:
        r = await s.scalar(
            select(Sale)
            .options(selectinload(Sale.warehouse), selectinload(Sale.product), selectinload(Sale.bank))
            .where(Sale.id == sale_id)
        )
    if not r:
        return await reply_in_menu(message, state, "Не найдено.")

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
        f"Куда: *{where_txt}*"
    )
    await message.answer(txt, parse_mode=ParseMode.HTML, reply_markup=sales_actions_kb(r.id, r.is_paid))


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
        async with s.begin():
            inc = await s.get(Income, income_id)
            if not inc:
                return await cq.answer("Не найдено", show_alert=True)

            await s.execute(delete(StockMovement).where(StockMovement.doc_type == "income", StockMovement.doc_id == income_id))
            await s.execute(delete(MoneyMovement).where(MoneyMovement.doc_type == "income", MoneyMovement.doc_id == income_id))
            await s.execute(delete(Income).where(Income.id == income_id))

            await recalc_stocks(s)
            await recalc_money_ledger(s)

    await cq.message.answer(f"🗑 Приход <b>#{income_id}</b> удалён (с откатом движений).", parse_mode=ParseMode.HTML)
    await cq.answer()



@router.message(F.text.regexp(r"(?i)^приход\s+#\d+$"))
async def inc_by_id(message: Message, state: FSMContext):
    inc_id = int(message.text.split("#")[1])
    async with Session() as s:
        r = await s.scalar(
            select(Income)
            .options(selectinload(Income.warehouse), selectinload(Income.product), selectinload(Income.bank))
            .where(Income.id == inc_id)
        )
    if not r:
        return await reply_in_menu(message, state, "Не найдено.")

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
        f"Куда: *{where_txt}*"
    )
    await message.answer(txt, parse_mode=ParseMode.HTML, reply_markup=income_actions_kb(r.id))


def debtor_actions_kb(debtor_id: int, paid: bool):
    ikb = InlineKeyboardBuilder()
    if not paid:
        ikb.button(text="✅ Оплачено", callback_data=f"deb_paid:{debtor_id}")
    ikb.button(text="🗑 Удалить", callback_data=f"deb_del:{debtor_id}")
    ikb.adjust(2)
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


async def list_debtors(message: Message, state: FSMContext):
    async with Session() as s:
        rows = (await s.execute(select(Debtor).order_by(Debtor.id.desc()).limit(50))).scalars().all()

    if not rows:
        return await reply_in_menu(message, state, "Должников нет ✅")

    table_rows = []
    for r in rows:
        status = "PAID" if r.is_paid else "DEBT"
        qty = fmt_kg(Decimal(r.qty_kg or 0))
        total = fmt_money(Decimal(r.total_amount or 0))
        who = safe_text(r.customer_name) or "-"
        table_rows.append([f"#{r.id}", str(r.doc_date), who, qty, total, status])

    txt = "📋 <b>Должники (последние 50)</b>\n" + render_pre_table(
        headers=["ID", "Дата", "Клиент", "кг", "сумма", "стат"],
        rows=table_rows
    )
    await message.answer(txt, parse_mode=ParseMode.HTML)

    await reply_in_menu(
        message,
        state,
        "Чтобы управлять: напиши <code>должник #ID</code> (например <code>должник #3</code>)",
        parse_mode=ParseMode.HTML
    )

@router.message(F.text.regexp(r"(?i)^должник\s+#\d+$"))
async def debtor_by_id(message: Message, state: FSMContext):
    d_id = int(message.text.split("#")[1])
    async with Session() as s:
        r = await s.get(Debtor, d_id)
    if not r:
        return await reply_in_menu(message, state, "Не найдено.")

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
        f"Статус: *{status}*"
    )
    await message.answer(txt, parse_mode=ParseMode.HTML, reply_markup=debtor_actions_kb(r.id, r.is_paid))


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await set_menu(state, "main")
    uid = message.from_user.id

    u = await upsert_user_from_tg(message.from_user)

    if await is_allowed(uid):
        if not safe_text(u.name):
            await state.set_state(AuthWizard.ask_name)
            return await message.answer("👋 Привет! Введи, пожалуйста, своё имя (как тебя записывать в системе):")
        return await message.answer("Привет! Выбери действие:", reply_markup=main_menu_kb(is_owner(uid)))

    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Разрешить", callback_data=f"acc_req:allow:{uid}")
    kb.button(text="❌ Запретить", callback_data=f"acc_req:deny:{uid}")
    kb.adjust(2)

    await message.answer("⛔ У вас нет доступа. Запрос отправлен владельцу.")
    try:
        username = f"@{message.from_user.username}" if message.from_user.username else "(нет)"
        text_ = (
            "🔐 Запрос доступа к боту\n"
            f"ID: {uid}\n"
            f"Имя TG: {safe_text(message.from_user.full_name)}\n"
            f"Юзернейм: {username}"
        )
        await message.bot.send_message(OWNER_ID, text_, reply_markup=kb.as_markup())
    except Exception:
        pass


@router.message(AuthWizard.ask_name)
async def auth_ask_name(message: Message, state: FSMContext):
    name = safe_text(message.text)
    if not name or len(name) < 2:
        return await message.answer("Пожалуйста, введи имя (минимум 2 символа).")

    uid = int(message.from_user.id)

    async with Session() as s:
        u = await s.get(User, uid)
        if not u:
            u = User(
                user_id=uid,
                full_name=safe_text(message.from_user.full_name),
                username=safe_text(message.from_user.username or ""),
                created_at=datetime.utcnow(),
                name=name
            )
            s.add(u)
        else:
            u.name = name
        await s.commit()

    await state.clear()
    await set_menu(state, "main")

    if await is_allowed(uid):
        return await message.answer(f"✅ Отлично, {name}! Выбери действие:", reply_markup=main_menu_kb(is_owner(uid)))

    return await message.answer("✅ Имя сохранено. Доступ к боту выдаёт владелец. Напиши /start после одобрения.")


@router.callback_query(F.data.startswith("acc_req:"))
async def cb_access_req(cq: CallbackQuery):
    if not is_owner(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)

    parts = (cq.data or "").split(":")
    if len(parts) != 3:
        return await cq.answer("Ошибка", show_alert=True)
    _, action, uid_s = parts
    if not uid_s.isdigit():
        return await cq.answer("Ошибка", show_alert=True)
    uid = int(uid_s)

    if action == "allow":
        await allow_user(uid, OWNER_ID, note="approved")
        await cq.message.edit_text(f"✅ Доступ разрешён пользователю {uid}")
        try:
            await cq.bot.send_message(uid, "✅ Вам выдан доступ к боту. Напишите /start")
        except Exception:
            pass
        return await cq.answer("Ок")

    await cq.message.edit_text(f"❌ Доступ отклонён пользователю {uid}")
    try:
        await cq.bot.send_message(uid, "⛔ Доступ к боту не выдан.")
    except Exception:
        pass
    return await cq.answer("Ок")


@router.message(Command("users"))
async def cmd_users(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return await message.answer("Нет доступа.")
    await set_menu(state, "reports")
    page = 0
    txt, users, allowed_ids, has_prev, has_next, real_page, _total = await render_users_page(page)
    kb = users_list_kb(real_page, users, allowed_ids, has_prev, has_next) if users else users_pager_kb(real_page, has_prev, has_next)
    await message.answer(txt, parse_mode=ParseMode.HTML, reply_markup=kb)


@router.message(Command("allow"))
async def cmd_allow(message: Message):
    if not is_owner(message.from_user.id):
        return await message.answer("Нет доступа.")
    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await message.answer("Использование: /allow <id>")
    uid = int(parts[1])
    await allow_user(uid, OWNER_ID, note="manual allow")
    await message.answer(f"✅ Разрешил доступ пользователю {uid}")


@router.message(Command("deny"))
async def cmd_deny(message: Message):
    if not is_owner(message.from_user.id):
        return await message.answer("Нет доступа.")
    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await message.answer("Использование: /deny <id>")
    uid = int(parts[1])
    if is_owner(uid):
        return await message.answer("OWNER нельзя запретить 🙂")
    await deny_user(uid)
    await message.answer(f"❌ Запретил доступ пользователю {uid}")


@router.message(Command("rmuser"))
async def cmd_rmuser(message: Message):
    if not is_owner(message.from_user.id):
        return await message.answer("Нет доступа.")
    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await message.answer("Использование: /rmuser <id>")
    uid = int(parts[1])
    if is_owner(uid):
        return await message.answer("OWNER нельзя удалять 🙂")
    await rm_user(uid)
    await deny_user(uid)
    await message.answer(f"🗑 Удалил user {uid} из users и убрал из allowed_users")


@router.callback_query(F.data.startswith("users:"))
async def users_inline_router(cq: CallbackQuery):
    if not is_owner(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)

    parts = (cq.data or "").split(":")
    if len(parts) < 3:
        return await cq.answer()

    action = parts[1]

    if action == "page":
        if not parts[2].lstrip("-").isdigit():
            return await cq.answer("Ошибка страницы", show_alert=True)
        page = int(parts[2])
        if page < 0:
            page = 0

        txt, users, allowed_ids, has_prev, has_next, real_page, _total = await render_users_page(page)
        if not users:
            await cq.message.edit_text(txt, parse_mode=ParseMode.HTML, reply_markup=None)
            return await cq.answer()

        kb = users_list_kb(real_page, users, allowed_ids, has_prev, has_next)
        await cq.message.edit_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await cq.answer()

    if action == "manage":
        if len(parts) != 4 or (not parts[2].isdigit()) or (not parts[3].isdigit()):
            return await cq.answer("Ошибка", show_alert=True)
        uid = int(parts[2])
        back_page = int(parts[3])

        card, allowed = await render_user_card(uid)
        await cq.message.edit_text(card, parse_mode=ParseMode.HTML, reply_markup=user_manage_kb(uid, allowed, back_page))
        return await cq.answer()

    if action in ("allow", "deny", "rm"):
        if len(parts) != 4 or (not parts[2].isdigit()) or (not parts[3].isdigit()):
            return await cq.answer("Ошибка", show_alert=True)
        uid = int(parts[2])
        back_page = int(parts[3])

        if action == "allow":
            await allow_user(uid, OWNER_ID, note="inline allow")
            try:
                await cq.bot.send_message(uid, "✅ Вам выдан доступ к боту. Напишите /start")
            except Exception:
                pass
        elif action == "deny":
            if is_owner(uid):
                return await cq.answer("OWNER нельзя deny", show_alert=True)
            await deny_user(uid)
            try:
                await cq.bot.send_message(uid, "⛔ Доступ к боту отключён.")
            except Exception:
                pass
        elif action == "rm":
            if is_owner(uid):
                return await cq.answer("OWNER нельзя rm", show_alert=True)
            await rm_user(uid)
            await deny_user(uid)

        card, allowed = await render_user_card(uid)
        await cq.message.edit_text(card, parse_mode=ParseMode.HTML, reply_markup=user_manage_kb(uid, allowed, back_page))
        return await cq.answer("OK")

    return await cq.answer()


async def show_reports_menu(message: Message, state: FSMContext):
    await set_menu(state, "reports")
    await state.clear()
    await set_menu(state, "reports")
    await message.answer("Отчеты:", reply_markup=reports_menu_kb(is_owner(message.from_user.id)))


# --- Restored functions (income wizard + reports lists) ---

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
                             parse_mode=ParseMode.HTML,
                             reply_markup=yes_no_kb("inc_confirm"))
        return

async def start_income(message: Message, state: FSMContext, is_admin: bool):
    await state.clear()
    await set_menu(state, "main")
    await income_go_to(state, "doc_date")
    await income_prompt(message, state)

async def list_sales(message: Message, state: FSMContext):
    async with Session() as s:
        rows = (await s.execute(
            select(Sale)
            .options(selectinload(Sale.warehouse), selectinload(Sale.product), selectinload(Sale.bank))
            .order_by(Sale.id.desc())
            .limit(30)
        )).scalars().all()

    if not rows:
        return await reply_in_menu(message, state, "Продаж пока нет.")

    data = []
    for r in rows:
        wh = r.warehouse.name if r.warehouse else "-"
        pr = r.product.name if r.product else "-"
        paid = "ДА" if r.is_paid else "НЕТ"
        data.append((
            str(r.id),
            r.doc_date.strftime("%d.%m"),
            (r.customer_name or "-")[:14],
            wh[:10],
            pr[:14],
            fmt_kg(Decimal(r.qty_kg)),
            fmt_money(Decimal(r.total_amount)),
            paid
        ))

    headers = ("ID", "Дата", "Клиент", "Склад", "Товар", "кг", "Сумма", "Опл")
    widths = [len(h) for h in headers]
    for row in data:
        for i, v in enumerate(row):
            widths[i] = max(widths[i], len(v))

    lines = []
    lines.append(" | ".join(headers[i].ljust(widths[i]) for i in range(len(headers))))
    lines.append("-+-".join("-" * widths[i] for i in range(len(headers))))
    for row in data:
        lines.append(" | ".join(row[i].ljust(widths[i]) for i in range(len(headers))))

    txt = "📄 <b>Последние продажи</b> (30):\n<pre>" + "\n".join(lines) + "</pre>"
    await message.answer(txt, parse_mode=ParseMode.HTML)



async def list_incomes(message: Message, state: FSMContext):
    async with Session() as s:
        rows = (await s.execute(
            select(Income)
            .options(selectinload(Income.warehouse), selectinload(Income.product), selectinload(Income.bank))
            .order_by(Income.id.desc())
            .limit(30)
        )).scalars().all()

    if not rows:
        return await reply_in_menu(message, state, "Приходов пока нет.")

    data = []
    for r in rows:
        wh = r.warehouse.name if r.warehouse else "-"
        pr = r.product.name if r.product else "-"
        paid = "ДА" if r.add_money_entry else "НЕТ"
        data.append((
            str(r.id),
            r.doc_date.strftime("%d.%m"),
            (r.supplier_name or "-")[:14],
            wh[:10],
            pr[:14],
            fmt_kg(Decimal(r.qty_kg)),
            fmt_money(Decimal(r.total_amount)),
            paid
        ))

    headers = ("ID", "Дата", "Поставщик", "Склад", "Товар", "кг", "Сумма", "Опл")
    widths = [len(h) for h in headers]
    for row in data:
        for i, v in enumerate(row):
            widths[i] = max(widths[i], len(v))

    lines = []
    lines.append(" | ".join(headers[i].ljust(widths[i]) for i in range(len(headers))))
    lines.append("-+-".join("-" * widths[i] for i in range(len(headers))))
    for row in data:
        lines.append(" | ".join(row[i].ljust(widths[i]) for i in range(len(headers))))

    txt = "📄 <b>Последние приходы</b> (30):\n<pre>" + "\n".join(lines) + "</pre>"
    await message.answer(txt, parse_mode=ParseMode.HTML)




async def start_debtor(message: Message, state: FSMContext):
    await state.clear()
    await set_menu(state, "reports")
    await state.set_state(DebtorWizard.doc_date)
    await message.answer("Дата (для должника):", reply_markup=choose_date_kb("deb"))


@router.message(F.text)
async def menu_router(message: Message, state: FSMContext):
    uid = message.from_user.id
    await upsert_user_from_tg(message.from_user)

    if not (await is_allowed(uid)):
        return await message.answer("Нет доступа. Напишите /start для запроса доступа.")

    text_ = message.text
    is_admin = is_owner(uid)

    ui = await get_ui_ctx(state)

    if text_ == BTN["cancel"]:
        await state.clear()
        await set_menu(state, "main")
        return await message.answer("Ок, отменил ✅", reply_markup=main_menu_kb(is_admin))

    if text_ == BTN["main_reports"]:
        return await show_reports_menu(message, state)

    if text_ == BTN["back"]:
        await state.clear()
        await set_menu(state, "main")
        return await message.answer("Меню:", reply_markup=main_menu_kb(is_admin))

    if text_ == BTN["back_reports"]:
        await state.clear()
        await set_menu(state, "reports")
        return await message.answer("Отчеты:", reply_markup=reports_menu_kb(is_admin))

    if text_ == BTN["main_stocks"]:
        await state.clear()
        if ui["cur_menu"] != "reports":
            await set_menu(state, "main")
        return await show_stocks_table(message, state)

    if text_ == BTN["main_money"]:
        await state.clear()
        if ui["cur_menu"] != "reports":
            await set_menu(state, "main")
        return await show_money(message, state)

    if text_ == BTN["main_income"]:
        await set_menu(state, "main")
        await state.clear()
        return await start_income(message, state, is_admin)

    if text_ == BTN["main_sale"]:
        await set_menu(state, "main")
        await state.clear()
        return await start_sale(message, state, is_admin)

    await set_menu(state, "reports")

    if text_ == BTN["rep_users"]:
        if not is_admin:
            return await message.answer("Нет доступа.", reply_markup=reports_menu_kb(is_admin))
        page = 0
        txt, users, allowed_ids, has_prev, has_next, real_page, _total = await render_users_page(page)
        kb = users_list_kb(real_page, users, allowed_ids, has_prev, has_next) if users else users_pager_kb(real_page, has_prev, has_next)
        return await message.answer(txt, parse_mode=ParseMode.HTML, reply_markup=kb)

    if text_ == BTN["rep_sales"]:
        await state.clear()
        await set_menu(state, "reports")
        await list_sales(message, state)
        return await message.answer("Отчеты:", reply_markup=reports_menu_kb(is_admin))

    if text_ == BTN["rep_incomes"]:
        await state.clear()
        await set_menu(state, "reports")
        await list_incomes(message, state)
        return await message.answer("Отчеты:", reply_markup=reports_menu_kb(is_admin))

    if text_ == BTN["rep_export"]:
        await state.clear()
        await set_menu(state, "reports")
        await export_menu(message, state)
        return

    if text_ == BTN["rep_debtors"]:
        await state.clear()
        await set_menu(state, "reports")
        await list_debtors(message, state)
        return await message.answer("Отчеты:", reply_markup=reports_menu_kb(is_admin))

    if text_ == BTN["rep_deb_add"]:
        await set_menu(state, "reports")
        await state.clear()
        return await start_debtor(message, state)

    if text_ == BTN["rep_wh"]:
        await state.clear()
        await set_menu(state, "reports")
        return await message.answer("Управление складами:", reply_markup=warehouses_menu_kb())

    if text_ == BTN["rep_pr"]:
        await state.clear()
        await set_menu(state, "reports")
        return await message.answer("Управление товарами:", reply_markup=products_menu_kb())

    if text_ == BTN["rep_bk"]:
        await state.clear()
        await set_menu(state, "reports")
        return await message.answer("Управление банками:", reply_markup=banks_menu_kb())

    if text_ == BTN["wh_add"]:
        await state.clear()
        await set_menu(state, "reports")
        await state.set_state(WarehousesAdmin.adding)
        return await message.answer("Напиши название склада:", reply_markup=warehouses_menu_kb())

    if text_ == BTN["wh_list"]:
        await state.clear()
        await set_menu(state, "reports")
        return await list_warehouses(message)

    if text_ == BTN["wh_del"]:
        await state.clear()
        await set_menu(state, "reports")
        await state.set_state(WarehousesAdmin.deleting)
        return await message.answer("Напиши EXACT название склада для удаления:", reply_markup=warehouses_menu_kb())

    if text_ == BTN["pr_add"]:
        await state.clear()
        await set_menu(state, "reports")
        await state.set_state(ProductsAdmin.adding)
        return await message.answer("Напиши название товара:", reply_markup=products_menu_kb())

    if text_ == BTN["pr_list"]:
        await state.clear()
        await set_menu(state, "reports")
        return await list_products(message)

    if text_ == BTN["pr_del"]:
        await state.clear()
        await set_menu(state, "reports")
        await state.set_state(ProductsAdmin.deleting)
        return await message.answer("Напиши EXACT название товара для удаления:", reply_markup=products_menu_kb())

    if text_ == BTN["bk_add"]:
        await state.clear()
        await set_menu(state, "reports")
        await state.set_state(BanksAdmin.adding)
        return await message.answer("Напиши название банка:", reply_markup=banks_menu_kb())

    if text_ == BTN["bk_list"]:
        await state.clear()
        await set_menu(state, "reports")
        return await list_banks(message)

    if text_ == BTN["bk_del"]:
        await state.clear()
        await set_menu(state, "reports")
        await state.set_state(BanksAdmin.deleting)
        return await message.answer("Напиши EXACT название банка для удаления:", reply_markup=banks_menu_kb())


@router.message(WarehousesAdmin.adding)
async def wh_add(message: Message, state: FSMContext):
    name = safe_text(message.text)
    if not name:
        return await message.answer("Пусто. Напиши название склада.")
    async with Session() as s:
        exists = await s.scalar(select(Warehouse).where(Warehouse.name == name))
        if exists:
            await state.clear()
            await set_menu(state, "reports")
            return await message.answer("Такой склад уже есть ✅", reply_markup=warehouses_menu_kb())
        s.add(Warehouse(name=name))
        await s.commit()
    await state.clear()
    await set_menu(state, "reports")
    await message.answer(f"✅ Склад добавлен: {name}", reply_markup=warehouses_menu_kb())


@router.message(WarehousesAdmin.deleting)
async def wh_del(message: Message, state: FSMContext):
    name = safe_text(message.text)
    async with Session() as s:
        w = await s.scalar(select(Warehouse).where(Warehouse.name == name))
        if not w:
            await state.clear()
            await set_menu(state, "reports")
            return await message.answer("Склад не найден.", reply_markup=warehouses_menu_kb())

        cnt = await s.scalar(select(func.count()).select_from(Stock).where(Stock.warehouse_id == w.id))
        if int(cnt) > 0:
            await state.clear()
            await set_menu(state, "reports")
            return await message.answer("Нельзя удалить: есть остатки/движения по этому складу.", reply_markup=warehouses_menu_kb())

        await s.execute(delete(Warehouse).where(Warehouse.id == w.id))
        await s.commit()

    await state.clear()
    await set_menu(state, "reports")
    await message.answer(f"🗑 Склад удалён: {name}", reply_markup=warehouses_menu_kb())


async def list_warehouses(message: Message):
    async with Session() as s:
        rows = (await s.execute(select(Warehouse).order_by(Warehouse.name))).scalars().all()
    if not rows:
        return await message.answer("Складов пока нет. Добавь через ➕", reply_markup=warehouses_menu_kb())
    txt = "🏬 *Склады:*\n" + "\n".join([f"• {w.name}" for w in rows])
    await message.answer(txt, parse_mode=ParseMode.HTML, reply_markup=warehouses_menu_kb())


@router.message(ProductsAdmin.adding)
async def prod_add(message: Message, state: FSMContext):
    name = safe_text(message.text)
    if not name:
        return await message.answer("Пусто. Напиши название товара.")
    async with Session() as s:
        exists = await s.scalar(select(Product).where(Product.name == name))
        if exists:
            await state.clear()
            await set_menu(state, "reports")
            return await message.answer("Такой товар уже есть ✅", reply_markup=products_menu_kb())
        s.add(Product(name=name))
        await s.commit()
    await state.clear()
    await set_menu(state, "reports")
    await message.answer(f"✅ Товар добавлен: {name}", reply_markup=products_menu_kb())


@router.message(ProductsAdmin.deleting)
async def prod_del(message: Message, state: FSMContext):
    name = safe_text(message.text)
    async with Session() as s:
        p = await s.scalar(select(Product).where(Product.name == name))
        if not p:
            await state.clear()
            await set_menu(state, "reports")
            return await message.answer("Товар не найден.", reply_markup=products_menu_kb())

        cnt = await s.scalar(select(func.count()).select_from(Stock).where(Stock.product_id == p.id))
        if int(cnt) > 0:
            await state.clear()
            await set_menu(state, "reports")
            return await message.answer("Нельзя удалить: есть остатки/движения по этому товару.", reply_markup=products_menu_kb())

        await s.execute(delete(Product).where(Product.id == p.id))
        await s.commit()

    await state.clear()
    await set_menu(state, "reports")
    await message.answer(f"🗑 Товар удалён: {name}", reply_markup=products_menu_kb())


async def list_products(message: Message):
    async with Session() as s:
        rows = (await s.execute(select(Product).order_by(Product.name))).scalars().all()
    if not rows:
        return await message.answer("Товаров пока нет. Добавь через ➕", reply_markup=products_menu_kb())
    txt = "🧺 *Товары:*\n" + "\n".join([f"• {p.name}" for p in rows])
    await message.answer(txt, parse_mode=ParseMode.HTML, reply_markup=products_menu_kb())


@router.message(BanksAdmin.adding)
async def bank_add(message: Message, state: FSMContext):
    name = safe_text(message.text)
    if not name:
        return await message.answer("Пусто. Напиши название банка.")
    async with Session() as s:
        exists = await s.scalar(select(Bank).where(Bank.name == name))
        if exists:
            await state.clear()
            await set_menu(state, "reports")
            return await message.answer("Такой банк уже есть ✅", reply_markup=banks_menu_kb())
        s.add(Bank(name=name))
        await s.commit()
    await state.clear()
    await set_menu(state, "reports")
    await message.answer(f"✅ Банк добавлен: {name}", reply_markup=banks_menu_kb())


@router.message(BanksAdmin.deleting)
async def bank_del(message: Message, state: FSMContext):
    name = safe_text(message.text)
    async with Session() as s:
        b = await s.scalar(select(Bank).where(Bank.name == name))
        if not b:
            await state.clear()
            await set_menu(state, "reports")
            return await message.answer("Банк не найден.", reply_markup=banks_menu_kb())

        cnt = await s.scalar(select(func.count()).select_from(MoneyLedger).where(MoneyLedger.bank_id == b.id))
        if int(cnt) > 0:
            await state.clear()
            await set_menu(state, "reports")
            return await message.answer("Нельзя удалить: есть операции по этому банку.", reply_markup=banks_menu_kb())

        await s.execute(delete(Bank).where(Bank.id == b.id))
        await s.commit()

    await state.clear()
    await set_menu(state, "reports")
    await message.answer(f"🗑 Банк удалён: {name}", reply_markup=banks_menu_kb())


async def list_banks(message: Message):
    async with Session() as s:
        rows = (await s.execute(select(Bank).order_by(Bank.name))).scalars().all()
    if not rows:
        return await message.answer("Банков пока нет. Добавь через ➕", reply_markup=banks_menu_kb())
    txt = "🏦 *Банки:*\n" + "\n".join([f"• {b.name}" for b in rows])
    await message.answer(txt, parse_mode=ParseMode.HTML, reply_markup=banks_menu_kb())


SALE_FLOW = [
    "doc_date", "customer_name", "customer_phone", "warehouse_id", "product_id",
    "qty", "price", "delivery", "paid_status", "pay_method", "account_type", "bank_pick", "confirm"
]

INCOME_FLOW = [
    "doc_date", "supplier_name", "supplier_phone", "warehouse_id", "product_id",
    "qty", "price", "delivery", "add_money", "pay_method", "account_type", "bank_pick", "confirm"
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
                             parse_mode=ParseMode.HTML,
                             reply_markup=yes_no_kb("sale_confirm"))
        return


async def start_sale(message: Message, state: FSMContext, is_admin: bool):
    await state.clear()
    await set_menu(state, "main")
    await sale_go_to(state, "doc_date")
    await sale_prompt(message, state)


@router.callback_query(F.data.startswith("cal:sale:"))
async def cal_sale_handler(cq: CallbackQuery, state: FSMContext):
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
            await set_menu(state, "main")
            await cq.message.answer("Отменено ✅", reply_markup=main_menu_kb(is_owner(cq.from_user.id)))
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
        await set_menu(state, "main")
        await cq.message.answer("Отменено ✅", reply_markup=main_menu_kb(is_owner(cq.from_user.id)))
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

    is_paid_ = bool(data.get("is_paid"))
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
        async with s.begin():
            w = await s.get(Warehouse, warehouse_id)
            p = await s.get(Product, product_id)
            if not w or not p:
                raise RuntimeError("warehouse/product not found")

            if account_type in ("bank", "ip"):
                b = await s.get(Bank, bank_id)
                if not b:
                    await cq.answer("Банк не найден", show_alert=True)
                    return

            # Check available stock from movements
            cur_qty = await s.scalar(
                select(func.coalesce(func.sum(StockMovement.qty_kg), 0))
                .where(StockMovement.warehouse_id == w.id, StockMovement.product_id == p.id)
            )
            cur_qty = Decimal(cur_qty or 0)
            if cur_qty < qty:
                await state.clear()
                await set_menu(state, "main")
                await cq.message.answer(
                    f"❗ Недостаточно товара.\nЕсть: {h(fmt_kg(cur_qty))} кг, нужно: {h(fmt_kg(qty))} кг",
                    reply_markup=main_menu_kb(is_owner(cq.from_user.id)),
                    parse_mode=ParseMode.HTML
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
                is_paid=is_paid_,
                payment_method=payment_method if is_paid_ else "",
                account_type=account_type if is_paid_ else "cash",
                bank_id=bank_id if (is_paid_ and account_type in ("bank", "ip")) else None
            )
            s.add(sale)
            await s.flush()

            # Stock movement for sale (negative)
            s.add(StockMovement(
                entry_date=doc_date,
                warehouse_id=w.id,
                product_id=p.id,
                qty_kg=-qty,
                doc_type="sale",
                doc_id=sale.id
            ))

            if is_paid_:
                # Money movement +amount
                s.add(MoneyMovement(
                    entry_date=doc_date,
                    direction="in",
                    method=payment_method or "cash",
                    account_type=account_type,
                    bank_id=bank_id if account_type in ("bank", "ip") else None,
                    amount=total,
                    doc_type="sale",
                    doc_id=sale.id,
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

            # Keep existing UI caches consistent
            await recalc_stocks(s)
            await recalc_money_ledger(s)

    await state.clear()
    await set_menu(state, "main")
    await cq.message.answer("✅ Продажа сохранена.", reply_markup=main_menu_kb(is_owner(cq.from_user.id)))
    await cq.answer()


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
            await set_menu(state, "main")
            await cq.message.answer("Отменено ✅", reply_markup=main_menu_kb(is_owner(cq.from_user.id)))
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
        await set_menu(state, "main")
        await cq.message.answer("Отменено ✅", reply_markup=main_menu_kb(is_owner(cq.from_user.id)))
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
        async with s.begin():
            w = await s.get(Warehouse, warehouse_id)
            p = await s.get(Product, product_id)
            if not w or not p:
                raise RuntimeError("warehouse/product not found")

            if account_type in ("bank", "ip"):
                b = await s.get(Bank, bank_id)
                if not b:
                    await cq.answer("Банк не найден", show_alert=True)
                    return

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

            # Stock movement for income (positive)
            s.add(StockMovement(
                entry_date=doc_date,
                warehouse_id=w.id,
                product_id=p.id,
                qty_kg=qty,
                doc_type="income",
                doc_id=inc.id
            ))

            if add_money_entry:
                s.add(MoneyMovement(
                    entry_date=doc_date,
                    direction="out",
                    method=payment_method or "cash",
                    account_type=account_type,
                    bank_id=bank_id if account_type in ("bank", "ip") else None,
                    amount=-total,
                    doc_type="income",
                    doc_id=inc.id,
                    note=f"Приход #{inc.id} (поставщик {supplier_name})"
                ))

            await recalc_stocks(s)
            await recalc_money_ledger(s)

    await state.clear()
    await set_menu(state, "main")
    await cq.message.answer("✅ Приход сохранён.", reply_markup=main_menu_kb(is_owner(cq.from_user.id)))
    await cq.answer()


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
            await set_menu(state, "reports")
            await cq.message.answer("Отчеты:", reply_markup=reports_menu_kb(is_owner(cq.from_user.id)))
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
                                   parse_mode=ParseMode.HTML,
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
                         parse_mode=ParseMode.HTML,
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
        await set_menu(state, "reports")
        await cq.message.answer("Отменено ✅", reply_markup=reports_menu_kb(is_owner(cq.from_user.id)))
        return await cq.answer()

    data = await state.get_data()
    d_ = datetime.strptime(data["doc_date"], "%Y-%m-%d").date()

    qty = Decimal(data["qty"])
    price = Decimal(data["price"])
    total = qty * price
    delivery = Decimal(data.get("delivery", "0"))

    async with Session() as s:
        s.add(Debtor(
            doc_date=d_,
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
    await set_menu(state, "reports")
    await cq.message.answer("✅ Должник добавлен.", reply_markup=reports_menu_kb(is_owner(cq.from_user.id)))
    await cq.answer()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await ensure_allowed_users_schema(conn)
        await ensure_users_schema(conn)


    # One-time migration: if there are sales/incomes but no movements, generate movements from existing docs.
    async with Session() as s:
        async with s.begin():
            sm_cnt = int(await s.scalar(select(func.count()).select_from(StockMovement)) or 0)
            mm_cnt = int(await s.scalar(select(func.count()).select_from(MoneyMovement)) or 0)
            if sm_cnt == 0 and mm_cnt == 0:
                sales = (await s.execute(select(Sale))).scalars().all()
                incomes = (await s.execute(select(Income))).scalars().all()

                for sale in sales:
                    s.add(StockMovement(entry_date=sale.doc_date, warehouse_id=sale.warehouse_id, product_id=sale.product_id,
                                        qty_kg=-Decimal(sale.qty_kg), doc_type="sale", doc_id=sale.id))
                    if sale.is_paid:
                        s.add(MoneyMovement(entry_date=sale.doc_date, direction="in", method=sale.payment_method or "cash",
                                            account_type=sale.account_type or "cash",
                                            bank_id=sale.bank_id if (sale.account_type in ("bank","ip")) else None,
                                            amount=Decimal(sale.total_amount), doc_type="sale", doc_id=sale.id,
                                            note=f"Продажа #{sale.id} ({sale.customer_name})"))
                for inc in incomes:
                    s.add(StockMovement(entry_date=inc.doc_date, warehouse_id=inc.warehouse_id, product_id=inc.product_id,
                                        qty_kg=Decimal(inc.qty_kg), doc_type="income", doc_id=inc.id))
                    if inc.add_money_entry:
                        s.add(MoneyMovement(entry_date=inc.doc_date, direction="out", method=inc.payment_method or "cash",
                                            account_type=inc.account_type or "cash",
                                            bank_id=inc.bank_id if (inc.account_type in ("bank","ip")) else None,
                                            amount=-Decimal(inc.total_amount), doc_type="income", doc_id=inc.id,
                                            note=f"Приход #{inc.id} (поставщик {inc.supplier_name})"))
                await recalc_stocks(s)
                await recalc_money_ledger(s)


    async with Session() as s:
        ex = await s.scalar(select(AllowedUser).where(AllowedUser.user_id == OWNER_ID))
        if not ex:
            s.add(AllowedUser(user_id=OWNER_ID, created_at=datetime.utcnow(), added_by=OWNER_ID, note="owner"))
            await s.commit()

    bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    print("=== BOT STARTED OK ===", flush=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())




