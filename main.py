import os
import asyncio
import datetime as dt
from decimal import Decimal, InvalidOperation

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from sqlalchemy import (
    String, Integer, Date, DateTime, Boolean, ForeignKey, Numeric,
    select, func, delete, update
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, selectinload
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession


# ---------------- Settings ----------------
load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

ADMIN_USER_IDS = set(
    int(x) for x in os.getenv("ADMIN_USER_IDS", "").split(",")
    if x.strip().isdigit()
)

DB_URL = "sqlite+aiosqlite:////var/data/data.db"  # Render persistent disk
engine = create_async_engine(DB_URL, echo=False, future=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


def is_admin(user_id: int) -> bool:
    return (len(ADMIN_USER_IDS) == 0) or (user_id in ADMIN_USER_IDS)


def money(s: str) -> Decimal:
    # allow "123", "123.45", "123,45"
    s = s.strip().replace(",", ".")
    return Decimal(s)


def weight(s: str) -> Decimal:
    s = s.strip().replace(",", ".")
    return Decimal(s)


def kb_main():
    kb = InlineKeyboardBuilder()
    kb.button(text="🛒 Продажа", callback_data="sale:new")
    kb.button(text="📦 Приход", callback_data="in:new")
    kb.button(text="📒 Продажи", callback_data="sale:list")
    kb.button(text="📥 Приходы", callback_data="in:list")
    kb.button(text="🧾 Должники", callback_data="debt:list")
    kb.button(text="➕ Добавить должника вручную", callback_data="debt:new")
    kb.adjust(2, 2, 2)
    return kb.as_markup()


def kb_yes_no(prefix: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Да", callback_data=f"{prefix}:yes")
    kb.button(text="❌ Нет", callback_data=f"{prefix}:no")
    kb.adjust(2)
    return kb.as_markup()


def kb_today_yesterday(prefix: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="📅 Сегодня", callback_data=f"{prefix}:today")
    kb.button(text="📅 Вчера", callback_data=f"{prefix}:yesterday")
    kb.button(text="✍️ Ввести вручную (YYYY-MM-DD)", callback_data=f"{prefix}:manual")
    kb.adjust(1)
    return kb.as_markup()


# ---------------- DB Models ----------------
class Base(DeclarativeBase):
    pass


class Warehouse(Base):
    __tablename__ = "warehouses"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, index=True)


class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, index=True)


class Stock(Base):
    __tablename__ = "stocks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    qty_kg: Mapped[Decimal] = mapped_column(Numeric(18, 3), default=Decimal("0"))

    warehouse: Mapped["Warehouse"] = relationship()
    product: Mapped["Product"] = relationship()


class Customer(Base):
    __tablename__ = "customers"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), index=True)
    phone: Mapped[str] = mapped_column(String(60), default="", index=True)


class Sale(Base):
    __tablename__ = "sales"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    sale_date: Mapped[dt.date] = mapped_column(Date, index=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow())

    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    customer_id: Mapped[int | None] = mapped_column(ForeignKey("customers.id"), nullable=True)

    qty_kg: Mapped[Decimal] = mapped_column(Numeric(18, 3))
    price_per_kg: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    delivery_cost: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0"))
    total_cost: Mapped[Decimal] = mapped_column(Numeric(18, 2))

    is_paid: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    paid_method: Mapped[str] = mapped_column(String(30), default="")  # cash / bank / none
    paid_bank: Mapped[str] = mapped_column(String(120), default="")  # bank name if bank

    warehouse: Mapped["Warehouse"] = relationship()
    product: Mapped["Product"] = relationship()
    customer: Mapped["Customer"] = relationship()


class Income(Base):
    __tablename__ = "incomes"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    in_date: Mapped[dt.date] = mapped_column(Date, index=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=lambda: dt.datetime.utcnow())

    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    supplier_name: Mapped[str] = mapped_column(String(200), default="")

    qty_kg: Mapped[Decimal] = mapped_column(Numeric(18, 3))
    price_per_kg: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    delivery_cost: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0"))
    total_cost: Mapped[Decimal] = mapped_column(Numeric(18, 2))

    warehouse: Mapped["Warehouse"] = relationship()
    product: Mapped["Product"] = relationship()


# ---------------- DB helpers ----------------
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_or_create_warehouse(session: AsyncSession, name: str) -> Warehouse:
    name = name.strip()
    res = await session.execute(select(Warehouse).where(Warehouse.name == name))
    w = res.scalar_one_or_none()
    if w:
        return w
    w = Warehouse(name=name)
    session.add(w)
    await session.flush()
    return w


async def get_or_create_product(session: AsyncSession, name: str) -> Product:
    name = name.strip()
    res = await session.execute(select(Product).where(Product.name == name))
    p = res.scalar_one_or_none()
    if p:
        return p
    p = Product(name=name)
    session.add(p)
    await session.flush()
    return p


async def get_or_create_customer(session: AsyncSession, name: str, phone: str) -> Customer:
    name = (name or "").strip()
    phone = (phone or "").strip()

    # if both empty -> no customer
    if not name and not phone:
        raise ValueError("empty customer")

    # find by both if possible
    q = select(Customer)
    if name:
        q = q.where(Customer.name == name)
    if phone:
        q = q.where(Customer.phone == phone)
    res = await session.execute(q)
    c = res.scalar_one_or_none()
    if c:
        return c

    # if not found, create
    c = Customer(name=name or "Без имени", phone=phone or "")
    session.add(c)
    await session.flush()
    return c


async def add_stock(session: AsyncSession, warehouse_id: int, product_id: int, delta_kg: Decimal):
    res = await session.execute(
        select(Stock).where(
            Stock.warehouse_id == warehouse_id,
            Stock.product_id == product_id
        )
    )
    st = res.scalar_one_or_none()
    if not st:
        st = Stock(warehouse_id=warehouse_id, product_id=product_id, qty_kg=Decimal("0"))
        session.add(st)
        await session.flush()
    st.qty_kg = (Decimal(st.qty_kg) + delta_kg)


async def get_stock_qty(session: AsyncSession, warehouse_id: int, product_id: int) -> Decimal:
    res = await session.execute(
        select(Stock.qty_kg).where(
            Stock.warehouse_id == warehouse_id,
            Stock.product_id == product_id
        )
    )
    v = res.scalar_one_or_none()
    return Decimal(v or 0)


# ---------------- FSM States ----------------
class SaleWizard(StatesGroup):
    date_choice = State()
    date_manual = State()
    warehouse = State()
    product = State()
    qty = State()
    price = State()
    delivery = State()
    customer_name = State()
    customer_phone = State()
    paid_choice = State()
    paid_method = State()
    paid_bank = State()
    confirm = State()


class IncomeWizard(StatesGroup):
    date_choice = State()
    date_manual = State()
    warehouse = State()
    product = State()
    qty = State()
    price = State()
    delivery = State()
    supplier = State()
    confirm = State()


class DebtWizard(StatesGroup):
    # manual debtor = like sale but always unpaid
    date_choice = State()
    date_manual = State()
    warehouse = State()
    product = State()
    qty = State()
    price = State()
    delivery = State()
    customer_name = State()
    customer_phone = State()
    confirm = State()


# ---------------- Bot handlers ----------------
dp = Dispatcher()


@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Доступ запрещён.")
        return
    await state.clear()
    await message.answer("Привет! Выбери действие:", reply_markup=kb_main())


@dp.callback_query(F.data == "sale:new")
async def sale_new(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.clear()
    await state.set_state(SaleWizard.date_choice)
    await cb.message.answer("Дата продажи:", reply_markup=kb_today_yesterday("sale_date"))


@dp.callback_query(F.data.startswith("sale_date:"))
async def sale_date_choice(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    choice = cb.data.split(":")[1]
    if choice == "manual":
        await state.set_state(SaleWizard.date_manual)
        await cb.message.answer("Введи дату продажи в формате YYYY-MM-DD:")
        return
    today = dt.date.today()
    d = today if choice == "today" else (today - dt.timedelta(days=1))
    await state.update_data(sale_date=d.isoformat())
    await state.set_state(SaleWizard.warehouse)
    await cb.message.answer("С какого склада? (напиши название)")


@dp.message(SaleWizard.date_manual)
async def sale_date_manual(message: Message, state: FSMContext):
    try:
        d = dt.date.fromisoformat(message.text.strip())
    except Exception:
        await message.answer("❌ Неверный формат. Пример: 2025-12-03")
        return
    await state.update_data(sale_date=d.isoformat())
    await state.set_state(SaleWizard.warehouse)
    await message.answer("С какого склада? (напиши название)")


@dp.message(SaleWizard.warehouse)
async def sale_warehouse(message: Message, state: FSMContext):
    await state.update_data(warehouse_name=message.text.strip())
    await state.set_state(SaleWizard.product)
    await message.answer("Какой товар? (название)")


@dp.message(SaleWizard.product)
async def sale_product(message: Message, state: FSMContext):
    await state.update_data(product_name=message.text.strip())
    await state.set_state(SaleWizard.qty)
    await message.answer("Количество (кг)? (например 1200 или 1200.5)")


@dp.message(SaleWizard.qty)
async def sale_qty(message: Message, state: FSMContext):
    try:
        q = weight(message.text)
        if q <= 0:
            raise ValueError()
    except Exception:
        await message.answer("❌ Введи число > 0 (кг). Пример: 1000 или 1000.5")
        return
    await state.update_data(qty_kg=str(q))
    await state.set_state(SaleWizard.price)
    await message.answer("Цена за 1 кг? (например 350 или 350.50)")


@dp.message(SaleWizard.price)
async def sale_price(message: Message, state: FSMContext):
    try:
        p = money(message.text)
        if p < 0:
            raise ValueError()
    except Exception:
        await message.answer("❌ Неверная цена. Пример: 350 или 350.50")
        return
    await state.update_data(price_per_kg=str(p))
    await state.set_state(SaleWizard.delivery)
    await message.answer("Расходы на доставку? (если нет — 0)")


@dp.message(SaleWizard.delivery)
async def sale_delivery(message: Message, state: FSMContext):
    try:
        d = money(message.text)
        if d < 0:
            raise ValueError()
    except Exception:
        await message.answer("❌ Неверно. Введи число, например 0 или 5000")
        return
    await state.update_data(delivery_cost=str(d))
    await state.set_state(SaleWizard.customer_name)
    await message.answer("Имя клиента? (можно написать '-' чтобы пропустить)")


@dp.message(SaleWizard.customer_name)
async def sale_customer_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if name == "-":
        name = ""
    await state.update_data(customer_name=name)
    await state.set_state(SaleWizard.customer_phone)
    await message.answer("Номер клиента? (можно '-' чтобы пропустить)")


@dp.message(SaleWizard.customer_phone)
async def sale_customer_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    if phone == "-":
        phone = ""
    await state.update_data(customer_phone=phone)

    # payment status
    await state.set_state(SaleWizard.paid_choice)
    await message.answer("Оплачено?", reply_markup=kb_yes_no("paid"))


@dp.callback_query(SaleWizard.paid_choice, F.data.startswith("paid:"))
async def sale_paid_choice(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    yn = cb.data.split(":")[1]
    is_paid = (yn == "yes")
    await state.update_data(is_paid=is_paid)

    if not is_paid:
        # unpaid -> no money method questions
        await state.update_data(paid_method="", paid_bank="")
        await state.set_state(SaleWizard.confirm)
        await show_sale_confirm(cb.message, state)
        return

    await state.set_state(SaleWizard.paid_method)
    kb = InlineKeyboardBuilder()
    kb.button(text="💵 Нал", callback_data="paymethod:cash")
    kb.button(text="🏦 Безнал", callback_data="paymethod:bank")
    kb.adjust(2)
    await cb.message.answer("Как получил деньги?", reply_markup=kb.as_markup())


@dp.callback_query(SaleWizard.paid_method, F.data.startswith("paymethod:"))
async def sale_paid_method(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    method = cb.data.split(":")[1]
    await state.update_data(paid_method=method)

    if method == "cash":
        await state.update_data(paid_bank="")
        await state.set_state(SaleWizard.confirm)
        await show_sale_confirm(cb.message, state)
        return

    await state.set_state(SaleWizard.paid_bank)
    await cb.message.answer("Какой банк? (например Kaspi, Halyk, ...)")

@dp.message(SaleWizard.paid_bank)
async def sale_paid_bank(message: Message, state: FSMContext):
    await state.update_data(paid_bank=message.text.strip())
    await state.set_state(SaleWizard.confirm)
    await show_sale_confirm(message, state)

async def show_sale_confirm(target, state: FSMContext):
    data = await state.get_data()
    qty = Decimal(data["qty_kg"])
    price = Decimal(data["price_per_kg"])
    delivery = Decimal(data["delivery_cost"])
    total = (qty * price) + delivery

    paid = "✅ Да" if data.get("is_paid") else "❌ Нет (в Должники)"
    bank = ""
    if data.get("is_paid"):
        if data.get("paid_method") == "cash":
            bank = "Способ: Нал"
        else:
            bank = f"Способ: Безнал, банк: {data.get('paid_bank','')}"
    cust = "—"
    if data.get("customer_name") or data.get("customer_phone"):
        cust = f"{data.get('customer_name','')}".strip() or "Без имени"
        if data.get("customer_phone"):
            cust += f" ({data.get('customer_phone')})"

    text = (
        "Проверь продажу:\n\n"
        f"📅 Дата: {data['sale_date']}\n"
        f"🏬 Склад: {data['warehouse_name']}\n"
        f"📦 Товар: {data['product_name']}\n"
        f"⚖️ Кол-во: {qty} кг\n"
        f"💰 Цена/кг: {price}\n"
        f"🚚 Доставка: {delivery}\n"
        f"🧮 Итого: {total}\n"
        f"👤 Клиент: {cust}\n"
        f"💳 Оплата: {paid}\n"
        + (f"{bank}\n" if bank else "")
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Сохранить", callback_data="sale:save")
    kb.button(text="❌ Отмена", callback_data="cancel")
    kb.adjust(2)

    await target.answer(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data == "sale:save")
async def sale_save(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()

    async with SessionLocal() as session:
        w = await get_or_create_warehouse(session, data["warehouse_name"])
        p = await get_or_create_product(session, data["product_name"])

        # stock check
        qty = Decimal(data["qty_kg"])
        current = await get_stock_qty(session, w.id, p.id)
        if current < qty:
            await cb.message.answer(f"❌ На складе недостаточно товара. Остаток: {current} кг")
            return

        # customer optional
        cust_id = None
        if data.get("customer_name") or data.get("customer_phone"):
            c = await get_or_create_customer(session, data.get("customer_name",""), data.get("customer_phone",""))
            cust_id = c.id

        price = Decimal(data["price_per_kg"])
        delivery = Decimal(data["delivery_cost"])
        total = (qty * price) + delivery

        sale = Sale(
            sale_date=dt.date.fromisoformat(data["sale_date"]),
            warehouse_id=w.id,
            product_id=p.id,
            customer_id=cust_id,
            qty_kg=qty,
            price_per_kg=price,
            delivery_cost=delivery,
            total_cost=total,
            is_paid=bool(data.get("is_paid")),
            paid_method=data.get("paid_method",""),
            paid_bank=data.get("paid_bank",""),
        )
        session.add(sale)

        # decrease stock
        await add_stock(session, w.id, p.id, -qty)

        await session.commit()

    await state.clear()
    await cb.message.answer("✅ Продажа сохранена.", reply_markup=kb_main())


# -------- Debtors --------
@dp.callback_query(F.data == "debt:list")
async def debt_list(cb: CallbackQuery):
    await cb.answer()
    async with SessionLocal() as session:
        res = await session.execute(
            select(Sale)
            .options(selectinload(Sale.customer), selectinload(Sale.warehouse), selectinload(Sale.product))
            .where(Sale.is_paid == False)
            .order_by(Sale.sale_date.desc(), Sale.id.desc())
            .limit(50)
        )
        items = res.scalars().all()

    if not items:
        await cb.message.answer("✅ Должников нет.", reply_markup=kb_main())
        return

    text = "🧾 Должники (последние 50):\n\n"
    kb = InlineKeyboardBuilder()
    for s in items:
        cust = "Без клиента"
        if s.customer:
            cust = s.customer.name
            if s.customer.phone:
                cust += f" ({s.customer.phone})"
        text += (
            f"#{s.id} | {s.sale_date} | {cust}\n"
            f"{s.warehouse.name} • {s.product.name} • {s.qty_kg}кг • Итого {s.total_cost}\n\n"
        )
        kb.button(text=f"✅ Закрыть #{s.id}", callback_data=f"debt:paid:{s.id}")
    kb.button(text="⬅️ Назад", callback_data="back")
    kb.adjust(1)
    await cb.message.answer(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("debt:paid:"))
async def debt_mark_paid(cb: CallbackQuery):
    await cb.answer()
    sale_id = int(cb.data.split(":")[2])

    # ask method
    kb = InlineKeyboardBuilder()
    kb.button(text="💵 Нал", callback_data=f"debtpay:cash:{sale_id}")
    kb.button(text="🏦 Безнал", callback_data=f"debtpay:bank:{sale_id}")
    kb.adjust(2)
    await cb.message.answer(f"Как оплатили долг по продаже #{sale_id}?", reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("debtpay:"))
async def debt_pay_method(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    _, method, sale_id = cb.data.split(":")
    sale_id = int(sale_id)

    if method == "cash":
        async with SessionLocal() as session:
            await session.execute(
                update(Sale)
                .where(Sale.id == sale_id)
                .values(is_paid=True, paid_method="cash", paid_bank="")
            )
            await session.commit()
        await cb.message.answer(f"✅ Долг по #{sale_id} закрыт (нал).", reply_markup=kb_main())
        return

    # bank -> ask bank name via FSM quick
    await state.clear()
    await state.update_data(debt_sale_id=sale_id)
    await state.set_state(SaleWizard.paid_bank)
    await cb.message.answer("Введи название банка (для оплаты долга):")


@dp.message(SaleWizard.paid_bank)
async def debt_bank_name(message: Message, state: FSMContext):
    data = await state.get_data()
    sale_id = data.get("debt_sale_id")
    if not sale_id:
        # this state is used in sale flow too, but it won't reach here without context
        await message.answer("❌ Контекст потерян. Нажми /start")
        await state.clear()
        return
    bank = message.text.strip()
    async with SessionLocal() as session:
        await session.execute(
            update(Sale)
            .where(Sale.id == int(sale_id))
            .values(is_paid=True, paid_method="bank", paid_bank=bank)
        )
        await session.commit()
    await state.clear()
    await message.answer(f"✅ Долг по #{sale_id} закрыт (безнал: {bank}).", reply_markup=kb_main())


# -------- Manual debtor --------
@dp.callback_query(F.data == "debt:new")
async def debt_new(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.clear()
    await state.set_state(DebtWizard.date_choice)
    await cb.message.answer("Дата (для должника):", reply_markup=kb_today_yesterday("debt_date"))


@dp.callback_query(F.data.startswith("debt_date:"))
async def debt_date_choice(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    choice = cb.data.split(":")[1]
    if choice == "manual":
        await state.set_state(DebtWizard.date_manual)
        await cb.message.answer("Введи дату YYYY-MM-DD:")
        return
    today = dt.date.today()
    d = today if choice == "today" else (today - dt.timedelta(days=1))
    await state.update_data(sale_date=d.isoformat())
    await state.set_state(DebtWizard.warehouse)
    await cb.message.answer("С какого склада? (название)")


@dp.message(DebtWizard.date_manual)
async def debt_date_manual(message: Message, state: FSMContext):
    try:
        d = dt.date.fromisoformat(message.text.strip())
    except Exception:
        await message.answer("❌ Неверный формат. Пример: 2025-12-03")
        return
    await state.update_data(sale_date=d.isoformat())
    await state.set_state(DebtWizard.warehouse)
    await message.answer("С какого склада? (название)")


@dp.message(DebtWizard.warehouse)
async def debt_wh(message: Message, state: FSMContext):
    await state.update_data(warehouse_name=message.text.strip())
    await state.set_state(DebtWizard.product)
    await message.answer("Какой товар? (название)")


@dp.message(DebtWizard.product)
async def debt_prod(message: Message, state: FSMContext):
    await state.update_data(product_name=message.text.strip())
    await state.set_state(DebtWizard.qty)
    await message.answer("Количество (кг)?")


@dp.message(DebtWizard.qty)
async def debt_qty(message: Message, state: FSMContext):
    try:
        q = weight(message.text)
        if q <= 0:
            raise ValueError()
    except Exception:
        await message.answer("❌ Введи число > 0.")
        return
    await state.update_data(qty_kg=str(q))
    await state.set_state(DebtWizard.price)
    await message.answer("Цена за 1 кг?")


@dp.message(DebtWizard.price)
async def debt_price(message: Message, state: FSMContext):
    try:
        p = money(message.text)
        if p < 0:
            raise ValueError()
    except Exception:
        await message.answer("❌ Неверная цена.")
        return
    await state.update_data(price_per_kg=str(p))
    await state.set_state(DebtWizard.delivery)
    await message.answer("Доставка? (0 если нет)")


@dp.message(DebtWizard.delivery)
async def debt_deliv(message: Message, state: FSMContext):
    try:
        d = money(message.text)
        if d < 0:
            raise ValueError()
    except Exception:
        await message.answer("❌ Неверно.")
        return
    await state.update_data(delivery_cost=str(d))
    await state.set_state(DebtWizard.customer_name)
    await message.answer("Имя клиента? (можно '-')")


@dp.message(DebtWizard.customer_name)
async def debt_cname(message: Message, state: FSMContext):
    name = message.text.strip()
    if name == "-":
        name = ""
    await state.update_data(customer_name=name)
    await state.set_state(DebtWizard.customer_phone)
    await message.answer("Номер клиента? (можно '-')")


@dp.message(DebtWizard.customer_phone)
async def debt_cphone(message: Message, state: FSMContext):
    phone = message.text.strip()
    if phone == "-":
        phone = ""
    await state.update_data(customer_phone=phone)
    await state.set_state(DebtWizard.confirm)

    data = await state.get_data()
    qty = Decimal(data["qty_kg"])
    price = Decimal(data["price_per_kg"])
    delivery = Decimal(data["delivery_cost"])
    total = (qty * price) + delivery
    cust = (data.get("customer_name") or "Без имени")
    if data.get("customer_phone"):
        cust += f" ({data.get('customer_phone')})"

    text = (
        "Проверь должника:\n\n"
        f"📅 Дата: {data['sale_date']}\n"
        f"🏬 Склад: {data['warehouse_name']}\n"
        f"📦 Товар: {data['product_name']}\n"
        f"⚖️ Кол-во: {qty} кг\n"
        f"💰 Цена/кг: {price}\n"
        f"🚚 Доставка: {delivery}\n"
        f"🧮 Итого: {total}\n"
        f"👤 Клиент: {cust}\n"
        f"💳 Оплата: ❌ Нет (Должник)\n"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Сохранить", callback_data="debt:save")
    kb.button(text="❌ Отмена", callback_data="cancel")
    kb.adjust(2)
    await message.answer(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data == "debt:save")
async def debt_save(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()

    async with SessionLocal() as session:
        w = await get_or_create_warehouse(session, data["warehouse_name"])
        p = await get_or_create_product(session, data["product_name"])

        qty = Decimal(data["qty_kg"])
        current = await get_stock_qty(session, w.id, p.id)
        if current < qty:
            await cb.message.answer(f"❌ На складе недостаточно товара. Остаток: {current} кг")
            return

        c = await get_or_create_customer(session, data.get("customer_name",""), data.get("customer_phone",""))
        price = Decimal(data["price_per_kg"])
        delivery = Decimal(data["delivery_cost"])
        total = (qty * price) + delivery

        sale = Sale(
            sale_date=dt.date.fromisoformat(data["sale_date"]),
            warehouse_id=w.id,
            product_id=p.id,
            customer_id=c.id,
            qty_kg=qty,
            price_per_kg=price,
            delivery_cost=delivery,
            total_cost=total,
            is_paid=False,
            paid_method="",
            paid_bank="",
        )
        session.add(sale)
        await add_stock(session, w.id, p.id, -qty)
        await session.commit()

    await state.clear()
    await cb.message.answer("✅ Должник добавлен.", reply_markup=kb_main())


# -------- Income (приход) --------
@dp.callback_query(F.data == "in:new")
async def income_new(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.clear()
    await state.set_state(IncomeWizard.date_choice)
    await cb.message.answer("Дата прихода:", reply_markup=kb_today_yesterday("in_date"))


@dp.callback_query(F.data.startswith("in_date:"))
async def income_date_choice(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    choice = cb.data.split(":")[1]
    if choice == "manual":
        await state.set_state(IncomeWizard.date_manual)
        await cb.message.answer("Введи дату прихода YYYY-MM-DD:")
        return
    today = dt.date.today()
    d = today if choice == "today" else (today - dt.timedelta(days=1))
    await state.update_data(in_date=d.isoformat())
    await state.set_state(IncomeWizard.warehouse)
    await cb.message.answer("На какой склад? (название)")


@dp.message(IncomeWizard.date_manual)
async def income_date_manual(message: Message, state: FSMContext):
    try:
        d = dt.date.fromisoformat(message.text.strip())
    except Exception:
        await message.answer("❌ Неверный формат. Пример: 2025-12-03")
        return
    await state.update_data(in_date=d.isoformat())
    await state.set_state(IncomeWizard.warehouse)
    await message.answer("На какой склад? (название)")


@dp.message(IncomeWizard.warehouse)
async def income_wh(message: Message, state: FSMContext):
    await state.update_data(warehouse_name=message.text.strip())
    await state.set_state(IncomeWizard.product)
    await message.answer("Какой товар? (название)")


@dp.message(IncomeWizard.product)
async def income_prod(message: Message, state: FSMContext):
    await state.update_data(product_name=message.text.strip())
    await state.set_state(IncomeWizard.qty)
    await message.answer("Количество (кг)?")


@dp.message(IncomeWizard.qty)
async def income_qty(message: Message, state: FSMContext):
    try:
        q = weight(message.text)
        if q <= 0:
            raise ValueError()
    except Exception:
        await message.answer("❌ Введи число > 0.")
        return
    await state.update_data(qty_kg=str(q))
    await state.set_state(IncomeWizard.price)
    await message.answer("Цена за 1 кг?")


@dp.message(IncomeWizard.price)
async def income_price(message: Message, state: FSMContext):
    try:
        p = money(message.text)
        if p < 0:
            raise ValueError()
    except Exception:
        await message.answer("❌ Неверная цена.")
        return
    await state.update_data(price_per_kg=str(p))
    await state.set_state(IncomeWizard.delivery)
    await message.answer("Доставка? (0 если нет)")


@dp.message(IncomeWizard.delivery)
async def income_delivery(message: Message, state: FSMContext):
    try:
        d = money(message.text)
        if d < 0:
            raise ValueError()
    except Exception:
        await message.answer("❌ Неверно.")
        return
    await state.update_data(delivery_cost=str(d))
    await state.set_state(IncomeWizard.supplier)
    await message.answer("Поставщик/откуда пришло? (можно '-')")


@dp.message(IncomeWizard.supplier)
async def income_supplier(message: Message, state: FSMContext):
    supplier = message.text.strip()
    if supplier == "-":
        supplier = ""
    await state.update_data(supplier_name=supplier)
    await state.set_state(IncomeWizard.confirm)

    data = await state.get_data()
    qty = Decimal(data["qty_kg"])
    price = Decimal(data["price_per_kg"])
    delivery = Decimal(data["delivery_cost"])
    total = (qty * price) + delivery

    text = (
        "Проверь приход:\n\n"
        f"📅 Дата: {data['in_date']}\n"
        f"🏬 Склад: {data['warehouse_name']}\n"
        f"📦 Товар: {data['product_name']}\n"
        f"⚖️ Кол-во: {qty} кг\n"
        f"💰 Цена/кг: {price}\n"
        f"🚚 Доставка: {delivery}\n"
        f"🧮 Итого: {total}\n"
        f"🚛 Поставщик: {supplier or '—'}\n"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Сохранить", callback_data="in:save")
    kb.button(text="❌ Отмена", callback_data="cancel")
    kb.adjust(2)
    await message.answer(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data == "in:save")
async def income_save(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    data = await state.get_data()

    async with SessionLocal() as session:
        w = await get_or_create_warehouse(session, data["warehouse_name"])
        p = await get_or_create_product(session, data["product_name"])

        qty = Decimal(data["qty_kg"])
        price = Decimal(data["price_per_kg"])
        delivery = Decimal(data["delivery_cost"])
        total = (qty * price) + delivery

        inc = Income(
            in_date=dt.date.fromisoformat(data["in_date"]),
            warehouse_id=w.id,
            product_id=p.id,
            supplier_name=data.get("supplier_name",""),
            qty_kg=qty,
            price_per_kg=price,
            delivery_cost=delivery,
            total_cost=total,
        )
        session.add(inc)
        await add_stock(session, w.id, p.id, qty)
        await session.commit()

    await state.clear()
    await cb.message.answer("✅ Приход сохранён.", reply_markup=kb_main())


# -------- Lists --------
@dp.callback_query(F.data == "sale:list")
async def sale_list(cb: CallbackQuery):
    await cb.answer()
    async with SessionLocal() as session:
        res = await session.execute(
            select(Sale)
            .options(selectinload(Sale.customer), selectinload(Sale.warehouse), selectinload(Sale.product))
            .order_by(Sale.sale_date.desc(), Sale.id.desc())
            .limit(30)
        )
        items = res.scalars().all()

    if not items:
        await cb.message.answer("Пока нет продаж.", reply_markup=kb_main())
        return

    text = "📒 Продажи (последние 30):\n\n"
    for s in items:
        cust = "—"
        if s.customer:
            cust = s.customer.name
            if s.customer.phone:
                cust += f" ({s.customer.phone})"
        paid = "✅" if s.is_paid else "❌"
        text += (
            f"#{s.id} | {s.sale_date} | {paid}\n"
            f"{s.warehouse.name} • {s.product.name} • {s.qty_kg}кг • Итого {s.total_cost}\n"
            f"Клиент: {cust}\n\n"
        )
    await cb.message.answer(text, reply_markup=kb_main())


@dp.callback_query(F.data == "in:list")
async def income_list(cb: CallbackQuery):
    await cb.answer()
    async with SessionLocal() as session:
        res = await session.execute(
            select(Income)
            .options(selectinload(Income.warehouse), selectinload(Income.product))
            .order_by(Income.in_date.desc(), Income.id.desc())
            .limit(30)
        )
        items = res.scalars().all()

    if not items:
        await cb.message.answer("Пока нет приходов.", reply_markup=kb_main())
        return

    text = "📥 Приходы (последние 30):\n\n"
    for i in items:
        text += (
            f"#{i.id} | {i.in_date}\n"
            f"{i.warehouse.name} • {i.product.name} • {i.qty_kg}кг • Итого {i.total_cost}\n"
            f"Поставщик: {i.supplier_name or '—'}\n\n"
        )
    await cb.message.answer(text, reply_markup=kb_main())


@dp.callback_query(F.data == "cancel")
@dp.callback_query(F.data == "back")
async def cancel(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.clear()
    await cb.message.answer("Ок.", reply_markup=kb_main())


async def main():
    await init_db()
    bot = Bot(TOKEN)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
