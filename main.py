import asyncio
import os
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)
from dotenv import load_dotenv

from sqlalchemy import (
    String, Integer, Float, ForeignKey, UniqueConstraint, select, desc
)
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import (
    DeclarativeBase, Mapped, mapped_column, relationship, selectinload
)

# ----------------- Settings -----------------
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")

ADMIN_USER_IDS = set(
    int(x) for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip().isdigit()
)

DB_URL = "sqlite+aiosqlite:////var/data/data.db"


# ----------------- DB Models -----------------
class Base(DeclarativeBase):
    pass


class Warehouse(Base):
    __tablename__ = "warehouses"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True)


class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True)
    unit: Mapped[str] = mapped_column(String, default="kg")


class Customer(Base):
    __tablename__ = "customers"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    phone: Mapped[str] = mapped_column(String, default="")
    __table_args__ = (UniqueConstraint("name", "phone", name="uq_customer_name_phone"),)


class Account(Base):
    __tablename__ = "accounts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True)
    type: Mapped[str] = mapped_column(String)  # cash / bank


class Stock(Base):
    __tablename__ = "stock"
    __table_args__ = (UniqueConstraint("warehouse_id", "product_id", name="uq_stock"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    qty_kg: Mapped[float] = mapped_column(Float, default=0.0)

    warehouse: Mapped["Warehouse"] = relationship()
    product: Mapped["Product"] = relationship()


class Money(Base):
    __tablename__ = "money"
    __table_args__ = (UniqueConstraint("account_id", name="uq_money"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))
    balance: Mapped[float] = mapped_column(Float, default=0.0)

    account: Mapped["Account"] = relationship()


class Purchase(Base):
    __tablename__ = "purchases"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dt: Mapped[str] = mapped_column(String)  # YYYY-MM-DD
    supplier_name: Mapped[str] = mapped_column(String)

    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))

    qty_kg: Mapped[float] = mapped_column(Float)
    price_per_kg: Mapped[float] = mapped_column(Float)
    total: Mapped[float] = mapped_column(Float)

    delivery_cost: Mapped[float] = mapped_column(Float, default=0.0)
    payment_method: Mapped[str] = mapped_column(String)  # cash/bank
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))
    comment: Mapped[str] = mapped_column(String, default="")


class Sale(Base):
    __tablename__ = "sales"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dt: Mapped[str] = mapped_column(String)  # YYYY-MM-DD

    customer_id: Mapped[int] = mapped_column(ForeignKey("customers.id"))
    customer: Mapped["Customer"] = relationship()

    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))

    qty_kg: Mapped[float] = mapped_column(Float)
    price_per_kg: Mapped[float] = mapped_column(Float)
    total: Mapped[float] = mapped_column(Float)

    delivery_cost: Mapped[float] = mapped_column(Float, default=0.0)
    payment_method: Mapped[str] = mapped_column(String)  # cash/bank
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))
    comment: Mapped[str] = mapped_column(String, default="")


engine = create_async_engine(DB_URL, echo=False)
Session = async_sessionmaker(engine, expire_on_commit=False)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# ----------------- UI helpers -----------------
def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📦 Остатки"), KeyboardButton(text="💰 Деньги")],
            [KeyboardButton(text="🟢 Приход"), KeyboardButton(text="🔴 Продажа")],
            [KeyboardButton(text="📄 Приходы"), KeyboardButton(text="📄 Продажи")],
            [KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True
    )


def is_admin(user_id: int) -> bool:
    return (not ADMIN_USER_IDS) or (user_id in ADMIN_USER_IDS)


def money_label(acc_type: str) -> str:
    return "Нал" if acc_type == "cash" else "Безнал"


def ikb_from_pairs(prefix: str, pairs: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=name, callback_data=f"{prefix}:{_id}")]
            for _id, name in pairs
        ]
    )


def skip_kb(callback: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭️ Пропустить", callback_data=callback)]
    ])


def date_choice_kb(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Сегодня", callback_data=f"{prefix}:today")],
        [InlineKeyboardButton(text="📅 Вчера", callback_data=f"{prefix}:yesterday")],
        [InlineKeyboardButton(text="✍️ Ввести дату", callback_data=f"{prefix}:manual")],
    ])


def parse_pos_float(text: str) -> float | None:
    t = text.strip().replace(",", ".")
    try:
        v = float(t)
        if v < 0:
            return None
        return v
    except Exception:
        return None


def normalize_phone(text: str) -> str:
    allowed = set("0123456789+")
    return "".join(ch for ch in text.strip() if ch in allowed)


def parse_date_yyyy_mm_dd(text: str) -> str | None:
    t = text.strip()
    try:
        dt = datetime.strptime(t, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


async def get_warehouses():
    async with Session() as s:
        rows = (await s.execute(select(Warehouse.id, Warehouse.name).order_by(Warehouse.name))).all()
        return [(r[0], r[1]) for r in rows]


async def get_products():
    async with Session() as s:
        rows = (await s.execute(select(Product.id, Product.name).order_by(Product.name))).all()
        return [(r[0], r[1]) for r in rows]


async def get_accounts_by_type(acc_type: str):
    async with Session() as s:
        rows = (await s.execute(
            select(Account.id, Account.name).where(Account.type == acc_type).order_by(Account.name)
        )).all()
        return [(r[0], r[1]) for r in rows]


async def get_name_by_id(model_cls, _id: int) -> str:
    async with Session() as s:
        obj = (await s.execute(select(model_cls).where(model_cls.id == _id))).scalar_one_or_none()
        return obj.name if obj else f"#{_id}"


async def get_account_label(account_id: int) -> str:
    async with Session() as s:
        acc = (await s.execute(select(Account).where(Account.id == account_id))).scalar_one_or_none()
        if not acc:
            return f"#{account_id}"
        return f"{money_label(acc.type)} | {acc.name}"


async def upsert_customer(session, name: str, phone: str) -> int:
    existing = (await session.execute(
        select(Customer).where(Customer.name == name, Customer.phone == phone)
    )).scalar_one_or_none()
    if existing is None:
        existing = Customer(name=name, phone=phone)
        session.add(existing)
        await session.flush()
    return int(existing.id)


async def stock_add(session, warehouse_id: int, product_id: int, qty: float):
    st = (await session.execute(
        select(Stock).where(Stock.warehouse_id == warehouse_id, Stock.product_id == product_id)
    )).scalar_one_or_none()
    if st is None:
        st = Stock(warehouse_id=warehouse_id, product_id=product_id, qty_kg=0.0)
        session.add(st)
    st.qty_kg = float(st.qty_kg) + float(qty)


async def stock_sub(session, warehouse_id: int, product_id: int, qty: float) -> bool:
    st = (await session.execute(
        select(Stock).where(Stock.warehouse_id == warehouse_id, Stock.product_id == product_id)
    )).scalar_one_or_none()
    if st is None or float(st.qty_kg) < float(qty):
        return False
    st.qty_kg = float(st.qty_kg) - float(qty)
    return True


async def money_add(session, account_id: int, amount: float):
    m = (await session.execute(select(Money).where(Money.account_id == account_id))).scalar_one()
    m.balance = float(m.balance) + float(amount)


async def money_sub(session, account_id: int, amount: float):
    m = (await session.execute(select(Money).where(Money.account_id == account_id))).scalar_one()
    m.balance = float(m.balance) - float(amount)


# ----------------- FSM -----------------
class IncomingFlow(StatesGroup):
    date_mode = State()
    dt = State()
    supplier_name = State()
    warehouse = State()
    product = State()
    qty = State()
    price = State()
    delivery = State()
    payment_method = State()
    account = State()
    comment = State()
    confirm = State()

    mode = State()          # "new" | "edit"
    edit_id = State()


class SaleFlow(StatesGroup):
    date_mode = State()
    dt = State()
    mode = State()          # "new" | "edit"
    edit_id = State()

    customer_name = State()
    customer_phone = State()
    warehouse = State()
    product = State()
    qty = State()
    price = State()
    delivery = State()
    payment_method = State()
    account = State()
    comment = State()
    confirm = State()


# ----------------- Bot -----------------
dp = Dispatcher()
router = Router()
dp.include_router(router)


@router.message(CommandStart())
async def start(message: Message):
    await message.answer(
        "✅ Бот учета запущен\n\n"
        "Сделай справочники:\n"
        "/add_warehouse Склад 1\n"
        "/add_product Сахар\n"
        "/add_account cash Касса\n"
        "/add_account bank Halyk\n\n"
        "Дальше: 🟢 Приход / 🔴 Продажа",
        reply_markup=main_menu_kb()
    )


@router.message(F.text == "❌ Отмена")
@router.message(Command("cancel"))
async def cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Ок, отменено.", reply_markup=main_menu_kb())


# --------- Admin add dictionaries ----------
@router.message(Command("add_warehouse"))
async def add_warehouse(message: Message):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа.")
    name = message.text.replace("/add_warehouse", "").strip()
    if not name:
        return await message.answer("Пример: /add_warehouse Склад 1")
    async with Session() as s:
        s.add(Warehouse(name=name))
        try:
            await s.commit()
        except Exception:
            await s.rollback()
            return await message.answer("Не удалось добавить (возможно, уже есть).")
    await message.answer(f"✅ Склад добавлен: {name}")


@router.message(Command("add_product"))
async def add_product(message: Message):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа.")
    name = message.text.replace("/add_product", "").strip()
    if not name:
        return await message.answer("Пример: /add_product Сахар")
    async with Session() as s:
        s.add(Product(name=name, unit="kg"))
        try:
            await s.commit()
        except Exception:
            await s.rollback()
            return await message.answer("Не удалось добавить (возможно, уже есть).")
    await message.answer(f"✅ Товар добавлен: {name}")


@router.message(Command("add_account"))
async def add_account(message: Message):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет доступа.")
    rest = message.text.replace("/add_account", "").strip()
    parts = rest.split(maxsplit=1)
    if len(parts) != 2:
        return await message.answer("Пример: /add_account cash Касса\nили: /add_account bank Halyk")
    acc_type, name = parts[0].strip(), parts[1].strip()
    if acc_type not in ("cash", "bank"):
        return await message.answer("Первый параметр должен быть cash или bank.")
    async with Session() as s:
        acc = Account(name=name, type=acc_type)
        s.add(acc)
        try:
            await s.commit()
        except Exception:
            await s.rollback()
            return await message.answer("Не удалось добавить (возможно, уже есть).")
        s.add(Money(account_id=acc.id, balance=0.0))
        await s.commit()
    await message.answer(f"✅ Счёт добавлен: {name} ({acc_type})")


# --------- Views ----------
@router.message(F.text == "📦 Остатки")
async def show_stock(message: Message):
    async with Session() as s:
        q = (
            select(Warehouse.name, Product.name, Stock.qty_kg)
            .select_from(Stock)
            .join(Warehouse, Stock.warehouse_id == Warehouse.id)
            .join(Product, Stock.product_id == Product.id)
            .order_by(Warehouse.name, Product.name)
        )
        rows = (await s.execute(q)).all()

    if not rows:
        return await message.answer("Пока нет остатков. Сделай приход.")
    lines = ["📦 Остатки:"]
    for wh, pr, qty in rows:
        lines.append(f"- {wh} | {pr}: {qty:.3f} кг")
    await message.answer("\n".join(lines))


@router.message(F.text == "💰 Деньги")
async def show_money(message: Message):
    async with Session() as s:
        q = (
            select(Account.type, Account.name, Money.balance)
            .select_from(Money)
            .join(Account, Money.account_id == Account.id)
            .order_by(Account.type, Account.name)
        )
        rows = (await s.execute(q)).all()

    if not rows:
        return await message.answer("Счета не настроены. Добавь: /add_account cash Касса или /add_account bank Halyk")
    lines = ["💰 Балансы:"]
    for acc_type, name, bal in rows:
        lines.append(f"- {money_label(acc_type)} | {name}: {bal:.2f}")
    await message.answer("\n".join(lines))


# ----------------- Date step (common) -----------------
async def ask_date(message: Message, prefix: str):
    await message.answer("📅 Выбери дату:", reply_markup=date_choice_kb(prefix))


def today_str():
    return datetime.now().strftime("%Y-%m-%d")


def yesterday_str():
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


# ===================== INCOMING (Приход) =====================
@router.message(F.text == "🟢 Приход")
async def incoming_start(message: Message, state: FSMContext):
    await state.clear()
    await state.update_data(mode="new", edit_id=None)
    await state.set_state(IncomingFlow.date_mode)
    await ask_date(message, "inc_date")


@router.callback_query(F.data.startswith("inc_date:"), IncomingFlow.date_mode)
async def incoming_date_mode(cb: CallbackQuery, state: FSMContext):
    mode = cb.data.split(":")[1]
    if mode == "today":
        await state.update_data(dt=today_str())
        await state.set_state(IncomingFlow.supplier_name)
        await cb.message.answer("1) Поставщик (имя):")
    elif mode == "yesterday":
        await state.update_data(dt=yesterday_str())
        await state.set_state(IncomingFlow.supplier_name)
        await cb.message.answer("1) Поставщик (имя):")
    else:
        await state.set_state(IncomingFlow.dt)
        await cb.message.answer("Введи дату в формате YYYY-MM-DD, например 2025-12-03:")
    await cb.answer()


@router.message(IncomingFlow.dt)
async def incoming_dt_manual(message: Message, state: FSMContext):
    d = parse_date_yyyy_mm_dd(message.text)
    if not d:
        return await message.answer("Неверный формат. Введи YYYY-MM-DD:")
    await state.update_data(dt=d)
    await state.set_state(IncomingFlow.supplier_name)
    await message.answer("1) Поставщик (имя):")


@router.message(IncomingFlow.supplier_name)
async def incoming_supplier(message: Message, state: FSMContext):
    supplier = message.text.strip()
    if not supplier:
        return await message.answer("Поставщик не может быть пустым. Введи имя:")
    await state.update_data(supplier_name=supplier)

    whs = await get_warehouses()
    if not whs:
        await state.clear()
        return await message.answer("Нет складов. Добавь: /add_warehouse Склад 1")
    await state.set_state(IncomingFlow.warehouse)
    await message.answer("2) Склад:", reply_markup=ikb_from_pairs("inc_wh", whs))


@router.callback_query(F.data.startswith("inc_wh:"), IncomingFlow.warehouse)
async def incoming_wh(cb: CallbackQuery, state: FSMContext):
    wh_id = int(cb.data.split(":")[1])
    await state.update_data(warehouse_id=wh_id)

    prods = await get_products()
    if not prods:
        await state.clear()
        return await cb.message.answer("Нет товаров. Добавь: /add_product Сахар")
    await state.set_state(IncomingFlow.product)
    await cb.message.answer("3) Товар:", reply_markup=ikb_from_pairs("inc_pr", prods))
    await cb.answer()


@router.callback_query(F.data.startswith("inc_pr:"), IncomingFlow.product)
async def incoming_product(cb: CallbackQuery, state: FSMContext):
    pr_id = int(cb.data.split(":")[1])
    await state.update_data(product_id=pr_id)
    await state.set_state(IncomingFlow.qty)
    await cb.message.answer("4) Кол-во (кг):")
    await cb.answer()


@router.message(IncomingFlow.qty)
async def incoming_qty(message: Message, state: FSMContext):
    qty = parse_pos_float(message.text)
    if qty is None or qty == 0:
        return await message.answer("Нужно число > 0. Введи кол-во (кг):")
    await state.update_data(qty_kg=qty)
    await state.set_state(IncomingFlow.price)
    await message.answer("5) Цена за 1 кг:")


@router.message(IncomingFlow.price)
async def incoming_price(message: Message, state: FSMContext):
    price = parse_pos_float(message.text)
    if price is None:
        return await message.answer("Нужно число >= 0. Введи цену за 1 кг:")
    await state.update_data(price_per_kg=price)
    data = await state.get_data()
    total = float(data["qty_kg"]) * float(price)
    await state.update_data(total=total)
    await state.set_state(IncomingFlow.delivery)
    await message.answer(f"6) Итого: {total:.2f}\n7) Доставка (0 если нет):")


@router.message(IncomingFlow.delivery)
async def incoming_delivery(message: Message, state: FSMContext):
    delivery = parse_pos_float(message.text)
    if delivery is None:
        return await message.answer("Нужно число >= 0. Введи доставку:")
    await state.update_data(delivery_cost=delivery)

    await state.set_state(IncomingFlow.payment_method)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Нал", callback_data="inc_pm:cash")],
        [InlineKeyboardButton(text="Безнал", callback_data="inc_pm:bank")],
    ])
    await message.answer("8) Оплата:", reply_markup=kb)


@router.callback_query(F.data.startswith("inc_pm:"), IncomingFlow.payment_method)
async def incoming_pm(cb: CallbackQuery, state: FSMContext):
    pm = cb.data.split(":")[1]
    await state.update_data(payment_method=pm)
    accs = await get_accounts_by_type(pm)
    if not accs:
        await state.clear()
        return await cb.message.answer(f"Нет счетов типа {pm}. Добавь: /add_account {pm} Название")
    await state.set_state(IncomingFlow.account)
    await cb.message.answer("Выбери счет:", reply_markup=ikb_from_pairs("inc_acc", accs))
    await cb.answer()


@router.callback_query(F.data.startswith("inc_acc:"), IncomingFlow.account)
async def incoming_account(cb: CallbackQuery, state: FSMContext):
    acc_id = int(cb.data.split(":")[1])
    await state.update_data(account_id=acc_id)
    await state.set_state(IncomingFlow.comment)
    await cb.message.answer("Комментарий:", reply_markup=skip_kb("inc_comment:skip"))
    await cb.answer()


@router.callback_query(F.data == "inc_comment:skip", IncomingFlow.comment)
async def incoming_comment_skip(cb: CallbackQuery, state: FSMContext):
    await state.update_data(comment="")
    await incoming_confirm_preview(cb.message, state)
    await cb.answer()


@router.message(IncomingFlow.comment)
async def incoming_comment(message: Message, state: FSMContext):
    await state.update_data(comment=message.text.strip())
    await incoming_confirm_preview(message, state)


async def incoming_confirm_preview(message: Message, state: FSMContext):
    data = await state.get_data()
    wh = await get_name_by_id(Warehouse, int(data["warehouse_id"]))
    pr = await get_name_by_id(Product, int(data["product_id"]))
    acc = await get_account_label(int(data["account_id"]))
    text = (
        "🟢 Приход (проверь):\n"
        f"Дата: {data['dt']}\n"
        f"Поставщик: {data['supplier_name']}\n"
        f"Склад: {wh}\n"
        f"Товар: {pr}\n"
        f"Кол-во: {data['qty_kg']} кг\n"
        f"Цена/кг: {data['price_per_kg']}\n"
        f"Итого: {data['total']:.2f}\n"
        f"Доставка: {data['delivery_cost']:.2f}\n"
        f"Деньги: {acc}\n"
        f"Комментарий: {data.get('comment') or '(нет)'}\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="inc_ok")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="inc_cancel")],
    ])
    await state.set_state(IncomingFlow.confirm)
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data.in_(["inc_ok", "inc_cancel"]), IncomingFlow.confirm)
async def incoming_confirm(cb: CallbackQuery, state: FSMContext):
    if cb.data == "inc_cancel":
        await state.clear()
        await cb.message.answer("Отменено.", reply_markup=main_menu_kb())
        return await cb.answer()

    data = await state.get_data()
    async with Session() as s:
        mode = data.get("mode", "new")
        edit_id = data.get("edit_id")

        # edit -> rollback old purchase
        if mode == "edit" and edit_id:
            old = (await s.execute(select(Purchase).where(Purchase.id == int(edit_id)))).scalar_one_or_none()
            if not old:
                await state.clear()
                await cb.message.answer("❌ Приход для редактирования не найден.", reply_markup=main_menu_kb())
                return await cb.answer()
            # rollback: stock -= old.qty, money += old.total
            ok = await stock_sub(s, int(old.warehouse_id), int(old.product_id), float(old.qty_kg))
            if not ok:
                # теоретически может не хватить, если уже продали больше чем осталось, тогда редактирование опасно
                await state.clear()
                await cb.message.answer("❌ Нельзя отредактировать: не хватает остатков для отката старого прихода.", reply_markup=main_menu_kb())
                return await cb.answer()
            await money_add(s, int(old.account_id), float(old.total))
            await s.delete(old)
            await s.flush()

        doc = Purchase(
            dt=str(data["dt"]),
            supplier_name=str(data["supplier_name"]),
            warehouse_id=int(data["warehouse_id"]),
            product_id=int(data["product_id"]),
            qty_kg=float(data["qty_kg"]),
            price_per_kg=float(data["price_per_kg"]),
            total=float(data["total"]),
            delivery_cost=float(data["delivery_cost"]),
            payment_method=str(data["payment_method"]),
            account_id=int(data["account_id"]),
            comment=str(data.get("comment", "")),
        )
        s.add(doc)
        await stock_add(s, int(data["warehouse_id"]), int(data["product_id"]), float(data["qty_kg"]))
        await money_sub(s, int(data["account_id"]), float(data["total"]))  # закупка
        await s.commit()

    await state.clear()
    await cb.message.answer("✅ Приход сохранён (или обновлён).", reply_markup=main_menu_kb())
    await cb.answer()


# ===================== SALE (Продажа) =====================
@router.message(F.text == "🔴 Продажа")
async def sale_start(message: Message, state: FSMContext):
    await state.clear()
    await state.update_data(mode="new", edit_id=None)
    await state.set_state(SaleFlow.date_mode)
    await ask_date(message, "sale_date")


@router.callback_query(F.data.startswith("sale_date:"), SaleFlow.date_mode)
async def sale_date_mode(cb: CallbackQuery, state: FSMContext):
    mode = cb.data.split(":")[1]
    if mode == "today":
        await state.update_data(dt=today_str())
        await state.set_state(SaleFlow.customer_name)
        await cb.message.answer("1) Введи имя клиента:")
    elif mode == "yesterday":
        await state.update_data(dt=yesterday_str())
        await state.set_state(SaleFlow.customer_name)
        await cb.message.answer("1) Введи имя клиента:")
    else:
        await state.set_state(SaleFlow.dt)
        await cb.message.answer("Введи дату YYYY-MM-DD:")
    await cb.answer()


@router.message(SaleFlow.dt)
async def sale_dt_manual(message: Message, state: FSMContext):
    d = parse_date_yyyy_mm_dd(message.text)
    if not d:
        return await message.answer("Неверный формат. Введи YYYY-MM-DD:")
    await state.update_data(dt=d)
    await state.set_state(SaleFlow.customer_name)
    await message.answer("1) Введи имя клиента:")


@router.message(SaleFlow.customer_name)
async def sale_customer_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if not name:
        return await message.answer("Имя не может быть пустым. Введи имя клиента:")
    await state.update_data(customer_name=name)
    await state.set_state(SaleFlow.customer_phone)
    await message.answer("2) Введи номер клиента (телефон):")


@router.message(SaleFlow.customer_phone)
async def sale_customer_phone(message: Message, state: FSMContext):
    phone = normalize_phone(message.text)
    if len(phone) < 5:
        return await message.answer("Номер слишком короткий. Введи телефон ещё раз (можно с +):")
    await state.update_data(customer_phone=phone)

    whs = await get_warehouses()
    if not whs:
        await state.clear()
        return await message.answer("Нет складов. Добавь: /add_warehouse Склад 1")
    await state.set_state(SaleFlow.warehouse)
    await message.answer("3) Склад:", reply_markup=ikb_from_pairs("sale_wh", whs))


@router.callback_query(F.data.startswith("sale_wh:"), SaleFlow.warehouse)
async def sale_wh(cb: CallbackQuery, state: FSMContext):
    wid = int(cb.data.split(":")[1])
    await state.update_data(warehouse_id=wid)

    prods = await get_products()
    if not prods:
        await state.clear()
        return await cb.message.answer("Нет товаров. Добавь: /add_product Сахар")
    await state.set_state(SaleFlow.product)
    await cb.message.answer("4) Товар:", reply_markup=ikb_from_pairs("sale_pr", prods))
    await cb.answer()


@router.callback_query(F.data.startswith("sale_pr:"), SaleFlow.product)
async def sale_product(cb: CallbackQuery, state: FSMContext):
    pid = int(cb.data.split(":")[1])
    await state.update_data(product_id=pid)
    await state.set_state(SaleFlow.qty)
    await cb.message.answer("5) Кол-во (кг):")
    await cb.answer()


@router.message(SaleFlow.qty)
async def sale_qty(message: Message, state: FSMContext):
    qty = parse_pos_float(message.text)
    if qty is None or qty == 0:
        return await message.answer("Нужно число > 0. Введи кол-во (кг):")

    data = await state.get_data()
    async with Session() as s:
        st = (await s.execute(
            select(Stock.qty_kg).where(
                Stock.warehouse_id == int(data["warehouse_id"]),
                Stock.product_id == int(data["product_id"]),
            )
        )).scalar_one_or_none()
    st = float(st or 0.0)
    if qty > st:
        return await message.answer(f"❌ Недостаточно на складе. Остаток: {st:.3f} кг. Введи другое кол-во:")

    await state.update_data(qty_kg=qty)
    await state.set_state(SaleFlow.price)
    await message.answer("6) Цена за 1 кг:")


@router.message(SaleFlow.price)
async def sale_price(message: Message, state: FSMContext):
    price = parse_pos_float(message.text)
    if price is None:
        return await message.answer("Нужно число >= 0. Введи цену за 1 кг:")
    await state.update_data(price_per_kg=price)
    data = await state.get_data()
    total = float(data["qty_kg"]) * float(price)
    await state.update_data(total=total)
    await state.set_state(SaleFlow.delivery)
    await message.answer(f"7) Итого: {total:.2f}\n8) Доставка (0 если нет):")


@router.message(SaleFlow.delivery)
async def sale_delivery(message: Message, state: FSMContext):
    delivery = parse_pos_float(message.text)
    if delivery is None:
        return await message.answer("Нужно число >= 0. Введи доставку:")
    await state.update_data(delivery_cost=delivery)

    await state.set_state(SaleFlow.payment_method)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Нал", callback_data="sale_pm:cash")],
        [InlineKeyboardButton(text="Безнал", callback_data="sale_pm:bank")],
    ])
    await message.answer("9) Оплата:", reply_markup=kb)


@router.callback_query(F.data.startswith("sale_pm:"), SaleFlow.payment_method)
async def sale_pm(cb: CallbackQuery, state: FSMContext):
    pm = cb.data.split(":")[1]
    await state.update_data(payment_method=pm)
    accs = await get_accounts_by_type(pm)
    if not accs:
        await state.clear()
        return await cb.message.answer(f"Нет счетов типа {pm}. Добавь: /add_account {pm} Название")
    await state.set_state(SaleFlow.account)
    await cb.message.answer("Выбери счет:", reply_markup=ikb_from_pairs("sale_acc", accs))
    await cb.answer()


@router.callback_query(F.data.startswith("sale_acc:"), SaleFlow.account)
async def sale_account(cb: CallbackQuery, state: FSMContext):
    acc_id = int(cb.data.split(":")[1])
    await state.update_data(account_id=acc_id)
    await state.set_state(SaleFlow.comment)
    await cb.message.answer("Комментарий:", reply_markup=skip_kb("sale_comment:skip"))
    await cb.answer()


@router.callback_query(F.data == "sale_comment:skip", SaleFlow.comment)
async def sale_comment_skip(cb: CallbackQuery, state: FSMContext):
    await state.update_data(comment="")
    await sale_confirm_preview(cb.message, state)
    await cb.answer()


@router.message(SaleFlow.comment)
async def sale_comment(message: Message, state: FSMContext):
    await state.update_data(comment=message.text.strip())
    await sale_confirm_preview(message, state)


async def sale_confirm_preview(message: Message, state: FSMContext):
    data = await state.get_data()
    wh = await get_name_by_id(Warehouse, int(data["warehouse_id"]))
    pr = await get_name_by_id(Product, int(data["product_id"]))
    acc = await get_account_label(int(data["account_id"]))
    text = (
        "🔴 Продажа (проверь):\n"
        f"Дата: {data['dt']}\n"
        f"Клиент: {data['customer_name']} ({data['customer_phone']})\n"
        f"Склад: {wh}\n"
        f"Товар: {pr}\n"
        f"Кол-во: {data['qty_kg']} кг\n"
        f"Цена/кг: {data['price_per_kg']}\n"
        f"Итого: {data['total']:.2f}\n"
        f"Доставка: {data['delivery_cost']:.2f}\n"
        f"Деньги: {acc}\n"
        f"Комментарий: {data.get('comment') or '(нет)'}\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="sale_ok")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="sale_cancel")],
    ])
    await state.set_state(SaleFlow.confirm)
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data.in_(["sale_ok", "sale_cancel"]), SaleFlow.confirm)
async def sale_confirm(cb: CallbackQuery, state: FSMContext):
    if cb.data == "sale_cancel":
        await state.clear()
        await cb.message.answer("Отменено.", reply_markup=main_menu_kb())
        return await cb.answer()

    data = await state.get_data()
    async with Session() as s:
        mode = data.get("mode", "new")
        edit_id = data.get("edit_id")

        # edit -> rollback old sale
        if mode == "edit" and edit_id:
            old = (await s.execute(select(Sale).where(Sale.id == int(edit_id)))).scalar_one_or_none()
            if not old:
                await state.clear()
                await cb.message.answer("❌ Продажа для редактирования не найдена.", reply_markup=main_menu_kb())
                return await cb.answer()
            await stock_add(s, int(old.warehouse_id), int(old.product_id), float(old.qty_kg))
            await money_sub(s, int(old.account_id), float(old.total))
            await s.delete(old)
            await s.flush()

        customer_id = await upsert_customer(s, data["customer_name"], data["customer_phone"])

        ok = await stock_sub(s, int(data["warehouse_id"]), int(data["product_id"]), float(data["qty_kg"]))
        if not ok:
            await state.clear()
            await cb.message.answer("❌ Недостаточно остатков на складе.", reply_markup=main_menu_kb())
            return await cb.answer()

        doc = Sale(
            dt=str(data["dt"]),
            customer_id=int(customer_id),
            warehouse_id=int(data["warehouse_id"]),
            product_id=int(data["product_id"]),
            qty_kg=float(data["qty_kg"]),
            price_per_kg=float(data["price_per_kg"]),
            total=float(data["total"]),
            delivery_cost=float(data["delivery_cost"]),
            payment_method=str(data["payment_method"]),
            account_id=int(data["account_id"]),
            comment=str(data.get("comment", "")),
        )
        s.add(doc)
        await money_add(s, int(data["account_id"]), float(data["total"]))
        await s.commit()

    await state.clear()
    await cb.message.answer("✅ Продажа сохранена (или обновлена).", reply_markup=main_menu_kb())
    await cb.answer()


# ===================== LISTS: Purchases & Sales =====================
@router.message(F.text == "📄 Продажи")
async def sales_list(message: Message):
    async with Session() as s:
        rows = (await s.execute(
            select(Sale).options(selectinload(Sale.customer)).order_by(desc(Sale.id)).limit(50)
        )).scalars().all()

    if not rows:
        return await message.answer("Продаж пока нет.")

    lines = ["📄 Последние 50 продаж:"]
    kb_rows = []
    for i, sale in enumerate(rows, start=1):
        wh = await get_name_by_id(Warehouse, int(sale.warehouse_id))
        pr = await get_name_by_id(Product, int(sale.product_id))
        acc = await get_account_label(int(sale.account_id))
        cust = f"{sale.customer.name} ({sale.customer.phone})" if sale.customer else f"#{sale.customer_id}"
        lines.append(f"{i}) [{sale.id}] {sale.dt} | {cust} | {wh} | {pr} | {sale.qty_kg}кг x {sale.price_per_kg} = {sale.total:.2f} | {acc}")
        kb_rows.append([InlineKeyboardButton(text=f"{i}) Открыть [{sale.id}]", callback_data=f"sale_open:{sale.id}")])

    await message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


@router.callback_query(F.data.startswith("sale_open:"))
async def sale_open(cb: CallbackQuery):
    sale_id = int(cb.data.split(":")[1])
    async with Session() as s:
        sale = (await s.execute(
            select(Sale).options(selectinload(Sale.customer)).where(Sale.id == sale_id)
        )).scalar_one_or_none()

    if not sale:
        await cb.message.answer("Не найдено.")
        return await cb.answer()

    wh = await get_name_by_id(Warehouse, int(sale.warehouse_id))
    pr = await get_name_by_id(Product, int(sale.product_id))
    acc = await get_account_label(int(sale.account_id))
    cust = f"{sale.customer.name} ({sale.customer.phone})" if sale.customer else f"#{sale.customer_id}"

    text = (
        f"🧾 Продажа [{sale.id}]\n"
        f"Дата: {sale.dt}\n"
        f"Клиент: {cust}\n"
        f"Склад: {wh}\n"
        f"Товар: {pr}\n"
        f"Кол-во: {sale.qty_kg} кг\n"
        f"Цена/кг: {sale.price_per_kg}\n"
        f"Итого: {sale.total:.2f}\n"
        f"Доставка: {sale.delivery_cost:.2f}\n"
        f"Деньги: {acc}\n"
        f"Комментарий: {sale.comment or '(нет)'}\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить", callback_data=f"sale_edit:{sale.id}")],
        [InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"sale_del:{sale.id}")],
    ])
    await cb.message.answer(text, reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("sale_del:"))
async def sale_delete(cb: CallbackQuery):
    sale_id = int(cb.data.split(":")[1])
    async with Session() as s:
        sale = (await s.execute(select(Sale).where(Sale.id == sale_id))).scalar_one_or_none()
        if not sale:
            await cb.message.answer("Не найдено.")
            return await cb.answer()

        await stock_add(s, int(sale.warehouse_id), int(sale.product_id), float(sale.qty_kg))
        await money_sub(s, int(sale.account_id), float(sale.total))
        await s.delete(sale)
        await s.commit()

    await cb.message.answer(f"✅ Продажа [{sale_id}] удалена. Остатки/деньги откатили.")
    await cb.answer()


@router.callback_query(F.data.startswith("sale_edit:"))
async def sale_edit(cb: CallbackQuery, state: FSMContext):
    sale_id = int(cb.data.split(":")[1])
    async with Session() as s:
        sale = (await s.execute(select(Sale).where(Sale.id == sale_id))).scalar_one_or_none()
    if not sale:
        await cb.message.answer("Не найдено.")
        return await cb.answer()

    await state.clear()
    await state.update_data(mode="edit", edit_id=sale_id)
    await state.set_state(SaleFlow.date_mode)
    await cb.message.answer(f"✏️ Редактирование продажи [{sale_id}]")
    await ask_date(cb.message, "sale_date")
    await cb.answer()


@router.message(F.text == "📄 Приходы")
async def purchases_list(message: Message):
    async with Session() as s:
        rows = (await s.execute(select(Purchase).order_by(desc(Purchase.id)).limit(50))).scalars().all()

    if not rows:
        return await message.answer("Приходов пока нет.")

    lines = ["📄 Последние 50 приходов:"]
    kb_rows = []
    for i, p in enumerate(rows, start=1):
        wh = await get_name_by_id(Warehouse, int(p.warehouse_id))
        pr = await get_name_by_id(Product, int(p.product_id))
        acc = await get_account_label(int(p.account_id))
        lines.append(f"{i}) [{p.id}] {p.dt} | {p.supplier_name} | {wh} | {pr} | {p.qty_kg}кг x {p.price_per_kg} = {p.total:.2f} | {acc}")
        kb_rows.append([InlineKeyboardButton(text=f"{i}) Открыть [{p.id}]", callback_data=f"pur_open:{p.id}")])

    await message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


@router.callback_query(F.data.startswith("pur_open:"))
async def purchase_open(cb: CallbackQuery):
    pid = int(cb.data.split(":")[1])
    async with Session() as s:
        p = (await s.execute(select(Purchase).where(Purchase.id == pid))).scalar_one_or_none()
    if not p:
        await cb.message.answer("Не найдено.")
        return await cb.answer()

    wh = await get_name_by_id(Warehouse, int(p.warehouse_id))
    pr = await get_name_by_id(Product, int(p.product_id))
    acc = await get_account_label(int(p.account_id))

    text = (
        f"🧾 Приход [{p.id}]\n"
        f"Дата: {p.dt}\n"
        f"Поставщик: {p.supplier_name}\n"
        f"Склад: {wh}\n"
        f"Товар: {pr}\n"
        f"Кол-во: {p.qty_kg} кг\n"
        f"Цена/кг: {p.price_per_kg}\n"
        f"Итого: {p.total:.2f}\n"
        f"Доставка: {p.delivery_cost:.2f}\n"
        f"Деньги: {acc}\n"
        f"Комментарий: {p.comment or '(нет)'}\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить", callback_data=f"pur_edit:{p.id}")],
        [InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"pur_del:{p.id}")],
    ])
    await cb.message.answer(text, reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("pur_del:"))
async def purchase_delete(cb: CallbackQuery):
    pid = int(cb.data.split(":")[1])
    async with Session() as s:
        p = (await s.execute(select(Purchase).where(Purchase.id == pid))).scalar_one_or_none()
        if not p:
            await cb.message.answer("Не найдено.")
            return await cb.answer()

        # откат: stock -= qty, money += total
        ok = await stock_sub(s, int(p.warehouse_id), int(p.product_id), float(p.qty_kg))
        if not ok:
            await cb.message.answer("❌ Нельзя удалить: не хватает остатков для отката (часть уже продана).")
            return await cb.answer()

        await money_add(s, int(p.account_id), float(p.total))
        await s.delete(p)
        await s.commit()

    await cb.message.answer(f"✅ Приход [{pid}] удалён. Остатки/деньги откатили.")
    await cb.answer()


@router.callback_query(F.data.startswith("pur_edit:"))
async def purchase_edit(cb: CallbackQuery, state: FSMContext):
    pid = int(cb.data.split(":")[1])
    async with Session() as s:
        p = (await s.execute(select(Purchase).where(Purchase.id == pid))).scalar_one_or_none()
    if not p:
        await cb.message.answer("Не найдено.")
        return await cb.answer()

    await state.clear()
    await state.update_data(mode="edit", edit_id=pid)
    await state.set_state(IncomingFlow.date_mode)
    await cb.message.answer(f"✏️ Редактирование прихода [{pid}]")
    await ask_date(cb.message, "inc_date")
    await cb.answer()


async def main():
    if not TOKEN:
        raise RuntimeError("Не найден BOT_TOKEN в .env")
    await init_db()
    bot = Bot(TOKEN)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


