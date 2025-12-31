import os
import json
from pathlib import Path
from fastapi import FastAPI, Request
from dotenv import load_dotenv
from telegram import Bot
from bot import (
    DATA_DIR,
    ORDERS_FILE,
    ADMINS_FILE,
    PENDING_FILE,
    PROD_FILE,
    read_json,
    write_json,
    create_order,
    clear_cart,
)

BASE_DIR = Path(__file__).resolve().parent
# Load .env relative to this file to avoid cwd-dependent failures on servers
load_dotenv(dotenv_path=BASE_DIR / ".env")
TOKEN = os.getenv("TOKEN")
app = FastAPI()

def read_pending():
    try:
        return json.loads(Path(PENDING_FILE).read_text(encoding="utf-8")) if isinstance(PENDING_FILE, Path) else json.loads(Path(PENDING_FILE).read_text(encoding="utf-8"))
    except Exception:
        return []

def write_pending(data):
    Path(PENDING_FILE).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

@app.post("/yookassa/webhook")
async def yookassa_webhook(request: Request):
    data = await request.json()
    event = data.get("event")
    if event == "payment.succeeded":
        payment = data.get("object", {})
        meta = payment.get("metadata", {})
        order_id = meta.get("order_id")
        user_id = int(meta.get("user_id")) if meta.get("user_id") else None
        pend = read_pending()
        pending = next((p for p in pend if int(p.get("id", 0)) == int(order_id)), None)
        if not pending:
            return {"status": "ignored"}
        # create real order
        class U:
            def __init__(self, uid, username):
                self.id = uid
                self.username = username
                self.first_name = None
                self.last_name = None
        user = U(user_id, None)
        items = pending.get("items", [])
        address = pending.get("address", "")
        delivery = pending.get("delivery")
        order = create_order(
            user,
            items,
            address,
            delivery,
            number=pending.get("number"),
            payment_id=pending.get("payment_id"),
            created_at=pending.get("created_at"),
        )
        # decrease stock and alert admins if low/out-of-stock
        try:
            prods_all = read_json(PROD_FILE)
            events = []
            for it in order.get("items", []):
                for p in prods_all:
                    if int(p.get("id", 0)) == int(it.get("product_id", 0)):
                        old_stock = int(p.get("stock", 0) or 0)
                        p["stock"] = max(0, old_stock - int(it.get("qty", 1)))
                        new_stock = int(p.get("stock", 0) or 0)
                        if new_stock == 0:
                            events.append(("out", p.copy()))
                        elif new_stock <= 3 and old_stock > 3:
                            events.append(("low", p.copy()))
                        break
            write_json(PROD_FILE, prods_all)
            if TOKEN:
                try:
                    bot = Bot(token=TOKEN)
                    admins = read_json(ADMINS_FILE)
                    for kind, prod_event in events:
                        for aid in admins:
                            try:
                                if kind == "out":
                                    await bot.send_message(chat_id=aid, text=(
                                        "⛔ Товар закончился\n\n"
                                        f"💊 {prod_event.get('name','-')}\n"
                                        f"🆔 ID: {prod_event.get('id')}"
                                    ))
                                else:
                                    await bot.send_message(chat_id=aid, text=(
                                        "⚠️ Мало товара\n\n"
                                        f"💊 {prod_event.get('name','-')}\n"
                                        f"📦 Осталось: {prod_event.get('stock', 0)} шт\n"
                                        f"🆔 ID: {prod_event.get('id')}"
                                    ))
                            except Exception:
                                pass
                except Exception:
                    pass
        except Exception:
            pass
        # clear cart on successful payment if checkout was from cart
        try:
            if pending.get("type") == "cart" and user_id:
                clear_cart(user_id)
        except Exception:
            pass
        # remove from pending
        pend = [p for p in pend if int(p.get("id", 0)) != int(order_id)]
        write_pending(pend)
        # notify user
        if TOKEN and user_id:
            try:
                bot = Bot(token=TOKEN)
                await bot.send_message(
                    chat_id=user_id,
                    text=(
                        f"✅ Оплата заказа #{order['number']} прошла успешно\n\n"
                        "📦 Заказ оформлен. Ожидайте, когда администратор начнет обработку.\n"
                        "Когда появится ссылка для отслеживания — мы сообщим.\n\n"
                        "Вы можете смотреть статус в разделе «📦 Мои заказы»."
                    ),
                )
            except Exception:
                pass

        # notify admins about new order
        if TOKEN:
            try:
                bot = Bot(token=TOKEN)
                admins = read_json(ADMINS_FILE)
                items = order.get("items", []) or []
                lines = []
                for it in items[:10]:
                    lines.append(f"• {it.get('name','-')} × {it.get('qty',1)}")
                if len(items) > 10:
                    lines.append(f"… ещё {len(items) - 10} поз.")
                delivery = order.get("delivery") or "-"
                text = (
                    "🆕 Новый оплаченный заказ\n\n"
                    f"🧾 Заказ #{order.get('number')}\n"
                    f"💰 Сумма: {order.get('total', 0)} ₽\n"
                    f"🚚 Доставка: {delivery}\n"
                    f"📍 Адрес: {order.get('address','-')}\n\n"
                    f"👤 Клиент: @{order.get('username','')} (ID {order.get('user_id')})\n\n"
                    "📦 Товары:\n" + ("\n".join(lines) if lines else "• -")
                )
                for aid in admins:
                    try:
                        await bot.send_message(chat_id=aid, text=text)
                    except Exception:
                        pass
            except Exception:
                pass
        return {"status": "ok"}
    return {"status": "ignored"}
