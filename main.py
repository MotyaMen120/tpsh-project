import asyncio
import os
import asyncpg
from groq import AsyncGroq
from aiogram import Bot, Dispatcher, types

bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()
groq_client = AsyncGroq(api_key=os.getenv("GROQ_API_KEY"))

SYSTEM_PROMPT = """
Ты — PostgreSQL эксперт. Твоя задача — писать ТОЛЬКО чистый SQL-запрос, который возвращает ровно ОДНО ЧИСЛО. Без markdown и текста.
Схема БД:
1. Таблица videos (итоговая статистика): id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count.
2. Таблица video_snapshots (динамика/прирост): video_id, views_count, likes_count, comments_count, reports_count, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at.

ПРАВИЛА И ПРИМЕРЫ (ОЧЕНЬ ВАЖНО):
- Если просят итоговую сумму за месяц публикации ("опубликованные в июне 2025"): SELECT SUM(views_count) FROM videos WHERE EXTRACT(MONTH FROM video_created_at) = 6 AND EXTRACT(YEAR FROM video_created_at) = 2025;
- Если просят суммарный прирост за конкретную дату ("28 ноября 2025"): SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';
- Если просят прирост за первые Х часов после публикации: SELECT SUM(video_snapshots.delta_comments_count) FROM video_snapshots JOIN videos ON videos.id = video_snapshots.video_id WHERE video_snapshots.created_at <= videos.video_created_at + INTERVAL '3 hours';
- Если просто количество видео по условию: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

Отвечай только SQL-кодом, результат которого 1 число.
"""

@dp.message()
async def handle_message(message: types.Message):
    try:
        completion = await groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": message.text}
            ]
        )
        sql_query = completion.choices[0].message.content.strip().replace('```sql', '').replace('```', '')
        
        conn = await asyncpg.connect(os.getenv("DB_URL"))
        result = await conn.fetchval(sql_query)
        await conn.close()
        
        await message.answer(str(result) if result is not None else "0")
    except Exception:
        await message.answer("0")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())