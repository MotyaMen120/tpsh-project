import asyncio
import os
import asyncpg
from groq import AsyncGroq
from aiogram import Bot, Dispatcher, types

bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()
groq_client = AsyncGroq(api_key=os.getenv("GROQ_API_KEY"))

SYSTEM_PROMPT = """
Ты — PostgreSQL эксперт. Пиши ТОЛЬКО чистый SQL-запрос, который возвращает ровно ОДНО ЧИСЛО. Без markdown и текста.
Схема БД:
1. videos: id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count.
2. video_snapshots: video_id (ссылка на videos.id), views_count, likes_count, comments_count, reports_count, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at.

КРИТИЧЕСКИЕ ПРАВИЛА:
1. Если вопрос про прирост за КОНКРЕТНУЮ ДАТУ в календаре (например, "28 ноября 2025"): фильтруй ТОЛЬКО по дате снимка: DATE(video_snapshots.created_at) = '2025-11-28'. Не привязывайся к дате публикации видео!
2. Если вопрос про прирост ЗА ВРЕМЯ ПОСЛЕ ПУБЛИКАЦИИ (например, "первые 3 часа"): делай JOIN videos ON videos.id = video_snapshots.video_id и фильтруй: video_snapshots.created_at <= videos.video_created_at + INTERVAL '3 hours'.
3. Ответ всегда одно число (например, SUM(delta_views_count)).
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