import asyncio
import os
import asyncpg
from groq import AsyncGroq
from aiogram import Bot, Dispatcher, types

bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()
groq_client = AsyncGroq(api_key=os.getenv("GROQ_API_KEY"))

SYSTEM_PROMPT = """
Ты — PostgreSQL эксперт. Пиши только чистый SQL, который возвращает ровно ОДНО ЧИСЛО. Никакого текста, только SQL-код.
Схема БД:
1. Таблица videos (итоговая статистика): id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count.
2. Таблица video_snapshots (динамика): video_id (ссылается на videos.id), views_count, likes_count, comments_count, reports_count, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at.

Важные правила:
- Если вопрос о приросте за время после публикации, делай JOIN (videos.id = video_snapshots.video_id).
- Используй сложение дат: video_snapshots.created_at <= videos.video_created_at + INTERVAL 'X hours'.
- Ответ должен быть одним числом (например, SUM(video_snapshots.delta_comments_count)).
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