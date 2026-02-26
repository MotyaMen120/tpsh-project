import asyncio
import os
import asyncpg
from groq import AsyncGroq
from aiogram import Bot, Dispatcher, types

bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()
groq_client = AsyncGroq(api_key=os.getenv("GROQ_API_KEY"))

SYSTEM_PROMPT = """
Ты — генератор SQL-запросов для PostgreSQL. 
Таблицы:
1. videos: id, creator_id, views_count, likes_count, comments_count, reports_count, video_created_at.
2. video_snapshots: video_id, views_count, likes_count, delta_views_count, delta_likes_count, created_at.
Правила:
- Пиши ТОЛЬКО чистый SQL-запрос.
- Запрос должен возвращать ровно ОДНО число.
- Никакого текста и оформления Markdown.
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