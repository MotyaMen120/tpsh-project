import json
import psycopg2
import os

conn = psycopg2.connect(
    dbname=os.environ.get("DB_NAME", "tpsh_db"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ.get("DB_PASS", "secretpassword"),
    host=os.environ.get("DB_HOST", "db"),
    port="5432"
)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS videos (
    id VARCHAR PRIMARY KEY,
    creator_id VARCHAR,
    video_created_at TIMESTAMP,
    views_count INT,
    likes_count INT,
    comments_count INT,
    reports_count INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS video_snapshots (
    id VARCHAR PRIMARY KEY,
    video_id VARCHAR REFERENCES videos(id),
    views_count INT,
    likes_count INT,
    comments_count INT,
    reports_count INT,
    delta_views_count INT,
    delta_likes_count INT,
    delta_comments_count INT,
    delta_reports_count INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
""")
conn.commit()

with open('videos.json', 'r', encoding='utf-8') as file:
    raw_data = json.load(file)

if isinstance(raw_data, dict):
    data = raw_data.get('videos', [])
else:
    data = raw_data

for video in data:
    cursor.execute("""
        INSERT INTO videos (id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """, (
        video.get('id'),
        video.get('creator_id'),
        video.get('video_created_at'),
        video.get('views_count'),
        video.get('likes_count'),
        video.get('comments_count'),
        video.get('reports_count'),
        video.get('created_at'),
        video.get('updated_at')
    ))
    
    snapshots = video.get('snapshots', [])
    for snapshot in snapshots:
        cursor.execute("""
            INSERT INTO video_snapshots (id, video_id, views_count, likes_count, comments_count, reports_count, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, (
            snapshot.get('id'),
            video.get('id'),
            snapshot.get('views_count'),
            snapshot.get('likes_count'),
            snapshot.get('comments_count'),
            snapshot.get('reports_count'),
            snapshot.get('delta_views_count'),
            snapshot.get('delta_likes_count'),
            snapshot.get('delta_comments_count'),
            snapshot.get('delta_reports_count'),
            snapshot.get('created_at'),
            snapshot.get('updated_at')
        ))

conn.commit()
cursor.close()
conn.close()