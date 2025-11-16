# db.py
import sqlite3
from typing import Optional, Sequence
import random

DB_NAME = "movies.db"
PAGE_SIZE = 10  # сколько фильмов показываем на странице по жанру


def get_connection():
    conn = sqlite3.connect(DB_NAME)
    return conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    # Таблица жанров
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS genres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
        """
    )

    # Таблица фильмов (БЕЗ genre_id!)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            director TEXT,
            file_id TEXT NOT NULL
        );
        """
    )

    # Связующая таблица фильм–жанр (many-to-many)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS movie_genres (
            movie_id INTEGER NOT NULL,
            genre_id INTEGER NOT NULL,
            PRIMARY KEY (movie_id, genre_id),
            FOREIGN KEY (movie_id) REFERENCES movies(id),
            FOREIGN KEY (genre_id) REFERENCES genres(id)
        );
        """
    )

    # История просмотров
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS watch_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            movie_id INTEGER NOT NULL,
            watched_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )

    conn.commit()
    conn.close()



def get_or_create_genre(name: str) -> int:
    """Возвращает id жанра, создаёт если не существует."""
    name = name.strip().lower()
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT id FROM genres WHERE name = ?;", (name,))
    row = cur.fetchone()
    if row:
        conn.close()
        return row[0]

    cur.execute("INSERT INTO genres (name) VALUES (?);", (name,))
    conn.commit()
    genre_id = cur.lastrowid
    conn.close()
    return genre_id


def get_genre_name(genre_id: int) -> str:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT name FROM genres WHERE id = ?;", (genre_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else "unknown"



def add_movie(
    title: str,
    file_id: str,
    director: Optional[str],
    genre_ids: Sequence[int]
) -> int:
    """
    Добавляет фильм и привязывает к нему жанры.
    Возвращает movie_id.
    """
    conn = get_connection()
    cur = conn.cursor()

    # создаём фильм
    cur.execute(
        "INSERT INTO movies (title, director, file_id) VALUES (?, ?, ?);",
        (title, director, file_id),
    )
    movie_id = cur.lastrowid

    # привязываем жанры
    for gid in genre_ids:
        cur.execute(
            "INSERT OR IGNORE INTO movie_genres (movie_id, genre_id) VALUES (?, ?);",
            (movie_id, gid),
        )

    conn.commit()
    conn.close()
    return movie_id



def get_all_genres():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM genres ORDER BY name;")
    rows = cur.fetchall()
    conn.close()
    return rows


def get_random_movie():
    """
    Случайный фильм.
    Возвращает: (movie_id, title, genres_string, director, file_id)
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT m.id,
               m.title,
               GROUP_CONCAT(DISTINCT g.name) AS genres,
               m.director,
               m.file_id
        FROM movies m
        LEFT JOIN movie_genres mg ON m.id = mg.movie_id
        LEFT JOIN genres g ON mg.genre_id = g.id
        GROUP BY m.id;
        """
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return None

    movie_id, title, genres, director, file_id = random.choice(rows)

    # genres приходит в виде "драма, фантастика" или None
    if genres:
        genres = ", ".join(genres.split(","))
    else:
        genres = "unknown"

    return movie_id, title, genres, director, file_id




def get_movies_by_genre_id(genre_id: int, offset=0, limit=PAGE_SIZE):
    """
    Фильмы, у которых есть этот жанр.
    Возвращает: (movie_id, title, genre_id, file_id)
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT m.id,
               m.title,
               mg.genre_id,
               m.file_id
        FROM movies m
        JOIN movie_genres mg ON m.id = mg.movie_id
        WHERE mg.genre_id = ?
        ORDER BY m.id
        LIMIT ? OFFSET ?;
        """,
        (genre_id, limit, offset),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def count_movies_by_genre_id(genre_id: int) -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(DISTINCT m.id)
        FROM movies m
        JOIN movie_genres mg ON m.id = mg.movie_id
        WHERE mg.genre_id = ?;
        """,
        (genre_id,),
    )
    (count,) = cur.fetchone()
    conn.close()
    return count



def delete_genre(genre_id: int) -> bool:
    """Удаляет жанр, если он не используется. Возвращает True, если удалён."""
    conn = get_connection()
    cur = conn.cursor()

    # есть ли фильмы с этим жанром?
    cur.execute("SELECT COUNT(*) FROM movie_genres WHERE genre_id = ?;", (genre_id,))
    (count_movies,) = cur.fetchone()
    if count_movies > 0:
        conn.close()
        return False

    cur.execute("DELETE FROM genres WHERE id = ?;", (genre_id,))
    conn.commit()
    deleted = cur.rowcount > 0
    conn.close()
    return deleted



def add_watch_history(user_id: int, movie_id: int):
    """Записать факт просмотра фильма пользователем."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO watch_history (user_id, movie_id) VALUES (?, ?);",
        (user_id, movie_id),
    )
    conn.commit()
    conn.close()


def get_movie_by_id(movie_id: int):
    """
    Возвращает: (id, title, director, file_id)
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, title, director, file_id FROM movies WHERE id = ?;",
        (movie_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def get_movie_genres(movie_id: int):
    """
    Список жанров фильма.
    Возвращает: ["драма", "фантастика", ...]
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT g.name
        FROM movie_genres mg
        JOIN genres g ON mg.genre_id = g.id
        WHERE mg.movie_id = ?
        ORDER BY g.name;
        """,
        (movie_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]



def get_user_history(user_id: int, limit: int = 10):
    """
    Последние просмотренные фильмы.
    Возвращает: (movie_id, title, genres_string, director, file_id, watched_at)
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT m.id,
               m.title,
               GROUP_CONCAT(DISTINCT g.name) AS genres,
               m.director,
               m.file_id,
               h.watched_at
        FROM watch_history h
        JOIN movies m ON h.movie_id = m.id
        LEFT JOIN movie_genres mg ON m.id = mg.movie_id
        LEFT JOIN genres g ON mg.genre_id = g.id
        WHERE h.user_id = ?
        GROUP BY m.id, h.id
        ORDER BY h.watched_at DESC
        LIMIT ?;
        """,
        (user_id, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def search_movies(query: str):
    """
    Поиск фильмов по названию, режиссёру или жанру.
    Возвращает: (movie_id, title, genres_string, director, file_id)
    """
    q = f"%{query}%"  # ищем как есть, без lower()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            m.id,
            m.title,
            COALESCE(GROUP_CONCAT(DISTINCT g.name), '') AS genres,
            m.director,
            m.file_id
        FROM movies m
        LEFT JOIN movie_genres mg ON m.id = mg.movie_id
        LEFT JOIN genres g ON mg.genre_id = g.id
        WHERE m.title    LIKE ?
           OR m.director LIKE ?
           OR g.name     LIKE ?
        GROUP BY
            m.id,
            m.title,
            m.director,
            m.file_id
        ORDER BY m.title;
        """,
        (q, q, q),
    )
    rows = cur.fetchall()
    conn.close()

    processed = []
    for movie_id, title, genres, director, file_id in rows:
        if genres:
            genres_text = ", ".join(genres.split(","))
        else:
            genres_text = "unknown"
        processed.append((movie_id, title, genres_text, director, file_id))

    return processed
def delete_movie(movie_id: int) -> bool:
    """
    Удаляет фильм и все связанные записи (жанры-связи, историю просмотров).
    Возвращает True, если фильм был удалён.
    """
    conn = get_connection()
    cur = conn.cursor()

    # сначала чистим зависимости
    cur.execute("DELETE FROM watch_history WHERE movie_id = ?;", (movie_id,))
    cur.execute("DELETE FROM movie_genres WHERE movie_id = ?;", (movie_id,))

    # потом удаляем сам фильм
    cur.execute("DELETE FROM movies WHERE id = ?;", (movie_id,))
    conn.commit()
    deleted = cur.rowcount > 0

    conn.close()
    return deleted

def update_movie_full(movie_id: int, title: str, director: str, genre_ids: list[int]) -> bool:
    """
    Полное обновление фильма:
    - title
    - director
    - список жанров (movie_genres перезаписывается)
    Возвращает True, если фильм существует и был обновлён.
    """
    conn = get_connection()
    cur = conn.cursor()

    # Проверим, что фильм есть
    cur.execute("SELECT id FROM movies WHERE id = ?;", (movie_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False

    # Обновляем title и director
    cur.execute(
        "UPDATE movies SET title = ?, director = ? WHERE id = ?;",
        (title, director, movie_id),
    )

    # Перезаписываем жанры
    cur.execute("DELETE FROM movie_genres WHERE movie_id = ?;", (movie_id,))
    for gid in genre_ids:
        cur.execute(
            "INSERT INTO movie_genres (movie_id, genre_id) VALUES (?, ?);",
            (movie_id, gid),
        )

    conn.commit()
    conn.close()
    return True



