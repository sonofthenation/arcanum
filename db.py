# db.py
import sqlite3
from typing import Optional, Sequence
import random

DB_NAME = "movies.db"
PAGE_SIZE = 10  # сколько фильмов показываем на странице по жанру


def _raise_db_error(operation: str, params: dict, exc: Exception) -> None:
    context = f"{operation} params={params}"
    raise RuntimeError(f"DB error during {context}") from exc


def get_connection():
    try:
        conn = sqlite3.connect(DB_NAME)
        return conn
    except Exception as exc:
        _raise_db_error("get_connection", {"db_name": DB_NAME}, exc)


def init_db():
    conn = None
    try:
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
    except Exception as exc:
        _raise_db_error("init_db", {}, exc)
    finally:
        if conn:
            conn.close()



def get_or_create_genre(name: str) -> int:
    """Возвращает id жанра, создаёт если не существует."""
    name = name.strip().lower()
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT id FROM genres WHERE name = ?;", (name,))
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute("INSERT INTO genres (name) VALUES (?);", (name,))
        conn.commit()
        return cur.lastrowid
    except Exception as exc:
        _raise_db_error("get_or_create_genre", {"name": name}, exc)
    finally:
        if conn:
            conn.close()


def get_genre_name(genre_id: int) -> str:
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT name FROM genres WHERE id = ?;", (genre_id,))
        row = cur.fetchone()
        return row[0] if row else "unknown"
    except Exception as exc:
        _raise_db_error("get_genre_name", {"genre_id": genre_id}, exc)
    finally:
        if conn:
            conn.close()



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
    conn = None
    try:
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
        return movie_id
    except Exception as exc:
        _raise_db_error(
            "add_movie",
            {"title": title, "director": director, "genre_ids": list(genre_ids)},
            exc,
        )
    finally:
        if conn:
            conn.close()



def get_all_genres():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM genres ORDER BY name;")
        rows = cur.fetchall()
        return rows
    except Exception as exc:
        _raise_db_error("get_all_genres", {}, exc)
    finally:
        if conn:
            conn.close()


def get_random_movie():
    """
    Случайный фильм.
    Возвращает: (movie_id, title, genres_string, director, file_id)
    """
    conn = None
    try:
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
    except Exception as exc:
        _raise_db_error("get_random_movie", {}, exc)
    finally:
        if conn:
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
    conn = None
    try:
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
        return rows
    except Exception as exc:
        _raise_db_error(
            "get_movies_by_genre_id",
            {"genre_id": genre_id, "offset": offset, "limit": limit},
            exc,
        )
    finally:
        if conn:
            conn.close()


def count_movies_by_genre_id(genre_id: int) -> int:
    conn = None
    try:
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
        return count
    except Exception as exc:
        _raise_db_error("count_movies_by_genre_id", {"genre_id": genre_id}, exc)
    finally:
        if conn:
            conn.close()



def delete_genre(genre_id: int) -> bool:
    """Удаляет жанр, если он не используется. Возвращает True, если удалён."""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # есть ли фильмы с этим жанром?
        cur.execute("SELECT COUNT(*) FROM movie_genres WHERE genre_id = ?;", (genre_id,))
        (count_movies,) = cur.fetchone()
        if count_movies > 0:
            return False

        cur.execute("DELETE FROM genres WHERE id = ?;", (genre_id,))
        conn.commit()
        deleted = cur.rowcount > 0
        return deleted
    except Exception as exc:
        _raise_db_error("delete_genre", {"genre_id": genre_id}, exc)
    finally:
        if conn:
            conn.close()



def add_watch_history(user_id: int, movie_id: int):
    """Записать факт просмотра фильма пользователем."""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO watch_history (user_id, movie_id) VALUES (?, ?);",
            (user_id, movie_id),
        )
        conn.commit()
    except Exception as exc:
        _raise_db_error(
            "add_watch_history",
            {"user_id": user_id, "movie_id": movie_id},
            exc,
        )
    finally:
        if conn:
            conn.close()


def get_movie_by_id(movie_id: int):
    """
    Возвращает: (id, title, director, file_id)
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, title, director, file_id FROM movies WHERE id = ?;",
            (movie_id,),
        )
        row = cur.fetchone()
        return row
    except Exception as exc:
        _raise_db_error("get_movie_by_id", {"movie_id": movie_id}, exc)
    finally:
        if conn:
            conn.close()


def get_movie_genres(movie_id: int):
    """
    Список жанров фильма.
    Возвращает: ["драма", "фантастика", ...]
    """
    conn = None
    try:
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
        return [r[0] for r in rows]
    except Exception as exc:
        _raise_db_error("get_movie_genres", {"movie_id": movie_id}, exc)
    finally:
        if conn:
            conn.close()



def get_user_history(user_id: int, limit: int = 10):
    """
    Последние просмотренные фильмы.
    Возвращает: (movie_id, title, genres_string, director, file_id, watched_at)
    """
    conn = None
    try:
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
        return rows
    except Exception as exc:
        _raise_db_error(
            "get_user_history",
            {"user_id": user_id, "limit": limit},
            exc,
        )
    finally:
        if conn:
            conn.close()


def search_movies(query: str):
    """
    Поиск фильмов по названию, режиссёру или жанру.
    Возвращает: (movie_id, title, genres_string, director, file_id)
    """
    q = f"%{query}%"  # ищем как есть, без lower()

    conn = None
    try:
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
    except Exception as exc:
        _raise_db_error("search_movies", {"query": query}, exc)
    finally:
        if conn:
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
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # сначала чистим зависимости
        cur.execute("DELETE FROM watch_history WHERE movie_id = ?;", (movie_id,))
        cur.execute("DELETE FROM movie_genres WHERE movie_id = ?;", (movie_id,))

        # потом удаляем сам фильм
        cur.execute("DELETE FROM movies WHERE id = ?;", (movie_id,))
        conn.commit()
        deleted = cur.rowcount > 0
        return deleted
    except Exception as exc:
        _raise_db_error("delete_movie", {"movie_id": movie_id}, exc)
    finally:
        if conn:
            conn.close()

def update_movie_full(movie_id: int, title: str, director: str, genre_ids: list[int]) -> bool:
    """
    Полное обновление фильма:
    - title
    - director
    - список жанров (movie_genres перезаписывается)
    Возвращает True, если фильм существует и был обновлён.
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Проверим, что фильм есть
        cur.execute("SELECT id FROM movies WHERE id = ?;", (movie_id,))
        row = cur.fetchone()
        if not row:
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
        return True
    except Exception as exc:
        _raise_db_error(
            "update_movie_full",
            {"movie_id": movie_id, "title": title, "director": director, "genre_ids": genre_ids},
            exc,
        )
    finally:
        if conn:
            conn.close()


def count_all_movies() -> int:
    """Сколько всего фильмов в базе (для админ-пагинации)."""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM movies;")
        (count,) = cur.fetchone()
        return count
    except Exception as exc:
        _raise_db_error("count_all_movies", {}, exc)
    finally:
        if conn:
            conn.close()


def get_all_movies_with_genres_paged(offset: int, limit: int):
    """
    Пагинированный список всех фильмов.
    Возвращает: (id, title, genres_text, director, file_id)
    """
    conn = None
    try:
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
            GROUP BY
                m.id,
                m.title,
                m.director,
                m.file_id
            ORDER BY m.id
            LIMIT ? OFFSET ?;
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        return rows
    except Exception as exc:
        _raise_db_error(
            "get_all_movies_with_genres_paged",
            {"offset": offset, "limit": limit},
            exc,
        )
    finally:
        if conn:
            conn.close()


def count_movies_by_genre_admin(genre_id: int) -> int:
    """
    Сколько фильмов относится к заданному жанру (уникальные фильмы).
    """
    conn = None
    try:
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
        return count
    except Exception as exc:
        _raise_db_error("count_movies_by_genre_admin", {"genre_id": genre_id}, exc)
    finally:
        if conn:
            conn.close()


def get_movies_by_genre_admin(genre_id: int, offset: int, limit: int):
    """
    Пагинированный список фильмов для конкретного жанра.
    Возвращает: (id, title, genres_text, director, file_id)
    """
    conn = None
    try:
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
            JOIN movie_genres mg ON m.id = mg.movie_id
            LEFT JOIN genres g ON mg.genre_id = g.id
            WHERE mg.genre_id = ?
            GROUP BY
                m.id,
                m.title,
                m.director,
                m.file_id
            ORDER BY m.id
            LIMIT ? OFFSET ?;
            """,
            (genre_id, limit, offset),
        )
        rows = cur.fetchall()
        return rows
    except Exception as exc:
        _raise_db_error(
            "get_movies_by_genre_admin",
            {"genre_id": genre_id, "offset": offset, "limit": limit},
            exc,
        )
    finally:
        if conn:
            conn.close()


