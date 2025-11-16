# bot.py
import logging
import asyncio

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
    CallbackQuery,
    BotCommand,
    BotCommandScopeChat,
)
from aiogram.filters import Command

from db import (
    init_db,
    add_movie,
    delete_genre,
    get_all_genres,
    get_genre_name,
    get_movies_by_genre_id,
    count_movies_by_genre_id,
    get_movie_by_id,
    add_watch_history,
    get_user_history,
    get_or_create_genre,
    PAGE_SIZE,
    get_random_movie,
    search_movies,
    get_movie_genres,
    delete_movie,
    update_movie_full,
)
import os

API_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "arcanumreelbot")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# "Состояния"
add_states: dict[int, dict] = {}       # добавление фильма
search_states: dict[int, bool] = {}    # поиск
edit_states: dict[int, dict] = {}      # редактирование фильма
genre_add_states: set[int] = set()     # добавление жанра (диалог)
admin_verified: set[int] = set()       # кто прошёл админ-верификацию

# Эмодзи для жанров
GENRE_EMOJIS = {
    "драма": "🎭",
    "боевик": "💥",
    "комедия": "😂",
    "ужасы": "👻",
    "хоррор": "👻",
    "научная фантастика": "🪐",
    "фэнтези": "🐉",
    "аниме": "🍥",
    "мультфильм": "🐭",
    "приключения": "🧭",
    "триллер": "😱",
    "романтика": "💖",
    "мелодрама": "💌",
    "документальный": "📚",
    "семейный": "👨‍👩‍👧",
}
DEFAULT_GENRE_EMOJI = "🎬"


def is_admin(user_id: int) -> bool:
    """Админ — тот, кто прошёл верификацию /admin."""
    return user_id in admin_verified


def format_genres_display(genres: list[str]) -> str:
    """Делаем жанры красивыми: заглавная буква + эмодзи."""
    if not genres:
        return "—"
    parts = []
    for g in genres:
        base = g.strip()
        if not base:
            continue
        lower = base.lower()
        emoji = GENRE_EMOJIS.get(lower, DEFAULT_GENRE_EMOJI)
        pretty = base.capitalize()
        parts.append(f"{emoji} {pretty}")
    return ", ".join(parts) if parts else "—"


def build_movie_caption(title: str, genres_source, director: str | None) -> str:
    """
    Единый формат описания фильма.
    genres_source — либо строка "драма, фантастика", либо список строк.
    """
    if isinstance(genres_source, str):
        genre_list = [g.strip() for g in genres_source.split(",") if g.strip()]
    else:
        genre_list = list(genres_source or [])

    genres_text = format_genres_display(genre_list)

    lines = [
        f"🎬 {title}",
        "",
        f"🎞 Жанры: {genres_text}",
    ]
    if director:
        lines.append(f"🎬 Режиссёр: {director}")
    return "\n".join(lines)


def build_movie_link_kb(movie_id: int) -> InlineKeyboardMarkup:
    """Клавиатура под фильмом — кнопка для получения ссылки."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔗 Скопировать ссылку",
                    callback_data=f"copylink|{movie_id}",
                )
            ]
        ]
    )


def build_genre_select_kb(selected_ids: set[int]) -> InlineKeyboardMarkup:
    """Клавиатура выбора жанров при добавлении фильма."""
    genres = get_all_genres()  # [(id, name), ...]
    rows: list[list[InlineKeyboardButton]] = []

    for genre_id, name in genres:
        mark = "✅" if genre_id in selected_ids else "▫️"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{mark} {name.capitalize()}",
                    callback_data=f"addg|{genre_id}",
                )
            ]
        )

    # Кнопка "Готово"
    rows.append(
        [
            InlineKeyboardButton(
                text="✅ Готово",
                callback_data="addg_done",
            )
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=rows)


# ==========================
#   АДМИН: ВЕРИФИКАЦИЯ /admin
# ==========================
@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    user_id = message.from_user.id

    if user_id != ADMIN_ID:
        await message.reply("Вы не являетесь администратором этого бота.")
        return

    first_time = user_id not in admin_verified
    admin_verified.add(user_id)

    # Настраиваем меню команд ТОЛЬКО для этого чата (т.е. твоего диалога с ботом)
    await bot.set_my_commands(
        commands=[
            BotCommand(command="add", description="Добавить фильм"),
            BotCommand(command="add_genre", description="Добавить жанр"),
            BotCommand(command="genres_admin", description="Список жанров / удалить"),
            BotCommand(command="edit", description="Редактировать фильм по id"),
            BotCommand(command="delete", description="Удалить фильм по id"),
            BotCommand(command="link", description="Ссылки на фильмы"),
        ],
        scope=BotCommandScopeChat(chat_id=message.chat.id),
    )

    text_lines = []
    if first_time:
        text_lines.append("👑 <b>Добро пожаловать в панель администратора Arcanum Movies!</b>")
    else:
        text_lines.append("👑 <b>Админ-режим уже активен.</b>")

    text_lines += [
        "",
        "Теперь в меню команд (после <code>/</code>) доступны:",
        "• <code>/add</code> — добавить фильм (в ответ на файл/видео)",
        "• <code>/add_genre</code> — добавить жанр",
        "• <code>/genres_admin</code> — список жанров и удаление",
        "• <code>/edit</code> id — редактировать фильм",
        "• <code>/delete</code> id — удалить фильм",
        "• <code>/link</code> текст — получить ссылки на фильмы",
    ]

    await message.reply("\n".join(text_lines), parse_mode="HTML")



# ==========================
#   АДМИН: РЕДАКТИРОВАНИЕ ФИЛЬМА
# ==========================
@dp.message(Command("edit"))
async def cmd_edit_movie(message: Message):
    if not is_admin(message.from_user.id):
        await message.reply("Эта команда доступна только администратору. Введите /admin.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "Использование: /edit id_фильма\n"
            "Например: /edit 5"
        )
        return

    try:
        movie_id = int(parts[1])
    except ValueError:
        await message.reply("id фильма должен быть числом. Пример: /edit 5")
        return

    movie = get_movie_by_id(movie_id)
    if not movie:
        await message.reply(f"Фильм с id={movie_id} не найден.")
        return

    _id, title, director, file_id = movie
    genres = get_movie_genres(_id)
    genres_text = ", ".join(format_genres_display(genres)) if genres else "unknown"

    edit_states[message.from_user.id] = {
        "stage": "waiting_title",
        "movie_id": movie_id,
        "orig_title": title,
        "orig_director": director or "",
        "orig_genres": format_genres_display(genres),
    }

    text_lines = [
        f"✏ Редактирование фильма id={movie_id}",
        f"Текущее название: {title}",
        f"Текущие жанры: {genres_text}",
    ]
    if director:
        text_lines.append(f"Текущий режиссёр: {director}")

    text_lines.append("")
    text_lines.append("Отправьте *новое название* фильма,")
    text_lines.append("или напишите `-`, чтобы оставить без изменений.")

    await message.reply("\n".join(text_lines), parse_mode="Markdown")


@dp.message(lambda m: m.from_user.id in edit_states)
async def process_edit_flow(message: Message):
    state = edit_states.get(message.from_user.id)
    if state is None:
        return

    stage = state["stage"]
    text = message.text.strip()

    if stage == "waiting_title":
        state["new_title"] = text if text != "-" else state["orig_title"]
        state["stage"] = "waiting_director"

        await message.reply(
            "Теперь отправьте *нового режиссёра*,\n"
            "или напишите `-`, чтобы оставить без изменений.",
            parse_mode="Markdown",
        )

    elif stage == "waiting_director":
        state["new_director"] = text if text != "-" else state["orig_director"]
        state["stage"] = "waiting_genres"

        orig_genres = state["orig_genres"]
        orig_genres_text = ", ".join(orig_genres) if orig_genres else "unknown"

        await message.reply(
            "Теперь отправьте *новые жанры* через запятую,\n"
            "например: `драма, фантастика`\n\n"
            f"Текущие жанры: {orig_genres_text}\n"
            "Или напишите `-`, чтобы оставить без изменений.",
            parse_mode="Markdown",
        )

    elif stage == "waiting_genres":
        if text != "-":
            raw_genres = [g.strip() for g in text.split(",") if g.strip()]
            if not raw_genres:
                await message.reply(
                    "Нужно указать хотя бы один жанр или `-`, чтобы оставить как есть."
                )
                return
            final_genres_names = raw_genres
        else:
            final_genres_names = state["orig_genres"] or []

        genre_ids: list[int] = []
        for g_name in final_genres_names:
            gid = get_or_create_genre(g_name)
            genre_ids.append(gid)

        movie_id = state["movie_id"]
        new_title = state.get("new_title", state["orig_title"])
        new_director = state.get("new_director", state["orig_director"])

        ok = update_movie_full(movie_id, new_title, new_director, genre_ids)
        edit_states.pop(message.from_user.id, None)

        if not ok:
            await message.reply("Ошибка при сохранении изменений. Возможно, фильм был удалён.")
            return

        genres_text = ", ".join(final_genres_names) if final_genres_names else "unknown"

        text_lines = [
            "✅ Фильм обновлён.",
            f"id: {movie_id}",
            f"Название: {new_title}",
            f"Жанры: {genres_text}",
        ]
        if new_director:
            text_lines.append(f"Режиссёр: {new_director}")

        await message.reply("\n".join(text_lines))


# ==========================
#   АДМИН: УДАЛЕНИЕ ФИЛЬМА
# ==========================
@dp.message(Command("delete"))
async def cmd_delete_movie(message: Message):
    if not is_admin(message.from_user.id):
        await message.reply("Эта команда доступна только администратору. Введите /admin.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "Использование: /delete id_фильма\n"
            "Например: /delete 5"
        )
        return

    try:
        movie_id = int(parts[1])
    except ValueError:
        await message.reply("id фильма должен быть числом. Пример: /delete 5")
        return

    movie = get_movie_by_id(movie_id)
    if not movie:
        await message.reply(f"Фильм с id={movie_id} не найден.")
        return

    _id, title, director, file_id = movie
    genres = get_movie_genres(_id)
    genres_text = ", ".join(format_genres_display(genres)) if genres else "unknown"

    deleted = delete_movie(movie_id)
    if not deleted:
        await message.reply("Не удалось удалить фильм (возможно, он уже удалён).")
        return

    text_lines = [
        "🗑 Фильм удалён из базы.",
        f"id: {movie_id}",
        f"Название: {title}",
        f"Жанры: {genres_text}",
    ]
    if director:
        text_lines.append(f"Режиссёр: {director}")

    await message.reply("\n".join(text_lines))


# ==========================
#   АДМИН: УДАЛЕНИЕ ЖАНРА
# ==========================
@dp.callback_query(F.data.startswith("genre_del|"))
async def process_genre_delete(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет прав. Введите /admin.", show_alert=True)
        return

    try:
        _, genre_id_str = callback.data.split("|", 1)
        genre_id = int(genre_id_str)
    except ValueError:
        await callback.answer("Ошибка id жанра.", show_alert=True)
        return

    genre_name = get_genre_name(genre_id)
    if genre_name == "unknown":
        await callback.answer("Жанр уже удалён или не найден.", show_alert=True)
        return

    success = delete_genre(genre_id)
    if not success:
        await callback.answer(
            f"Нельзя удалить жанр «{genre_name}»: к нему привязаны фильмы.",
            show_alert=True,
        )
        return

    await callback.answer(f"Жанр «{genre_name}» удалён.", show_alert=True)
    await callback.message.edit_text(
        "Жанр удалён. Обновлённый список можно посмотреть командой /genres_admin."
    )


# ==========================
#   АДМИН: СПИСОК ЖАНРОВ
# ==========================
@dp.message(Command("genres_admin"))
async def cmd_genres_admin(message: Message):
    if not is_admin(message.from_user.id):
        await message.reply("Доступ только для администратора. Введите /admin.")
        return

    genres = get_all_genres()
    if not genres:
        await message.reply("Жанров пока нет.")
        return

    text_lines = ["Список жанров (id — название):", ""]

    rows = []
    for genre_id, name in genres:
        text_lines.append(f"{genre_id} — {name}")
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🗑 Удалить «{name}»",
                    callback_data=f"genre_del|{genre_id}",
                )
            ]
        )

    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    text = "\n".join(text_lines)
    await message.reply(text, reply_markup=kb)


# ==========================
#   СТАРТ
# ==========================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    # deep-link: /start m123
    parts = message.text.split(maxsplit=1)
    payload = parts[1].strip() if len(parts) > 1 else None

    if payload and payload.startswith("m"):
        try:
            movie_id = int(payload[1:])
        except ValueError:
            await message.reply("Неверная ссылка на фильм.")
            return

        movie = get_movie_by_id(movie_id)
        if not movie:
            await message.reply("Фильм по этой ссылке не найден.")
            return

        _id, title, director, file_id = movie
        genres = get_movie_genres(_id)

        caption = build_movie_caption(title, genres, director)

        add_watch_history(message.from_user.id, _id)

        try:
            await message.reply_video(
                file_id,
                caption=caption,
                reply_markup=build_movie_link_kb(_id),
            )
        except Exception:
            await message.reply_document(
                file_id,
                caption=caption,
                reply_markup=build_movie_link_kb(_id),
            )

        return

    # Обычный старт
    text_lines = [
        "🎬 Добро пожаловать в <b><i>Arcanum Movies</i></b>!",
        "",
        "Я — твой личный архив фильмов:\n",
        "🔄 <b>Случайный фильм</b> — /random или кнопка «🔄Рандом»\n",
        "🎥 <b>Подбор по жанру</b> — /by_genre или «🎥По жанрам»\n",
        "🔎 <b>Поиск по названию</b>, режиссёру или жанру — /search\n",
        "⌛️ <b>История просмотров</b> — /history\n\n",
        "",
        "<i>Если вы администратор — введите</i> /admin, чтобы открыть панель управления.",
    ]

    kb = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🔄Рандом"),
                KeyboardButton(text="🎥По жанрам"),
            ],
            [
                KeyboardButton(text="🔎Поиск"),
                KeyboardButton(text="⌛️История"),
            ],
        ],
        resize_keyboard=True,
    )

    await message.reply("\n".join(text_lines), reply_markup=kb, parse_mode="HTML")


# ==========================
#   АДМИН: ДОБАВИТЬ ЖАНР (диалог)
# ==========================
@dp.message(Command("add_genre"))
async def cmd_add_genre(message: Message):
    if not is_admin(message.from_user.id):
        await message.reply("Доступ только для администратора. Введите /admin.")
        return

    genre_add_states.add(message.from_user.id)
    await message.reply("Введите название нового жанра:")


@dp.message(lambda m: m.from_user.id in genre_add_states and not m.text.startswith("/"))
async def process_add_genre_name(message: Message):
    user_id = message.from_user.id
    name = message.text.strip()
    genre_add_states.discard(user_id)

    if not name:
        await message.reply("Название жанра не может быть пустым. Попробуйте снова: /add_genre")
        return

    genre_id = get_or_create_genre(name)
    await message.reply(f"Жанр «{name}» сохранён (id={genre_id}).")


# ==========================
#   АДМИН: ДОБАВИТЬ ФИЛЬМ
# ==========================
@dp.message(Command("add"))
async def cmd_add(message: Message):
    if not is_admin(message.from_user.id):
        await message.reply("У вас нет прав добавлять фильмы. Введите /admin.")
        return

    if not message.reply_to_message:
        await message.reply("Ответьте командой /add на сообщение с фильмом или файлом.")
        return

    reply = message.reply_to_message
    file_id = None

    if reply.video:
        file_id = reply.video.file_id
    elif reply.document:
        file_id = reply.document.file_id
    else:
        await message.reply("Не вижу видео или файла в сообщении, на которое вы ответили.")
        return

    add_states[message.from_user.id] = {
        "stage": "waiting_title",
        "file_id": file_id,
    }

    await message.reply("Окей. Напишите название фильма.")


@dp.message(lambda m: m.from_user.id in add_states)
async def process_add_flow(message: Message):
    state = add_states.get(message.from_user.id)
    if state is None:
        return

    stage = state["stage"]

    if stage == "waiting_title":
        state["title"] = message.text.strip()
        state["stage"] = "waiting_director"
        await message.reply("Записал название. Теперь напишите режиссёра (можно просто имя или «не знаю»).")

    elif stage == "waiting_director":
        state["director"] = message.text.strip()
        state["stage"] = "choosing_genres"

        genres = get_all_genres()
        if not genres:
            await message.reply(
                "Пока нет ни одного жанра. Сначала добавьте жанры через /add_genre."
            )
            add_states.pop(message.from_user.id, None)
            return

        state["selected_genres"] = set()
        kb = build_genre_select_kb(set())

        await message.reply(
            "Теперь выберите жанры для фильма.\n"
            "Можно нажать несколько жанров, затем кнопку «✅ Готово».",
            reply_markup=kb,
        )


# Выбор жанров для добавления фильма
@dp.callback_query(F.data.startswith("addg|"))
async def callback_add_genre_choose(callback: CallbackQuery):
    user_id = callback.from_user.id
    state = add_states.get(user_id)
    if not state or state.get("stage") != "choosing_genres":
        await callback.answer()
        return

    try:
        _, gid_str = callback.data.split("|", 1)
        genre_id = int(gid_str)
    except ValueError:
        await callback.answer()
        return

    selected: set[int] = state.get("selected_genres", set())
    if genre_id in selected:
        selected.remove(genre_id)
    else:
        selected.add(genre_id)
    state["selected_genres"] = selected

    kb = build_genre_select_kb(selected)
    await callback.message.edit_reply_markup(reply_markup=kb)
    await callback.answer()


@dp.callback_query(F.data == "addg_done")
async def callback_add_genre_done(callback: CallbackQuery):
    user_id = callback.from_user.id
    state = add_states.get(user_id)
    if not state or state.get("stage") != "choosing_genres":
        await callback.answer()
        return

    selected: set[int] = state.get("selected_genres") or set()
    if not selected:
        await callback.answer("Выберите хотя бы один жанр.", show_alert=True)
        return

    title = state.get("title")
    director = state.get("director")
    file_id = state.get("file_id")

    if not (title and file_id):
        await callback.answer("Ошибка при сохранении фильма.", show_alert=True)
        add_states.pop(user_id, None)
        return

    genre_ids = list(selected)
    movie_id = add_movie(
        title=title,
        file_id=file_id,
        director=director,
        genre_ids=genre_ids,
    )

    # Получим названия выбранных жанров
    all_genres = dict(get_all_genres())  # id -> name
    names = [all_genres.get(gid, str(gid)) for gid in genre_ids]

    add_states.pop(user_id, None)

    text_lines = [
        "✅ Фильм добавлен в базу.",
        f"id: {movie_id}",
        f"Название: {title}",
        f"Жанры: {', '.join(names)}",
    ]
    if director:
        text_lines.append(f"Режиссёр: {director}")

    await callback.message.edit_text("\n".join(text_lines))
    await callback.answer("Фильм сохранён.")


# ==========================
#   /RANDOM + кнопка
# ==========================
@dp.message(F.text == "🔄Рандом")
async def btn_random(message: Message):
    await cmd_random(message)


@dp.message(Command("random"))
async def cmd_random(message: Message):
    movie = get_random_movie()
    if not movie:
        await message.reply("Пока нет фильмов в базе.")
        return

    movie_id, title, genres, director, file_id = movie

    caption = build_movie_caption(title, genres, director)

    add_watch_history(message.from_user.id, movie_id)

    try:
        await message.reply_video(
            file_id,
            caption=caption,
            reply_markup=build_movie_link_kb(movie_id),
        )
    except Exception:
        await message.reply_document(
            file_id,
            caption=caption,
            reply_markup=build_movie_link_kb(movie_id),
        )


# ==========================
#   /BY_GENRE + кнопка
# ==========================
@dp.message(F.text == "🎥По жанрам")
async def btn_by_genre(message: Message):
    await cmd_by_genre(message)


@dp.message(Command("by_genre"))
async def cmd_by_genre(message: Message):
    genres = get_all_genres()
    if not genres:
        await message.reply("Жанров пока нет. Сначала добавьте фильмы.")
        return

    rows = []
    for genre_id, name in genres:
        rows.append(
            [
                InlineKeyboardButton(
                    text=name.capitalize(),
                    callback_data=f"genre|{genre_id}|0",
                )
            ]
        )

    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await message.reply("Выберите жанр:", reply_markup=kb)


@dp.callback_query(F.data == "genres_list")
async def process_genres_list(callback_query: CallbackQuery):
    genres = get_all_genres()
    if not genres:
        await callback_query.message.edit_text("Жанров пока нет.")
        await callback_query.answer()
        return

    rows = []
    for genre_id, name in genres:
        rows.append(
            [
                InlineKeyboardButton(
                    text=name.capitalize(),
                    callback_data=f"genre|{genre_id}|0",
                )
            ]
        )

    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await callback_query.message.edit_text("Выберите жанр:", reply_markup=kb)
    await callback_query.answer()


def build_genre_page_kb(genre_id: int, page: int, total: int, movies):
    rows: list[list[InlineKeyboardButton]] = []

    for movie_id, title, genre_id_db, file_id in movies:
        rows.append(
            [
                InlineKeyboardButton(
                    text=title,
                    callback_data=f"movie|{movie_id}",
                )
            ]
        )

    max_page = (total - 1) // PAGE_SIZE if total > 0 else 0

    nav_buttons: list[InlineKeyboardButton] = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"genre|{genre_id}|{page - 1}",
            )
        )
    if page < max_page:
        nav_buttons.append(
            InlineKeyboardButton(
                text="Вперёд ➡️",
                callback_data=f"genre|{genre_id}|{page + 1}",
            )
        )
    if nav_buttons:
        rows.append(nav_buttons)

    rows.append(
        [
            InlineKeyboardButton(
                text="📚 Все жанры",
                callback_data="genres_list",
            )
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data.startswith("genre|"))
async def process_genre_page(callback_query: CallbackQuery):
    try:
        _, genre_id_str, page_str = callback_query.data.split("|", 2)
        genre_id = int(genre_id_str)
        page = int(page_str)
    except ValueError:
        await callback_query.answer("Ошибка параметров жанра.", show_alert=True)
        return

    total = count_movies_by_genre_id(genre_id)
    if total == 0:
        await callback_query.message.edit_text("В этом жанре пока нет фильмов.")
        await callback_query.answer()
        return

    offset = page * PAGE_SIZE
    movies = get_movies_by_genre_id(genre_id, offset=offset, limit=PAGE_SIZE)
    if not movies:
        await callback_query.answer("На этой странице нет фильмов.", show_alert=True)
        return

    genre_name = get_genre_name(genre_id)
    max_page = (total - 1) // PAGE_SIZE if total > 0 else 0

    text_lines = [
        f"🎭 Жанр: {genre_name.capitalize()}",
        f"Фильмов в жанре: {total}",
        f"Страница: {page + 1} из {max_page + 1}",
        "",
        "Выберите фильм:",
    ]
    text = "\n".join(text_lines)

    kb = build_genre_page_kb(genre_id, page, total, movies)
    await callback_query.message.edit_text(text, reply_markup=kb)
    await callback_query.answer()


# ==========================
#   ВЫБОР ФИЛЬМА
# ==========================
@dp.callback_query(F.data.startswith("movie|"))
async def process_movie_select(callback_query: types.CallbackQuery):
    try:
        _, movie_id_str = callback_query.data.split("|", 1)
        movie_id = int(movie_id_str)
    except ValueError:
        await callback_query.answer("Некорректный фильм.", show_alert=True)
        return

    movie = get_movie_by_id(movie_id)
    if not movie:
        await callback_query.answer("Фильм не найден.", show_alert=True)
        return

    _id, title, director, file_id = movie
    genres = get_movie_genres(_id)

    caption = build_movie_caption(title, genres, director)

    add_watch_history(callback_query.from_user.id, _id)

    try:
        await callback_query.message.answer_video(
            file_id,
            caption=caption,
            reply_markup=build_movie_link_kb(_id),
        )
    except Exception:
        await callback_query.message.answer_document(
            file_id,
            caption=caption,
            reply_markup=build_movie_link_kb(_id),
        )

    await callback_query.answer()


# ==========================
#   КНОПКА "СКОПИРОВАТЬ ССЫЛКУ"
# ==========================
@dp.callback_query(F.data.startswith("copylink|"))
async def process_copy_link(callback: CallbackQuery):
    try:
        _, movie_id_str = callback.data.split("|", 1)
        movie_id = int(movie_id_str)
    except ValueError:
        await callback.answer("Ошибка ссылки.", show_alert=True)
        return

    movie = get_movie_by_id(movie_id)
    if not movie:
        await callback.answer("Фильм не найден.", show_alert=True)
        return

    link = f"https://t.me/{BOT_USERNAME}?start=m{movie_id}"

    # Ссылка внутри предложения, кликабельная
    text = (
        f"🔗 <b>Ссылка на фильм:</b> "
        f"<a href=\"{link}\">открыть в Arcanum Movies</a>\n\n"
        f"Если хотите, можете зажать ссылку и скопировать её."
    )

    await callback.message.answer(text, parse_mode="HTML")
    await callback.answer("Отправил ссылку в сообщении.")



# ==========================
#   ИСТОРИЯ ПРОСМОТРОВ
# ==========================
@dp.message(F.text == "⌛️История")
async def btn_history(message: Message):
    await cmd_history(message)


@dp.message(Command("history"))
async def cmd_history(message: Message):
    rows = get_user_history(message.from_user.id, limit=10)
    if not rows:
        await message.reply("Вы ещё не смотрели фильмы через бота.")
        return

    lines = ["📜 Ваша история просмотров (последние 10):", ""]
    for idx, (movie_id, title, genres, director, file_id, watched_at) in enumerate(
        rows, start=1
    ):
        genre_list = [g.strip() for g in (genres or "").split(",") if g.strip()]
        genres_pretty = format_genres_display(genre_list)
        base = f"{idx}. {title} — {genres_pretty}"
        if director:
            base += f", реж. {director}"
        base += f" — {watched_at}"
        lines.append(base)

    await message.reply("\n".join(lines))


# ==========================
#   ПОИСК
# ==========================
@dp.message(F.text == "🔎Поиск")
async def btn_search(message: Message):
    await cmd_search(message)


@dp.message(Command("search"))
async def cmd_search(message: Message):
    search_states[message.from_user.id] = True
    await message.reply("Введите текст для поиска:")


@dp.message(lambda m: m.from_user.id in search_states and not m.text.startswith("/"))
async def process_search_input(message: Message):
    user_id = message.from_user.id
    query = message.text.strip()
    search_states.pop(user_id, None)

    if not query:
        await message.reply("Пустой запрос. Попробуйте снова /search.")
        return

    results = search_movies(query)
    if not results:
        await message.reply("Ничего не найдено 😕")
        return

    lines = [f"🔎 Найдено фильмов: {len(results)}", ""]
    rows = []

    for movie_id, title, genres, director, file_id in results:
        genre_list = [g.strip() for g in (genres or "").split(",") if g.strip()]
        genres_pretty = format_genres_display(genre_list)
        text = f"{title} — {genres_pretty}"
        if director:
            text += f", реж. {director}"

        lines.append(f"• {text}")
        rows.append(
            [
                InlineKeyboardButton(
                    text=text,
                    callback_data=f"movie|{movie_id}",
                )
            ]
        )

    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await message.reply("\n".join(lines), reply_markup=kb)


# ==========================
#   /LINK — ссылки для админа
# ==========================
@dp.message(Command("link"))
async def cmd_link(message: Message):
    if not is_admin(message.from_user.id):
        await message.reply("Эта команда доступна только администратору. Введите /admin.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "Использование: /link текст_поиска\n"
            "Например: /link интерстеллар"
        )
        return

    query = parts[1].strip()
    results = search_movies(query)
    if not results:
        await message.reply("Ничего не найдено по этому запросу.")
        return

    max_results = 15
    shown = results[:max_results]

    lines = [f"🔗 Найдено фильмов: {len(results)} (показано {len(shown)}):", ""]
    for idx, (movie_id, title, genre_name, director, file_id) in enumerate(shown, start=1):
        link = f"https://t.me/{BOT_USERNAME}?start=m{movie_id}"
        line = f"{idx}. {title} ({genre_name}"
        if director:
            line += f", реж. {director}"
        line += f")\n<a href=\"{link}\">Ссылка на фильм🔗</a>"
        lines.append(line)

    if len(results) > max_results:
        lines.append("")
        lines.append("…показаны не все, сузьте запрос для более точного списка.")

    await message.reply("\n".join(lines))


# ==========================
#   ЗАПУСК
# ==========================
async def main():
    init_db()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
