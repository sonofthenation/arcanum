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
    Message as TgMessage,
    CallbackQuery as TgCallback,
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
    get_all_movies_with_genres_paged,
    count_all_movies,
    count_movies_by_genre_admin,
    get_movies_by_genre_admin,
)
import os


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value.strip()


missing_vars = []
for required_var in ("API_TOKEN", "ADMIN_ID", "BOT_USERNAME"):
    if not os.getenv(required_var):
        missing_vars.append(required_var)

if missing_vars:
    raise RuntimeError(
        "Missing required environment variables: "
        + ", ".join(missing_vars)
        + ". Set them before starting the bot."
    )

API_TOKEN = get_required_env("API_TOKEN")
BOT_USERNAME = get_required_env("BOT_USERNAME")
try:
    ADMIN_ID = int(get_required_env("ADMIN_ID"))
except ValueError as exc:
    raise RuntimeError("ADMIN_ID must be an integer Telegram user ID.") from exc
ADMIN_MOVIES_PAGE_SIZE = 10  # сколько фильмов показывать администратору на странице

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

numbers={
    1:'1️⃣',
    2:'2️⃣',
    3:'3️⃣',
    4:'4️⃣',
    5:'5️⃣',
    6:'6️⃣',
    7:'7️⃣',
    8:'8️⃣',
    9:'9️⃣',
    0:'0️⃣',
}
def num_to_sticker(num):
    return numbers.get(num)


def is_admin(user_id: int) -> bool:
    """Админ — тот, кто прошёл верификацию /admin."""
    return user_id in admin_verified

def format_admin_movie_block(movie_id: int, title: str, genres: str, director: str | None, file_id: str) -> str:
    genres_text = genres if genres else "—"
    lines = [
        f"<b>{num_to_sticker(movie_id)}</b>",
        f"<b>file_id:</b> <code>{file_id}</code>",
        f"<b>Название:</b> {title}",
        f"<b>Жанры:</b> {genres_text}",
    ]
    link = f"https://t.me/{BOT_USERNAME}?start=m{movie_id}"
    if director:
        lines.append(f"<b>Режиссёр:</b> {director}")
    lines.append(f"<b>link:</b> <code>{link}</code>")
    return "\n".join(lines)


def build_admin_movies_nav_kb(mode: str, page: int, total: int, genre_id: int | None = None) -> InlineKeyboardMarkup:
    """
    mode: "all" или "genre"
    """
    rows: list[list[InlineKeyboardButton]] = []

    if total > 0:
        max_page = (total - 1) // ADMIN_MOVIES_PAGE_SIZE
    else:
        max_page = 0

    nav_buttons: list[InlineKeyboardButton] = []

    if page > 0:
        if mode == "all":
            cb = f"adm_movies|{page - 1}"
        else:
            cb = f"adm_movies_g|{genre_id}|{page - 1}"
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=cb))

    if page < max_page:
        if mode == "all":
            cb = f"adm_movies|{page + 1}"
        else:
            cb = f"adm_movies_g|{genre_id}|{page + 1}"
        nav_buttons.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=cb))

    if nav_buttons:
        rows.append(nav_buttons)

    # Кнопка "Фильтр по жанру" — только для режима all или genre
    rows.append(
        [
            InlineKeyboardButton(
                text="🎭 Фильтр по жанру",
                callback_data="adm_movies_genres",
            )
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=rows)


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
            BotCommand(command="movies_admin", description="Список всех фильмов"),
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


@dp.message(Command("movies_admin"))
async def cmd_movies_admin(message: Message):
    if not is_admin(message.from_user.id):
        await message.reply("Доступ только для администратора.")
        return

    total = count_all_movies()
    if total == 0:
        await message.reply("В базе пока нет фильмов.")
        return

    page = 0
    offset = page * ADMIN_MOVIES_PAGE_SIZE
    rows = get_all_movies_with_genres_paged(offset, ADMIN_MOVIES_PAGE_SIZE)

    lines = [
        "🎞 <b>Список всех фильмов</b>",
        f"Страница <b>{page + 1}</b> из <b>{(total - 1) // ADMIN_MOVIES_PAGE_SIZE + 1}</b>",
        f"Всего фильмов: <b>{total}</b>",
        "",
    ]

    for movie_id, title, genres, director, file_id in rows:
        block = format_admin_movie_block(movie_id, title, genres, director, file_id)
        lines.append(block)
        lines.append("")  # пустая строка между блоками

    kb = build_admin_movies_nav_kb(mode="all", page=page, total=total)

    await message.reply("\n".join(lines), parse_mode="HTML", reply_markup=kb)


@dp.callback_query(F.data.startswith("adm_movies|"))
async def cb_admin_movies_page(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет прав.", show_alert=True)
        return

    try:
        _, page_str = callback.data.split("|", 1)
        page = int(page_str)
    except ValueError:
        await callback.answer("Ошибка страницы.", show_alert=True)
        return

    total = count_all_movies()
    if total == 0:
        await callback.message.edit_text("В базе пока нет фильмов.")
        await callback.answer()
        return

    max_page = (total - 1) // ADMIN_MOVIES_PAGE_SIZE
    if page < 0 or page > max_page:
        await callback.answer("Такой страницы нет.", show_alert=True)
        return

    offset = page * ADMIN_MOVIES_PAGE_SIZE
    rows = get_all_movies_with_genres_paged(offset, ADMIN_MOVIES_PAGE_SIZE)

    lines = [
        "🎞 <b>Список всех фильмов</b>",
        f"Страница <b>{page + 1}</b> из <b>{max_page + 1}</b>",
        f"Всего фильмов: <b>{total}</b>",
        "",
    ]

    for movie_id, title, genres, director, file_id in rows:
        block = format_admin_movie_block(movie_id, title, genres, director, file_id)
        lines.append(block)
        lines.append("")

    kb = build_admin_movies_nav_kb(mode="all", page=page, total=total)

    await callback.message.edit_text("\n".join(lines), parse_mode="HTML", reply_markup=kb)
    await callback.answer()


@dp.callback_query(F.data == "adm_movies_genres")
async def cb_admin_movies_genres(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет прав.", show_alert=True)
        return

    genres = get_all_genres()
    if not genres:
        await callback.message.edit_text("Жанров пока нет.")
        await callback.answer()
        return

    lines = ["🎭 <b>Выберите жанр для фильтрации:</b>", ""]
    rows: list[list[InlineKeyboardButton]] = []

    for genre_id, name in genres:
        lines.append(f"{genre_id}. {name.capitalize()} — {count_movies_by_genre_id(genre_id)}")
        rows.append(
            [
                InlineKeyboardButton(
                    text=name.capitalize(),
                    callback_data=f"adm_movies_g|{genre_id}|0",
                )
            ]
        )

    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    await callback.message.edit_text("\n".join(lines), parse_mode="HTML", reply_markup=kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("adm_movies_g|"))
async def cb_admin_movies_by_genre(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет прав.", show_alert=True)
        return

    try:
        _, genre_id_str, page_str = callback.data.split("|", 2)
        genre_id = int(genre_id_str)
        page = int(page_str)
    except ValueError:
        await callback.answer("Ошибка параметров жанра.", show_alert=True)
        return

    total = count_movies_by_genre_admin(genre_id)
    if total == 0:
        await callback.message.edit_text("В этом жанре пока нет фильмов.")
        await callback.answer()
        return

    max_page = (total - 1) // ADMIN_MOVIES_PAGE_SIZE
    if page < 0 or page > max_page:
        await callback.answer("Такой страницы нет.", show_alert=True)
        return

    offset = page * ADMIN_MOVIES_PAGE_SIZE
    rows = get_movies_by_genre_admin(genre_id, offset, ADMIN_MOVIES_PAGE_SIZE)

    genre_name = get_genre_name(genre_id)

    lines = [
        f"🎭 <b>Жанр:</b> {genre_name}",
        f"Страница <b>{page + 1}</b> из <b>{max_page + 1}</b>",
        f"Фильмов в этом жанре: <b>{total}</b>",
        "",
    ]

    for movie_id, title, genres, director, file_id in rows:
        block = format_admin_movie_block(movie_id, title, genres, director, file_id)
        lines.append(block)
        lines.append("")

    kb = build_admin_movies_nav_kb(mode="genre", page=page, total=total, genre_id=genre_id)

    await callback.message.edit_text("\n".join(lines), parse_mode="HTML", reply_markup=kb)
    await callback.answer()

@dp.message(Command("cancel"))
async def cmd_cancel(message: Message):
    user_id = message.from_user.id
    cancelled = False

    if user_id in edit_states:
        edit_states.pop(user_id, None)
        cancelled = True

    if user_id in add_states:
        add_states.pop(user_id, None)
        cancelled = True

    if user_id in search_states:
        search_states.pop(user_id, None)
        cancelled = True

    if cancelled:
        await message.reply("❌ Текущая операция отменена.")
    else:
        await message.reply("Сейчас нечего отменять.")

@dp.callback_query(F.data.startswith("editg|"))
async def cb_edit_genre_toggle(callback: CallbackQuery):
    user_id = callback.from_user.id
    state = edit_states.get(user_id)
    if not state or state.get("stage") != "choosing_genres":
        await callback.answer("Сейчас жанры не редактируются.", show_alert=True)
        return

    try:
        _, genre_id_str = callback.data.split("|", 1)
        genre_id = int(genre_id_str)
    except ValueError:
        await callback.answer("Ошибка жанра.", show_alert=True)
        return

    selected: list[int] = state.get("selected_genre_ids", [])
    if genre_id in selected:
        selected.remove(genre_id)
    else:
        selected.append(genre_id)
    state["selected_genre_ids"] = selected

    # Обновляем сообщение
    await callback.message.edit_reply_markup(
        reply_markup=build_edit_genres_keyboard(selected)
    )

    await callback.answer()


@dp.callback_query(F.data == "editg_done")
async def cb_edit_genres_done(callback: CallbackQuery):
    user_id = callback.from_user.id
    state = edit_states.get(user_id)
    if not state or state.get("stage") != "choosing_genres":
        await callback.answer("Сейчас жанры не редактируются.", show_alert=True)
        return

    selected: list[int] = state.get("selected_genre_ids", [])
    if not selected:
        await callback.answer("Выберите хотя бы один жанр или нажмите «Оставить жанры без изменений».", show_alert=True)
        return

    from db import update_movie_full, get_all_genres

    movie_id = state["movie_id"]
    new_title = state.get("new_title", state["orig_title"])
    new_director = state.get("new_director", state["orig_director"])

    ok = update_movie_full(movie_id, new_title, new_director, selected)

    if not ok:
        edit_states.pop(user_id, None)
        await callback.message.edit_text("Ошибка при сохранении изменений. Возможно, фильм был удалён.")
        await callback.answer()
        return

    # Красивый итог
    all_genres = get_all_genres()
    id_to_name = {gid: name for gid, name in all_genres}
    final_names = [id_to_name[gid] for gid in selected if gid in id_to_name]
    genres_text = ", ".join(final_names) if final_names else "unknown"

    text_lines = [
        "✅ Фильм обновлён.",
        f"id: {movie_id}",
        f"Название: {new_title}",
        f"Жанры: {genres_text}",
    ]
    if new_director:
        text_lines.append(f"Режиссёр: {new_director}")

    edit_states.pop(user_id, None)

    await callback.message.edit_text("\n".join(text_lines))
    await callback.answer("Сохранено.")


@dp.callback_query(F.data == "editg_skip")
async def cb_edit_genres_skip(callback: CallbackQuery):
    user_id = callback.from_user.id
    state = edit_states.get(user_id)
    if not state or state.get("stage") != "choosing_genres":
        await callback.answer("Сейчас жанры не редактируются.", show_alert=True)
        return

    from db import get_or_create_genre, update_movie_full

    # Используем оригинальные жанры
    orig_genres = state.get("orig_genres") or []
    genre_ids: list[int] = [get_or_create_genre(name) for name in orig_genres]

    movie_id = state["movie_id"]
    new_title = state.get("new_title", state["orig_title"])
    new_director = state.get("new_director", state["orig_director"])

    ok = update_movie_full(movie_id, new_title, new_director, genre_ids)

    if not ok:
        edit_states.pop(user_id, None)
        await callback.message.edit_text("Ошибка при сохранении изменений. Возможно, фильм был удалён.")
        await callback.answer()
        return

    genres_text = ", ".join(orig_genres) if orig_genres else "unknown"

    text_lines = [
        "✅ Фильм обновлён (жанры оставлены без изменений).",
        f"id: {movie_id}",
        f"Название: {new_title}",
        f"Жанры: {genres_text}",
    ]
    if new_director:
        text_lines.append(f"Режиссёр: {new_director}")

    edit_states.pop(user_id, None)

    await callback.message.edit_text("\n".join(text_lines))
    await callback.answer("Сохранено.")


@dp.callback_query(F.data == "editg_cancel")
async def cb_edit_genres_cancel(callback: CallbackQuery):
    user_id = callback.from_user.id
    if user_id in edit_states:
        edit_states.pop(user_id, None)
        await callback.message.edit_text("❌ Редактирование отменено.")
    else:
        await callback.answer("Сейчас нечего отменять.", show_alert=True)
        return

    await callback.answer()


# ==========================
#   АДМИН: РЕДАКТИРОВАНИЕ ФИЛЬМА
# ==========================
@dp.message(Command("edit"))
async def cmd_edit_movie(message: Message):
    if not is_admin(message.from_user.id):
        await message.reply("Эта команда доступна только администратору.")
        return

    total = count_all_movies()
    if total == 0:
        await message.reply("В базе пока нет фильмов.")
        return

    page = 0
    await send_edit_page(message, page)

async def send_edit_page(message_or_callback, page: int):
    total = count_all_movies()
    if total == 0:
        if isinstance(message_or_callback, Message):
            await message_or_callback.reply("В базе пока нет фильмов.")
        else:
            await message_or_callback.message.edit_text("В базе пока нет фильмов.")
        return

    max_page = (total - 1) // ADMIN_MOVIES_PAGE_SIZE
    if page < 0:
        page = 0
    if page > max_page:
        page = max_page

    offset = page * ADMIN_MOVIES_PAGE_SIZE
    rows = get_all_movies_with_genres_paged(offset, ADMIN_MOVIES_PAGE_SIZE)

    start_num = offset + 1

    lines = [
        "✏️ <b>Редактирование фильма</b>",
        f"Страница <b>{page + 1}</b> из <b>{max_page + 1}</b>",
        f"Всего фильмов: <b>{total}</b>",
        "",
        "Выберите фильм, который хотите изменить:",
        "",
    ]

    kb_rows: list[list[InlineKeyboardButton]] = []

    for i, (movie_id, title, genres, director, file_id) in enumerate(rows, start=start_num):
        lines.append(f"{i}. {title} ({genres if genres else '—'})")
        btn_text = f"{i}"
        kb_rows.append(
            [
                InlineKeyboardButton(
                    text=btn_text,
                    callback_data=f"editpick|{movie_id}|{page}",
                )
            ]
        )

    nav_buttons: list[InlineKeyboardButton] = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"editpage|{page - 1}",
            )
        )
    if page < max_page:
        nav_buttons.append(
            InlineKeyboardButton(
                text="Вперёд ➡️",
                callback_data=f"editpage|{page + 1}",
            )
        )
    if nav_buttons:
        kb_rows.append(nav_buttons)

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    text = "\n".join(lines)

    if isinstance(message_or_callback, TgMessage):
        await message_or_callback.reply(text, parse_mode="HTML", reply_markup=kb)
    else:
        await message_or_callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data.startswith("editpage|"))
async def cb_edit_page(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет прав.", show_alert=True)
        return

    try:
        _, page_str = callback.data.split("|", 1)
        page = int(page_str)
    except ValueError:
        await callback.answer("Ошибка страницы.", show_alert=True)
        return

    await send_edit_page(callback, page)
    await callback.answer()

@dp.callback_query(F.data.startswith("editpick|"))
async def cb_edit_pick(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет прав.", show_alert=True)
        return

    try:
        _, movie_id_str, page_str = callback.data.split("|", 2)
        movie_id = int(movie_id_str)
        # page = int(page_str)  # если нужно будет потом вернуться
    except ValueError:
        await callback.answer("Ошибка данных.", show_alert=True)
        return

    movie = get_movie_by_id(movie_id)
    if not movie:
        await callback.answer("Фильм не найден.", show_alert=True)
        return

    _id, title, director, file_id = movie
    genres = get_movie_genres(_id)
    genres_text = ", ".join(genres) if genres else "unknown"

    # Сохраняем состояние редактирования
    edit_states[callback.from_user.id] = {
        "stage": "waiting_title",
        "movie_id": movie_id,
        "orig_title": title,
        "orig_director": director or "",
        "orig_genres": genres,  # список строк
    }

    text_lines = [
        f"✏️ <b>Редактирование фильма id={movie_id}</b>",
        f"Текущее название: <b>{title}</b>",
        f"Текущие жанры: {genres_text}",
    ]
    if director:
        text_lines.append(f"Текущий режиссёр: {director}")

    text_lines += [
        "",
        "Отправьте <b>новое название</b> фильма,",
        "или напишите <code>-</code>, чтобы оставить без изменений.",
        "",
        "Для отмены в любой момент напишите <code>/cancel</code>.",
    ]

    await callback.message.edit_text("\n".join(text_lines), parse_mode="HTML")
    await callback.answer()


@dp.message(lambda m: m.from_user.id in edit_states and not m.text.startswith("/"))
async def process_edit_flow(message: Message):
    state = edit_states.get(message.from_user.id)
    if state is None:
        return

    stage = state["stage"]
    text = message.text.strip()

    # 1) Новое название
    if stage == "waiting_title":
        state["new_title"] = text if text != "-" else state["orig_title"]
        state["stage"] = "waiting_director"

        await message.reply(
            "Теперь отправьте *нового режиссёра*,\n"
            "или напишите `-`, чтобы оставить без изменений.\n\n"
            "Для отмены в любой момент используйте /cancel.",
            parse_mode="Markdown",
        )

    # 2) Новый режиссёр
    elif stage == "waiting_director":
        state["new_director"] = text if text != "-" else state["orig_director"]

        # Переходим к выбору жанров по кнопкам
        state["stage"] = "choosing_genres"

        from db import get_all_genres  # если не импортировано сверху, можно убрать и использовать общий импорт

        all_genres = get_all_genres()  # [(id, name), ...]
        orig_genres = state.get("orig_genres") or []
        # выберем по умолчанию те жанры, которые уже были у фильма
        selected_ids = [gid for gid, name in all_genres if name in orig_genres]
        state["selected_genre_ids"] = selected_ids

        await send_edit_genres_message(message.chat.id, message.from_user.id)

    # 3) На этапе выбора жанров текст не нужен
    elif stage == "choosing_genres":
        await message.reply(
            "Сейчас идёт выбор жанров.\n"
            "Пожалуйста, используйте кнопки под сообщением.\n\n"
            "Если хотите отменить — /cancel."
        )

def build_edit_genres_keyboard(selected_ids: list[int]) -> InlineKeyboardMarkup:
    all_genres = get_all_genres()  # [(id, name), ...]
    rows: list[list[InlineKeyboardButton]] = []

    for genre_id, name in all_genres:
        mark = "✅" if genre_id in selected_ids else "▫️"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{mark} {name}",
                    callback_data=f"editg|{genre_id}",
                )
            ]
        )

    # Управляющие кнопки
    rows.append(
        [
            InlineKeyboardButton(
                text="✅ Готово",
                callback_data="editg_done",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="↩️ Оставить жанры без изменений",
                callback_data="editg_skip",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="❌ Отмена",
                callback_data="editg_cancel",
            )
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def send_edit_genres_message(chat_id: int, user_id: int):
    """
    Показываем сообщение с выбором жанров для редактирования.
    """
    state = edit_states.get(user_id)
    if not state:
        return

    from db import get_all_genres

    all_genres = get_all_genres()
    selected_ids: list[int] = state.get("selected_genre_ids", [])

    # Текст о фильме
    title = state.get("new_title", state.get("orig_title"))
    orig_genres = state.get("orig_genres") or []
    orig_genres_text = ", ".join(orig_genres) if orig_genres else "unknown"

    # Выбранные сейчас
    id_to_name = {gid: name for gid, name in all_genres}
    selected_names = [id_to_name[gid] for gid in selected_ids if gid in id_to_name]
    selected_text = ", ".join(selected_names) if selected_names else "пока ничего не выбрано"

    text_lines = [
        f"✏️ Редактирование фильма: {title}",
        "",
        f"Текущие жанры: {orig_genres_text}",
        f"Выбранные жанры: {selected_text}",
        "",
        "Нажимайте на жанры, чтобы включать/выключать их.",
        "Когда закончите — нажмите «Готово».",
        "Или «Оставить жанры без изменений».",
        "",
        "Для отмены также можно использовать /cancel.",
    ]

    kb = build_edit_genres_keyboard(selected_ids)

    # Отправляем новое сообщение (не edit, чтобы было проще)
    await bot.send_message(chat_id, "\n".join(text_lines), reply_markup=kb)



# ==========================
#   АДМИН: УДАЛЕНИЕ ФИЛЬМА
# ==========================
@dp.message(Command("delete"))
async def cmd_delete_movie(message: Message):
    if not is_admin(message.from_user.id):
        await message.reply("Эта команда доступна только администратору.")
        return

    total = count_all_movies()
    if total == 0:
        await message.reply("В базе пока нет фильмов.")
        return

    page = 0
    await send_delete_page(message, page)

async def send_delete_page(message_or_callback, page: int):
    """
    Рисует страницу выбора фильма для удаления.
    message_or_callback: Message или CallbackQuery
    """
    total = count_all_movies()
    if total == 0:
        # сюда редко попадём, но на всякий случай
        if isinstance(message_or_callback, Message):
            await message_or_callback.reply("В базе пока нет фильмов.")
        else:
            await message_or_callback.message.edit_text("В базе пока нет фильмов.")
        return

    max_page = (total - 1) // ADMIN_MOVIES_PAGE_SIZE
    if page < 0:
        page = 0
    if page > max_page:
        page = max_page

    offset = page * ADMIN_MOVIES_PAGE_SIZE
    rows = get_all_movies_with_genres_paged(offset, ADMIN_MOVIES_PAGE_SIZE)

    start_num = offset + 1

    lines = [
        "🗑 <b>Удаление фильма</b>",
        f"Страница <b>{page + 1}</b> из <b>{max_page + 1}</b>",
        f"Всего фильмов: <b>{total}</b>",
        "",
        "Выберите фильм, который хотите удалить:",
        "",
    ]

    kb_rows: list[list[InlineKeyboardButton]] = []

    for i, (movie_id, title, genres, director, file_id) in enumerate(rows, start=start_num):
        short_title = title if len(title) <= 40 else title[:37] + "..."
        lines.append(f"{i}. {title} ({genres if genres else '—'})")
        btn_text = f"{i}"
        kb_rows.append(
            [
                InlineKeyboardButton(
                    text=btn_text,
                    callback_data=f"delpick|{movie_id}|{page}",
                )
            ]
        )

    nav_buttons: list[InlineKeyboardButton] = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"delpage|{page - 1}",
            )
        )
    if page < max_page:
        nav_buttons.append(
            InlineKeyboardButton(
                text="Вперёд ➡️",
                callback_data=f"delpage|{page + 1}",
            )
        )
    if nav_buttons:
        kb_rows.append(nav_buttons)

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    text = "\n".join(lines)

    from aiogram.types import Message as TgMessage, CallbackQuery as TgCallback

    if isinstance(message_or_callback, TgMessage):
        await message_or_callback.reply(text, parse_mode="HTML", reply_markup=kb)
    else:
        await message_or_callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data.startswith("delpage|"))
async def cb_delete_page(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет прав.", show_alert=True)
        return

    try:
        _, page_str = callback.data.split("|", 1)
        page = int(page_str)
    except ValueError:
        await callback.answer("Ошибка страницы.", show_alert=True)
        return

    await send_delete_page(callback, page)
    await callback.answer()

@dp.callback_query(F.data.startswith("delpick|"))
async def cb_delete_pick(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет прав.", show_alert=True)
        return

    try:
        _, movie_id_str, page_str = callback.data.split("|", 2)
        movie_id = int(movie_id_str)
        page = int(page_str)
    except ValueError:
        await callback.answer("Ошибка данных.", show_alert=True)
        return

    movie = get_movie_by_id(movie_id)
    if not movie:
        await callback.answer("Фильм не найден (возможно, уже удалён).", show_alert=True)
        return

    _id, title, director, file_id = movie
    genres = get_movie_genres(_id)
    genres_text = ", ".join(genres) if genres else "—"

    text_lines = [
        "🗑 <b>Подтверждение удаления</b>",
        "",
        format_admin_movie_block(_id, title, genres_text, director, file_id),
        "",
        "Вы уверены, что хотите удалить этот фильм?",
    ]

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Да, удалить",
                    callback_data=f"delyes|{movie_id}|{page}",
                ),
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data=f"delno|{page}",
                ),
            ]
        ]
    )

    await callback.message.edit_text("\n".join(text_lines), parse_mode="HTML", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("delyes|"))
async def cb_delete_yes(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет прав.", show_alert=True)
        return

    try:
        _, movie_id_str, page_str = callback.data.split("|", 2)
        movie_id = int(movie_id_str)
        page = int(page_str)
    except ValueError:
        await callback.answer("Ошибка данных.", show_alert=True)
        return

    movie = get_movie_by_id(movie_id)
    if not movie:
        await callback.answer("Фильм уже удалён.", show_alert=True)
    else:
        _id, title, director, file_id = movie
        genres = get_movie_genres(_id)
        genres_text = ", ".join(genres) if genres else "—"

        deleted = delete_movie(movie_id)
        if not deleted:
            await callback.answer("Не удалось удалить фильм.", show_alert=True)
            return

        await callback.message.edit_text(
            "🗑 Фильм удалён:\n\n" +
            format_admin_movie_block(_id, title, genres_text, director, file_id),
            parse_mode="HTML",
        )

    await callback.answer("Фильм удалён.")
    # можно сразу показать ту же страницу заново:
    await send_delete_page(callback, page)


@dp.callback_query(F.data.startswith("delno|"))
async def cb_delete_no(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет прав.", show_alert=True)
        return

    try:
        _, page_str = callback.data.split("|", 1)
        page = int(page_str)
    except ValueError:
        page = 0

    await send_delete_page(callback, page)
    await callback.answer("Отменено.")



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
