# Arcanum Movies Bot

## Environment variables

Before starting the bot, set the required environment variables:

- `API_TOKEN` — Telegram bot token.
- `ADMIN_ID` — Telegram user ID of the admin (integer).
- `BOT_USERNAME` — Bot username without the `@` sign.

You can copy `env.example` and fill in your values.

### Example

```bash
export API_TOKEN="123456:ABCDEF..."
export ADMIN_ID="123456789"
export BOT_USERNAME="mybot"
```

Then run:

```bash
python bot.py
```
