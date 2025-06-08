FROM python:3.11-slim

WORKDIR /app

# Устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY collector.py .

# Создаем non-root пользователя для безопасности
RUN addgroup --system app && \
    adduser --system --group app

# Переключаемся на non-root пользователя
USER app

# Запускаем наш пылесос
CMD ["python", "collector.py"]