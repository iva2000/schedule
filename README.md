# Streamlit: Шаг 1 → Шаг 2 (одна страница)

## Быстрый старт локально
```bash
python -m venv .venv
. .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

## Развёртывание на streamlit.io
1. Залейте файлы репозитория на GitHub.
2. На streamlit.io создайте новое приложение, укажите `app.py` как Entry point.
3. Готово.

**Что умеет:**
- Загрузка исходного Excel → Шаг 1 (нормализация, «зебра» недель в XLSX).
- Результат Шага 1 автоматически идёт в Шаг 2.
- Диаграммы показываются на странице и доступны для скачивания (по одной и пакетом ZIP).
- XLSX Шага 2 содержит лист Legend, раскраску групп и «зебру» учебных недель.
