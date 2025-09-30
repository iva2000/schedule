# -*- coding: utf-8 -*-
import streamlit as st
from io import BytesIO
import hashlib, time, zipfile

from step1_core import process_workbook_step1, preview_tables_html
from step2_core import process_step2_and_plot

st.set_page_config(page_title="Обработка расписаний: Шаг 1 → Шаг 2", layout="wide")

st.title("Обработка расписаний: Шаг 1 → Шаг 2")
st.caption("Загрузка исходного Excel → нормализация (Шаг 1) → дополнительная обработка и диаграммы (Шаг 2).")

with st.expander("Параметры отображения", expanded=False):
    demo_delay = st.slider("Искусственное замедление для прогресса (сек.)", 0.0, 1.0, 0.0, 0.1)

with st.form("upload_form", clear_on_submit=False):
    uploaded = st.file_uploader("Загрузите исходный Excel", type=["xls", "xlsx"])
    run = st.form_submit_button("Обработать")

if not (run and uploaded):
    st.info("Загрузите файл и нажмите «Обработать».")
    st.stop()

raw_bytes = uploaded.read()
file_hash = hashlib.sha256(raw_bytes).hexdigest()

@st.cache_data(show_spinner=False)
def _run_step1(_raw_bytes: bytes):
    return process_workbook_step1(_raw_bytes)

@st.cache_data(show_spinner=False)
def _run_step2(_xlsx_from_step1: bytes):
    return process_step2_and_plot(_xlsx_from_step1)

progress = st.progress(0, text="Шаг 1: обработка…")
time.sleep(demo_delay)

# === ШАГ 1 ===
processed_sheets, step1_xlsx_bytes = _run_step1(raw_bytes)
progress.progress(45, text="Шаг 1: предпросмотр…")
time.sleep(demo_delay)

with st.expander("Предпросмотр данных после Шага 1 (по листам)"):
    html = preview_tables_html(processed_sheets, n=20)
    st.components.v1.html(html, height=420, scrolling=True)

st.download_button(
    label="Скачать «файл обработан.xlsx» (Шаг 1)",
    data=step1_xlsx_bytes,
    file_name="обработан.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True
)

progress.progress(60, text="Шаг 2: построение диаграмм и итогового XLSX…")
time.sleep(demo_delay)

# === ШАГ 2 ===
step2_xlsx_bytes, charts = _run_step2(step1_xlsx_bytes)
progress.progress(90, text="Шаг 2: вывод результатов…")
time.sleep(demo_delay)

st.subheader("Диаграммы (Шаг 2)")
zip_buf = BytesIO()
with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
    if charts:
        for sheet_name, png_bytes in charts:
            st.markdown(f"**Лист:** {sheet_name}")
            st.image(png_bytes, use_column_width=True)
            safe_sheet = sheet_name.replace('/', '_').replace('\\', '_')
            zf.writestr(f"диаграмма — {safe_sheet}.png", png_bytes)
            st.download_button(
                label=f"Скачать PNG — {safe_sheet}",
                data=png_bytes,
                file_name=f"диаграмма — {safe_sheet}.png",
                mime="image/png",
                use_container_width=True,
                key=f"dwn_{safe_sheet}"
            )
    else:
        st.warning("Диаграммы не сформированы.")
zip_buf.seek(0)
st.download_button(
    label="Скачать все диаграммы (ZIP)",
    data=zip_buf.getvalue(),
    file_name="все диаграммы.zip",
    mime="application/zip",
    use_container_width=True
)

st.download_button(
    label="Скачать «группы помечены цветом.xlsx» (Шаг 2)",
    data=step2_xlsx_bytes,
    file_name="группы помечены цветом.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True
)

progress.progress(100, text="Готово.")
st.success("Обработка завершена.")
