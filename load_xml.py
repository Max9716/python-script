import os
import orjson
import datetime
from dateutil import parser
from django.db import connection
from django.utils import timezone
from django.conf import settings
from concurrent.futures import ThreadPoolExecutor, as_completed

JSONL_FOLDER = os.path.join(settings.BASE_DIR, "main", "base_flat")
BATCH_SIZE = 5000
MAX_WORKERS = 6


def process_file(filepath):
    """Генератор построчного чтения .jsonl без загрузки в память"""
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            flat = orjson.loads(line)
            flat_id = flat.get("flat_id")
            if not flat_id:
                continue
            yield (
                str(flat_id),
                flat.get("number", ""),
                flat.get("number_on_floor", ""),
                flat.get("complex", ""),
                flat.get("complex_id", ""),
                flat.get("house", ""),
                flat.get("house_id", ""),
                flat.get("floor", ""),
                flat.get("section", ""),
                flat.get("rooms", ""),
                flat.get("type_room", ""),
                flat.get("price", ""),
                flat.get("price_base", ""),
                flat.get("area", ""),
                flat.get("areaH", ""),
                flat.get("areaK", ""),
                flat.get("status", ""),
                flat.get("decoration", ""),
                flat.get("plan", ""),
                flat.get("floor_plan", ""),
                flat.get("fid_id", ""),
                flat.get("date", None)
            )


def get_or_create_roomtype(cursor, source_id, roomtype_name):
    roomtype_name = roomtype_name.strip() if roomtype_name else ""
    roomtype_id = f"{source_id}_{roomtype_name}"

    cursor.execute("SELECT normalized_name FROM main_roomtype WHERE id = %s", [roomtype_id])
    row = cursor.fetchone()
    if not row:
        cursor.execute("""
            INSERT INTO main_roomtype (id, source_id, name, normalized_name)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE name = VALUES(name)
        """, [roomtype_id, source_id, roomtype_name, ""])
        return ""
    return row[0] or ""

def get_or_create_statustype(cursor, source_id, statustype_name):
    statustype_name = statustype_name.strip() if statustype_name else ""
    statustype_id = f"{source_id}_{statustype_name}"

    cursor.execute("SELECT normalized_name FROM main_statustype WHERE id = %s", [statustype_id])
    row = cursor.fetchone()
    if not row:
        cursor.execute("""
            INSERT INTO main_statustype (id, source_id, name, normalized_name)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE name = VALUES(name)
        """, [statustype_id, source_id, statustype_name, ""])
        return ""
    return row[0] or ""


def load_from_xml(filename=None, batch_size=BATCH_SIZE, max_workers=MAX_WORKERS):
    """Загружает данные из JSONL в таблицу main_flat без удаления старых квартир."""

    # 🔹 Определяем список файлов
    if filename:
        files_to_process = [os.path.join(JSONL_FOLDER, filename)]
    else:
        files_to_process = [
            os.path.join(JSONL_FOLDER, f)
            for f in os.listdir(JSONL_FOLDER)
            if f.endswith(".jsonl") and os.path.getsize(os.path.join(JSONL_FOLDER, f)) > 0
        ]

    if not files_to_process:
        print("⚠️ Нет файлов для загрузки.")
        return

    columns = [
        "id_flat", "number", "number_on_floor", "complex", "id_complex",
        "house", "id_house", "floor", "section", "rooms",
        "flat_type", "price", "price_base", "square", "square_live",
        "square_hook", "status", "decoration", "plan", "floor_plan", "fid_id", "date"
    ]

    insert_sql = f"""
    INSERT INTO main_flats ({", ".join(columns)})
    VALUES ({", ".join(["%s"] * len(columns))})
    ON DUPLICATE KEY UPDATE
        number=VALUES(number),
        number_on_floor=VALUES(number_on_floor),
        complex=VALUES(complex),
        id_complex=VALUES(id_complex),
        house=VALUES(house),
        id_house=VALUES(id_house),
        floor=VALUES(floor),
        section=VALUES(section),
        rooms=VALUES(rooms),
        flat_type=VALUES(flat_type),
        price=VALUES(price),
        price_base=VALUES(price_base),
        square=VALUES(square),
        square_live=VALUES(square_live),
        square_hook=VALUES(square_hook),
        status=VALUES(status),
        decoration=VALUES(decoration),
        plan=VALUES(plan),
        floor_plan=VALUES(floor_plan),
        fid_id=VALUES(fid_id),
        date=VALUES(date)
    """

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, file): file for file in files_to_process}

        with connection.cursor() as cursor:
            for fut in as_completed(futures):
                file = futures[fut]
                gen = fut.result()
                buffer = []

                for flat_row in gen:
                    (
                        flat_id, number, number_on_floor, complex_name, complex_id,
                        house, house_id, floor, section, rooms, type_room,
                        price, price_base, area, areaH, areaK, status,
                        decoration, plan, floor_plan, fid_id, raw_date
                    ) = flat_row

                    flat_type_value = get_or_create_roomtype(cursor, fid_id, type_room)
                    status_value = get_or_create_statustype(cursor, fid_id, status)

                    if raw_date:
                        try:
                            date_value = parser.isoparse(raw_date)
                        except Exception:
                            date_value = timezone.now()
                    else:
                        date_value = timezone.now()

                    buffer.append((
                        flat_id, number, number_on_floor, complex_name, complex_id,
                        house, house_id, floor, section, rooms,
                        flat_type_value, price, price_base, area, areaH, areaK,
                        status_value, decoration, plan, floor_plan, fid_id, date_value
                    ))

                    if len(buffer) >= batch_size:
                        cursor.executemany(insert_sql, buffer)
                        buffer.clear()

                if buffer:
                    cursor.executemany(insert_sql, buffer)

                print(f"✅ Обработан файл: {os.path.basename(file)}")

    print("✅ Загрузка данных завершена.")
