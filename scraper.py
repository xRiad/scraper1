import time
import mysql.connector
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from config import DB_CONFIG
import logging
import os
import requests
import re, unicodedata
import mimetypes
from PIL import Image
import io

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service


from urllib.parse import urlparse

service = Service("C:/webdrivers/chrome138/chromedriver.exe")

chrome_options = Options()
# chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--no-sandbox")

logging.basicConfig(
    filename='parser.log',           # Имя файла лога
    level=logging.INFO,              # Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Формат записи
    encoding='utf-8'                 # Чтобы русские символы не портились
)

driver = webdriver.Chrome(service=service, options=chrome_options)

def connect_db():
    return mysql.connector.connect(**DB_CONFIG)

# КАТЕГОРИИ

def open_kataloq_menu():
    driver.get("https://irshad.az/az")
    time.sleep(2)
    kataloq_button = driver.find_element(By.ID, "open-menu")
    kataloq_button.click()
    time.sleep(2)

def get_all_categories():
    open_kataloq_menu()
    time.sleep(1)
    wait = WebDriverWait(driver, 10)
    action = ActionChains(driver)

    db = connect_db()
    cursor = db.cursor()

    menu_items = driver.find_elements(By.CSS_SELECTOR, ".menu-section .menu__item")[1:]

    for menu in menu_items:
        try:
            # === Уровень 1 ===
            name = menu.find_element(By.CLASS_NAME, "menu__item__link").text.strip()
            slug = name.lower().replace(" ", "-")
            url = None

            first_id = save_category(cursor, name, slug, url, 1, None)

            action.move_to_element(menu).perform()
            time.sleep(2)

            sub_block = menu.find_element(By.CLASS_NAME, "menu__item__sub")

            # === Уровень 2 ===
            second_level_blocks = sub_block.find_elements(By.CSS_SELECTOR, ".menu__item__sub__item")
            # second_level_blocks = WebDriverWait(sub_block, 15).until(
            #     EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".menu__item__sub a.menu__item__sub__item__link"))
            # )
            for block in second_level_blocks:
                try:
                    link = block.find_element(By.CSS_SELECTOR, ".menu__item__sub__item__link")
                    name = link.text.strip()
                    url = link.get_attribute("href")
                    slug = url.strip("/").split("/")[-1]

                    second_id = save_category(cursor, name, slug, url, 2, first_id)

                    # === Уровень 3 ===
                    third_level_links = block.find_elements(By.CSS_SELECTOR, ".menu__item__sub__item__sub2__item a")
                    for a in third_level_links:
                        try:
                            name = a.text.strip()
                            url = a.get_attribute("href")
                            slug = url.strip("/").split("/")[-1]

                            save_category(cursor, name, slug, url, 3, second_id)
                        except Exception as e3:
                            logging.warning(f"⚠️ Ошибка при сохранении подкатегории уровня 3: {e3}")
                        continue

                except Exception as e2:
                    logging.warning(f"⛔ Ошибка уровня 2: {e2}")
                    continue

        except Exception as e:
            logging.warning(f"❌ Ошибка уровня 1: {e}")
            continue

    db.commit()
    db.close()

# БД ЗАПРОСЫ

def clear_categories():
    # Очистка таблицы categories перед выполнением скрипта
    db = connect_db()
    cursor = db.cursor()
    cursor.execute("DELETE FROM categories")
    db.commit()
    db.close()

def save_category(cursor, name, slug, url, level, parent_id):
    cursor.execute("""
        INSERT INTO categories (name, slug, url, level, parent_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
        ON DUPLICATE KEY UPDATE 
            name = VALUES(name),
            url = VALUES(url),
            level = VALUES(level),
            parent_id = VALUES(parent_id),
            updated_at = NOW()
    """, (name, slug, url, level, parent_id))
    return cursor.lastrowid

# def select_all_categories ():
#     db = connect_db()
#     try:
#         cur = db.cursor(dictionary=True)
#         cur.execute("""
#             SELECT 
#                 c.id,
#                 c.url,
#                 c.parent_id,
#                 p.name AS parent_name
#             FROM categories AS c
#             LEFT JOIN categories AS p ON c.parent_id = p.id
#             WHERE c.level = 3
#         """)
#         rows = cur.fetchall()
#         return rows  # [(id, url, parent_id, parent_name), ...]
#     finally:
#         db.close()

# Начиная с 696 возьмем
def select_all_categories(min_id=838):
    db = connect_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.execute("""
            SELECT
                c.id,
                c.url,
                c.parent_id,
                p.name AS parent_name
            FROM categories AS c
            LEFT JOIN categories AS p ON c.parent_id = p.id
            WHERE c.level = %s
              AND c.id >= %s
            ORDER BY c.id ASC
        """, (3, min_id))
        rows = cur.fetchall()
        return rows
    finally:
        db.close()

def is_product_in_db(title_key):
    db = connect_db()
    cursor = db.cursor()
    cursor.execute("SELECT id FROM products WHERE title_key = %s", (title_key,))
    result = cursor.fetchone()
    db.close()
    return result is not None

def insert_product(title, slug, url, price, sale, html_description, category_id, title_key):
    db = connect_db()
    try:
        cur = db.cursor()
        cur.execute("""
            INSERT INTO products
                (title, slug, url, price, sale, html_description, category_id, title_key)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                price = VALUES(price),
                sale = VALUES(sale),
                html_description = VALUES(html_description),
                category_id = VALUES(category_id)
        """, (title, slug, url, price, sale, html_description, category_id, title_key))

        product_id = cur.lastrowid

        if product_id != 0:
            cur.execute(
                "UPDATE products SET slug = CONCAT(%s, '-', %s) WHERE id = %s",
                (slug, product_id, product_id)
            )

        db.commit()
        return product_id
    finally:
        db.close()

def select_one_category():
    db = connect_db()
    try:
        cur = db.cursor(dictionary=True)
        cur.execute("""
            SELECT 
                c.id,
                c.url,
                c.parent_id,
                p.name AS parent_name
            FROM categories AS c
            LEFT JOIN categories AS p ON c.parent_id = p.id
            WHERE c.level = 3
            AND c.id = %s
        """, (643,))
        row = cur.fetchone()  # так как нужна одна запись
        return row  # (id, url, parent_id, parent_name)
    finally:
        db.close()

def test_request ():
    db = connect_db()
    cursor = db.cursor()
    cursor.execute("SELECT id, level FROM categories WHERE id=642;")
    result = cursor.fetchone()
    db.close()
    return result is not None
# СКРИПТЫ ДЛЯ ПРОДУКТОВ

# Парсим списки продуктов по категориям
def parse_products_for_categories():
    categories = select_all_categories()
    for category in categories:
        # category = select_one_category()
        # for category in categories:
        driver.get(category["url"])
        load_all_products(driver)
        time.sleep(3)

        products = driver.find_elements(By.CSS_SELECTOR, ".products__list__body a.product__name")

        for product in products:
            product_url = product.get_attribute("href")
            # if not is_product_in_db(product_url):  
            parse_product_details(product_url, category)

# Клики по кнопке загрузить больше продуктов
def load_all_products(driver, timeout=8):
    wait = WebDriverWait(driver, timeout)

    wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, ".products__list__body")
    ))

    last_count = 0

    while True:
        try:
            products = driver.find_elements(By.CSS_SELECTOR, ".products__list__body a.product__name")
            count = len(products)

            # Если карточек больше, чем в прошлый раз — продолжаем
            if count > last_count:
                last_count = count
            else:
                break  # новых товаров не появилось → выходим

            # Ищем кнопку
            btn = driver.find_element(By.CSS_SELECTOR, "#loadMoreBlock #loadMore")
            driver.execute_script("arguments[0].click();", btn)

            # Ждём, пока количество карточек увеличится
            wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, ".products__list__body a.product__name")) > last_count)

        except Exception:
            break
# Cохраняем картинки для продуктов
# 2025-08-09 11:01:47,508 - ERROR - Ошибка при обработке картинки https://irshad.az/az/mehsullar/iphone-16e-256-gb-white: Not enough parameters for the SQL statement
# def _guess_ext(response, url):
#     # 1) сначала по Content-Type
#     ct = (response.headers.get("Content-Type") or "").split(";")[0].strip()
#     ext = mimetypes.guess_extension(ct) if ct else None
#     # 2) если не вышло — по урлу
#     if not ext:
#         path = urlparse(url).path
#         _, ext = os.path.splitext(path)
#     # 3) дефолт
#     if not ext:
#         ext = ".jpg"
#     return ext


def save_product_images(productWrapper, category_parent_name, product_url, product_id):
    try:
        images_folder = 'uploads/products/images'  # Папка для остальных изображений
        main_image_folder = 'main_images'  # Папка для главных изображений

        # Создаем папки, если их нет
        os.makedirs(images_folder, exist_ok=True)
        os.makedirs(main_image_folder, exist_ok=True)

        imagesSlider = productWrapper.find_element(
            By.CSS_SELECTOR, ".product-view__fixed-bar__slider__thumbs"
        )

        thumb_selector = ".product-view__fixed-bar__slider__thumbs__item img"
        thumbs_count = len(imagesSlider.find_elements(By.CSS_SELECTOR, thumb_selector))

        db = connect_db()
        cursor = db.cursor()

        # Получаем title_key для этого product_id
        cursor.execute("SELECT title_key FROM products WHERE id = %s", (product_id,))
        row = cursor.fetchone()
        title_key = row[0] if row and row[0] else "no_title"

        # Чистим title_key для имени файла
        safe_title_key = "".join(
            c if c.isalnum() or c in ("-", "_") else "_" for c in title_key
        )

        main_image_set = False  # флаг для главного изображения, чтобы сделать UPDATE только один раз

        for idx in range(thumbs_count):
            try:
                # Пропускаем второе изображение (idx == 1)
                if idx == 1:
                    continue

                # Берём элемент заново на каждой итерации
                img_el = imagesSlider.find_elements(By.CSS_SELECTOR, thumb_selector)[idx]

                # Пробуем все возможные атрибуты
                img_url = (
                    img_el.get_attribute("src")
                    or img_el.get_attribute("data-src")
                    or img_el.get_attribute("data-lazy")
                    or (
                        img_el.get_attribute("srcset").split()[0]
                        if img_el.get_attribute("srcset")
                        else None
                    )
                )

                alt_text = img_el.get_attribute("alt") or ""
                if not img_url:
                    continue

                # Скачиваем изображение
                resp = requests.get(img_url, timeout=15)
                if resp.status_code != 200:
                    logging.error(f"Не удалось скачать {img_url}, статус {resp.status_code}")
                    continue

                # --- КОНВЕРТАЦИЯ В WEBP ---
                try:
                    image = Image.open(io.BytesIO(resp.content))
                    # Сохраним прозрачность, если она есть, иначе в RGB
                    if image.mode in ("RGBA", "LA", "P"):
                        # Конвертируем P (палитра) в RGBA, остальное оставим
                        if image.mode == "P":
                            image = image.convert("RGBA")
                    else:
                        image = image.convert("RGB")
                except Exception as e:
                    logging.error(f"Ошибка чтения изображения {img_url}: {e}")
                    continue

                # Генерация имени файла
                filename = f"{idx+1}_{product_id}_{safe_title_key}.webp"

                # Если это главное изображение, сохраняем в main_images/
                if not main_image_set:
                    main_image_path = os.path.join(main_image_folder, f"{product_id}_{safe_title_key}_main.webp")
                    image.save(main_image_path, "webp", quality=90, method=6)

                    # Обновляем запись с главным изображением в БД
                    cursor.execute(
                        """
                        UPDATE products
                        SET main_image = %s,
                            main_image_alt_text = %s
                        WHERE id = %s
                        """,
                        (f"main_images/{product_id}_{safe_title_key}_main.webp", alt_text, product_id),
                    )
                    main_image_set = True
                    continue  # Пропускаем остальные шаги для главного изображения

                # Сохраняем остальные изображения в uploads/products/images/
                file_path = os.path.join(images_folder, filename)
                try:
                    image.save(file_path, "webp", quality=90, method=6)
                except Exception as e:
                    logging.error(f"Ошибка сохранения WEBP {file_path}: {e}")
                    continue

                # Записываем данные в БД для дополнительных изображений
                cursor.execute(
                    """
                    INSERT INTO product_images (product_id, image_url, image_alt_text, image_url_native)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        product_id,
                        filename,  # В базе сохраняем только имя файла без пути
                        alt_text,
                        img_url,
                    ),
                )

            except Exception as e:
                logging.error(f"Ошибка при обработке картинки {product_url}: {str(e)}")
                continue

        print('----------------------------------------')
        db.commit()
        cursor.close()
        db.close()

    except Exception as e:
        logging.error(f"Ошибка при обработке картинок продукта {product_url}: {str(e)}")
# Парсим внутренню страницу продукта
def parse_product_details(product_url, category):
    # driver.get(product_url)
    driver.execute_script("window.open(arguments[0], '_blank');", product_url)
    driver.switch_to.window(driver.window_handles[-1])
    time.sleep(3)

    # Удаляем .product-slider, если он есть
    try:
        product_slider = driver.find_element(By.CSS_SELECTOR, ".product-slider")
        driver.execute_script("arguments[0].remove();", product_slider)
    except Exception:
        pass

    productWrappersInnerPage = driver.find_elements(By.CSS_SELECTOR, ".product-view")
    
    for productWrapper in productWrappersInnerPage:
        try:
            # notInStock = productWrapper.find_elements(By.CSS_SELECTOR, ".product__label.product__label--light-red")
            out_of_stock_labels = productWrapper.find_elements(By.CSS_SELECTOR, ".product__label.product__label--light-red")
            if len(out_of_stock_labels) == 0:
                title = get_title_from_wrapper(productWrapper)
                title_key = make_title_key(title)

                if is_product_in_db(title_key):
                    continue

                print(title)
                description = get_description_from_wrapper(productWrapper)
                # Собираем старую цену
                old_price_el = WebDriverWait(driver, 10).until(
                    lambda d: productWrapper.find_element(By.CSS_SELECTOR, ".prod-info__bottom__price .old-price")
                )
                new_price_el = WebDriverWait(driver, 10).until(
                    lambda d: productWrapper.find_element(By.CSS_SELECTOR, ".prod-info__bottom__price .new-price")
                )
                # Собираем новую цену
  
                old_price_num = price_from_outer_html(old_price_el)
                new_price_num = price_from_outer_html(new_price_el)
                percent_of_old = ((old_price_num - new_price_num) / old_price_num) * 100 if old_price_num > 0 else 0.0 # Процент новой цены от старой
                url = product_url
                slug = slugify(title)
                

                product_id = insert_product(
                    title=title,
                    slug=slug,
                    url=url,
                    price=new_price_num,
                    sale = percent_of_old,
                    html_description=description,
                    category_id=category["id"],
                    title_key=title_key,
                )
                save_product_images(productWrapper, category["parent_name"], product_url, product_id)
            else:
                continue
        except Exception as e:
            logging.error(f"Ошибка при обработке враппера продукта {product_url}: {str(e)}")
            continue  # Пропускаем цвет в случае ошибки
    
    driver.close()
    driver.switch_to.window(driver.window_handles[0])
    # ------------------------------------------------------------------------------


# ПАРСЫ ОТДЕЛЬНЫХ ЧАСТЕЙ ПРОДУКТА

# Парсим цену
def price_from_outer_html(el) -> float:
    html = el.get_attribute("outerHTML") or ""
    # Ищем число между > ... <, допускаем запятую/точку и пробелы, валюту AZN/₼
    m = re.search(r'>\s*([0-9]+(?:[.,][0-9]+)?)\s*(?:AZN|₼)?\s*<', html, re.IGNORECASE)
    if m:
        return float(m.group(1).replace(',', '.'))
    # fallback: вдруг цена лежит в атрибуте
    for attr in ("content", "data-price", "value"):
        v = el.get_attribute(attr)
        if v:
            return float(v.replace(',', '.').strip())
    raise ValueError(f"Не удалось распарсить цену из: {html[:120]}...")

#Парсим тайтл
def get_title_from_wrapper(wrapper, timeout=12):
    def _cond(d):
        el = wrapper.find_element(By.CSS_SELECTOR, ".container-fluid > h1")
        txt = (el.get_attribute("textContent") or el.text or "").replace("\xa0", " ").strip()
        return txt if txt else False
    return WebDriverWait(driver, timeout).until(_cond)

#Парсим описания
def get_description_from_wrapper(wrapper, timeout=12):
    def _cond(d):
        el = wrapper.find_element(
            By.CSS_SELECTOR,
            ".container-fluid .product-view__details .product-view__details__technical-info"
        )
        txt = (el.get_attribute("textContent") or el.text or "").replace("\xa0", " ").strip()
        return txt if txt else False
    return WebDriverWait(driver, timeout).until(_cond)

# Название ключ для поиска уникальности по чистому
def make_title_key(title: str) -> str:
    # убираем неразрывные пробелы, схлопываем пробелы, в нижний регистр
    s = (title or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

# АЗ ТРАНС Алфавит

AZ_TRANS = str.maketrans({
    "ə": "e", "ı": "i", "İ": "i",
    "ş": "s", "Ş": "s",
    "ç": "c", "Ç": "c",
    "ğ": "g", "Ğ": "g",
    "ö": "o", "Ö": "o",
    "ü": "u", "Ü": "u",
})

# Слагификация тайтла

def slugify(title: str) -> str:
    s = (title or "").strip().translate(AZ_TRANS)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^\w\s-]", "", s)         # убираем всё, кроме букв/цифр/дефисов
    s = re.sub(r"[\s_]+", "-", s.lower())  # пробелы и _ -> дефисы
    return re.sub(r"-{2,}", "-", s).strip("-")

if __name__ == "__main__":
    parse_products_for_categories()
    # clear_categories()
    # get_all_categories()

# Нет логики захода в категории
# Нет логики клика "daha cox" для дополнительного рендера карточек продуктов

# Я не должен вставлять ссылку на товар. Должен быть :click на кнопку "Kataloq". После :hover на категорию
