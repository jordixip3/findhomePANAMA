"""
Database layer for FindHome Panamá
Supports MySQL (MariaDB) with graceful SQLite fallback
"""
import os
import json
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

DB_TYPE_ENV = os.getenv("DB_TYPE", "").strip().lower()
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "lookingforhome")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "LookingForHome2026!")
MYSQL_DB = os.getenv("MYSQL_DB", "findhome_panama")

SQLITE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "findhome.db")
_ACTIVE_ENGINE = None

INITIAL_PROPERTIES = [
    {
        "title": "Moderno Apartamento de 3 Habitaciones con Vista al Parque Omar en San Francisco",
        "subdivision": "Panamá Centro",
        "subdivision_code": "PAN",
        "city": "Ciudad de Panamá",
        "address": "Calle 74 Este, San Francisco, Ciudad de Panamá",
        "neighborhood": "San Francisco",
        "latitude": 8.9912,
        "longitude": -79.5028,
        "total_rent": 1400,
        "bedrooms": 3,
        "bathrooms": 2,
        "surface": 130,
        "portal": "Encuentra24 Panamá",
        "url": "https://www.encuentra24.com/panama-es/bienes-raices-alquiler/prop1",
        "desc": "Espacioso apartamento con vista directa al pulmón verde de la ciudad. Área social completa con piscina olímpica, gimnasio y coworking.",
        "contact_name": "San Francisco Living",
        "contact_phone": "+507 6555-0182",
        "contact_agency": "Panamá Canal Properties"
    },
    {
        "title": "Apartamento de 4 Habitaciones con Línea Blanca en Costa del Este",
        "subdivision": "Panamá Centro",
        "subdivision_code": "PAN",
        "city": "Ciudad de Panamá",
        "address": "Avenida Paseo del Mar, Costa del Este",
        "neighborhood": "Costa del Este",
        "latitude": 9.0145,
        "longitude": -79.4674,
        "total_rent": 2200,
        "bedrooms": 4,
        "bathrooms": 3,
        "surface": 185,
        "portal": "CompreoAlquile Panamá",
        "url": "https://www.compreoalquile.com/inmuebles/costa-del-este/prop2",
        "desc": "El centro corporativo y financiero por excelencia. Acabados de lujo, aire acondicionado central, planta eléctrica total.",
        "contact_name": "Costa del Este Executives",
        "contact_phone": "+507 6555-0199",
        "contact_agency": "Skyline Panamá"
    },
    {
        "title": "Loft Bohemio Restaurado de 3 Habitaciones en Casco Antiguo",
        "subdivision": "Panamá Centro",
        "subdivision_code": "PAN",
        "city": "Ciudad de Panamá",
        "address": "Avenida Central y Calle 4ta, Casco Antiguo",
        "neighborhood": "Casco Antiguo",
        "latitude": 8.9515,
        "longitude": -79.5342,
        "total_rent": 1600,
        "bedrooms": 3,
        "bathrooms": 2,
        "surface": 140,
        "portal": "Encuentra24 Panamá",
        "url": "https://www.encuentra24.com/panama-es/casco-antiguo/prop3",
        "desc": "Encanto colonial con muros de calicanto, techos altos de madera y terraza privada con vista al Océano Pacífico.",
        "contact_name": "Casco Heritage Co-Living",
        "contact_phone": "+507 6555-0144",
        "contact_agency": "Colonial Living"
    },
    {
        "title": "Apartamento de Playa de 3 Dormitorios en Coronado",
        "subdivision": "Panamá Oeste",
        "subdivision_code": "PO",
        "city": "Playa Coronado",
        "address": "Avenida Roberto Eisenmann, Coronado",
        "neighborhood": "Coronado Beach",
        "latitude": 8.5298,
        "longitude": -79.8895,
        "total_rent": 1100,
        "bedrooms": 3,
        "bathrooms": 2,
        "surface": 120,
        "portal": "Properati Panamá",
        "url": "https://www.properati.com.pa/inmuebles/coronado/prop4",
        "desc": "Vida tranquila cerca del mar con amenidades de club de golf, piscinas y centro comercial cercano para nómadas digitales.",
        "contact_name": "Pacífico Beach Club",
        "contact_phone": "+507 6555-0131",
        "contact_agency": "Riviera Panamá"
    },
    {
        "title": "Hermosa Casona de Montaña de 4 Dormitorios en Boquete",
        "subdivision": "Chiriquí",
        "subdivision_code": "CHI",
        "city": "Boquete",
        "address": "Bajo Boquete, Vía Alto Lino, Boquete",
        "neighborhood": "Bajo Boquete",
        "latitude": 8.7812,
        "longitude": -82.4332,
        "total_rent": 1200,
        "bedrooms": 4,
        "bathrooms": 3,
        "surface": 210,
        "portal": "Encuentra24 Panamá",
        "url": "https://www.encuentra24.com/panama-es/boquete/prop5",
        "desc": "Clima primaveral todo el año entre cafetales. Chimenea, amplios jardines y vistas espectaculares al Volcán Barú.",
        "contact_name": "Tierras Altas Co-Living",
        "contact_phone": "+507 6555-0177",
        "contact_agency": "Chiriquí Homes"
    }
]
SUBDIVISIONS = [
    [
        "Panamá Centro",
        "PAN"
    ],
    [
        "Panamá Oeste",
        "PO"
    ],
    [
        "Chiriquí",
        "CHI"
    ],
    [
        "Colón",
        "COL"
    ],
    [
        "Coclé",
        "COC"
    ],
    [
        "Bocas del Toro",
        "BOC"
    ],
    [
        "Herrera",
        "HER"
    ],
    [
        "Los Santos",
        "LS"
    ],
    [
        "Veraguas",
        "VER"
    ]
]

def detect_engine() -> str:
    global _ACTIVE_ENGINE
    if _ACTIVE_ENGINE is not None:
        return _ACTIVE_ENGINE
    if DB_TYPE_ENV == "sqlite":
        _ACTIVE_ENGINE = "sqlite"
        return _ACTIVE_ENGINE
    try:
        import pymysql
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
            password=MYSQL_PASSWORD, database=MYSQL_DB, connect_timeout=3
        )
        conn.close()
        _ACTIVE_ENGINE = "mysql"
        return _ACTIVE_ENGINE
    except Exception:
        _ACTIVE_ENGINE = "sqlite"
        return _ACTIVE_ENGINE

def get_raw_connection():
    engine = detect_engine()
    if engine == "mysql":
        import pymysql
        import pymysql.cursors
        return pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
            password=MYSQL_PASSWORD, database=MYSQL_DB, charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor, autocommit=False
        )
    else:
        import sqlite3
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row
        return conn

def init_db():
    engine = detect_engine()
    conn = get_raw_connection()
    cursor = conn.cursor()

    if engine == "mysql":
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS properties (
            id VARCHAR(64) PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            description TEXT,
            city VARCHAR(100) NOT NULL,
            subdivision VARCHAR(100) NOT NULL,
            subdivision_code VARCHAR(10),
            address VARCHAR(255),
            neighborhood VARCHAR(100),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            total_rent DOUBLE PRECISION NOT NULL,
            ideal_roommates INT DEFAULT 4,
            bedrooms INT NOT NULL,
            bathrooms INT NOT NULL,
            surface INT NOT NULL,
            has_elevator TINYINT(1) DEFAULT 1,
            is_exterior TINYINT(1) DEFAULT 1,
            is_furnished TINYINT(1) DEFAULT 1,
            has_terrace TINYINT(1) DEFAULT 0,
            has_air_con TINYINT(1) DEFAULT 0,
            source_portal VARCHAR(100) DEFAULT 'Direct Portal',
            source_url VARCHAR(500),
            property_type VARCHAR(50) DEFAULT 'apartment',
            images JSON,
            contact_name VARCHAR(150),
            contact_phone VARCHAR(50),
            contact_agency VARCHAR(150),
            status VARCHAR(30) DEFAULT 'available',
            created_at VARCHAR(50)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS access_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ip VARCHAR(64) NOT NULL,
            country VARCHAR(100),
            country_code VARCHAR(10),
            region VARCHAR(100),
            city VARCHAR(100),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            path VARCHAR(255),
            method VARCHAR(10),
            status_code INT,
            user_agent TEXT,
            device_type VARCHAR(30),
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS whatsapp_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sender_phone VARCHAR(50),
            sender_name VARCHAR(100),
            message_body TEXT,
            response_body TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        conn.commit()

        # Check if properties exist
        cursor.execute("SELECT COUNT(*) AS cnt FROM properties")
        row = cursor.fetchone()
        cnt = row["cnt"] if isinstance(row, dict) else row[0]
        if cnt == 0:
            seed_data(cursor, engine)
            conn.commit()

    else:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS properties (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT,
            city TEXT NOT NULL,
            subdivision TEXT NOT NULL,
            subdivision_code TEXT,
            address TEXT,
            neighborhood TEXT,
            latitude REAL,
            longitude REAL,
            total_rent REAL NOT NULL,
            ideal_roommates INTEGER DEFAULT 4,
            bedrooms INTEGER NOT NULL,
            bathrooms INTEGER NOT NULL,
            surface INTEGER NOT NULL,
            has_elevator INTEGER DEFAULT 1,
            is_exterior INTEGER DEFAULT 1,
            is_furnished INTEGER DEFAULT 1,
            has_terrace INTEGER DEFAULT 0,
            has_air_con INTEGER DEFAULT 0,
            source_portal TEXT,
            source_url TEXT,
            property_type TEXT,
            images TEXT,
            contact_name TEXT,
            contact_phone TEXT,
            contact_agency TEXT,
            status TEXT DEFAULT 'available',
            created_at TEXT
        );
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS access_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ip TEXT NOT NULL,
            country TEXT,
            country_code TEXT,
            region TEXT,
            city TEXT,
            latitude REAL,
            longitude REAL,
            path TEXT,
            method TEXT,
            status_code INTEGER,
            user_agent TEXT,
            device_type TEXT,
            timestamp TEXT
        );
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS whatsapp_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_phone TEXT,
            sender_name TEXT,
            message_body TEXT,
            response_body TEXT,
            timestamp TEXT
        );
        """)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM properties")
        if cursor.fetchone()[0] == 0:
            seed_data(cursor, engine)
            conn.commit()

    conn.close()

def seed_data(cursor, engine):
    img_list = [
        "static/images/prop-mad-01-1.jpg", "static/images/prop-mad-01-2.jpg",
        "static/images/prop-bcn-01-1.jpg", "static/images/prop-par-01-1.jpg",
        "static/images/prop-ber-01-1.jpg", "static/images/prop-lis-01-1.jpg"
    ]
    for idx, p in enumerate(INITIAL_PROPERTIES):
        pid = f"panama-prop-{idx+1:02d}"
        imgs = json.dumps([img_list[idx % len(img_list)]])
        now = datetime.now(timezone.utc).isoformat()
        if engine == "mysql":
            cursor.execute("""
            INSERT INTO properties (
                id, title, description, city, subdivision, subdivision_code, address, neighborhood,
                latitude, longitude, total_rent, ideal_roommates, bedrooms, bathrooms, surface,
                has_elevator, is_exterior, is_furnished, has_terrace, has_air_con,
                source_portal, source_url, property_type, images, contact_name, contact_phone, contact_agency, status, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                pid, p["title"], p["desc"], p["city"], p["subdivision"], p.get("subdivision_code", ""),
                p["address"], p["neighborhood"], p["latitude"], p["longitude"], p["total_rent"],
                p.get("ideal_roommates", p["bedrooms"]), p["bedrooms"], p["bathrooms"], p["surface"],
                1, 1, 1, 1, 1, p["portal"], p["url"], "apartment", imgs,
                p["contact_name"], p["contact_phone"], p["contact_agency"], "available", now
            ))
        else:
            cursor.execute("""
            INSERT INTO properties (
                id, title, description, city, subdivision, subdivision_code, address, neighborhood,
                latitude, longitude, total_rent, ideal_roommates, bedrooms, bathrooms, surface,
                has_elevator, is_exterior, is_furnished, has_terrace, has_air_con,
                source_portal, source_url, property_type, images, contact_name, contact_phone, contact_agency, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pid, p["title"], p["desc"], p["city"], p["subdivision"], p.get("subdivision_code", ""),
                p["address"], p["neighborhood"], p["latitude"], p["longitude"], p["total_rent"],
                p.get("ideal_roommates", p["bedrooms"]), p["bedrooms"], p["bathrooms"], p["surface"],
                1, 1, 1, 1, 1, p["portal"], p["url"], "apartment", imgs,
                p["contact_name"], p["contact_phone"], p["contact_agency"], "available", now
            ))

def get_properties(city=None, subdivision=None, portal=None, min_bedrooms=None, max_total_rent=None) -> List[Dict[str, Any]]:
    conn = get_raw_connection()
    cursor = conn.cursor()
    query = "SELECT * FROM properties WHERE 1=1"
    params = []

    if city:
        query += " AND LOWER(city) LIKE %s" if detect_engine() == "mysql" else " AND LOWER(city) LIKE ?"
        params.append(f"%{city.lower()}%")
    if subdivision:
        query += " AND (LOWER(subdivision) LIKE %s OR LOWER(subdivision_code) LIKE %s)" if detect_engine() == "mysql" else " AND (LOWER(subdivision) LIKE ? OR LOWER(subdivision_code) LIKE ?)"
        params.extend([f"%{subdivision.lower()}%", f"%{subdivision.lower()}%"])
    if portal:
        query += " AND LOWER(source_portal) LIKE %s" if detect_engine() == "mysql" else " AND LOWER(source_portal) LIKE ?"
        params.append(f"%{portal.lower()}%")
    if min_bedrooms:
        query += " AND bedrooms >= %s" if detect_engine() == "mysql" else " AND bedrooms >= ?"
        params.append(int(min_bedrooms))
    if max_total_rent:
        query += " AND total_rent <= %s" if detect_engine() == "mysql" else " AND total_rent <= ?"
        params.append(float(max_total_rent))

    query += " ORDER BY total_rent ASC"
    cursor.execute(query, tuple(params))
    rows = cursor.fetchall()

    results = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("images"), str):
            try:
                d["images"] = json.loads(d["images"])
            except Exception:
                d["images"] = []
        d["rent_per_person"] = round(d["total_rent"] / max(1, d.get("ideal_roommates", d.get("bedrooms", 4))))
        results.append(d)

    conn.close()
    return results

def get_property(prop_id: str) -> Optional[Dict[str, Any]]:
    conn = get_raw_connection()
    cursor = conn.cursor()
    q = "SELECT * FROM properties WHERE id = %s" if detect_engine() == "mysql" else "SELECT * FROM properties WHERE id = ?"
    cursor.execute(q, (prop_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    if isinstance(d.get("images"), str):
        try:
            d["images"] = json.loads(d["images"])
        except Exception:
            d["images"] = []
    d["rent_per_person"] = round(d["total_rent"] / max(1, d.get("ideal_roommates", d.get("bedrooms", 4))))
    return d

def record_access_log(ip, country, country_code, region, city, latitude, longitude, path, method, status_code, user_agent, device_type):
    try:
        conn = get_raw_connection()
        cursor = conn.cursor()
        engine = detect_engine()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if engine == "mysql":
            cursor.execute("""
            INSERT INTO access_logs (ip, country, country_code, region, city, latitude, longitude, path, method, status_code, user_agent, device_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (ip, country, country_code, region, city, latitude, longitude, path, method, status_code, user_agent, device_type))
        else:
            cursor.execute("""
            INSERT INTO access_logs (ip, country, country_code, region, city, latitude, longitude, path, method, status_code, user_agent, device_type, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (ip, country, country_code, region, city, latitude, longitude, path, method, status_code, user_agent, device_type, now))
        conn.commit()
        conn.close()
    except Exception:
        pass

def get_access_logs(limit: int = 100) -> List[Dict[str, Any]]:
    conn = get_raw_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM access_logs ORDER BY id DESC LIMIT {int(limit)}")
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def record_whatsapp_log(sender_phone, sender_name, message_body, response_body):
    try:
        conn = get_raw_connection()
        cursor = conn.cursor()
        engine = detect_engine()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if engine == "mysql":
            cursor.execute("""
            INSERT INTO whatsapp_logs (sender_phone, sender_name, message_body, response_body)
            VALUES (%s, %s, %s, %s)
            """, (sender_phone, sender_name, message_body, response_body))
        else:
            cursor.execute("""
            INSERT INTO whatsapp_logs (sender_phone, sender_name, message_body, response_body, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """, (sender_phone, sender_name, message_body, response_body, now))
        conn.commit()
        conn.close()
    except Exception:
        pass

def get_whatsapp_logs(limit: int = 50) -> List[Dict[str, Any]]:
    conn = get_raw_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM whatsapp_logs ORDER BY id DESC LIMIT {int(limit)}")
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]
