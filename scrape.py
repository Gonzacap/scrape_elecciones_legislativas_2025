#!/usr/bin/env python3
# RUN:
#   python3 scrape.py           → recupera por mesa (completo)
#   python3 scrape.py --por-circuito  → recupera por circuito (sin bajar mesas)

import requests
import csv
import time
import sys
import socket
import os
import json
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

# ---------------- CONFIG ----------------
BASE_URL = "https://resultados.elecciones.gob.ar/backend-difu/scope/data/getScopeDataMap"
DIPUTADOS_SENADORES = 3
LEVEL_MESA = -1
LEVEL_PROVINCIA = 30
LEVEL_LOCALIDAD = 50
LEVEL_CIRCUITO = 70
CACHE_DIR = "cache"
OUTPUT_DIR = "resultados"
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

REQUEST_DELAY = 0.2
RETRIES = 2
RETRY_DELAY = 4
MAX_WORKERS = 5

HEADERS = {
    'accept': '*/*',
    'accept-language': 'es-ES,es;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://resultados.elecciones.gob.ar/territorios/1/1/10',
    'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36'
}

SCOPES = SCOPES = [
    # {"scopeId": "0000000000000000000000d2", "name": "BUENOS AIRES"},
    # {"scopeId": "00000000000000000000006e", "name": "CABA"},
    # {"scopeId": "000000000000000000000136", "name": "CATAMARCA"},
    # {"scopeId": "000000000000000000000262", "name": "CHACO"},
    # {"scopeId": "0000000000000000000002c6", "name": "CHUBUT"},
    # {"scopeId": "0000000000000000000001fe", "name": "CORRIENTES"},
    # {"scopeId": "00000000000000000000019a", "name": "CÓRDOBA"},
    # {"scopeId": "00000000000000000000032a", "name": "ENTRE RÍOS"},
    # {"scopeId": "00000000000000000000038e", "name": "FORMOSA"},
    # {"scopeId": "0000000000000000000003f2", "name": "JUJUY"},
    # {"scopeId": "000000000000000000000456", "name": "LA PAMPA"},
    # {"scopeId": "0000000000000000000004ba", "name": "LA RIOJA"},
    # {"scopeId": "00000000000000000000051e", "name": "MENDOZA"},
    # {"scopeId": "000000000000000000000582", "name": "MISIONES"},
    # {"scopeId": "0000000000000000000005e6", "name": "NEUQUÉN"},
    # {"scopeId": "00000000000000000000064a", "name": "RÍO NEGRO"},
    # {"scopeId": "0000000000000000000006ae", "name": "SALTA"},
    # {"scopeId": "000000000000000000000712", "name": "SAN JUAN"},
    # {"scopeId": "000000000000000000000776", "name": "SAN LUIS"},
    # {"scopeId": "0000000000000000000007da", "name": "SANTA CRUZ"},
    {"scopeId": "00000000000000000000083e", "name": "SANTA FE"},
    # {"scopeId": "0000000000000000000008a2", "name": "SANTIAGO DEL ESTERO"},.
    # {"scopeId": "00000000000000000000096a", "name": "TIERRA DEL FUEGO AeIAS"},
    # {"scopeId": "000000000000000000000906", "name": "TUCUMÁN"}
]

# ---------------- UTILIDADES ----------------
def print_progress(current, total, prefix="Progreso"):
    percent = (current / total) * 100 if total else 0
    bar_len = 40
    filled_len = int(round(bar_len * current / float(total)))
    bar = "=" * filled_len + "-" * (bar_len - filled_len)
    sys.stdout.write(f"\r{prefix}: [{bar}] {percent:.1f}% ({current}/{total})")
    sys.stdout.flush()
    if current == total:
        print()

def check_dns(host="resultados.elecciones.gob.ar"):
    try:
        ip = socket.gethostbyname(host)
        print(f"✅ DNS resuelto: {host} -> {ip}")
        return True
    except Exception as e:
        print(f"⚠️ Error de DNS: {e}")
        return False

# ---------------- DESCARGA + CACHE ----------------
@lru_cache(maxsize=None)
def get_scope_data(scope_id, level):
    url = f"{BASE_URL}/{scope_id}/{DIPUTADOS_SENADORES}/1/{level}"
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            if level == LEVEL_MESA:
                mesa_info = data["id"]["idAmbito"]
                return [{
                    "scopeId": mesa_info.get("scopeId"),
                    "codigo": mesa_info.get("codigo"),
                    "name": mesa_info.get("name"),
                    "partidos": data.get("partidos", []),
                    "census": data.get("census"),
                    "pollingCensus": data.get("pollingCensus"),
                    "nulos": data.get("nulos"),
                    "recurridos": data.get("recurridos"),
                    "blancos": data.get("blancos"),
                    "comando": data.get("comando"),
                    "impugnados": data.get("impugnados"),
                    "totalVotos": data.get("totalVotos"),
                    "afirmativos": data.get("afirmativos"),
                    "participation": data.get("participation"),
                }]

            if "mapa" in data and len(data["mapa"]) > 0:
                scopes = data["mapa"][0].get("scopes", [])
                return [
                    {"scopeId": s.get("scopeId"), "name": s.get("name"), "codigo": s.get("codigo")}
                    for s in scopes if s.get("scopeId")
                ]

            print(f"⚠️ Formato inesperado para {url}")
            return None

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Error en {scope_id} (nivel {level}) intento {attempt}: {e}")
            if attempt < RETRIES:
                print(f"⏳ Reintentando en {RETRY_DELAY} s...")
                time.sleep(RETRY_DELAY)
            else:
                return None

def cached_request(scope_id, level):
    cache_file = os.path.join(CACHE_DIR, f"{scope_id}_{level}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
    data = get_scope_data(scope_id, level)
    if data:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    return data

def get_parallel(scope_list, level):
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(cached_request, s["scopeId"], level): s for s in scope_list}
        for future in as_completed(futures):
            scope = futures[future]
            try:
                data = future.result()
                if data:
                    results.append((scope, data))
            except Exception as e:
                print(f"⚠️ Error paralelo en {scope['name']}: {e}")
    return results

# ---------------- MAIN ----------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--por-circuito", action="store_true", help="Recuperar datos por circuito en lugar de por mesa")
    args = parser.parse_args()

    if not check_dns():
        print("❌ No se puede resolver el dominio. Verifica tu conexión.")
        return

    print(f"📦 {len(SCOPES)} provincias cargadas\n")

    mesa_count = 0
    provincias = get_parallel(SCOPES, LEVEL_PROVINCIA)

    for provincia_scope, localidades in provincias:
        provincia = provincia_scope["name"]
        print(f"\n📍 Provincia: {provincia}")

        localidades_data = get_parallel(localidades, LEVEL_LOCALIDAD)

        for localidad_scope, circuitos in localidades_data:
            localidad = localidad_scope["name"]
            print(f"   🏙️ Localidad: {localidad}")

            # --- crear archivo por provincia-localidad ---
            file_name = f"{provincia}_{localidad}.csv"
            file_name = file_name.replace(" ", "_").replace("/", "-")
            file_path = os.path.join(OUTPUT_DIR, file_name)

            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "provincia", "localidad", "circuito", "mesa", "census", "pollingCensus",
                    "nulos", "recurridos", "blancos", "comando", "impugnados",
                    "totalVotos", "afirmativos", "participacion", "partido", "votos"
                ])

                circuitos_data = get_parallel(circuitos, LEVEL_CIRCUITO)
                for circuito_scope, mesas in circuitos_data:
                    circuito = circuito_scope["name"]
                    print(f"      🔄 Circuito: {circuito} ({len(mesas)} mesas aprox.)")

                    if args.por_circuito:
                        # Tratar el circuito como si fuera LEVEL_MESA
                        circuito_mesa_data = cached_request(circuito_scope["scopeId"], LEVEL_MESA)
                        time.sleep(REQUEST_DELAY)
                        if not circuito_mesa_data:
                            continue
                        for mesa_final in circuito_mesa_data:
                            for partido in mesa_final.get("partidos", []):
                                writer.writerow([
                                    provincia, localidad, circuito, mesa_final.get("name", ""),
                                    mesa_final.get("census", ""),
                                    mesa_final.get("pollingCensus", ""),
                                    mesa_final.get("nulos", ""), mesa_final.get("recurridos", ""),
                                    mesa_final.get("blancos", ""), mesa_final.get("comando", ""),
                                    mesa_final.get("impugnados", ""), mesa_final.get("totalVotos", ""),
                                    mesa_final.get("afirmativos", ""), mesa_final.get("participation", ""),
                                    partido.get("name", ""), partido.get("votos", 0),
                                ])
                                mesa_count += 1
                    else:
                        # 🧩 Modo normal: bajar todas las mesas
                        for mesa in mesas:
                            mesas_data = cached_request(mesa["scopeId"], LEVEL_MESA)
                            time.sleep(REQUEST_DELAY)
                            if not mesas_data:
                                continue

                            for mesa_final in mesas_data:
                                for partido in mesa_final.get("partidos", []):
                                    writer.writerow([
                                        provincia, localidad, circuito, mesa_final.get("name", ""),
                                        mesa_final.get("census", ""),
                                        mesa_final.get("pollingCensus", ""),
                                        mesa_final.get("nulos", ""), mesa_final.get("recurridos", ""),
                                        mesa_final.get("blancos", ""), mesa_final.get("comando", ""),
                                        mesa_final.get("impugnados", ""), mesa_final.get("totalVotos", ""),
                                        mesa_final.get("afirmativos", ""), mesa_final.get("participation", ""),
                                        partido.get("name", ""), partido.get("votos", 0),
                                    ])
                                    mesa_count += 1

            print(f"   ✅ Localidad {localidad} completada → archivo: {file_path}")

    print(f"\n✅ Descarga completada. Total de unidades procesadas: {mesa_count}")

if __name__ == "__main__":
    main()
