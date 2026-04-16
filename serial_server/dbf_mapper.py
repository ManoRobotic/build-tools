#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DBF Mapper - Analiza recursivamente archivos .dbf en el directorio actual
y genera un reporte detallado en .txt

Uso:
    dbf_mapper.exe          -> analiza el directorio donde esta el exe
    dbf_mapper.exe C:\ruta  -> analiza la ruta especificada

Reporte: dbf_report_YYYYMMDD_HHMMSS.txt
"""

import os
import sys
import traceback
from pathlib import Path
from datetime import datetime
from dbfread import DBF, FieldParser


# ─── helpers ────────────────────────────────────────────────────────────────

RELEVANCE_KEYWORDS = {
    "ORDENES_PRODUCCION": [
        "OPRO", "ORDEN", "ORDPRO", "PRODUC", "NO_OP", "NOOPRO", "NO_OPRO",
        "OPROD", "BOLSA", "MEDIDA", "PIEZAS", "LOTE", "PESO",
    ],
    "PRODUCTOS": [
        "PROD", "ARTICULO", "PRODUCT", "CVE_PROD", "CLAVE", "SKU",
        "DESCRIPCION", "UNIDAD", "PRECIO", "COSTO",
    ],
    "STOCK_INVENTARIO": [
        "STOCK", "INVENT", "EXISTEN", "SALDO", "CANT", "CANTIDAD",
        "ALMACEN", "UBICACION", "BODEGA", "EXIST",
    ],
    "CLIENTES_PROVEEDORES": [
        "CLIE", "CLIENT", "PROVEEDOR", "PROV", "CVE_CLIE", "CVE_PROV",
        "NOMBRE", "RFC", "DIREC",
    ],
    "MOVIMIENTOS_REMISIONES": [
        "REMD", "REMISION", "MOVIM", "TRANSAC", "ENTRADA", "SALIDA",
        "FACTURA", "PEDIDO", "TRASPASO",
    ],
    "CONFIGURACION_CATALOGOS": [
        "CONFIG", "PARAM", "CAT", "CATALOGO", "TABLA",
    ],
}


def classify_table(filename: str, field_names: list[str]) -> list[str]:
    """Retorna las categorias relevantes detectadas para esta tabla."""
    upper_name = filename.upper()
    upper_fields = [f.upper() for f in field_names]
    categories = []

    for category, keywords in RELEVANCE_KEYWORDS.items():
        score = 0
        for kw in keywords:
            if kw in upper_name:
                score += 3
            for field in upper_fields:
                if kw in field:
                    score += 1
        if score >= 2:
            categories.append(category)

    return categories if categories else ["SIN_CLASIFICAR"]


def safe_value(val) -> str:
    """Convierte cualquier valor a string seguro para el reporte."""
    if val is None:
        return "NULL"
    if isinstance(val, bytes):
        try:
            return val.decode("latin-1").strip()
        except Exception:
            return repr(val)
    return str(val).strip()


class LenientFieldParser(FieldParser):
    """Parser tolerante a encodings y tipos desconocidos."""

    def parseN(self, field, data):
        try:
            return super().parseN(field, data)
        except Exception:
            return data.strip().decode("latin-1") if isinstance(data, bytes) else data

    def parseD(self, field, data):
        try:
            return super().parseD(field, data)
        except Exception:
            return safe_value(data)


def read_dbf(path: Path) -> dict:
    """
    Lee un archivo DBF y retorna metadata + muestra de registros.
    Maneja errores de encoding y tipos de campo desconocidos.
    """
    result = {
        "path": str(path),
        "filename": path.name,
        "size_kb": round(path.stat().st_size / 1024, 1),
        "record_count": 0,
        "field_count": 0,
        "fields": [],
        "sample_records": [],
        "categories": [],
        "error": None,
    }

    try:
        table = DBF(
            str(path),
            encoding="latin-1",
            char_decode_errors="replace",
            parserclass=LenientFieldParser,
            ignore_missing_memofile=True,
        )

        result["record_count"] = len(table)

        fields = []
        for f in table.fields:
            fields.append({
                "name": f.name,
                "type": f.type,
                "length": f.length,
                "decimal_count": getattr(f, "decimal_count", 0),
            })
        result["fields"] = fields
        result["field_count"] = len(fields)

        field_names = [f["name"] for f in fields]
        result["categories"] = classify_table(path.stem, field_names)

        # Muestra de hasta 5 registros no vacios
        sample = []
        for record in table:
            if len(sample) >= 5:
                break
            row = {k: safe_value(v) for k, v in record.items() if k != "deleted"}
            # Filtrar registros completamente vacios
            if any(v not in ("", "NULL", "0", "0.0", "0.00") for v in row.values()):
                sample.append(row)
        result["sample_records"] = sample

    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"

    return result


def find_dbf_files(root: Path) -> list[Path]:
    """Busca recursivamente todos los .dbf desde root."""
    dbf_files = []
    try:
        for entry in root.rglob("*.dbf"):
            if entry.is_file():
                dbf_files.append(entry)
        for entry in root.rglob("*.DBF"):
            if entry.is_file() and entry not in dbf_files:
                dbf_files.append(entry)
    except PermissionError as e:
        print(f"  [ADVERTENCIA] Sin permisos para acceder a alguna carpeta: {e}")
    return sorted(dbf_files)


# ─── reporte ────────────────────────────────────────────────────────────────

def write_report(tables: list[dict], root: Path, output_path: Path):
    lines = []

    def ln(text=""):
        lines.append(text)

    # ── Encabezado ──────────────────────────────────────────────────────────
    ln("=" * 80)
    ln("  DBF MAPPER - REPORTE DE ANALISIS DE BASES DE DATOS")
    ln("=" * 80)
    ln(f"  Generado:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    ln(f"  Directorio:    {root}")
    ln(f"  Tablas encontradas: {len(tables)}")
    ln(f"  Tablas con error:   {sum(1 for t in tables if t['error'])}")
    ln("=" * 80)
    ln()

    # ── Resumen ejecutivo por categoria ─────────────────────────────────────
    ln("RESUMEN POR CATEGORIA")
    ln("-" * 80)

    from collections import defaultdict
    by_category = defaultdict(list)
    for t in tables:
        for cat in t["categories"]:
            by_category[cat].append(t)

    for cat, cat_tables in sorted(by_category.items()):
        ln(f"\n  [{cat}]")
        for t in cat_tables:
            status = f"  ({t['error'][:60]}...)" if t["error"] else f"  {t['record_count']:,} registros, {t['field_count']} campos"
            ln(f"    • {t['filename']:<30} {status}")

    ln()
    ln("=" * 80)
    ln()

    # ── Detalle por tabla ────────────────────────────────────────────────────
    ln("DETALLE DE TABLAS")
    ln("=" * 80)

    for i, table in enumerate(tables, 1):
        ln()
        ln(f"[{i:03d}] {table['filename']}")
        ln(f"      Ruta:       {table['path']}")
        ln(f"      Tamaño:     {table['size_kb']} KB")
        ln(f"      Categorias: {', '.join(table['categories'])}")

        if table["error"]:
            ln(f"      *** ERROR AL LEER: {table['error']} ***")
            ln()
            continue

        ln(f"      Registros:  {table['record_count']:,}")
        ln(f"      Campos:     {table['field_count']}")

        # Campos
        ln()
        ln("      CAMPOS:")
        ln(f"      {'#':<4} {'NOMBRE':<20} {'TIPO':<6} {'LONG':<6} {'DECIMALES'}")
        ln("      " + "-" * 50)
        for j, f in enumerate(table["fields"], 1):
            type_desc = {
                "C": "C (Char)",
                "N": "N (Num)",
                "D": "D (Date)",
                "L": "L (Bool)",
                "M": "M (Memo)",
                "F": "F (Float)",
            }.get(f["type"], f["type"])
            ln(f"      {j:<4} {f['name']:<20} {type_desc:<10} {f['length']:<6} {f['decimal_count']}")

        # Muestra de datos
        if table["sample_records"]:
            ln()
            ln("      MUESTRA DE DATOS (primeros 5 registros no vacios):")
            for k, record in enumerate(table["sample_records"], 1):
                ln(f"      Registro {k}:")
                for field_name, value in record.items():
                    if len(value) > 80:
                        value = value[:77] + "..."
                    ln(f"        {field_name:<20} = {value}")
        else:
            ln()
            ln("      (Tabla vacia o sin registros no vacios)")

        ln()
        ln("      " + "-" * 74)

    # ── Recomendaciones para DBF uploader ───────────────────────────────────
    ln()
    ln("=" * 80)
    ln("RECOMENDACIONES PARA DBF UPLOADER")
    ln("=" * 80)
    ln()

    prod_tables = by_category.get("ORDENES_PRODUCCION", [])
    stock_tables = by_category.get("STOCK_INVENTARIO", [])
    mov_tables = by_category.get("MOVIMIENTOS_REMISIONES", [])
    prod_cat_tables = by_category.get("PRODUCTOS", [])

    if prod_tables:
        ln("  ORDENES DE PRODUCCION:")
        for t in prod_tables:
            fields_preview = ", ".join(f["name"] for f in t["fields"][:10])
            ln(f"    -> {t['filename']} | Campos clave: {fields_preview}")
        ln()

    if stock_tables:
        ln("  STOCK / INVENTARIO:")
        for t in stock_tables:
            fields_preview = ", ".join(f["name"] for f in t["fields"][:10])
            ln(f"    -> {t['filename']} | Campos clave: {fields_preview}")
        ln()

    if prod_cat_tables:
        ln("  CATALOGO DE PRODUCTOS:")
        for t in prod_cat_tables:
            fields_preview = ", ".join(f["name"] for f in t["fields"][:10])
            ln(f"    -> {t['filename']} | Campos clave: {fields_preview}")
        ln()

    if mov_tables:
        ln("  MOVIMIENTOS / REMISIONES:")
        for t in mov_tables:
            fields_preview = ", ".join(f["name"] for f in t["fields"][:10])
            ln(f"    -> {t['filename']} | Campos clave: {fields_preview}")
        ln()

    if not (prod_tables or stock_tables or mov_tables or prod_cat_tables):
        ln("  No se detectaron tablas claramente relevantes. Revisar detalle arriba.")
        ln()

    ln("=" * 80)
    ln(f"  FIN DEL REPORTE - {len(tables)} tablas analizadas")
    ln("=" * 80)

    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


# ─── main ────────────────────────────────────────────────────────────────────

def main():
    # Directorio raiz: argumento CLI o directorio del exe/script
    if len(sys.argv) > 1:
        root = Path(sys.argv[1]).resolve()
    elif getattr(sys, "frozen", False):
        # Ejecutable: usa el directorio donde el usuario corre el .exe
        root = Path(os.getcwd()).resolve()
    else:
        root = Path(__file__).parent.resolve()

    if not root.exists() or not root.is_dir():
        print(f"ERROR: El directorio '{root}' no existe.")
        sys.exit(1)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path(os.getcwd()) / f"dbf_report_{timestamp}.txt"

    print("=" * 60)
    print("  DBF MAPPER")
    print("=" * 60)
    print(f"  Directorio raiz: {root}")
    print(f"  Buscando archivos .dbf recursivamente...")
    print()

    dbf_files = find_dbf_files(root)

    if not dbf_files:
        print("  No se encontraron archivos .dbf en el directorio.")
        print(f"  Directorio analizado: {root}")
        sys.exit(0)

    print(f"  Encontrados: {len(dbf_files)} archivos .dbf")
    print()

    tables = []
    for idx, dbf_path in enumerate(dbf_files, 1):
        rel = dbf_path.relative_to(root) if dbf_path.is_relative_to(root) else dbf_path
        print(f"  [{idx:03d}/{len(dbf_files):03d}] Analizando: {rel} ...", end=" ", flush=True)
        info = read_dbf(dbf_path)
        tables.append(info)

        if info["error"]:
            print(f"ERROR: {info['error'][:50]}")
        else:
            print(f"OK ({info['record_count']:,} registros, {info['field_count']} campos)")

    print()
    print(f"  Generando reporte -> {output_path.name} ...")

    try:
        write_report(tables, root, output_path)
        print(f"  Reporte guardado: {output_path}")
        print()
        print("  Categorias detectadas:")

        from collections import defaultdict
        by_category = defaultdict(list)
        for t in tables:
            for cat in t["categories"]:
                by_category[cat].append(t)

        for cat, cat_tables in sorted(by_category.items()):
            print(f"    [{cat}]: {', '.join(t['filename'] for t in cat_tables)}")

    except Exception as exc:
        print(f"  ERROR al escribir reporte: {exc}")
        traceback.print_exc()
        sys.exit(1)

    print()
    print("  Listo. Presiona Enter para salir...")
    input()


if __name__ == "__main__":
    main()
