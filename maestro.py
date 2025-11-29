# maestro.py

import os
import pandas as pd
# Importa tus funciones de los archivos src
from src.raw_data_generator import generate_and_ingest_raw_data 
from src.bronze_transform import transform_to_bronze
from src.silver_transform import transform_to_silver
from src.gold_aggregate import aggregate_to_gold

OUTPUT_DIR = "data"

def run_pipeline():
    # 1. Preparar la carpeta de datos
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Directorio de salida creado: {OUTPUT_DIR}")

    # --- 1. CAPA RAW (Generación) ---
    print("\n--- Ejecutando Capa RAW (Generando datos) ---")
    df_raw = generate_and_ingest_raw_data()
    # Escribir a un archivo RAW si es necesario, pero pasamos el DF en memoria a Bronze
    
    # --- 2. CAPA BRONZE (Limpieza) ---
    print("\n--- Ejecutando Capa BRONZE (Estandarización) ---")
    df_bronze = transform_to_bronze(df_raw)
    df_bronze.to_parquet(f"{OUTPUT_DIR}/bronze_sales.parquet", index=False) 
    print(f"✅ Capa Bronze: {len(df_bronze)} registros estandarizados.")
    
    # --- 3. CAPA SILVER (Lógica de Negocio) ---
    print("\n--- Ejecutando Capa SILVER (Enriquecimiento) ---")
    df_silver = transform_to_silver(df_bronze)
    df_silver.to_parquet(f"{OUTPUT_DIR}/silver_sales.parquet", index=False) 
    print(f"✅ Capa Silver: {len(df_silver)} registros enriquecidos.")

    # --- 4. CAPA GOLD (Agregación) ---
    print("\n--- Ejecutando Capa GOLD (Resumen) ---")
    df_gold = aggregate_to_gold(df_silver)
    df_gold.to_parquet(f"{OUTPUT_DIR}/gold_sales_summary.parquet", index=False) 
    print(f"✅ Capa Gold: {len(df_gold)} registros de resumen escritos.")
    
    print("\n✅ ¡FLUJO COMPLETO! Archivos Parquet creados en la carpeta 'data/'.")

if __name__ == "__main__":
    run_pipeline()