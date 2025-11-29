# verify_data.py
import pandas as pd
from datetime import timedelta

def check_project_quality():
    print("\n--- INICIANDO VERIFICACIÓN DE CALIDAD ---")
    
    # Lectura de los archivos de salida
    df_bronze = pd.read_parquet('data/bronze_sales.parquet')
    df_silver = pd.read_parquet('data/silver_sales.parquet')
    df_gold = pd.read_parquet('data/gold_sales_summary.parquet')

    # 1. PRUEBA BRONZE: Estandarización y Limpieza
    dirty_countries = df_bronze[df_bronze['country'] != df_bronze['country'].str.upper()].shape[0]
    assert dirty_countries == 0, f"ERROR BRONZE: {dirty_countries} países no están en mayúsculas."
    print("✅ Bronze OK: Estandarización de países verificada.")

    # 2. PRUEBA SILVER: Lógica de Negocio (Recent vs Historical)
    current_date = df_silver['order_date'].max()
    cutoff_date = current_date - timedelta(days=90)
    # Buscamos errores: registros 'Recent' que son viejos
    wrong_recent = df_silver[
        (df_silver['partition_group'] == 'Recent') & 
        (df_silver['order_date'] < cutoff_date)
    ].shape[0]
    assert wrong_recent == 0, f"ERROR SILVER: {wrong_recent} registros 'Recent' son incorrectos."
    print("✅ Silver OK: Lógica 'Reciente/Histórico' verificada.")

    # 3. PRUEBA GOLD: Consistencia (Agregación)
    total_silver = df_silver['total_revenue'].sum()
    total_gold = df_gold['total_revenue_sum'].sum()
    assert abs(total_silver - total_gold) < 0.01, "ERROR GOLD: La suma total no coincide entre Silver y Gold."
    print("✅ Gold OK: Consistencia de ingresos verificada.")
    
    print("\n🎉 ¡PROYECTO VALIDADO CON ÉXITO!")

if __name__ == "__main__":
    check_project_quality()