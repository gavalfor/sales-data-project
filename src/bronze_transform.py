from datetime import date
import pandas as pd

def transform_to_bronze(df_raw):
    """Lee Raw, limpia y estandariza, y escribe a BQ.bronze_sales."""
    
    # Lectura de la Capa Raw (conceptual)
    # df_raw = pd.read_gbq(f"SELECT * FROM {dataset_id}.raw_sales", project_id=project_id)
    
    df_bronze = df_raw.copy()
    
    # 1. Limpieza de columnas: Renombrar y convertir tipos
    df_bronze.columns = [col.replace('_str', '') for col in df_bronze.columns.str.lower()]
    
    # 2. Tipificación (Conversión de amount de string a float)
    df_bronze['amount'] = df_bronze['amount'].str.replace('.00', '', regex=False).astype(float)
    df_bronze['order_date'] = pd.to_datetime(df_bronze['order_date'])
    
    # 3. Estandarización de datos (ejemplo: Capitalizar Country)
    df_bronze['country'] = df_bronze['country'].str.upper()
    
    # 4. Manejo de nulos/duplicados (eliminación simple para el ejemplo)
    df_bronze.dropna(inplace=True) 
    df_bronze.drop_duplicates(subset=['order_id'], keep='first', inplace=True)
    
    # Escritura a la Capa Bronze (conceptual)
    # df_bronze.to_gbq(f"{dataset_id}.bronze_sales", project_id=project_id, if_exists='replace') 
    print(f"✅ Capa Bronze: {len(df_bronze)} registros estandarizados escritos.")
    return df_bronze