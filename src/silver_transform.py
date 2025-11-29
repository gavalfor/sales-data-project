from datetime import timedelta
import pandas as pd
import numpy as np

def transform_to_silver(df_bronze):
    """Lee Bronze y aplica la lógica de negocio para crear la columna partition_group."""
    
    # Lectura de la Capa Bronze (conceptual)
    # df_bronze = pd.read_gbq(f"SELECT * FROM {dataset_id}.bronze_sales", project_id=project_id)
    
    df_silver = df_bronze.copy()
    
    # 1. Definición de la Lógica de Negocio (3 meses o 90 días)
    CUTOFF_DAYS = 90
    
    # Se define la fecha actual (o la fecha máxima en el set de datos para un procesamiento histórico)
    current_date = df_silver['order_date'].max()
    cutoff_date = current_date - timedelta(days=CUTOFF_DAYS)
    
    # 2. Creación de la Feature (columna enriquecida)
    df_silver['partition_group'] = np.where(
        df_silver['order_date'] >= cutoff_date,
        'Recent', # Reciente (menos de 90 días)
        'Historical' # Histórico (más de 90 días)
    )
    
    # 3. Creación de la clave de negocio (Revenue)
    df_silver['total_revenue'] = df_silver['amount'] * (1 - df_silver['discount'])
    
    # Escritura a la Capa Silver (conceptual)
    # df_silver.to_gbq(f"{dataset_id}.silver_sales", project_id=project_id, if_exists='replace') 
    print(f"✅ Capa Silver: {len(df_silver)} registros enriquecidos escritos.")
    return df_silver