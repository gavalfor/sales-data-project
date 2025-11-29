import pandas as pd
import numpy as np
# Importar clientes de BigQuery y GCS (conceptualmente)
# from google.cloud import bigquery
# BQ_CLIENT = bigquery.Client()

def generate_and_ingest_raw_data(num_rows=1000000):
    """Genera datos sintéticos (simulando una ingesta) y escribe a BQ.raw_sales."""
    
    # [Generación de datos similar al paso anterior]
    start_date = pd.to_datetime('2024-01-01')
    data = {
        'order_id': np.random.randint(10000000, 99999999, num_rows),
        'product_name': np.random.choice(['Laptop', 'Phone', 'Tablet', 'Headphones', 'Monitor'], num_rows),
        'amount_str': np.random.randint(100, 7000, num_rows).astype(str) + '.00', # Simula un tipo de dato incorrecto (string)
        'country': np.random.choice(['usa', 'Canada', 'Mexico', 'UK', 'Germany'], num_rows),
        'order_date': start_date + pd.to_timedelta(np.random.randint(1, 365, num_rows), unit='D'),
        'discount': np.random.uniform(0.0, 0.2, num_rows).round(2)
    }
    df_raw = pd.DataFrame(data)

    # Lógica de escritura a BigQuery (ejemplo conceptual):
    # destination_table = f"{project_id}.{dataset_id}.{table_id}"
    # df_raw.to_gbq(destination_table, project_id=project_id, if_exists='replace') 
    
    print(f"✅ Capa Raw: {num_rows} registros generados.")
    return df_raw # Retorno para fines de simulación en este código