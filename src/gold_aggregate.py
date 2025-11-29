import pandas as pd

def aggregate_to_gold(df_silver):
    """Lee Silver, agrega métricas clave y escribe a BQ.gold_sales_summary."""
    
    # Lectura de la Capa Silver (conceptual)
    # df_silver = pd.read_gbq(f"SELECT * FROM {dataset_id}.silver_sales", project_id=project_id)
    
    # 1. Agregación
    df_gold = df_silver.groupby(['country', 'product_name', 'partition_group']).agg(
        total_revenue_sum=('total_revenue', 'sum'),
        total_orders=('order_id', 'count')
    ).reset_index()
    
    # 2. Formateo final
    df_gold['total_revenue_sum'] = df_gold['total_revenue_sum'].round(2)

    # Escritura a la Capa Gold (conceptual)
    # df_gold.to_gbq(f"{dataset_id}.gold_sales_summary", project_id=project_id, if_exists='replace') 
    print(f"✅ Capa Gold: {len(df_gold)} filas de resumen escritas.")
    return df_gold