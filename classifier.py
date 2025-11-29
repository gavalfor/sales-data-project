import sqlite3
from datetime import datetime, timedelta

DATABASE_NAME = 'sales_data.db'
CURRENT_DATE = datetime(2025, 3, 5) # Usamos la fecha del video como referencia

def connect_db():
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row # Permite acceder a las columnas por nombre
    return conn

def classify_date(order_date_str, interval_days=90):
    """
    Recrea la lógica del macro del video: clasifica la fecha como 'Recent' o 'Historical'.
    
    Args:
        order_date_str (str): Fecha de la venta en formato YYYY-MM-DD.
        interval_days (int): Número de días para la clasificación 'Recent'.
    
    Returns:
        str: 'Recent' o 'Historical'.
    """
    try:
        order_date = datetime.strptime(order_date_str, '%Y-%m-%d')
        # La venta es 'Recent' si la fecha de venta está dentro del intervalo desde la fecha actual.
        if order_date >= CURRENT_DATE - timedelta(days=interval_days):
            return "Recent"
        else:
            return "Historical"
    except ValueError:
        return "Invalid Date"

def create_raw_layer():
    """Crea la tabla RAW con datos de prueba (incluye fechas históricas)."""
    sql_create = """
    CREATE TABLE IF NOT EXISTS ventas_raw (
        id_transaccion INTEGER,
        fecha_venta TEXT,
        producto_id TEXT,
        monto REAL
    );
    """
    execute_sql(sql_create)
    execute_sql("DELETE FROM ventas_raw")
    
    RAW_DATA = [
        (1, '2025-03-01', 'Laptop', 1200.00), # Recent (dentro de 90 días de 2025-03-05)
        (2, '2025-01-15', 'Phone', 500.00),   # Recent
        (3, '2024-08-20', 'Tablet', 300.00),  # Historical (más de 90 días antes de 2025-03-05)
        (4, '2025-02-10', 'Laptop', 1500.00), # Recent
        (5, '2024-11-25', 'Monitor', 400.00)  # Historical
    ]
    
    sql_insert = "INSERT INTO ventas_raw VALUES (?, ?, ?, ?)"
    conn = connect_db()
    conn.cursor().executemany(sql_insert, RAW_DATA)
    conn.commit()
    conn.close()

def execute_sql(sql_query):
    """Helper para ejecutar comandos SQL."""
    conn = connect_db()
    conn.cursor().execute(sql_query)
    conn.commit()
    conn.close()

if __name__ == '__main__':
    create_raw_layer()
    print(f"Fecha de referencia: {CURRENT_DATE.strftime('%Y-%m-%d')}")
    print("Muestra de clasificación (simulación del macro):")
    print(f"2025-03-01 -> {classify_date('2025-03-01')}")
    print(f"2024-08-20 -> {classify_date('2024-08-20')}")