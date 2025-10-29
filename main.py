from utils.connection_sql import CreateModel
from utils.pipeline import FileETL

if __name__ == '__main__':
    df_final = FileETL.run_pipeline('./input/*.xlsx', list(range(2018, 2026)))
    excel_path = './resultados/Unido.xlsx'
    FileETL.export_to_excel(df_final, excel_path)
    print(f"Archivo Excel generado en: {excel_path}")

    # Llamar a la migracion
    if CreateModel.ejecutar_migracion(excel_path, 'localhost', 'BD_Unica', 'ODBC Driver 17 for SQL Server'):
        print("Migración completada con éxito.")
    else:
        print("Error en la migración.")
