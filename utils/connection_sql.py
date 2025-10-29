import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.types import NVARCHAR, Integer, Float, SMALLINT
import urllib
from typing import Dict, Any, Tuple

class CreateModel:
    """Clase para orquestar la migración ETL desde Excel a SQL Server (Modelo Estrella)."""
    
    @staticmethod
    def _configurar_conexion(server: str, database: str, driver: str) -> str:
        """Configura y devuelve el string de conexión a SQL Server."""
        params = urllib.parse.quote_plus(
            f'DRIVER={{{driver}}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'Trusted_Connection=yes;'
        )
        return f'mssql+pyodbc:///?odbc_connect={params}'

    @staticmethod
    def _limpiar_datos(df: pd.DataFrame) -> pd.DataFrame:
        """Realiza la limpieza y preparación inicial del DataFrame."""
        
        # Limpiar espacios en columnas de texto
        columnas_texto = df.select_dtypes(include=['object']).columns
        for col in columnas_texto:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True)

        # Limpiar comillas en nombres
        df['APELLIDOS Y NOMBRES'] = df['APELLIDOS Y NOMBRES'].str.replace('"', '', regex=False)
        df['APELLIDOS Y NOMBRES'] = df['APELLIDOS Y NOMBRES'].str.replace("'", '', regex=False)

        # Limpiar DNI
        df['DNI'] = df['DNI'].str.strip()
        df['DNI'] = df['DNI'].replace('nan', np.nan)

        # Eliminar registros con DNI nulo
        df = df[df['DNI'].notna()]

        return df

    @staticmethod
    def _crear_dimensiones(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Crea todos los DataFrames de dimensiones a partir del DataFrame principal."""
        
        dimensiones = {}

        # Dim_Postulante (DNI es PK)
        dim_postulante = df[['DNI', 'APELLIDOS Y NOMBRES']].drop_duplicates(subset=['DNI'])
        dim_postulante = dim_postulante[dim_postulante['DNI'].notna()]
        dim_postulante = dim_postulante.reset_index(drop=True)
        dimensiones['Dim_Postulante'] = dim_postulante[['DNI', 'APELLIDOS Y NOMBRES']]

        # Dim_Area
        dim_area = df[['AREA']].drop_duplicates()
        dim_area = dim_area[dim_area['AREA'].notna()]
        dim_area = dim_area.reset_index(drop=True)
        dim_area['ID_AREA'] = range(1, len(dim_area) + 1)
        dimensiones['Dim_Area'] = dim_area[['ID_AREA', 'AREA']]

        # Dim_Periodo
        dim_periodo = df[['PERIODO']].drop_duplicates()
        dim_periodo = dim_periodo[dim_periodo['PERIODO'].notna()]
        dim_periodo = dim_periodo.reset_index(drop=True)
        dim_periodo['ID_Periodo'] = range(1, len(dim_periodo) + 1)
        dimensiones['Dim_Periodo'] = dim_periodo[['ID_Periodo', 'PERIODO']]

        # Dim_Modalidad
        dim_modalidad = df[['MODALIDAD NORMALIZADA']].drop_duplicates()
        dim_modalidad = dim_modalidad[dim_modalidad['MODALIDAD NORMALIZADA'].notna()]
        dim_modalidad = dim_modalidad.reset_index(drop=True)
        dim_modalidad['ID_Modalidad'] = range(1, len(dim_modalidad) + 1)
        dim_modalidad.columns = ['MODALIDAD', 'ID_Modalidad']
        dimensiones['Dim_Modalidad'] = dim_modalidad[['ID_Modalidad', 'MODALIDAD']]

        # Dim_Facultad
        dim_facultad = df[['FACULTAD']].drop_duplicates()
        dim_facultad = dim_facultad[dim_facultad['FACULTAD'].notna()]
        dim_facultad = dim_facultad.reset_index(drop=True)
        dim_facultad['ID_Facultad'] = range(1, len(dim_facultad) + 1)
        dimensiones['Dim_Facultad'] = dim_facultad[['ID_Facultad', 'FACULTAD']]

        # Dim_Carreras 
        dim_carreras = df[['CARRERA NORMALIZADA']].drop_duplicates()
        dim_carreras = dim_carreras[dim_carreras['CARRERA NORMALIZADA'].notna()]
        dim_carreras = dim_carreras.reset_index(drop=True)
        dim_carreras['ID_Carrera'] = range(1, len(dim_carreras) + 1)
        dim_carreras.columns = ['CARRERA', 'ID_Carrera']
        dimensiones['Dim_Carreras'] = dim_carreras[['ID_Carrera', 'CARRERA']]

        # Dim_Condicion
        dim_condicion = df[['CONDICION']].drop_duplicates()
        dim_condicion = dim_condicion[dim_condicion['CONDICION'].notna()]
        dim_condicion = dim_condicion.reset_index(drop=True)
        dim_condicion['ID_Condicion'] = range(1, len(dim_condicion) + 1)
        dimensiones['Dim_Condicion'] = dim_condicion[['ID_Condicion', 'CONDICION']]

        # Dim_Escala
        dim_escala = df[['Escala']].drop_duplicates()
        dim_escala = dim_escala[dim_escala['Escala'].notna()]
        dim_escala = dim_escala.reset_index(drop=True)
        dim_escala['ID_Escala'] = range(1, len(dim_escala) + 1)
        dim_escala.columns = ['ESCALA', 'ID_Escala']
        dimensiones['Dim_Escala'] = dim_escala[['ID_Escala', 'ESCALA']]

        # Calen_Año
        calen_año = df[['AÑO']].drop_duplicates()
        calen_año = calen_año[calen_año['AÑO'].notna()]
        calen_año = calen_año.sort_values('AÑO').reset_index(drop=True)
        calen_año.columns = ['Anio']
        dimensiones['Calen_Año'] = calen_año

        return dimensiones

    @staticmethod
    def _crear_fact_table(df: pd.DataFrame, dimensiones: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Crea la tabla de hechos Fact_Admision con las Foreign Keys."""
        
        fact_admision = df.copy()

        # Crear columna AñoPeriodo
        fact_admision['AñoPeriodo'] = fact_admision['AÑO'].astype(str) + '-' + fact_admision['PERIODO']

        # Hacer merge con dimensiones para obtener IDs
        fact_admision = fact_admision.merge(dimensiones['Dim_Area'][['ID_AREA', 'AREA']], on='AREA', how='left')
        fact_admision = fact_admision.merge(dimensiones['Dim_Periodo'][['ID_Periodo', 'PERIODO']], on='PERIODO', how='left')
        fact_admision = fact_admision.merge(dimensiones['Dim_Modalidad'][['ID_Modalidad', 'MODALIDAD']], 
                                            left_on='MODALIDAD NORMALIZADA', right_on='MODALIDAD', how='left')
        fact_admision = fact_admision.merge(dimensiones['Dim_Facultad'][['ID_Facultad', 'FACULTAD']], on='FACULTAD', how='left')
        fact_admision = fact_admision.merge(dimensiones['Dim_Carreras'][['ID_Carrera', 'CARRERA']], 
                                            left_on='CARRERA NORMALIZADA', right_on='CARRERA', how='left')
        fact_admision = fact_admision.merge(dimensiones['Dim_Condicion'][['ID_Condicion', 'CONDICION']], on='CONDICION', how='left')
        fact_admision = fact_admision.merge(dimensiones['Dim_Escala'][['ID_Escala', 'ESCALA']], 
                                            left_on='Escala', right_on='ESCALA', how='left')

        # Seleccionar columnas finales
        fact_admision = fact_admision[[
            'DNI',
            'APELLIDOS Y NOMBRES',
            'AÑO',
            'AñoPeriodo',
            'ID_Periodo',
            'ID_Modalidad',
            'ID_Carrera',
            'ID_Facultad',
            'ID_Condicion',
            'ID_Escala',
            'ID_AREA',
            'PUNTAJE',
            'Puntaje_normalizado'
        ]]

        return fact_admision

    @staticmethod
    def _definir_tipos_sql() -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
        """Define los tipos de datos de SQLAlchemy para la migración."""
        dtype_dimensiones = {
            'Dim_Postulante': {
                'DNI': NVARCHAR(20),
                'APELLIDOS Y NOMBRES': NVARCHAR(60)
            },
            'Dim_Area': {
                'ID_AREA': Integer,
                'AREA': NVARCHAR(10)
            },
            'Dim_Periodo': {
                'ID_Periodo': Integer,
                'PERIODO': NVARCHAR(10)
            },
            'Dim_Modalidad': {
                'ID_Modalidad': Integer,
                'MODALIDAD': NVARCHAR(60)
            },
            'Dim_Facultad': {
                'ID_Facultad': Integer,
                'FACULTAD': NVARCHAR(60)
            },
            'Dim_Carreras': {
                'ID_Carrera': Integer,
                'CARRERA': NVARCHAR(80)
            },
            'Dim_Condicion': {
                'ID_Condicion': Integer,
                'CONDICION': NVARCHAR(20)
            },
            'Dim_Escala': {
                'ID_Escala': Integer,
                'ESCALA': NVARCHAR(20)
            },
            'Calen_Año': {
                'Anio': SMALLINT
            }
        }
        
        dtype_fact = {
            'DNI': NVARCHAR(20),
            'APELLIDOS Y NOMBRES': NVARCHAR(60),
            'AÑO': SMALLINT,
            'AñoPeriodo': NVARCHAR(10),
            'ID_Periodo': Integer,
            'ID_Modalidad': Integer,
            'ID_Carrera': Integer,
            'ID_Facultad': Integer,
            'ID_Condicion': Integer,
            'ID_Escala': Integer,
            'ID_AREA': Integer,
            'PUNTAJE': Float,
            'Puntaje_normalizado': Float
        }
        
        return dtype_dimensiones, dtype_fact

    @staticmethod
    def _migrar_tablas(engine, dimensiones: Dict[str, pd.DataFrame], fact_admision: pd.DataFrame):
        """Migra todas las dimensiones y la tabla de hechos a SQL Server."""
        
        dtype_dimensiones, dtype_fact = CreateModel._definir_tipos_sql()
        
        # Migrar dimensiones
        for nombre, df_dim in dimensiones.items():
            df_dim.to_sql(nombre, engine, if_exists='replace', index=False, dtype=dtype_dimensiones[nombre])

        # Migrar Fact Table
        fact_admision.to_sql('Fact_Admision', engine, if_exists='replace', index=False, 
                            dtype=dtype_fact, chunksize=1000)

    @staticmethod
    def _crear_constraints(engine):
        """Aplica las restricciones NOT NULL, Primary Keys y Foreign Keys en SQL Server."""
        
        with engine.connect() as conn:
            # Modificar columnas a NOT NULL
            conn.execute(text("ALTER TABLE Dim_Postulante ALTER COLUMN DNI NVARCHAR(20) NOT NULL"))
            conn.execute(text("ALTER TABLE Dim_Area ALTER COLUMN ID_AREA INT NOT NULL"))
            conn.execute(text("ALTER TABLE Dim_Area ALTER COLUMN AREA NVARCHAR(10) NOT NULL"))
            conn.execute(text("ALTER TABLE Dim_Periodo ALTER COLUMN ID_Periodo INT NOT NULL"))
            conn.execute(text("ALTER TABLE Dim_Periodo ALTER COLUMN PERIODO NVARCHAR(10) NOT NULL"))
            conn.execute(text("ALTER TABLE Dim_Modalidad ALTER COLUMN ID_Modalidad INT NOT NULL"))
            conn.execute(text("ALTER TABLE Dim_Modalidad ALTER COLUMN MODALIDAD NVARCHAR(60) NOT NULL"))
            conn.execute(text("ALTER TABLE Dim_Facultad ALTER COLUMN ID_Facultad INT NOT NULL"))
            conn.execute(text("ALTER TABLE Dim_Facultad ALTER COLUMN FACULTAD NVARCHAR(60) NOT NULL"))
            conn.execute(text("ALTER TABLE Dim_Carreras ALTER COLUMN ID_Carrera INT NOT NULL"))
            conn.execute(text("ALTER TABLE Dim_Carreras ALTER COLUMN CARRERA NVARCHAR(80) NOT NULL"))
            conn.execute(text("ALTER TABLE Dim_Condicion ALTER COLUMN ID_Condicion INT NOT NULL"))
            conn.execute(text("ALTER TABLE Dim_Condicion ALTER COLUMN CONDICION NVARCHAR(20) NOT NULL"))
            conn.execute(text("ALTER TABLE Dim_Escala ALTER COLUMN ID_Escala INT NOT NULL"))
            conn.execute(text("ALTER TABLE Dim_Escala ALTER COLUMN ESCALA NVARCHAR(20) NOT NULL"))
            conn.commit()
            
            # Primary Keys
            conn.execute(text("ALTER TABLE Dim_Postulante ADD CONSTRAINT PK_Postulante PRIMARY KEY (DNI)"))
            conn.execute(text("ALTER TABLE Dim_Area ADD CONSTRAINT PK_Area PRIMARY KEY (ID_AREA)"))
            conn.execute(text("ALTER TABLE Dim_Periodo ADD CONSTRAINT PK_Periodo PRIMARY KEY (ID_Periodo)"))
            conn.execute(text("ALTER TABLE Dim_Modalidad ADD CONSTRAINT PK_Modalidad PRIMARY KEY (ID_Modalidad)"))
            conn.execute(text("ALTER TABLE Dim_Facultad ADD CONSTRAINT PK_Facultad PRIMARY KEY (ID_Facultad)"))
            conn.execute(text("ALTER TABLE Dim_Carreras ADD CONSTRAINT PK_Carreras PRIMARY KEY (ID_Carrera)"))
            conn.execute(text("ALTER TABLE Dim_Condicion ADD CONSTRAINT PK_Condicion PRIMARY KEY (ID_Condicion)"))
            conn.execute(text("ALTER TABLE Dim_Escala ADD CONSTRAINT PK_Escala PRIMARY KEY (ID_Escala)"))
            conn.commit()
            
            # Foreign Keys
            conn.execute(text("ALTER TABLE Fact_Admision ADD CONSTRAINT FK_Fact_Postulante FOREIGN KEY (DNI) REFERENCES Dim_Postulante(DNI)"))
            conn.execute(text("ALTER TABLE Fact_Admision ADD CONSTRAINT FK_Fact_Area FOREIGN KEY (ID_AREA) REFERENCES Dim_Area(ID_AREA)"))
            conn.execute(text("ALTER TABLE Fact_Admision ADD CONSTRAINT FK_Fact_Periodo FOREIGN KEY (ID_Periodo) REFERENCES Dim_Periodo(ID_Periodo)"))
            conn.execute(text("ALTER TABLE Fact_Admision ADD CONSTRAINT FK_Fact_Modalidad FOREIGN KEY (ID_Modalidad) REFERENCES Dim_Modalidad(ID_Modalidad)"))
            conn.execute(text("ALTER TABLE Fact_Admision ADD CONSTRAINT FK_Fact_Facultad FOREIGN KEY (ID_Facultad) REFERENCES Dim_Facultad(ID_Facultad)"))
            conn.execute(text("ALTER TABLE Fact_Admision ADD CONSTRAINT FK_Fact_Carreras FOREIGN KEY (ID_Carrera) REFERENCES Dim_Carreras(ID_Carrera)"))
            conn.execute(text("ALTER TABLE Fact_Admision ADD CONSTRAINT FK_Fact_Condicion FOREIGN KEY (ID_Condicion) REFERENCES Dim_Condicion(ID_Condicion)"))
            conn.execute(text("ALTER TABLE Fact_Admision ADD CONSTRAINT FK_Fact_Escala FOREIGN KEY (ID_Escala) REFERENCES Dim_Escala(ID_Escala)"))
            conn.commit()
            
    @staticmethod
    def ejecutar_migracion(archivo_excel: str, server: str, database: str, driver: str) -> bool:
        """Orquesta el proceso completo de migración ETL a SQL Server."""
        try:
            # Configurar conexión
            connection_string = CreateModel._configurar_conexion(server, database, driver)
            
            # Leer datos con DNI como String
            df = pd.read_excel(archivo_excel, dtype={'DNI': str})
            
            # Limpieza de datos
            df_limpio = CreateModel._limpiar_datos(df)
            
            # Crear dimensiones
            dimensiones = CreateModel._crear_dimensiones(df_limpio)
            
            # Crear Fact Table
            fact_admision = CreateModel._crear_fact_table(df_limpio, dimensiones)
            
            # Conectar a SQL Server
            engine = create_engine(connection_string, fast_executemany=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            # Migrar tablas
            CreateModel._migrar_tablas(engine, dimensiones, fact_admision)
            
            # Crear constraints
            CreateModel._crear_constraints(engine)
            
            return True
            
        except Exception as e:
            print(f"Error en la migracion: {e}")
            return False