from typing import List, Optional
import polars as pl
import glob
import os
from utils.mapeo import dict_modalidades, dict_carreras, dict_facultades, dict_area


class FileETL:

    @staticmethod
    def load_files(path_pattern: str, years: List[int]) -> pl.DataFrame:
        """Carga y concatena todos los archivos Excel que coincidan con los años indicados."""
        files = [f for f in glob.glob(path_pattern) if any(str(y) in f for y in years)]
        df = pl.concat([
            pl.read_excel(f)
              .drop('facultad', strict=False)
              .rename({'escuela': 'carrera'}, strict=False)
            for f in files
        ], how='vertical')
        return df

    @staticmethod
    def rename_columns(df: pl.DataFrame) -> pl.DataFrame:
        """Renombra columnas a un estándar uniforme."""
        return df.rename({
            'dni': 'DNI',
            'apellidos_nombres': 'APELLIDOS Y NOMBRES',
            'puntaje': 'PUNTAJE',
            'condicion': 'CONDICION',
            'anio': 'AÑO',
            'periodo': 'PERIODO',
            'modalidad_ingreso': 'MODALIDAD',
            'carrera': 'CARRERA'
        })

    @staticmethod
    def clean_dni(df: pl.DataFrame) -> pl.DataFrame:
        """Convierte la columna DNI a tipo texto."""
        return df.with_columns(pl.col("DNI").cast(pl.Utf8).alias("DNI"))

    @staticmethod
    def clean_names(df: pl.DataFrame) -> pl.DataFrame:
        """Convierte los nombres completos a mayúsculas."""
        return df.with_columns(pl.col('APELLIDOS Y NOMBRES').str.to_uppercase().alias('APELLIDOS Y NOMBRES'))

    @staticmethod
    def convert_year(df: pl.DataFrame) -> pl.DataFrame:
        """Convierte la columna año a tipo entero."""
        return df.with_columns(pl.col('AÑO').cast(pl.Int64).alias('AÑO'))

    @staticmethod
    def fix_scores_and_condition(df: pl.DataFrame) -> pl.DataFrame:
        """Corrige valores de puntaje para ausentes/anulados y ajusta la condición."""
        
        # Usar el texto ('AUSENTE', 'ANULADO') de PUNTAJE a CONDICION
        # y convertir PUNTAJE a None en esos casos
        df = df.with_columns([
            pl.when(pl.col('PUNTAJE').is_in(['AUSENTE', 'ANULADO']))
            .then(pl.col('PUNTAJE')) 
            .otherwise(pl.col('CONDICION'))
            .alias('CONDICION'),
            
            pl.when(pl.col('PUNTAJE').is_in(['AUSENTE', 'ANULADO']))
            .then(None) 
            .otherwise(pl.col('PUNTAJE').cast(pl.Float64, strict=False))
            .alias('PUNTAJE')
        ])
        
        # Si la  CONDICION es 'AUSENTE' o 'ANULADO', asegurar que PUNTAJE sea None
        df = df.with_columns([
            pl.when(pl.col('CONDICION').is_in(['AUSENTE', 'ANULADO']))
            .then(None)
            .otherwise(pl.col('PUNTAJE'))
            .alias('PUNTAJE')
        ])
        
        return df


    @staticmethod
    def normalize_scores(df: pl.DataFrame) -> pl.DataFrame:
        """Normaliza los puntajes según la escala del examen y guarda puntaje original."""
        return df.with_columns([
            pl.when(
                ((pl.col('AÑO') >= 2018) & (pl.col('AÑO') < 2023)) |
                ((pl.col('AÑO') == 2023) & (pl.col('PERIODO') == "I"))
            ).then(pl.lit('0-2000')).otherwise(pl.lit('0-20')).alias('Escala'),

            pl.col('PUNTAJE').alias('PuntajeOriginal'),

            pl.when(
                pl.col("PUNTAJE").is_not_null() &
                (((pl.col('AÑO') >= 2018) & (pl.col('AÑO') < 2023)) |
                 ((pl.col('AÑO') == 2023) & (pl.col('PERIODO') == "I"))) &
                (pl.col('PUNTAJE') >= 0)
            ).then(pl.col('PUNTAJE') / 100)
             .otherwise(pl.col('PUNTAJE'))
             .alias('Puntaje_normalizado')
        ])

    @staticmethod
    def clean_modalidad(df: pl.DataFrame) -> pl.DataFrame:
        """Rellena valores nulos y normaliza las modalidades según el diccionario."""
        df = df.with_columns(
            pl.col('MODALIDAD').fill_null('ORDINARIA').alias('MODALIDAD')
        )
        df = df.with_columns(
            pl.col("MODALIDAD").map_elements(lambda x: dict_modalidades.get(x, x), return_dtype=pl.Utf8)
            .alias("MODALIDAD NORMALIZADA")
        )
        return df

    @staticmethod
    def clean_period(df: pl.DataFrame) -> pl.DataFrame:
        """Rellena periodos nulos con 'I'."""
        return df.with_columns(pl.col('PERIODO').fill_null('I'))

    @staticmethod
    def clean_carrera(df: pl.DataFrame) -> pl.DataFrame:
        """Limpia, normaliza carreras y agrega facultad y área."""
        df = df.with_columns(
            pl.col('CARRERA').str.replace(r"^.*?:\s*", "").alias('CARRERA')
        )
        df = df.with_columns(
            pl.col('CARRERA').map_elements(lambda x: dict_carreras.get(x, x), return_dtype=pl.Utf8)
            .alias('CARRERA NORMALIZADA') 
        )
        df = df.with_columns(
            pl.col('CARRERA NORMALIZADA').map_elements(lambda x: dict_facultades.get(x, x), return_dtype=pl.Utf8)  
            .alias('FACULTAD')
        )
        df = df.with_columns(
            pl.col('CARRERA NORMALIZADA').map_elements(lambda x: dict_area.get(x, x), return_dtype=pl.Utf8) 
            .alias('AREA')
        )
        return df

    @staticmethod
    def export_to_excel(df: pl.DataFrame, filepath: str) -> None:
        """Exporta un DataFrame de Polars a Excel, creando la carpeta si no existe."""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        df.to_pandas().to_excel(filepath, index=False, na_rep="")

    @staticmethod
    def run_pipeline(path_pattern: str, years: List[int]) -> pl.DataFrame:
        """Ejecuta todo el pipeline y devuelve el DataFrame procesado."""
        df = FileETL.load_files(path_pattern, years)
        df = FileETL.rename_columns(df)
        df = FileETL.clean_dni(df)
        df = FileETL.clean_names(df)
        df = FileETL.convert_year(df)
        df = FileETL.fix_scores_and_condition(df)
        df = FileETL.normalize_scores(df)
        df = FileETL.clean_modalidad(df)
        df = FileETL.clean_period(df)
        df = FileETL.clean_carrera(df)
        return df
