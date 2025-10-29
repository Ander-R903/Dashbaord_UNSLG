# Documentación de Medidas DAX

## Medidas Base

### Total Postulantes
**Expresión:** `COUNTROWS(FACT_ADMISION)`  
**Descripción:** Conteo total de postulantes registrados en el proceso de admisión.

### TotalIngresantes
**Expresión:** `CALCULATE([Total Postulantes], Dim_Condicion[CONDICION] = "INGRESO")`  
**Descripción:** Cantidad de postulantes que obtuvieron condición de ingreso aprobatorio.

### Total no Ingresantes
**Expresión:** `CALCULATE([Total Postulantes], Dim_Condicion[CONDICION] = "NO INGRESO")`  
**Descripción:** Cantidad de postulantes que no alcanzaron la condición de ingreso.

### TotalAusentes
**Expresión:** `COALESCE(CALCULATE([Total Postulantes], Dim_Condicion[CONDICION] = "AUSENTE"), 0)`  
**Descripción:** Cantidad de postulantes que no asistieron al examen de admisión.

### TotalAnulados
**Expresión:** `COALESCE(CALCULATE([Total Postulantes], Dim_Condicion[CONDICION] = "ANULADO"), 0)`  
**Descripción:** Cantidad de postulantes con examen anulado por irregularidades o incumplimiento de normas.

## Indicadores de Rendimiento

### Tasa de Ingreso
**Expresión:** `DIVIDE([TotalIngresantes], [Total Postulantes], 0)`  
**Descripción:** Ratio de ingresantes sobre total de postulantes. Mide el porcentaje de aprobación del proceso.

### PuntajePromedio
**Expresión:** `AVERAGEX(Fact_Admision, SWITCH(TRUE(), AnioSeleccionado >= 2023 && PeriodoSeleccionado = "II", Fact_Admision[Puntaje_normalizado], Fact_Admision[PUNTAJE]))`  
**Descripción:** Promedio de puntajes obtenidos por los postulantes. A partir del examen 2023-II se utiliza escala vigesimal (0-20), en exámenes anteriores se usa escala milésima (0-2000).

### PuntajeMaximo
**Expresión:** `SWITCH(TRUE(), AnioSeleccionado >= 2023 && PeriodoSeleccionado = "II", MAX(Fact_Admision[Puntaje_normalizado]), MAX(Fact_Admision[PUNTAJE]))`  
**Descripción:** Puntaje más alto registrado en el examen. Aplica escala vigesimal desde 2023-II, escala milésima en periodos anteriores.

### PuntajeMinimo
**Expresión:** `SWITCH(TRUE(), AnioSeleccionado >= 2023 && PeriodoSeleccionado = "II", MIN(Fact_Admision[Puntaje_normalizado]), MIN(Fact_Admision[PUNTAJE]))`  
**Descripción:** Puntaje más bajo registrado en el examen. Aplica escala vigesimal desde 2023-II, escala milésima en periodos anteriores.

### Puntaje_Promedio_Ingresantes
**Expresión:** `ROUND(CALCULATE(AVERAGE(Fact_Admision[Puntaje_normalizado]), Dim_Condicion[CONDICION] = "INGRESO"), 2)`  
**Descripción:** Promedio de puntajes exclusivamente de postulantes ingresantes. Indicador de rendimiento del grupo aprobado.

### Indice_Selectividad
**Expresión:** `DIVIDE([Total Postulantes], [TotalIngresantes], 0)`  
**Descripción:** Ratio de competencia por vacante. Indica cuántos postulantes compiten por cada cupo disponible.

### Porcentaje_Sobre_Promedio
**Expresión:** `DIVIDE(PostulantesSobrePromedio, TotalPostulantes, 0)`  
**Descripción:** Porcentaje de postulantes que superaron el promedio general del examen.

## Análisis por Carrera

### RankingCarrera
**Expresión:** `RANKX(ALLSELECTED(Dim_Carreras[CARRERA]), [Total Postulantes], , DESC, DENSE)`  
**Descripción:** Ranking de carreras por demanda de postulantes. Utilizar como filtro TopN para obtener las carreras más solicitadas.

### ColorCarrera
**Expresión:** `SWITCH(TRUE(), RankCarrera = 1, "#DD0314", RankCarrera <= 5, "#F08216", "#808080")`  
**Descripción:** Código de color para visualización jerárquica de demanda. Rojo para primer lugar, naranja para top 5, gris para resto.

## Análisis de Modalidad

### % Ordinaria
**Expresión:** `DIVIDE(TotalOrdinaria, TotalCarrera, 0)`  
**Descripción:** Proporción de postulantes que ingresaron por modalidad ordinaria respecto al total de la carrera.

### % Extraordinaria
**Expresión:** `1 - [% Ordinaria]`  
**Descripción:** Proporción de postulantes que ingresaron por modalidades extraordinarias (Deportistas, Primeros Puestos, etc.).

## Análisis Temporal

### Postulantes_2018
**Expresión:** `CALCULATE(COUNT(Fact_Admision[DNI]), 'Calen_Año'[Anio] = 2018)`  
**Descripción:** Total de postulantes en el año base 2018. Utilizado como referencia para cálculos de variación.

### Postulantes_2025
**Expresión:** `CALCULATE(COUNT(Fact_Admision[DNI]), 'Calen_Año'[Anio] = 2025)`  
**Descripción:** Total de postulantes en el año 2025. Utilizado para comparaciones de crecimiento.

### Crecimiento Interanual Postulantes (%)
**Expresión:** `DIVIDE(Postulantes_Actual - Postulantes_Anterior, Postulantes_Anterior, 0)`  
**Descripción:** Variación porcentual del último año respecto al año inmediato anterior. Mide tendencia de crecimiento.

### Crecimiento Anual Postulantes (%)
**Expresión:** `DIVIDE(Postulantes_Actual - Postulantes_Anterior, Postulantes_Anterior, 0)`  
**Descripción:** Variación porcentual del año seleccionado respecto a su año anterior disponible. Permite análisis año por año.

### Variacion_Porcentual
**Expresión:** `DIVIDE([Postulantes_2025] - [Postulantes_2018], [Postulantes_2018])`  
**Descripción:** Variación porcentual acumulada entre 2018 y 2025. Muestra crecimiento total en el periodo.

## Utilidades de Interfaz

### Titulo_Dinamico
**Expresión:** `"RESULTADOS DEL EXAMEN DE ADMISIÓN " & AnioActual & TextoPeriodo & TextoCarrera`  
**Descripción:** Genera título contextual según filtros aplicados. Incluye año, periodo y carrera cuando están seleccionados.

### Filtrar_Periodo
**Expresión:** `IF(COUNTROWS(FILTER(...)) > 0, 1, BLANK())`  
**Descripción:** Filtra periodos sin datos para años específicos. Oculta Periodo II en años 2019, 2022 y 2025 donde solo hubo examen en Periodo I.

## Medidas de Visualización HTML

### CARDS_DETALLE
**Tipo:** Medida HTML  
**Descripción:** Genera dashboard de tarjetas KPI con métricas principales y sus variaciones interanuales.

**Componentes:**
- **Postulantes:** Total con variación porcentual vs. año anterior
- **Ingresantes:** Total con variación porcentual vs. año anterior
- **Tasa de Ingreso:** Porcentaje con variación vs. año anterior
- **No Ingresantes:** Total sin variación (valor absoluto)
- **Puntaje Mínimo/Máximo:** Valores extremos del examen
- **Total Ausentes:** Postulantes no presentados

**Lógica principal:**
- Identifica dinámicamente el año anterior con datos disponibles
- Calcula diferencias absolutas y porcentuales para métricas comparables
- Genera indicadores visuales (flechas SVG, colores) según tendencia
- Aplica formato condicional: verde/naranja para crecimiento, rojo para decrecimiento
- Renderiza tarjetas HTML con estilos CSS embebidos

**Uso:** Visual HTML en página de análisis anual detallado.

---

### CARDS_PRINCIPAL
**Tipo:** Medida HTML  
**Descripción:** Versión simplificada de tarjetas KPI para vista principal del dashboard.

**Componentes:**
- **Postulantes:** Total con variación porcentual
- **Ingresantes:** Total con variación porcentual
- **Tasa de Ingreso:** Porcentaje con variación
- **No Ingresantes:** Total sin variación

**Diferencia con CARDS_DETALLE:** Excluye métricas de Ausentes, Anulados y Puntajes para mantener vista ejecutiva concisa.

**Uso:** Visual HTML en página principal (Home) del dashboard.

---

### Ficha_Postulante
**Tipo:** Medida HTML  
**Descripción:** Genera ficha individual de postulante con todos sus datos del proceso de admisión.

**Información mostrada:**
- Datos personales (DNI, nombres)
- Año y periodo del examen
- Carrera y facultad postulada
- Modalidad de ingreso
- Puntaje obtenido
- Condición final (Ingreso/No Ingreso/Ausente)

**Lógica principal:**
- Filtra registros por DNI ingresado
- Extrae información desde tablas dimensionales relacionadas
- Formatea datos en estructura de ficha legible
- Renderiza HTML con diseño de tarjeta informativa

**Uso:** Visual HTML en página de búsqueda por DNI.

---

### Home
**Tipo:** Medida HTML  
**Descripción:** Genera página de bienvenida con presentación institucional del dashboard.

**Contenido:**
- Título principal del sistema
- Descripción del propósito del dashboard
- Información institucional relevante
- Elementos visuales de presentación (logos, estilos corporativos)

**Lógica principal:**
- HTML estático con estilos CSS embebidos
- Sin cálculos DAX, puramente presentacional
- Diseño responsive para diferentes tamaños de pantalla

**Uso:** Página de inicio del dashboard (landing page).