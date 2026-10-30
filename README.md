# UNAD-BigData-Tarea3-Daniel-Olist

Tarea 3 – Procesamiento de Datos con Apache Spark (Big Data, UNAD). Procesamiento batch y en tiempo real con Apache Spark y Apache Kafka sobre el conjunto de datos *Brazilian E-Commerce Public Dataset by Olist*.

**Autor:** Daniel Mauricio Medina Trujillo · Curso Big Data (código 202016911), grupo 103 · Tutor: Jaime Rubiano Llorente

## Problema
Analizar las ventas de una tienda en línea para identificar qué categorías de productos generan más ingresos, cómo evoluciona el número de pedidos mes a mes y cuánto tardan las entregas según el estado del cliente.

## Conjunto de datos
Olist (Kaggle): https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce  
Archivos usados en el análisis batch: `olist_orders_dataset.csv`, `olist_order_items_dataset.csv`, `olist_products_dataset.csv`, `olist_customers_dataset.csv` y `product_category_name_translation.csv`.

## Contenido del repositorio

| Archivo | Descripción |
|---|---|
| `analisis_olist.py` | Procesamiento batch con PySpark: carga los CSV, filtra los pedidos entregados, calcula los días de entrega y muestra (1) las 10 categorías con más ingresos, (2) el total de pedidos por mes y (3) el promedio de días de entrega por estado (los 10 más rápidos). |
| `productor_kafka.py` | Productor de Kafka: cada 2 segundos envía al topic `topic_olist` un pedido en formato JSON (`id_pedido`, `estado`, `valor`). |
| `streaming_kafka.py` | Spark Structured Streaming: lee el topic `topic_olist`, interpreta el JSON y cuenta los pedidos por estado, mostrando el resultado en consola. |

> **Nota:** los pedidos que envía `productor_kafka.py` son **simulados** (valores aleatorios con la estructura de un pedido); no son filas reales del dataset de Olist.

## Requisitos
- Python 3 y Java
- Apache Spark (PySpark)
- Apache Kafka funcionando en `localhost:9092`
- Librerías de Python: `pip install pyspark kafka-python`

## Cómo ejecutarlo

### 1. Procesamiento batch
Descarga el dataset de Kaggle, descomprímelo en la misma carpeta de los scripts y ejecuta:
```bash
spark-submit analisis_olist.py
