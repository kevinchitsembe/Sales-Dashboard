-- Databricks notebook source
-- Create Schemas

CREATE SCHEMA IF NOT EXISTS ods

CREATE SCHEMA IF NOT EXISTS stg

CREATE SCHEMA IF NOT EXISTS dw

CREATE SCHEMA IF NOT EXISTS views

-- Create ods table

  %python
 
  odsDF = spark.sql(f''' 
      SELECT * FROM datasetvendas
  ''')
 
  odsDF.write.format('delta').mode('overwrite').saveAsTable('ods.sales_dataset')
 
  --#odsDF.write.format("delta").option("delta.columnMapping.mode", "name").saveAsTable("ods.sales_dataset")
  --#with this code we can have columns with spaces eg. code ID (codeID)