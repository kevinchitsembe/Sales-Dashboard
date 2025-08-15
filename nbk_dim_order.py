# Databricks notebook source
from pyspark.sql.functions import col
#Staging

substitution_subset = ['bk_dim_order','val_order_date','val_order_quantity','dsc_product_name','dsc_product_category','dsc_product_sub_category','dsc_promotion_name','dsc_channel','dsc_manufacturer','dsc_country','dsc_city','dsc_region']

stgDF = spark.read.table('ods.sales_dataset')

stgDF = (
    stgDF
    .select(
        col('OrderID').alias('bk_dim_order'),
        col('OrderDate').alias('val_order_date'),
        col('OrderQty').alias('val_order_quantity'),
        col('ProductName').alias('dsc_product_name'),
        col('ProductCategory').alias('dsc_product_category'),
        col('ProductSubCategory').alias('dsc_product_sub_category'),
        col('PromotionName').alias('dsc_promotion_name'),
        col('Channel').alias('dsc_channel'),
        col('Manufacturer').alias('dsc_manufacturer'),
        col('Country').alias('dsc_country'),
        col('City').alias('dsc_city'),
        col('Region').alias('dsc_region')
    )
    .fillna("UNKNOWN", subset = substitution_subset)
).distinct()

stgDF.write.format('delta').mode('overwrite').saveAsTable('stg.dim_order')

#DW

dwDF = spark.sql(f'''
    CREATE TABLE dw.dim_order(
        sk_dim_order BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1),
        bk_dim_order INT,
        val_order_date INT,
        val_order_quantity INT,
        dsc_product_name STRING,
        dsc_product_category STRING,
        dsc_product_sub_category STRING,
        dsc_promotion_name STRING,
        dsc_channel STRING,
        dsc_manufacturer STRING,
        dsc_country STRING,
        dsc_city STRING,
        dsc_region STRING
    )
''')

#merge Staging and DW

  %sql
  MERGE INTO dw.dim_order AS target
  USING stg.dim_order AS source
  ON target.bk_dim_order = source.bk_dim_order
  WHEN MATCHED THEN 
  UPDATE SET
    target.val_order_date = source.val_order_date,
    target.val_order_quantity = source.val_order_quantity,
    target.dsc_product_name = source.dsc_product_name,
    target.dsc_product_category = source.dsc_product_category,
    target.dsc_promotion_name = source.dsc_promotion_name,
    target.dsc_channel = source.dsc_channel,
    target.dsc_manufacturer = source.dsc_manufacturer,
    target.dsc_country = source.dsc_country,
    target.dsc_city = source.dsc_city,
    target.dsc_region = source.dsc_region
  WHEN NOT MATCHED THEN
  INSERT(
    bk_dim_order,
    val_order_date,
    val_order_quantity,
    dsc_product_name,
    dsc_product_category,
    dsc_product_sub_category,
    dsc_promotion_name,
    dsc_channel,
    dsc_manufacturer,
    dsc_country,
    dsc_city,
    dsc_region 
  )
  VALUES(
    source.bk_dim_order,
    source.val_order_date,
    source.val_order_quantity,
    source.dsc_product_name,
    source.dsc_product_category,
    source.dsc_product_sub_category,
    source.dsc_promotion_name,
    source.dsc_channel,
    source.dsc_manufacturer,
    source.dsc_country,
    source.dsc_city,
    source.dsc_region
  )


#View table

  %sql
  CREATE OR REPLACE VIEW views.dim_order AS
  SELECT
   sk_dim_order AS `Ordem (Cód.)`,
   bk_dim_order AS `Ordem`,
   val_order_date AS `Data da ordem`,
   val_order_quantity AS `Quantidade da ordem`,
   dsc_product_name AS `Nome do produto`,
   dsc_product_category AS `Categoria do produto`,
   dsc_product_sub_category AS `Sub categoria do produto`,
   dsc_promotion_name AS `Promoção`,
   dsc_channel AS `Canal da venda`,
   dsc_manufacturer AS `Produtor`,
   dsc_country AS `País da venda`,
   dsc_city AS `City da venda`,
   dsc_region AS `Região da venda` 
  FROM dw.dim_order