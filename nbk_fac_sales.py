# Databricks notebook source
from pyspark.sql.functions import col, expr
#STAGING
stgDF = spark.read.table('ods.sales_dataset')

stgDF = (
    stgDF
    .select(
        col('EmployeeID').alias('bk_dim_employee'),
        col('OrderID').alias('bk_dim_order'),
        col('CostofSales').alias('amt_cost_of_sales'),
        col('Sales').alias('val_sales'),
        col('Profit').alias('amt_profit'),
        col('UnitCost').alias('amt_unit_cost'),
        col('Price').alias('amt_price')
    )
)

stgDF.write.format('delta').mode('overwrite').saveAsTable('stg.fac_sales')

#DW

dbutils.fs.rm('dbfs:/user/hive/warehouse/dw.db/fac_sales',True)

dwDF = spark.sql(f'''
    CREATE TABLE IF NOT EXISTS dw.fac_sales(
        sk_dim_employee INT,
        sk_dim_order INT,
        bk_employee INT,
        bk_order INT,
        amt_cost_of_sales STRING,
        val_sales STRING,
        amt_profit STRING,
        amt_unit_cost STRING,
        amt_price STRING
    )
''')

#Join dim table with fact table

  %sql
  INSERT INTO dw.fac_sales(
    SELECT
      IFNULL(employee.sk_dim_employee, -1) AS sk_dim_employee,
      IFNULL(orders.sk_dim_order, -1) AS sk_dim_order,
      
      IFNULL(stg_fac.bk_dim_employee, -1) AS bk_dim_employee,
      IFNULL(stg_fac.bk_dim_order, -1) AS bk_dim_order,
 
      stg_fac.amt_cost_of_sales AS amt_cost_of_sales,
      stg_fac.val_sales AS val_sales,
      stg_fac.amt_profit AS amt_profit,
      stg_fac.amt_unit_cost AS amt_unit_cost,
      stg_fac.amt_price AS amt_price
      
    FROM stg.fac_sales AS stg_fac
 
    LEFT JOIN dw.dim_employee AS employee
    ON stg_fac.bk_dim_employee = employee.bk_dim_employee
    
    LEFT JOIN dw.dim_order AS orders
    ON stg_fac.bk_dim_order = orders.bk_dim_order
  )

#View table

  %sql
  CREATE OR REPLACE VIEW views.fac_sales AS
  SELECT
    sk_dim_employee AS `Funcionário (Cód.)`,
    sk_dim_order AS `Ordem (Cód.)`,
    bk_employee AS `Funcionário`,
    bk_order AS `Ordem`,
    amt_cost_of_sales AS `Custo das vendas`,
    val_sales AS `Vendas`,
    amt_profit AS `Lucro`,
    amt_unit_cost AS `Custo por unidade`,
    amt_price AS `Preço`
  FROM dw.fac_sales