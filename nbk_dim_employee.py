# Databricks notebook source
from pyspark.sql.functions import col,encode
#Staging
substitution_subset= ["bk_dim_employee","nme_employee_name","val_employee_age","dsc_employee_marital_status","dsc_employee_gender",
                      "dat_employee_hire_date","dsc_employee_department","val_employee_salary","dsc_employee_job_grade"]

stgDF = spark.read.table('ods.sales_dataset') 

stgDF = (#sobrescrever este dataframe com novos dados
    stgDF
    .select(#selecione as colunas que queremos inserir no dataframe
        col('EmployeeID').alias('bk_dim_employee'),
        col('EmployeeName').alias('nme_employee_name'),
        col('Age').alias('val_employee_age'),
        col('MaritalStatus').alias('dsc_employee_marital_status'),
        col('Gender').alias('dsc_employee_gender'),
        col('HireDate').alias('dat_employee_hire_date'),
        col('Dept').alias('dsc_employee_department'),
        col('Salary').alias('val_employee_salary'),
        col('JobGrade').alias('dsc_employee_job_grade')
    )
    .fillna("UNKNOWN", subset = substitution_subset)
).distinct()

stgDF.write.format('delta').mode('overwrite').saveAsTable('stg.dim_employee')

#DW
dwDF = spark.sql(f'''
    CREATE TABLE dw.dim_employee(
        sk_dim_employee BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
        bk_dim_employee STRING,
        nme_employee_name STRING,
        val_employee_age STRING,
        dsc_employee_marital_status STRING,
        dsc_employee_gender STRING,
        dat_employee_hire_date STRING,
        dsc_employee_department STRING,
        val_employee_salary STRING,
        dsc_employee_job_grade STRING
    )
''')

#merge Staging and DW

%sql
 MERGE INTO dw.dim_employee AS target
 USING stg.dim_employee AS source
 ON target.bk_dim_employee = source.bk_dim_employee
 WHEN MATCHED THEN
 UPDATE SET
   target.nme_employee_name = source.nme_employee_name,
   target.val_employee_age = source.val_employee_age,
   target.dsc_employee_marital_status = source.dsc_employee_marital_status,
   target.dsc_employee_gender = source.dsc_employee_gender,
   target.dat_employee_hire_date = source.dat_employee_hire_date,
   target.dsc_employee_department = source.dsc_employee_department,
   target.val_employee_salary = source.val_employee_salary,
   target.dsc_employee_job_grade = source.dsc_employee_job_grade
 WHEN NOT MATCHED THEN
 INSERT(
   bk_dim_employee,
   nme_employee_name,
   val_employee_age,
   dsc_employee_marital_status,
   dsc_employee_gender,
   dat_employee_hire_date,
   dsc_employee_department,
   val_employee_salary,
   dsc_employee_job_grade
 )
 VALUES(
   source.bk_dim_employee,
   source.nme_employee_name,
   source.val_employee_age,
   source.dsc_employee_marital_status,
   source.dsc_employee_gender,
   source.dat_employee_hire_date,
   source.dsc_employee_department,
   source.val_employee_salary,
   source.dsc_employee_job_grade
 )


#View table

 %sql
 CREATE OR REPLACE VIEW views.dim_employee AS
 SELECT
   sk_dim_employee AS `Funcionário (Cód.)`,
   bk_dim_employee AS `Funcionário`,
   nme_employee_name AS `Nome do Funcionário`,
   val_employee_age AS `Idade do Funcionário`,
   dsc_employee_marital_status AS `Estado civil`,
   dsc_employee_gender AS `Genero do Funcionário`,
   dat_employee_hire_date AS `Data de contratação`,
   dsc_employee_department AS `Departamento`,
   val_employee_salary AS `Salário`,
   dsc_employee_job_grade AS `Categoria`
 FROM dw.dim_employee