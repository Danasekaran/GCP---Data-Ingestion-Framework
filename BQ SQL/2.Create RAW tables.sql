-- CREATE OR REPLACE  DS_RAW TABLES

CREATE OR REPLACE  TABLE ds_raw.fact_sales
(
  SalesOrderLineKey STRING,
  ResellerKey STRING,
  CustomerKey STRING,
  ProductKey STRING,
  OrderDateKey STRING,
  DueDateKey STRING,
  ShipDateKey STRING,
  SalesTerritoryKey STRING,
  OrderQuantity STRING,
  UnitPrice STRING,
  ExtendedAmount STRING,
  UnitPriceDiscountPct STRING,
  ProductStandardCost STRING,
  TotalProductCost STRING,
  SalesAmount STRING,
  LastModifiedOn STRING,
  Ingestion_Date TIMESTAMP,
  src_File_Name STRING
);
CREATE OR REPLACE  TABLE ds_raw.dim_date
(
  DateKey STRING,
  Date STRING,
  FiscalYear STRING,
  FiscalQuarter STRING,
  Month STRING,
  FullDate STRING,
  MonthKey STRING,
  LastModifiedOn STRING,
  Ingestion_Date TIMESTAMP,
  src_file_name STRING
);
CREATE OR REPLACE  TABLE ds_raw.dim_customer
(
  CustomerKey STRING,
  CustomerID STRING,
  Customer STRING,
  City STRING,
  StateProvince STRING,
  CountryRegion STRING,
  PostalCode STRING,
  LastModifiedOn STRING,
  Ingestion_Date TIMESTAMP,
  src_file_name STRING
);
CREATE OR REPLACE  TABLE ds_raw.dim_territory
(
  TerritoryKey STRING,
  Region STRING,
  Country STRING,
  CountryGroup STRING,
  LastModifiedOn STRING,
  Ingestion_Date TIMESTAMP,
  src_file_name STRING
);
CREATE OR REPLACE  TABLE ds_raw.fact_sales
(
  SalesOrderLineKey STRING,
  ResellerKey STRING,
  CustomerKey STRING,
  ProductKey STRING,
  OrderDateKey STRING,
  DueDateKey STRING,
  ShipDateKey STRING,
  SalesTerritoryKey STRING,
  OrderQuantity STRING,
  UnitPrice STRING,
  ExtendedAmount STRING,
  UnitPriceDiscountPct STRING,
  ProductStandardCost STRING,
  TotalProductCost STRING,
  SalesAmount STRING,
  LastModifiedOn STRING,
  Ingestion_Date TIMESTAMP,
  src_file_name STRING
);
CREATE OR REPLACE  TABLE ds_raw.dim_reseller
(
  ResellerKey STRING,
  ResellerID STRING,
  BusinessType STRING,
  Reseller STRING,
  City STRING,
  StateProvince STRING,
  CountryRegion STRING,
  PostalCode STRING,
  LastModifiedOn STRING,
  Ingestion_Date TIMESTAMP,
  src_file_name STRING
);
CREATE OR REPLACE  TABLE ds_raw.dim_sales_order
(
  Channel STRING,
  SalesOrderLineKey STRING,
  SalesOrder STRING,
  SalesOrderLine STRING,
  LastModifiedOn STRING,
  Ingestion_Date TIMESTAMP,
  src_file_name STRING
);
CREATE OR REPLACE  TABLE ds_raw.dim_product
(
  ProductKey STRING,
  SKU STRING,
  Product STRING,
  StandardCost STRING,
  Color STRING,
  ListPrice STRING,
  Model STRING,
  SubCategory STRING,
  Category STRING,
  LastModifiedOn STRING,
  Ingestion_Date TIMESTAMP,
  src_file_name STRING
);