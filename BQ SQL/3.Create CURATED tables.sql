-- create ds_cUrated tables

CREATE OR REPLACE  TABLE ds_curated.dim_date
(
  DateKey STRING,
  Date STRING,
  FiscalYear STRING,
  FiscalQuarter STRING,
  Month STRING,
  FullDate STRING,
  MonthKey STRING,
  LastModifiedOn STRING,
  Ingestion_Date TIMESTAMP NOT NULL,
  src_file_name STRING NOT NULL
);
CREATE OR REPLACE  TABLE ds_curated.dim_customer
(
  CustomerKey STRING,
  CustomerID STRING,
  Customer STRING,
  City STRING,
  StateProvince STRING,
  CountryRegion STRING,
  PostalCode STRING,
  LastModifiedOn STRING,
  Ingestion_Date TIMESTAMP NOT NULL,
  src_file_name STRING NOT NULL
);
CREATE OR REPLACE  TABLE ds_curated.dim_territory
(
  TerritoryKey STRING,
  Region STRING,
  Country STRING,
  CountryGroup STRING,
  LastModifiedOn STRING,
  Ingestion_Date TIMESTAMP NOT NULL,
  src_file_name STRING NOT NULL
);
CREATE OR REPLACE  TABLE ds_curated.fact_sales
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
  Ingestion_Date TIMESTAMP NOT NULL,
  src_file_name STRING NOT NULL
);
CREATE OR REPLACE  TABLE ds_curated.dim_reseller
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
  Ingestion_Date TIMESTAMP NOT NULL,
  src_file_name STRING NOT NULL
);
CREATE OR REPLACE  TABLE ds_curated.dim_sales_order
(
  Channel STRING,
  SalesOrderLineKey STRING,
  SalesOrder STRING,
  SalesOrderLine STRING,
  LastModifiedOn STRING,
  Ingestion_Date TIMESTAMP NOT NULL,
  src_file_name STRING NOT NULL
);
CREATE OR REPLACE  TABLE ds_curated.dim_product
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
  Ingestion_Date TIMESTAMP NOT NULL,
  src_file_name STRING NOT NULL
);