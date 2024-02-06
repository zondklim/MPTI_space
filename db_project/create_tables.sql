CREATE TYPE gender_type AS ENUM ('F', 'M', 'U');

CREATE TYPE deceased_type AS ENUM ('N', 'Y');

CREATE TYPE car_own_type AS ENUM ('Yes', 'No');


CREATE TABLE "customer" (
  "customer_id" integer PRIMARY KEY,
  "first_name" varchar(50),
  "last_name" varchar(50),
  "gender" gender_type,
  "DOB" date,
  "deceased_indicator" deceased_type,
  "address_id" varchar(36),
  "job_id" varchar(36),
  "wealth_id" varchar(36)
);

CREATE TABLE "address" (
  "address_id" varchar(36) PRIMARY KEY,
  "address" text,
  "postcode" varchar(4),
  "state" text,
  "country" varchar(50)
);

CREATE TABLE "job_title" (
  "job_id" varchar(36) PRIMARY KEY,
  "job_title" text,
  "job_industry_category" text
);

CREATE TABLE "wealth" (
  "wealth_id" varchar(36) PRIMARY KEY,
  "wealth_segment" text,
  "owns_car" car_own_type,
  "property_valuation" smallint
);

CREATE TABLE "transaction" (
  "transaction_id" integer PRIMARY KEY,
  "product_id" integer,
  "customer_id" integer,
  "transaction_date" date,
  "online_order" BOOLEAN,
  "order_status" varchar(9),
  "list_price" float(4)
);

CREATE TABLE "product" (
  "product_id" integer,
  "brand" text,
  "product_class" varchar(6),
  "product_line" varchar(8),
  "product_size" varchar(6), 
  "standard_cost" float(4)
);


