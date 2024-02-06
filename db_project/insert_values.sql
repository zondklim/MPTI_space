INSERT INTO "customer" (
  "customer_id",
  "first_name",
  "last_name",
  "gender",
  "DOB",
  "deceased_indicator",
  "address_id",
  "job_id",
  "wealth_id") VALUES %s;

INSERT INTO "address" (
  "address_id",
  "address",
  "postcode",
  "state",
  "country"
) VALUES %s;

INSERT INTO "job_title" (
  "job_id",
  "job_title",
  "job_industry_category" 
) VALUES %s;

INSERT INTO "wealth" (
  "wealth_id",
  "wealth_segment",
  "owns_car",
  "property_valuation"
) VALUES %s;

INSERT INTO "transaction" (
  "transaction_id",
  "product_id",
  "customer_id",
  "transaction_date",
  "online_order",
  "order_status",
  "list_price" 
) VALUES %s;

INSERT INTO "product" (
  "product_id",
  "brand",
  "product_class",
  "product_line",
  "product_size",
  "standard_cost"
) VALUES %s;