---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-context-examples.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Context example data (eCommerce) [painless-context-examples]

Complete the following installation steps to index the sample eCommerce orders data into {{es}}. You can run any of the context examples against this sample data after you've configured it. To add the eCommerce data you need to have {{kib}} installed.

Each document in this dataset represents a complete eCommerce order. Every order contains complete transaction data including product details, pricing information, customer data, and geographic location. Orders might include multiple products, with product-specific information stored as values within individual fields.

## Install the eCommerce sample data [painless-sample-data-install]

1. Go to **Integrations** and search for **Sample Data**.   
2. On the **Sample data** page, expand the **Other sample data sets**.
3. Click **Add data** to install **Sample eCommerce orders**.   
4. **Verify Installation:** Navigate to **Analytics \> Discover** and select the `kibana_sample_data_ecommerce` data view. You should see eCommerce order documents with complete customer, product, and transaction information.


## Sample document structure

Here’s an example of a complete eCommerce order document with two products (basic T-shirt and boots):

```json
{
  "order_id": 578286,
  "order_date": "2025-08-13T16:53:46+00:00",
  "type": "order",
  "currency": "EUR",
  "customer_id": 39,
  "customer_full_name": "Kamal Brock",
  "customer_first_name": "Kamal",
  "customer_last_name": "Brock",
  "customer_gender": "MALE",
  "customer_phone": "",
  "email": "kamal@brock-family.zzz",
  "user": "kamal",
  "day_of_week": "Wednesday",
  "day_of_week_i": 2,
  "geoip": {
    "city_name": "Istanbul",
    "region_name": "Istanbul",
    "country_iso_code": "TR",
    "continent_name": "Asia",
    "location": {
  "lat": 41,
  "lon": 29
    }
  },
  "category": [
    "Men's Clothing",
    "Men's Shoes"
  ],
  "manufacturer": [
    "Elitelligence",
    "Oceanavigations"
  ],
  "sku": [
    "ZO0548305483",
    "ZO0256702567"
  ],
  "products": [
    {
  "_id": "sold_product_578286_15939",
  "product_id": 15939,
  "product_name": "Basic T-shirt - khaki",
  "category": "Men's Clothing",
  "manufacturer": "Elitelligence",
  "sku": "ZO0548305483",
  "base_price": 7.99,
  "base_unit_price": 7.99,
  "price": 7.99,
  "taxful_price": 7.99,
  "taxless_price": 7.99,
  "quantity": 1,
  "discount_amount": 0,
  "discount_percentage": 0,
  "tax_amount": 0,
  "created_on": "2016-12-21T16:53:46+00:00"
    },
    {
  "_id": "sold_product_578286_1844",
  "product_id": 1844,
  "product_name": "Boots - beige",
  "category": "Men's Shoes",
  "manufacturer": "Oceanavigations",
  "sku": "ZO0256702567",
  "base_price": 84.99,
  "base_unit_price": 84.99,
  "price": 84.99,
  "taxful_price": 84.99,
  "taxless_price": 84.99,
  "quantity": 1,
  "discount_amount": 0,
  "discount_percentage": 0,
  "tax_amount": 0,
  "created_on": "2016-12-21T16:53:46+00:00"
    }
  ],
  "taxful_total_price": 92.98,
  "taxless_total_price": 92.98,
  "total_quantity": 2,
  "total_unique_products": 2,
  "event": {
    "dataset": "sample_ecommerce"
  }
}
```

## Field Reference

* **Order information**

    `order_id` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): Unique identifier for each order.
    
    `order_date` ([`date`](/reference/elasticsearch/mapping-reference/date.md)): Timestamp when the order was placed.
    
    `type` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): Document type (always "order").
    
    `currency` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): Transaction currency (EUR, USD, etc.).
    
* **Customer information**
    
    `customer_id` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): Unique customer identifier.
    
    `customer_full_name` ([`text`](/reference/elasticsearch/mapping-reference/text.md)): Complete customer name with keyword subfield.
    
    `customer_first_name` ([`text`](/reference/elasticsearch/mapping-reference/text.md)): Customer's first name with keyword subfield.
    
    `customer_last_name` ([`text`](/reference/elasticsearch/mapping-reference/text.md)): Customer's last name with keyword subfield.
    
    `customer_gender` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): Customer gender (MALE, FEMALE).
    
    `email` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): Customer email address.
    
    `user` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): Username derived from customer name.

* **Geographic Information**

    `geoip.city_name` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): City where the order was placed.
    
    `geoip.continent_name` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): Continent of order origin.
    
    `geoip.country_iso_code` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): Two-letter country code.
    
    `geoip.location` ([`geo_point`](/reference/elasticsearch/mapping-reference/geo-point.md)): Geographic coordinates of order location.
    
    `geoip.region_name` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): State or region name.

* **Order timing**

    `day_of_week` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): Day when the order was placed (Monday, Tuesday, and so on).
    
    `day_of_week_i` ([`integer`](/reference/elasticsearch/mapping-reference/number.md)): Numeric day of week (0-6).

* **Product information**

    `category` ([`text`](/reference/elasticsearch/mapping-reference/text.md)): Primary product categories with keyword subfield.
    
    `manufacturer` ([`text`](/reference/elasticsearch/mapping-reference/text.md)): Manufacturer names with keyword subfield.
    
    `sku` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): Stock Keeping Unit codes.
    
    `products.product_name` ([`text`](/reference/elasticsearch/mapping-reference/text.md)): Product names (comma-separated for multiple products).
    
    `products.product_id` ([`long`](/reference/elasticsearch/mapping-reference/number.md)): Product identifiers.
    
    `products._id` ([`text`](/reference/elasticsearch/mapping-reference/text.md)): Internal product identifiers with keyword subfield.
    
    `products.sku` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): Product-specific SKU codes.
    
    `products.category` ([`text`](/reference/elasticsearch/mapping-reference/text.md)): Individual product categories with keyword subfield.
    
    `products.manufacturer` ([`text`](/reference/elasticsearch/mapping-reference/text.md)): Product-specific manufacturers with keyword subfield.
    
    `products.created_on` ([`date`](/reference/elasticsearch/mapping-reference/date.md)): Product catalog creation dates.
    
    `products.quantity` ([`integer`](/reference/elasticsearch/mapping-reference/number.md)): Quantity of each product ordered.

* **Pricing and financial information**

    `products.base_price` ([`half_float`](/reference/elasticsearch/mapping-reference/number.md)): Original product prices before discounts.
    
    `products.base_unit_price` ([`half_float`](/reference/elasticsearch/mapping-reference/number.md)): Base price per unit.
    
    `products.price` ([`half_float`](/reference/elasticsearch/mapping-reference/number.md)): Final product prices.
    
    `products.min_price` ([`half_float`](/reference/elasticsearch/mapping-reference/number.md)): Minimum price thresholds.
    
    `products.discount_amount` ([`half_float`](/reference/elasticsearch/mapping-reference/number.md)): Discount amounts applied.
    
    `products.discount_percentage` ([`half_float`](/reference/elasticsearch/mapping-reference/number.md)) : Percentage discounts applied.
    
    `products.unit_discount_amount` ([`half_float`](/reference/elasticsearch/mapping-reference/number.md)): Discount amount per unit.
    
    `products.tax_amount` ([`half_float`](/reference/elasticsearch/mapping-reference/number.md)): Tax amounts for products.
    
    `products.taxful_price` ([`half_float`](/reference/elasticsearch/mapping-reference/number.md)): Product prices including tax.
    
    `products.taxless_price` ([`half_float`](/reference/elasticsearch/mapping-reference/number.md)): Product prices excluding tax.
    
    `taxful_total_price` ([`half_float`](/reference/elasticsearch/mapping-reference/number.md)): Total order amount including tax.
    
    `taxless_total_price` ([`half_float`](/reference/elasticsearch/mapping-reference/number.md)): Total order amount excluding tax.

* **Order summary**

    `total_quantity` ([`integer`](/reference/elasticsearch/mapping-reference/number.md)): Total items in the order.
    
    `total_unique_products` ([`integer`](/reference/elasticsearch/mapping-reference/number.md)): Number of different products in the order.

* **Metadata**

    `event.dataset` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md)): Dataset identifier ("sample\_ecommerce").

