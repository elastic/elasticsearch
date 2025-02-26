## `CATEGORIZE` [esql-categorize]

::::{warning}
Do not use on production environments. This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


**Syntax**

:::{image} ../../../../../images/categorize.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Expression to categorize

**Description**

Groups text messages into categories of similarly formatted text values.

`CATEGORIZE` has the following limitations:

* can’t be used within other expressions
* can’t be used with multiple groupings
* can’t be used or referenced within aggregate functions

**Supported types**

| field | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

This example categorizes server logs messages into categories and aggregates their counts.

```esql
FROM sample_data
| STATS count=COUNT() BY category=CATEGORIZE(message)
```

| count:long | category:keyword |
| --- | --- |
| 3 | .**?Connected.+?to.**? |
| 3 | .**?Connection.+?error.**? |
| 1 | .**?Disconnected.**? |
