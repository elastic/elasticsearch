## `NOW` [esql-now]

**Syntax**

:::{image} ../../../../../images/now.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

**Description**

Returns current date and time.

**Supported types**

| result |
| --- |
| date |

**Examples**

```esql
ROW current_date = NOW()
```

| y:keyword |
| --- |
| 20 |

To retrieve logs from the last hour:

```esql
FROM sample_data
| WHERE @timestamp > NOW() - 1 hour
```

| @timestamp:date | client_ip:ip | event_duration:long | message:keyword |
| --- | --- | --- | --- |
