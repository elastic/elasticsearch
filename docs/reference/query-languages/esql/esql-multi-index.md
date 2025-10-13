---
navigation_title: Query multiple indices
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-multi-index.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Use ES|QL to query multiple indices [esql-multi-index]

With {{esql}}, you can execute a single query across multiple indices, data streams, or aliases. To do so, use wildcards and date arithmetic. The following example uses a comma-separated list and a wildcard:

```esql
FROM employees-00001,other-employees-*
```

Use the format `<remote_cluster_name>:<target>` to [query data streams and indices on remote clusters](esql-cross-clusters.md):

```esql
FROM cluster_one:employees-00001,cluster_two:other-employees-*
```


## Field type mismatches [esql-multi-index-invalid-mapping]

When querying multiple indices, data streams, or aliases, you might find that the same field is mapped to multiple different types. For example, consider the two indices with the following field mappings:

**index: events_ip**

```
{
  "mappings": {
    "properties": {
      "@timestamp":     { "type": "date" },
      "client_ip":      { "type": "ip" },
      "event_duration": { "type": "long" },
      "message":        { "type": "keyword" }
    }
  }
}
```

**index: events_keyword**

```
{
  "mappings": {
    "properties": {
      "@timestamp":     { "type": "date" },
      "client_ip":      { "type": "keyword" },
      "event_duration": { "type": "long" },
      "message":        { "type": "keyword" }
    }
  }
}
```

When you query each of these individually with a simple query like `FROM events_ip`, the results are provided with type-specific columns:

```esql
FROM events_ip
| SORT @timestamp DESC
```

| @timestamp:date | client_ip:ip | event_duration:long | message:keyword |
| --- | --- | --- | --- |
| 2023-10-23T13:55:01.543Z | 172.21.3.15 | 1756467 | Connected to 10.1.0.1 |
| 2023-10-23T13:53:55.832Z | 172.21.3.15 | 5033755 | Connection error |
| 2023-10-23T13:52:55.015Z | 172.21.3.15 | 8268153 | Connection error |

Note how the `client_ip` column is correctly identified as type `ip`, and all values are displayed. However, if instead the query sources two conflicting indices with `FROM events_*`, the type of the `client_ip` column cannot be determined and is reported as `unsupported` with all values returned as `null`.

$$$query-unsupported$$$

```esql
FROM events_*
| SORT @timestamp DESC
```

| @timestamp:date | client_ip:unsupported | event_duration:long | message:keyword |
| --- | --- | --- | --- |
| 2023-10-23T13:55:01.543Z | null | 1756467 | Connected to 10.1.0.1 |
| 2023-10-23T13:53:55.832Z | null | 5033755 | Connection error |
| 2023-10-23T13:52:55.015Z | null | 8268153 | Connection error |
| 2023-10-23T13:51:54.732Z | null | 725448 | Connection error |
| 2023-10-23T13:33:34.937Z | null | 1232382 | Disconnected |
| 2023-10-23T12:27:28.948Z | null | 2764889 | Connected to 10.1.0.2 |
| 2023-10-23T12:15:03.360Z | null | 3450233 | Connected to 10.1.0.3 |

In addition, if the query refers to this unsupported field directly, the query fails:

```esql
FROM events_*
| SORT client_ip DESC
```

```bash
Cannot use field [client_ip] due to ambiguities being mapped as
[2] incompatible types:
    [ip] in [events_ip],
    [keyword] in [events_keyword]
```


## Union types [esql-multi-index-union-types]

::::{warning}
This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


{{esql}} has a way to handle [field type mismatches](#esql-multi-index-invalid-mapping). When the same field is mapped to multiple types in multiple indices, the type of the field is understood to be a *union* of the various types in the index mappings. As seen in the preceding examples, this *union type* cannot be used in the results, and cannot be referred to by the query — except in `KEEP`, `DROP` or when it’s passed to a type conversion function that accepts all the types in the *union* and converts the field to a single type. {{esql}} offers a suite of [type conversion functions](functions-operators/type-conversion-functions.md) to achieve this.

In the above examples, the query can use a command like `EVAL client_ip = TO_IP(client_ip)` to resolve the union of `ip` and `keyword` to just `ip`. You can also use the type-conversion syntax `EVAL client_ip = client_ip::IP`. Alternatively, the query could use [`TO_STRING`](functions-operators/type-conversion-functions.md#esql-to_string) to convert all supported types into `KEYWORD`.

For example, the [query](#query-unsupported) that returned `client_ip:unsupported` with `null` values can be improved using the `TO_IP` function or the equivalent `field::ip` syntax. These changes also resolve the error message. As long as the only reference to the original field is to pass it to a conversion function that resolves the type ambiguity, no error results.

```esql
FROM events_*
| EVAL client_ip = TO_IP(client_ip)
| KEEP @timestamp, client_ip, event_duration, message
| SORT @timestamp DESC
```

| @timestamp:date | client_ip:ip | event_duration:long | message:keyword |
| --- | --- | --- | --- |
| 2023-10-23T13:55:01.543Z | 172.21.3.15 | 1756467 | Connected to 10.1.0.1 |
| 2023-10-23T13:53:55.832Z | 172.21.3.15 | 5033755 | Connection error |
| 2023-10-23T13:52:55.015Z | 172.21.3.15 | 8268153 | Connection error |
| 2023-10-23T13:51:54.732Z | 172.21.3.15 | 725448 | Connection error |
| 2023-10-23T13:33:34.937Z | 172.21.0.5 | 1232382 | Disconnected |
| 2023-10-23T12:27:28.948Z | 172.21.2.113 | 2764889 | Connected to 10.1.0.2 |
| 2023-10-23T12:15:03.360Z | 172.21.2.162 | 3450233 | Connected to 10.1.0.3 |

### Date and date_nanos union type [esql-multi-index-date-date-nanos-union]
```{applies_to}
stack: ga 9.2.0
```
When the type of an {{esql}} field is a *union* of `date` and `date_nanos` across different indices, {{esql}} automatically casts all values to the `date_nanos` type during query execution. This implicit casting ensures that all values are handled with nanosecond precision, regardless of their original type. As a result, users can write queries against such fields without needing to perform explicit type conversions, and the query engine will seamlessly align the types for consistent and precise results.

`date_nanos` fields offer higher precision but have a narrower range of valid values compared to `date` fields. This limits their representable dates roughly from 1970 to 2262. This is because dates are stored as a `long` representing nanoseconds since the epoch. When a field is mapped as both `date` and `date_nanos` across different indices, {{esql}} defaults to the more precise `date_nanos` type. This behavior ensures that no precision is lost when querying multiple indices with differing date field types. For dates that fall outside the valid range of `date_nanos` in fields that are mapped to both `date` and `date_nanos` across different indices, {{esql}} returns null by default. However, users can explicitly cast these fields to the `date` type to obtain a valid value, with precision limited to milliseconds.

For example, if the `@timestamp` field is mapped as `date` in one index and `date_nanos` in another, {{esql}} will automatically treat all `@timestamp` values as `date_nanos` during query execution. This allows users to write queries that utilize the `@timestamp` field without encountering type mismatch errors, ensuring accurate time-based operations and comparisons across the combined dataset.

**index: events_date**

```
{
  "mappings": {
    "properties": {
      "@timestamp":     { "type": "date" },
      "client_ip":      { "type": "ip" },
      "event_duration": { "type": "long" },
      "message":        { "type": "keyword" }
    }
  }
}
```

**index: events_date_nanos**

```
{
  "mappings": {
    "properties": {
      "@timestamp":     { "type": "date_nanos" },
      "client_ip":      { "type": "ip" },
      "event_duration": { "type": "long" },
      "message":        { "type": "keyword" }
    }
  }
}
```

```esql
FROM events_date*
| EVAL date = @timestamp::date
| KEEP @timestamp, date, client_ip, event_duration, message
| SORT date
```

| @timestamp:date_nanos | date:date   | client_ip:ip | event_duration:long | message:keyword |
|--------------------------| --- |--------------|---------| --- |
| null                          |1969-10-23T13:33:34.937Z| 172.21.0.5   | 1232382 |Disconnected|
| 2023-10-23T12:15:03.360Z      |2023-10-23T12:15:03.360Z| 172.21.2.162 | 3450233 |Connected to 10.1.0.3|
| 2023-10-23T12:15:03.360103847Z|2023-10-23T12:15:03.360Z| 172.22.2.162 | 3450233 |Connected to 10.1.0.3|
| 2023-10-23T12:27:28.948Z      |2023-10-23T12:27:28.948Z| 172.22.2.113 | 2764889 |Connected to 10.1.0.2|
| 2023-10-23T12:27:28.948Z      |2023-10-23T12:27:28.948Z| 172.21.2.113 | 2764889 |Connected to 10.1.0.2|
| 2023-10-23T13:33:34.937193Z   |2023-10-23T13:33:34.937Z| 172.22.0.5   | 1232382 |Disconnected|
| null                          |2263-10-23T13:51:54.732Z| 172.21.3.15  | 725448  |Connection error|

## Index metadata [esql-multi-index-index-metadata]

It can be helpful to know the particular index from which each row is sourced. To get this information, use the [`METADATA`](esql-metadata-fields.md) option on the [`FROM`](commands/from.md) command.

```esql
FROM events_* METADATA _index
| EVAL client_ip = TO_IP(client_ip)
| KEEP _index, @timestamp, client_ip, event_duration, message
| SORT @timestamp DESC
```

| _index:keyword | @timestamp:date | client_ip:ip | event_duration:long | message:keyword |
| --- | --- | --- | --- | --- |
| events_ip | 2023-10-23T13:55:01.543Z | 172.21.3.15 | 1756467 | Connected to 10.1.0.1 |
| events_ip | 2023-10-23T13:53:55.832Z | 172.21.3.15 | 5033755 | Connection error |
| events_ip | 2023-10-23T13:52:55.015Z | 172.21.3.15 | 8268153 | Connection error |
| events_keyword | 2023-10-23T13:51:54.732Z | 172.21.3.15 | 725448 | Connection error |
| events_keyword | 2023-10-23T13:33:34.937Z | 172.21.0.5 | 1232382 | Disconnected |
| events_keyword | 2023-10-23T12:27:28.948Z | 172.21.2.113 | 2764889 | Connected to 10.1.0.2 |
| events_keyword | 2023-10-23T12:15:03.360Z | 172.21.2.162 | 3450233 | Connected to 10.1.0.3 |

