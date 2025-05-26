---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/api-conventions.html
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster.html
applies_to:
  stack: all
  serverless: all
navigation_title: API conventions
---

# Elasticsearch API conventions [api-conventions]

The {{es}} REST APIs are exposed over HTTP. Except where noted, the following conventions apply across all APIs.


## Content-type requirements [_content_type_requirements]

The type of the content sent in a request body must be specified using the `Content-Type` header. The value of this header must map to one of the supported formats that the API supports. Most APIs support JSON, YAML, CBOR, and SMILE. The bulk and multi-search APIs support NDJSON, JSON, and SMILE; other types will result in an error response.

When using the `source` query string parameter, the content type must be specified using the `source_content_type` query string parameter.

{{es}} only supports UTF-8-encoded JSON. {{es}} ignores any other encoding headings sent with a request. Responses are also UTF-8 encoded.


## `X-Opaque-Id` HTTP header [x-opaque-id]

You can pass an `X-Opaque-Id` HTTP header to track the origin of a request in {{es}} logs and tasks. If provided, {{es}} surfaces the `X-Opaque-Id` value in the:

* Response of any request that includes the header
* [Task management API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-tasks) response
* [Slow logs](/reference/elasticsearch/index-settings/slow-log.md)
* [Deprecation logs](docs-content://deploy-manage/monitor/logging-configuration/update-elasticsearch-logging-levels.md#deprecation-logging)

For the deprecation logs, {{es}} also uses the `X-Opaque-Id` value to throttle and deduplicate deprecation warnings. See [Deprecation logs throttling](docs-content://deploy-manage/monitor/logging-configuration/update-elasticsearch-logging-levels.md#_deprecation_logs_throttling).

The `X-Opaque-Id` header accepts any arbitrary value. However, we recommend you limit these values to a finite set, such as an ID per client. Don’t generate a unique `X-Opaque-Id` header for every request. Too many unique `X-Opaque-Id` values can prevent {{es}} from deduplicating warnings in the deprecation logs.


## `traceparent` HTTP header [traceparent]

{{es}} also supports a `traceparent` HTTP header using the [official W3C trace context spec](https://www.w3.org/TR/trace-context/#traceparent-header). You can use the `traceparent` header to trace requests across Elastic products and other services. Because it’s only used for traces, you can safely generate a unique `traceparent` header for each request.

If provided, {{es}} surfaces the header’s `trace-id` value as `trace.id` in the:

* [JSON {{es}} server logs](docs-content://deploy-manage/monitor/logging-configuration/update-elasticsearch-logging-levels.md)
* [Slow logs](/reference/elasticsearch/index-settings/slow-log.md)
* [Deprecation logs](docs-content://deploy-manage/monitor/logging-configuration/update-elasticsearch-logging-levels.md#deprecation-logging)

For example, the following `traceparent` value would produce the following `trace.id` value in the above logs.

```txt
`traceparent`: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
`trace.id`: 0af7651916cd43dd8448eb211c80319c
```

## GET and POST requests [get-requests]

A number of {{es}} GET APIs— most notably the search API— support a request body. While the GET action makes sense in the context of retrieving information, GET requests with a body are not supported by all HTTP libraries. All {{es}} GET APIs that require a body can also be submitted as POST requests. Alternatively, you can pass the request body as the [`source` query string parameter](#api-request-body-query-string) when using GET.


## Cron expressions [api-cron-expressions]

A cron expression is a string of the following form:

```txt
    <seconds> <minutes> <hours> <day_of_month> <month> <day_of_week> [year]
```

{{es}} uses the cron parser from the [Quartz Job Scheduler](https://quartz-scheduler.org). For more information about writing Quartz cron expressions, see the [Quartz CronTrigger Tutorial](http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html).

All schedule times are in coordinated universal time (UTC); other timezones are not supported.

::::{tip}
You can use the [*elasticsearch-croneval*](/reference/elasticsearch/command-line-tools/elasticsearch-croneval.md) command line tool to validate your cron expressions.
::::



### Cron expression elements [cron-elements]

All elements are required except for `year`. See [Cron special characters](#cron-special-characters) for information about the allowed special characters.

`<seconds>`
:   (Required) Valid values: `0`-`59` and the special characters `,` `-` `*` `/`

`<minutes>`
:   (Required) Valid values: `0`-`59` and the special characters `,` `-` `*` `/`

`<hours>`
:   (Required) Valid values: `0`-`23` and the special characters `,` `-` `*` `/`

`<day_of_month>`
:   (Required) Valid values: `1`-`31` and the special characters `,` `-` `*` `/` `?` `L` `W`

`<month>`
:   (Required) Valid values: `1`-`12`, `JAN`-`DEC`, `jan`-`dec`, and the special characters `,` `-` `*` `/`

`<day_of_week>`
:   (Required) Valid values: `1`-`7`, `SUN`-`SAT`, `sun`-`sat`,  and the special characters `,` `-` `*` `/` `?` `L` `#`

`<year>`
:   (Optional) Valid values: `1970`-`2099` and the special characters `,` `-` `*` `/`


### Cron special characters [cron-special-characters]

`*`
:   Selects every possible value for a field. For example, `*` in the `hours` field means "every hour".

`?`
:   No specific value. Use when you don’t care what the value is. For example, if you want the schedule to trigger on a particular day of the month, but don’t care what day of the week that happens to be, you can specify `?` in the `day_of_week` field.

`-`
:   A range of values (inclusive). Use to separate a minimum and maximum value. For example, if you want the schedule to trigger every hour between 9:00 a.m. and 5:00 p.m., you could specify `9-17` in the `hours` field.

`,`
:   Multiple values. Use to separate multiple values for a field. For example, if you want the schedule to trigger every Tuesday and Thursday, you could specify `TUE,THU` in the `day_of_week` field.

`/`
:   Increment. Use to separate values when specifying a time increment. The first value represents the starting point, and the second value represents the interval. For example, if you want the schedule to trigger every 20 minutes starting at the top of the hour, you could specify `0/20` in the `minutes` field. Similarly, specifying `1/5` in `day_of_month` field will trigger every 5 days starting on the first day of the month.

`L`
:   Last. Use in the `day_of_month` field to mean the last day of the month— day 31 for January, day 28 for February in non-leap years, day 30 for April, and so on. Use alone in the `day_of_week` field in place of `7` or `SAT`, or after a particular day of the week to select the last day of that type in the month. For example `6L` means the last Friday of the month. You can specify `LW` in the `day_of_month` field to specify the last weekday of the month. Avoid using the `L` option when specifying lists or ranges of values, as the results likely won’t be what you expect.

`W`
:   Weekday. Use to specify the weekday (Monday-Friday) nearest the given day. As an example, if you specify `15W` in the `day_of_month` field and the 15th is a Saturday, the schedule will trigger on the 14th. If the 15th is a Sunday, the schedule will trigger on Monday the 16th. If the 15th is a Tuesday, the schedule will trigger on Tuesday the 15th. However if you specify `1W` as the value for `day_of_month`, and the 1st is a Saturday, the schedule will trigger on Monday the 3rd— it won’t jump over the month boundary. You can specify `LW` in the `day_of_month` field to specify the last weekday of the month. You can only use the `W` option when the `day_of_month` is a single day— it is not valid when specifying a range or list of days.

`#`
:   Nth XXX day in a month. Use in the `day_of_week` field to specify the nth XXX day of the month. For example, if you specify `6#1`, the schedule will trigger on the first Friday of the month. Note that if you specify `3#5` and there are not 5 Tuesdays in a particular month, the schedule won’t trigger that month.


### Examples [cron-expression-examples]


#### Setting daily triggers [cron-example-daily]

`0 5 9 * * ?`
:   Trigger at 9:05 a.m. UTC every day.

`0 5 9 * * ? 2020`
:   Trigger at 9:05 a.m. UTC every day during the year 2020.


#### Restricting triggers to a range of days or times [cron-example-range]

`0 5 9 ? * MON-FRI`
:   Trigger at 9:05 a.m. UTC Monday through Friday.

`0 0-5 9 * * ?`
:   Trigger every minute starting at 9:00 a.m. UTC and ending at 9:05 a.m. UTC every day.


#### Setting interval triggers [cron-example-interval]

`0 0/15 9 * * ?`
:   Trigger every 15 minutes starting at 9:00 a.m. UTC and ending at 9:45 a.m. UTC every day.

`0 5 9 1/3 * ?`
:   Trigger at 9:05 a.m. UTC every 3 days every month, starting on the first day of the month.


#### Setting schedules that trigger on a particular day [cron-example-day]

`0 1 4 1 4 ?`
:   Trigger every April 1st at 4:01 a.m. UTC.

`0 0,30 9 ? 4 WED`
:   Trigger at 9:00 a.m. UTC and at 9:30 a.m. UTC every Wednesday in the month of April.

`0 5 9 15 * ?`
:   Trigger at 9:05 a.m. UTC on the 15th day of every month.

`0 5 9 15W * ?`
:   Trigger at 9:05 a.m. UTC on the nearest weekday to the 15th of every month.

`0 5 9 ? * 6#1`
:   Trigger at 9:05 a.m. UTC on the first Friday of every month.


#### Setting triggers using last [cron-example-last]

`0 5 9 L * ?`
:   Trigger at 9:05 a.m. UTC on the last day of every month.

`0 5 9 ? * 2L`
:   Trigger at 9:05 a.m. UTC on the last Monday of every month.

`0 5 9 LW * ?`
:   Trigger at 9:05 a.m. UTC on the last weekday of every month.


## Date math support in index and index alias names [api-date-math-index-names]

Date math name resolution lets you to search a range of time series indices or index aliases rather than searching all of your indices and filtering the results. Limiting the number of searched indices reduces cluster load and improves search performance. For example, if you are searching for errors in your daily logs, you can use a date math name template to restrict the search to the past two days.

Most APIs that accept an index or index alias argument support date math. A date math name takes the following form:

```txt
<static_name{date_math_expr{date_format|time_zone}}>
```

Where:

`static_name`
:   Static text

`date_math_expr`
:   Dynamic date math expression that computes the date dynamically

`date_format`
:   Optional format in which the computed date should be rendered. Defaults to `yyyy.MM.dd`. Format should be compatible with java-time [https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.md)

`time_zone`
:   Optional time zone. Defaults to `UTC`.

::::{note}
Pay attention to the usage of small vs capital letters used in the `date_format`. For example: `mm` denotes minute of hour, while `MM` denotes month of year. Similarly `hh` denotes the hour in the `1-12` range in combination with `AM/PM`, while `HH` denotes the hour in the `0-23` 24-hour range.
::::


Date math expressions are resolved locale-independent. Consequently, it is not possible to use any other calendars than the Gregorian calendar.

You must enclose date math names in angle brackets. If you use the name in a request path, special characters must be URI encoded. For example:

```console
# PUT /<my-index-{now/d}>
PUT /%3Cmy-index-%7Bnow%2Fd%7D%3E
```

::::{admonition} Percent encoding of date math characters
:class: note

The special characters used for date rounding must be URI encoded as follows:

| | |
|---|---|
| `<` | `%3C` |
| `>` | `%3E` |
| `/` | `%2F` |
| `{` | `%7B` |
| `}` | `%7D` |
| `|` | `%7C` |
| `+` | `%2B` |
| `:` | `%3A` |
| `,` | `%2C` |
::::


The following example shows different forms of date math names and the final names they resolve to given the current time is 22nd March 2024 noon UTC.

| Expression | Resolves to |
| --- | --- |
| `<logstash-{now/d}>` | `logstash-2024.03.22` |
| `<logstash-{now/M}>` | `logstash-2024.03.01` |
| `<logstash-{now/M{yyyy.MM}}>` | `logstash-2024.03` |
| `<logstash-{now/M-1M{yyyy.MM}}>` | `logstash-2024.02` |
| `<logstash-{now/d{yyyy.MM.dd&#124;+12:00}}>` | `logstash-2024.03.23` |

To use the characters `{` and `}` in the static part of a name template, escape them with a backslash `\`, for example:

* `<elastic\\{ON\\}-{now/M}>` resolves to `elastic{{ON}}-2024.03.01`

The following example shows a search request that searches the Logstash indices for the past three days, assuming the indices use the default Logstash index name format, `logstash-YYYY.MM.dd`.

```console
# GET /<logstash-{now/d-2d}>,<logstash-{now/d-1d}>,<logstash-{now/d}>/_search
GET /%3Clogstash-%7Bnow%2Fd-2d%7D%3E%2C%3Clogstash-%7Bnow%2Fd-1d%7D%3E%2C%3Clogstash-%7Bnow%2Fd%7D%3E/_search
{
  "query" : {
    "match": {
      "test": "data"
    }
  }
}
```


## Multi-target syntax [api-multi-index]

Most APIs that accept a `<data-stream>`, `<index>`, or `<target>` request path parameter also support *multi-target syntax*.

In multi-target syntax, you can use a comma-separated list to run a request on multiple resources, such as data streams, indices, or aliases: `test1,test2,test3`. You can also use [glob-like](https://en.wikipedia.org/wiki/Glob_(programming)) wildcard (`*`) expressions to target resources that match a pattern: `test*` or `*test` or `te*t` or `*test*`.

You can exclude targets using the `-` character: `test*,-test3`.

::::{important}
Aliases are resolved after wildcard expressions. This can result in a request that targets an excluded alias. For example, if `test3` is an index alias, the pattern `test*,-test3` still targets the indices for `test3`. To avoid this, exclude the concrete indices for the alias instead.
::::


You can also exclude clusters from a list of clusters to search using the `-` character: `remote*:*,-remote1:*,-remote4:*` will search all clusters with an alias that starts with "remote" except for "remote1" and "remote4". Note that to exclude a cluster with this notation you must exclude all of its indexes. Excluding a subset of indexes on a remote cluster is currently not supported. For example, this will throw an exception: `remote*:*,-remote1:logs*`.

Multi-target APIs that can target indices support the following query string parameters:

`ignore_unavailable`
:   (Optional, Boolean) If `false`, the request returns an error if it targets a missing or closed index. Defaults to `false`.

`allow_no_indices`
:   (Optional, Boolean) If `false`, the request returns an error if any wildcard expression, [index alias](docs-content://manage-data/data-store/aliases.md), or `_all` value targets only missing or closed indices. This behavior applies even if the request targets other open indices. For example, a request targeting `foo*,bar*` returns an error if an index starts with `foo` but no index starts with `bar`.

`expand_wildcards`
:   (Optional, string) Type of index that wildcard patterns can match. If the request can target data streams, this argument determines whether wildcard expressions match hidden data streams. Supports comma-separated values, such as `open,hidden`. Valid values are:

`all`
:   Match any data stream or index, including [hidden](#multi-hidden) ones.

`open`
:   Match open, non-hidden indices. Also matches any non-hidden data stream.

`closed`
:   Match closed, non-hidden indices. Also matches any non-hidden data stream. Data streams cannot be closed.

`hidden`
:   Match hidden data streams and hidden indices. Must be combined with `open`, `closed`, or both.

`none`
:   Wildcard patterns are not accepted.


The defaults settings for the above parameters depend on the API being used.

Some multi-target APIs that can target indices also support the following query string parameter:

`ignore_throttled`
:   (Optional, Boolean) If `true`, concrete, expanded or aliased indices are ignored when frozen. Defaults to `true`.

    :::{admonition} Deprecated in 7.16.0
    This parameter was deprecated in 7.16.0.
    :::

::::{note}
APIs with a single target, such as the [get document API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get), do not support multi-target syntax.
::::



### Hidden data streams and indices [multi-hidden]

For most APIs, wildcard expressions do not match hidden data streams and indices by default. To match hidden data streams and indices using a wildcard expression, you must specify the `expand_wildcards` query parameter.

Alternatively, querying an index pattern starting with a dot, such as `.watcher_hist*`, will match hidden indices by default. This is intended to mirror Unix file-globbing behavior and provide a smoother transition path to hidden indices.

You can create hidden data streams by setting `data_stream.hidden` to `true` in the stream’s matching [index template](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-index-template). You can hide indices using the [`index.hidden`](/reference/elasticsearch/index-settings/index-modules.md#index-hidden) index setting.

The backing indices for data streams are hidden automatically. Some features, such as {{ml}}, store information in hidden indices.

Global index templates that match all indices are not applied to hidden indices.


### System indices [system-indices]

{{es}} modules and plugins can store configuration and state information in internal *system indices*. You should not directly access or modify system indices as they contain data essential to the operation of the system.

::::{important}
Direct access to system indices is deprecated and will no longer be allowed in a future major version.
::::

To view system indices within cluster:

```console
GET _cluster/state/metadata?filter_path=metadata.indices.*.system
```

::::{warning}
When overwriting current cluster state, system indices should be restored as part of their [feature state](docs-content://deploy-manage/tools/snapshot-and-restore.md#feature-state).
::::

### Node specification [cluster-nodes]

Some cluster-level APIs may operate on a subset of the nodes which can be specified with node filters.
For example,  [task management]({{es-apis}}group/endpoint-tasks), [node stats]({{es-apis}}operation/operation-nodes-stats), and [node info]({{es-apis}}operation/operation-nodes-info-1) APIs can all report results from a filtered set of nodes rather than from all nodes.

Node filters are written as a comma-separated list of individual filters, each of which adds or removes nodes from the chosen subset.
Each filter can be one of the following:

* `_all`, to add all nodes to the subset.
* `_local`, to add the local node to the subset.
* `_master`, to add the currently-elected master node to the subset.
* a node ID or name, to add this node to the subset.
* an IP address or hostname, to add all matching nodes to the subset.
* a pattern, using `*` wildcards, which adds all nodes to the subset whose name, address, or hostname matches the pattern.
* `master:true`, `data:true`, `ingest:true`, `voting_only:true`, `ml:true`, or `coordinating_only:true`, which respectively add to the subset all master-eligible nodes, all data nodes, all ingest nodes, all voting-only nodes, all machine learning nodes, and all coordinating-only nodes.
* `master:false`, `data:false`, `ingest:false`, `voting_only:false`, `ml:false`, or `coordinating_only:false`, which respectively remove from the subset all master-eligible nodes, all data nodes, all ingest nodes, all voting-only nodes, all machine learning nodes, and all coordinating-only nodes.
* a pair of patterns, using `*` wildcards, of the form `attrname:attrvalue`, which adds to the subset all nodes with a [custom node attribute](/reference/elasticsearch/configuration-reference/node-settings.md#custom-node-attributes) whose name and value match the respective patterns. Custom node attributes are configured by setting properties in the configuration file of the form `node.attr.attrname: attrvalue`.

Node filters run in the order in which they are given, which is important if using filters that remove nodes from the set.
For example, `_all,master:false` means all the nodes except the master-eligible ones.
`master:false,_all` means the same as `_all` because the `_all` filter runs after the `master:false` filter.

If no filters are given, the default is to select all nodes.
If any filters are specified, they run starting with an empty chosen subset.
This means that filters such as `master:false` which remove nodes from the chosen subset are only useful if they come after some other filters.
When used on its own, `master:false` selects no nodes.

Here are some examples of the use of node filters with some [cluster APIs]({{es-apis}}group/endpoint-cluster):

```sh
# If no filters are given, the default is to select all nodes
GET /_nodes
# Explicitly select all nodes
GET /_nodes/_all
# Select just the local node
GET /_nodes/_local
# Select the elected master node
GET /_nodes/_master
# Select nodes by name, which can include wildcards
GET /_nodes/node_name_goes_here
GET /_nodes/node_name_goes_*
# Select nodes by address, which can include wildcards
GET /_nodes/10.0.0.3,10.0.0.4
GET /_nodes/10.0.0.*
# Select nodes by role
GET /_nodes/_all,master:false
GET /_nodes/data:true,ingest:true
GET /_nodes/coordinating_only:true
GET /_nodes/master:true,voting_only:false
# Select nodes by custom attribute
# (for example, with something like `node.attr.rack: 2` in the configuration file)
GET /_nodes/rack:2
GET /_nodes/ra*:2
GET /_nodes/ra*:2*
```

## Parameters [api-conventions-parameters]

Rest parameters (when using HTTP, map to HTTP URL parameters) follow the convention of using underscore casing.


## Request body in query string [api-request-body-query-string]

For libraries that don’t accept a request body for non-POST requests, you can pass the request body as the `source` query string parameter instead. When using this method, the `source_content_type` parameter should also be passed with a media type value that indicates the format of the source, such as `application/json`.


## REST API version compatibility [api-compatibility]

Major version upgrades often include a number of breaking changes that impact how you interact with {{es}}. While we recommend that you monitor the deprecation logs and update applications before upgrading {{es}}, having to coordinate the necessary changes can be an impediment to upgrading.

You can enable an existing application to function without modification after an upgrade by including API compatibility headers, which tell {{es}} you are still using the previous version of the REST API. Using these headers allows the structure of requests and responses to remain the same; it does not guarantee the same behavior.

You set version compatibility on a per-request basis in the `Content-Type` and `Accept` headers. Setting `compatible-with` to the same major version as the version you’re running has no impact, but ensures that the request will still work after {{es}} is upgraded.

To tell {{es}} 8.0 you are using the 7.x request and response format, set `compatible-with=7`:

```sh
Content-Type: application/vnd.elasticsearch+json; compatible-with=7
Accept: application/vnd.elasticsearch+json; compatible-with=7
```


## HTTP `429 Too Many Requests` status code push back [api-push-back]

{{es}} APIs may respond with the HTTP `429 Too Many Requests` status code, indicating that the cluster is too busy to handle the request. When this happens, consider retrying after a short delay. If the retry also receives a `429 Too Many Requests` response, extend the delay by backing off exponentially before each subsequent retry.


## URL-based access control [api-url-access-control]

Many users use a proxy with URL-based access control to secure access to {{es}} data streams and indices. For [multi-search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-msearch), [multi-get](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-mget), and [bulk](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk) requests, the user has the choice of specifying a data stream or  index in the URL and on each individual request within the request body. This can make URL-based access control challenging.

To prevent the user from overriding the data stream or index specified in the URL, set `rest.action.multi.allow_explicit_index` to `false` in `elasticsearch.yml`.

This causes  {{es}} to reject requests that explicitly specify a data stream or index in the request body.


## Boolean Values [_boolean_values]

All REST API parameters (both request parameters and JSON body) support providing boolean "false" as the value `false` and boolean "true" as the value `true`. All other values will raise an error.


## Number Values [api-conventions-number-values]

When passing a numeric parameter in a request body, you may use a `string` containing the number instead of the native numeric type. For example:

```console
POST /_search
{
  "size": "1000"
}
```

Integer-valued fields in a response body are described as `integer` (or occasionally `long`) in this manual, but there are generally no explicit bounds on such values. JSON, SMILE, CBOR and YAML all permit arbitrarily large integer values. Do not assume that `integer` fields in a response body will always fit into a 32-bit signed integer.


## Byte size units [byte-units]

Whenever the byte size of data needs to be specified, e.g. when setting a buffer size parameter, the value must specify the unit, like `10kb` for 10 kilobytes. Note that these units use powers of 1024, so `1kb` means 1024 bytes. The supported units are:

`b`
:   Bytes

`kb`
:   Kilobytes

`mb`
:   Megabytes

`gb`
:   Gigabytes

`tb`
:   Terabytes

`pb`
:   Petabytes


## Distance Units [distance-units]

Wherever distances need to be specified, such as the `distance` parameter in the [Geo-distance](/reference/query-languages/query-dsl/query-dsl-geo-distance-query.md)), the default unit is meters if none is specified. Distances can be specified in other units, such as `"1km"` or `"2mi"` (2 miles).

The full list of units is listed below:

Mile
:   `mi` or `miles`

Yard
:   `yd` or `yards`

Feet
:   `ft` or `feet`

Inch
:   `in` or `inch`

Kilometer
:   `km` or `kilometers`

Meter
:   `m` or `meters`

Centimeter
:   `cm` or `centimeters`

Millimeter
:   `mm` or `millimeters`

Nautical mile
:   `NM`, `nmi`, or `nauticalmiles`


## Time units [time-units]

Whenever durations need to be specified, e.g. for a `timeout` parameter, the duration must specify the unit, like `2d` for 2 days. The supported units are:

`d`
:   Days

`h`
:   Hours

`m`
:   Minutes

`s`
:   Seconds

`ms`
:   Milliseconds

`micros`
:   Microseconds

`nanos`
:   Nanoseconds


## Unit-less quantities [size-units]

Unit-less quantities means that they don’t have a "unit" like "bytes" or "Hertz" or "meter" or "long tonne".

If one of these quantities is large we’ll print it out like 10m for 10,000,000 or 7k for 7,000. We’ll still print 87 when we mean 87 though. These are the supported multipliers:

`k`
:   Kilo

`m`
:   Mega

`g`
:   Giga

`t`
:   Tera

`p`
:   Peta

