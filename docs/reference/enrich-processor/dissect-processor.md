---
navigation_title: "Dissect"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/dissect-processor.html
---

# Dissect processor [dissect-processor]


Similar to the [Grok Processor](/reference/enrich-processor/grok-processor.md), dissect also extracts structured fields out of a single text field within a document. However unlike the [Grok Processor](/reference/enrich-processor/grok-processor.md), dissect does not use [Regular Expressions](https://en.wikipedia.org/wiki/Regular_expression). This allows dissect’s syntax to be simple and for some cases faster than the [Grok Processor](/reference/enrich-processor/grok-processor.md).

Dissect matches a single text field against a defined pattern.

For example the following pattern:

```txt
%{clientip} %{ident} %{auth} [%{@timestamp}] \"%{verb} %{request} HTTP/%{httpversion}\" %{status} %{size}
```

will match a log line of this format:

```txt
1.2.3.4 - - [30/Apr/1998:22:00:52 +0000] \"GET /english/venues/cities/images/montpellier/18.gif HTTP/1.0\" 200 3171
```

and result in a document with the following fields:

```js
"doc": {
  "_index": "_index",
  "_type": "_type",
  "_id": "_id",
  "_source": {
    "request": "/english/venues/cities/images/montpellier/18.gif",
    "auth": "-",
    "ident": "-",
    "verb": "GET",
    "@timestamp": "30/Apr/1998:22:00:52 +0000",
    "size": "3171",
    "clientip": "1.2.3.4",
    "httpversion": "1.0",
    "status": "200"
  }
}
```

A dissect pattern is defined by the parts of the string that will be discarded. In the previous example, the first part to be discarded is a single space. Dissect finds this space, then assigns the value of `clientip` everything up until that space. Next, dissect matches the `[` and then `]` and then assigns `@timestamp` to everything in-between `[` and `]`. Paying special attention to the parts of the string to discard will help build successful dissect patterns.

Successful matches require all keys in a pattern to have a value. If any of the `%{{keyname}}` defined in the pattern do not have a value, then an exception is thrown and may be handled by the [`on_failure`](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures) directive. An empty key `%{}` or a [named skip key](#dissect-modifier-named-skip-key) can be used to match values, but exclude the value from the final document. All matched values are represented as string data types. The [convert processor](/reference/enrich-processor/convert-processor.md) may be used to convert to expected data type.

Dissect also supports [key modifiers](#dissect-key-modifiers) that can change dissect’s default behavior. For example you can instruct dissect to ignore certain fields, append fields, skip over padding, etc. See [below](#dissect-key-modifiers) for more information.

$$$dissect-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field to dissect |
| `pattern` | yes | - | The pattern to apply to the field |
| `append_separator` | no | "" (empty string) | The character(s) that separate the appended fields. |
| `ignore_missing` | no | false | If `true` and `field` does not exist or is `null`, the processor quietly exits without modifying the document |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

```js
{
  "dissect": {
    "field": "message",
    "pattern" : "%{clientip} %{ident} %{auth} [%{@timestamp}] \"%{verb} %{request} HTTP/%{httpversion}\" %{status} %{size}"
   }
}
```

## Dissect key modifiers [dissect-key-modifiers]

Key modifiers can change the default behavior for dissection. Key modifiers may be found on the left or right of the `%{{keyname}}` always inside the `%{` and `}`. For example `%{+keyname ->}` has the append and right padding modifiers.

$$$dissect-key-modifiers-table$$$

| Modifier | Name | Position | Example | Description | Details |
| --- | --- | --- | --- | --- | --- |
| `->` | Skip right padding | (far) right | `%{keyname1->}` | Skips any repeated characters to the right | [link](#dissect-modifier-skip-right-padding) |
| `+` | Append | left | `%{+keyname} %{+keyname}` | Appends two or more fields together | [link](#dissect-modifier-append-key) |
| `+` with `/n` | Append with order | left and right | `%{+keyname/2} %{+keyname/1}` | Appends two or more fields together in the order specified | [link](#dissect-modifier-append-key-with-order) |
| `?` | Named skip key | left | `%{?ignoreme}` | Skips the matched value in the output. Same behavior as `%{}` | [link](#dissect-modifier-named-skip-key) |
| `*` and `&` | Reference keys | left | `%{*r1} %{&r1}` | Sets the output key as value of `*` and output value of `&` | [link](#dissect-modifier-reference-keys) |

### Right padding modifier (`->`) [dissect-modifier-skip-right-padding]

The algorithm that performs the dissection is very strict in that it requires all characters in the pattern to match the source string. For example, the pattern `%{{fookey}} %{{barkey}}` (1 space), will match the string "foo bar" (1 space), but will not match the string "foo  bar" (2 spaces) since the pattern has only 1 space and the source string has 2 spaces.

The right padding modifier helps with this case. Adding the right padding modifier to the pattern `%{fookey->} %{{barkey}}`, It will now will match "foo bar" (1 space) and "foo  bar" (2 spaces) and even "foo          bar" (10 spaces).

Use the right padding modifier to allow for repetition of the characters after a `%{keyname->}`.

The right padding modifier may be placed on any key with any other modifiers. It should always be the furthest right modifier. For example: `%{+keyname/1->}` and `%{->}`

Right padding modifier example

|     |     |
| --- | --- |
| **Pattern** | `%{ts->} %{{level}}` |
| **Input** | 1998-08-10T17:15:42,466          WARN |
| **Result** | * ts = 1998-08-10T17:15:42,466<br>* level = WARN<br> |

The right padding modifier may be used with an empty key to help skip unwanted data. For example, the same input string, but wrapped with brackets requires the use of an empty right padded key to achieve the same result.

Right padding modifier with empty key example

|     |     |
| --- | --- |
| **Pattern** | `[%{{ts}}]%{->}[%{{level}}]` |
| **Input** | [1998-08-10T17:15:42,466]            [WARN] |
| **Result** | * ts = 1998-08-10T17:15:42,466<br>* level = WARN<br> |


### Append modifier (`+`) [append-modifier]

$$$dissect-modifier-append-key$$$
Dissect supports appending two or more results together for the output. Values are appended left to right. An append separator can be specified. In this example the append_separator is defined as a space.

Append modifier example

|     |     |
| --- | --- |
| **Pattern** | `%{+name} %{+name} %{+name} %{+name}` |
| **Input** | john jacob jingleheimer schmidt |
| **Result** | * name = john jacob jingleheimer schmidt<br> |


### Append with order modifier (`+` and `/n`) [append-order-modifier]

$$$dissect-modifier-append-key-with-order$$$
Dissect supports appending two or more results together for the output. Values are appended based on the order defined (`/n`). An append separator can be specified. In this example the append_separator is defined as a comma.

Append with order modifier example

|     |     |
| --- | --- |
| **Pattern** | `%{+name/2} %{+name/4} %{+name/3} %{+name/1}` |
| **Input** | john jacob jingleheimer schmidt |
| **Result** | * name = schmidt,john,jingleheimer,jacob<br> |


### Named skip key (`?`) [named-skip-key]

$$$dissect-modifier-named-skip-key$$$
Dissect supports ignoring matches in the final result. This can be done with an empty key `%{}`, but for readability it may be desired to give that empty key a name.

Named skip key modifier example

|     |     |
| --- | --- |
| **Pattern** | `%{{clientip}} %{?ident} %{?auth} [%{@timestamp}]` |
| **Input** | 1.2.3.4 - - [30/Apr/1998:22:00:52 +0000] |
| **Result** | * clientip = 1.2.3.4<br>* @timestamp = 30/Apr/1998:22:00:52 +0000<br> |


### Reference keys (`*` and `&`) [reference-keys]

$$$dissect-modifier-reference-keys$$$
Dissect support using parsed values as the key/value pairings for the structured content. Imagine a system that partially logs in key/value pairs. Reference keys allow you to maintain that key/value relationship.

Reference key modifier example

|     |     |
| --- | --- |
| **Pattern** | `[%{{ts}}] [%{{level}}] %{*p1}:%{&p1} %{*p2}:%{&p2}` |
| **Input** | [2018-08-10T17:15:42,466] [ERR] ip:1.2.3.4 error:REFUSED |
| **Result** | * ts = 2018-08-10T17:15:42,466<br>* level = ERR<br>* ip = 1.2.3.4<br>* error = REFUSED<br> |



