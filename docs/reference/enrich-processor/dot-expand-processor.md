---
navigation_title: "Dot expander"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/dot-expand-processor.html
---

# Dot expander processor [dot-expand-processor]


Expands a field with dots into an object field. This processor allows fields with dots in the name to be accessible by other processors in the pipeline. Otherwise these fields can’t be accessed by any processor.

$$$dot-expander-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field to expand into an object field. If set to `*`, all top-level fields will be expanded. |
| `path` | no | - | The field that contains the field to expand. Only required if the field to expand is part another object field, because the `field` option can only understand leaf fields. |
| `override` | no | false | Controls the behavior when there is already an existing nested object that conflicts with the expanded field. When `false`, the processor will merge conflicts by combining the old and the new values into an array. When `true`, the value from the expanded field will overwrite the existing value. |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

```js
{
  "dot_expander": {
    "field": "foo.bar"
  }
}
```
% NOTCONSOLE

For example the dot expand processor would turn this document:

```js
{
  "foo.bar" : "value"
}
```
% NOTCONSOLE

into:

```js
{
  "foo" : {
    "bar" : "value"
  }
}
```
% NOTCONSOLE

If there is already a `bar` field nested under `foo` then this processor merges the `foo.bar` field into it. If the field is a scalar value then it will turn that field into an array field.

For example, the following document:

```js
{
  "foo.bar" : "value2",
  "foo" : {
    "bar" : "value1"
  }
}
```
% NOTCONSOLE

is transformed by the `dot_expander` processor into:

```js
{
  "foo" : {
    "bar" : ["value1", "value2"]
  }
}
```
% NOTCONSOLE

Contrast that with when the `override` option is set to `true`.

```js
{
  "dot_expander": {
    "field": "foo.bar",
    "override": true
  }
}
```
% NOTCONSOLE

In that case, the value of the expanded field overrides the value of the nested object.

```js
{
  "foo" : {
    "bar" : "value2"
  }
}
```
% NOTCONSOLE

<hr>
The value of `field` can also be set to a `*` to expand all top-level dotted field names:

```js
{
  "dot_expander": {
    "field": "*"
  }
}
```
% NOTCONSOLE

The dot expand processor would turn this document:

```js
{
  "foo.bar" : "value",
  "baz.qux" : "value"
}
```
% NOTCONSOLE

into:

```js
{
  "foo" : {
    "bar" : "value"
  },
  "baz" : {
    "qux" : "value"
  }
}
```
% NOTCONSOLE

<hr>
If the dotted field is nested within a non-dotted structure, then use the `path` option to navigate the non-dotted structure:

```js
{
  "dot_expander": {
    "path": "foo"
    "field": "*"
  }
}
```
% NOTCONSOLE

The dot expand processor would turn this document:

```js
{
  "foo" : {
    "bar.one" : "value",
    "bar.two" : "value"
  }
}
```
% NOTCONSOLE

into:

```js
{
  "foo" : {
    "bar" : {
      "one" : "value",
      "two" : "value"
    }
  }
}
```
% NOTCONSOLE

<hr>
If any field outside of the leaf field conflicts with a pre-existing field of the same name, then that field needs to be renamed first.

Consider the following document:

```js
{
  "foo": "value1",
  "foo.bar": "value2"
}
```
% NOTCONSOLE

Then the `foo` needs to be renamed first before the `dot_expander` processor is applied. So in order for the `foo.bar` field to properly be expanded into the `bar` field under the `foo` field the following pipeline should be used:

```js
{
  "processors" : [
    {
      "rename" : {
        "field" : "foo",
        "target_field" : "foo.bar"
      }
    },
    {
      "dot_expander": {
        "field": "foo.bar"
      }
    }
  ]
}
```
% NOTCONSOLE

The reason for this is that Ingest doesn’t know how to automatically cast a scalar field to an object field.

