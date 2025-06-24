## `SAMPLE` [esql-sample]

```yaml {applies_to}
stack: preview 9.1.0
```

The `SAMPLE` command samples a fraction of the table rows.

**Syntax**

```esql
SAMPLE probability
```

**Parameters**

`probability`
:   The probability that a row is included in the sample. The value must be between 0 and 1, exclusive.

**Examples**

:::{include} ../examples/sample.csv-spec/sampleForDocs.md
:::
