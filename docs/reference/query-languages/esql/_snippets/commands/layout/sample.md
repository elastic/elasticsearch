## `SAMPLE` [esql-sample]

::::{warning}
This functionality is in technical preview and may be
changed or removed in a future release. Elastic will work to fix any
issues, but features in technical preview are not subject to the support
SLA of official GA features.
::::

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
