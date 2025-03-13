## `TO_DATE_NANOS` [esql-to_date_nanos]

**Syntax**

:::{image} ../../../../../images/to_date_nanos.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts an input to a nanosecond-resolution date value (aka date_nanos).

::::{note}
The range for date nanos is 1970-01-01T00:00:00.000000000Z to 2262-04-11T23:47:16.854775807Z, attepting to convertvalues outside of that range will result in null with a warning..  Additionally, integers cannot be converted into date nanos, as the range of integer nanoseconds only covers about 2 seconds after epoch.
::::


**Supported types**

| field | result |
| --- | --- |
| date | date_nanos |
| date_nanos | date_nanos |
| double | date_nanos |
| keyword | date_nanos |
| long | date_nanos |
| text | date_nanos |
| unsigned_long | date_nanos |


