## Binary operators [esql-binary-operators]

:::{include} ../lists/binary-operators.md
:::

## Equality [esql-equals]

**Syntax**

:::{image} ../../../../../images/equals.svg
:alt: Embedded
:class: text-center
:::

Check if two fields are equal. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

::::{note}
This is pushed to the underlying search index if one side of the comparison is constant and the other side is a field in the index that has both an [`index`](/reference/elasticsearch/mapping-reference/mapping-index.md) and [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md).
::::


**Supported types**

| lhs | rhs | result |
| --- | --- | --- |
| boolean | boolean | boolean |
| cartesian_point | cartesian_point | boolean |
| cartesian_shape | cartesian_shape | boolean |
| date | date | boolean |
| date | date_nanos | boolean |
| date_nanos | date | boolean |
| date_nanos | date_nanos | boolean |
| double | double | boolean |
| double | integer | boolean |
| double | long | boolean |
| geo_point | geo_point | boolean |
| geo_shape | geo_shape | boolean |
| integer | double | boolean |
| integer | integer | boolean |
| integer | long | boolean |
| ip | ip | boolean |
| keyword | keyword | boolean |
| keyword | text | boolean |
| long | double | boolean |
| long | integer | boolean |
| long | long | boolean |
| text | keyword | boolean |
| text | text | boolean |
| unsigned_long | unsigned_long | boolean |
| version | version | boolean |


## Inequality `!=` [esql-not_equals]

:::{image} ../../../../../images/not_equals.svg
:alt: Embedded
:class: text-center
:::

Check if two fields are unequal. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

::::{note}
This is pushed to the underlying search index if one side of the comparison is constant and the other side is a field in the index that has both an [`index`](/reference/elasticsearch/mapping-reference/mapping-index.md) and [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md).
::::


**Supported types**

| lhs | rhs | result |
| --- | --- | --- |
| boolean | boolean | boolean |
| cartesian_point | cartesian_point | boolean |
| cartesian_shape | cartesian_shape | boolean |
| date | date | boolean |
| date | date_nanos | boolean |
| date_nanos | date | boolean |
| date_nanos | date_nanos | boolean |
| double | double | boolean |
| double | integer | boolean |
| double | long | boolean |
| geo_point | geo_point | boolean |
| geo_shape | geo_shape | boolean |
| integer | double | boolean |
| integer | integer | boolean |
| integer | long | boolean |
| ip | ip | boolean |
| keyword | keyword | boolean |
| keyword | text | boolean |
| long | double | boolean |
| long | integer | boolean |
| long | long | boolean |
| text | keyword | boolean |
| text | text | boolean |
| unsigned_long | unsigned_long | boolean |
| version | version | boolean |


## Less than `<` [esql-less_than]

:::{image} ../../../../../images/less_than.svg
:alt: Embedded
:class: text-center
:::

Check if one field is less than another. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

::::{note}
This is pushed to the underlying search index if one side of the comparison is constant and the other side is a field in the index that has both an [`index`](/reference/elasticsearch/mapping-reference/mapping-index.md) and [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md).
::::


**Supported types**

| lhs | rhs | result |
| --- | --- | --- |
| date | date | boolean |
| date | date_nanos | boolean |
| date_nanos | date | boolean |
| date_nanos | date_nanos | boolean |
| double | double | boolean |
| double | integer | boolean |
| double | long | boolean |
| integer | double | boolean |
| integer | integer | boolean |
| integer | long | boolean |
| ip | ip | boolean |
| keyword | keyword | boolean |
| keyword | text | boolean |
| long | double | boolean |
| long | integer | boolean |
| long | long | boolean |
| text | keyword | boolean |
| text | text | boolean |
| unsigned_long | unsigned_long | boolean |
| version | version | boolean |


## Less than or equal to `<=` [esql-less_than_or_equal]

:::{image} ../../../../../images/less_than_or_equal.svg
:alt: Embedded
:class: text-center
:::

Check if one field is less than or equal to another. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

::::{note}
This is pushed to the underlying search index if one side of the comparison is constant and the other side is a field in the index that has both an [`index`](/reference/elasticsearch/mapping-reference/mapping-index.md) and [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md).
::::


**Supported types**

| lhs | rhs | result |
| --- | --- | --- |
| date | date | boolean |
| date | date_nanos | boolean |
| date_nanos | date | boolean |
| date_nanos | date_nanos | boolean |
| double | double | boolean |
| double | integer | boolean |
| double | long | boolean |
| integer | double | boolean |
| integer | integer | boolean |
| integer | long | boolean |
| ip | ip | boolean |
| keyword | keyword | boolean |
| keyword | text | boolean |
| long | double | boolean |
| long | integer | boolean |
| long | long | boolean |
| text | keyword | boolean |
| text | text | boolean |
| unsigned_long | unsigned_long | boolean |
| version | version | boolean |


## Greater than `>` [esql-greater_than]

:::{image} ../../../../../images/greater_than.svg
:alt: Embedded
:class: text-center
:::

Check if one field is greater than another. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

::::{note}
This is pushed to the underlying search index if one side of the comparison is constant and the other side is a field in the index that has both an [`index`](/reference/elasticsearch/mapping-reference/mapping-index.md) and [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md).
::::


**Supported types**

| lhs | rhs | result |
| --- | --- | --- |
| date | date | boolean |
| date | date_nanos | boolean |
| date_nanos | date | boolean |
| date_nanos | date_nanos | boolean |
| double | double | boolean |
| double | integer | boolean |
| double | long | boolean |
| integer | double | boolean |
| integer | integer | boolean |
| integer | long | boolean |
| ip | ip | boolean |
| keyword | keyword | boolean |
| keyword | text | boolean |
| long | double | boolean |
| long | integer | boolean |
| long | long | boolean |
| text | keyword | boolean |
| text | text | boolean |
| unsigned_long | unsigned_long | boolean |
| version | version | boolean |


## Greater than or equal to `>=` [esql-greater_than_or_equal]

:::{image} ../../../../../images/greater_than_or_equal.svg
:alt: Embedded
:class: text-center
:::

Check if one field is greater than or equal to another. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

::::{note}
This is pushed to the underlying search index if one side of the comparison is constant and the other side is a field in the index that has both an [`index`](/reference/elasticsearch/mapping-reference/mapping-index.md) and [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md).
::::


**Supported types**

| lhs | rhs | result |
| --- | --- | --- |
| date | date | boolean |
| date | date_nanos | boolean |
| date_nanos | date | boolean |
| date_nanos | date_nanos | boolean |
| double | double | boolean |
| double | integer | boolean |
| double | long | boolean |
| integer | double | boolean |
| integer | integer | boolean |
| integer | long | boolean |
| ip | ip | boolean |
| keyword | keyword | boolean |
| keyword | text | boolean |
| long | double | boolean |
| long | integer | boolean |
| long | long | boolean |
| text | keyword | boolean |
| text | text | boolean |
| unsigned_long | unsigned_long | boolean |
| version | version | boolean |


## Add `+` [esql-add]

:::{image} ../../../../../images/add.svg
:alt: Embedded
:class: text-center
:::

Add two numbers together. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.


**Supported types**

| lhs | rhs | result |
| --- | --- | --- |
| date | date_period | date |
| date | time_duration | date |
| date_nanos | date_period | date_nanos |
| date_nanos | time_duration | date_nanos |
| date_period | date | date |
| date_period | date_nanos | date_nanos |
| date_period | date_period | date_period |
| double | double | double |
| double | integer | double |
| double | long | double |
| integer | double | double |
| integer | integer | integer |
| integer | long | long |
| long | double | double |
| long | integer | long |
| long | long | long |
| time_duration | date | date |
| time_duration | date_nanos | date_nanos |
| time_duration | time_duration | time_duration |
| unsigned_long | unsigned_long | unsigned_long |


## Subtract `-` [esql-sub]

:::{image} ../../../../../images/sub.svg
:alt: Embedded
:class: text-center
:::

Subtract one number from another. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.


**Supported types**

| lhs | rhs | result |
| --- | --- | --- |
| date | date_period | date |
| date | time_duration | date |
| date_nanos | date_period | date_nanos |
| date_nanos | time_duration | date_nanos |
| date_period | date_nanos | date_nanos |
| date_period | date_period | date_period |
| double | double | double |
| double | integer | double |
| double | long | double |
| integer | double | double |
| integer | integer | integer |
| integer | long | long |
| long | double | double |
| long | integer | long |
| long | long | long |
| time_duration | date_nanos | date_nanos |
| time_duration | time_duration | time_duration |
| unsigned_long | unsigned_long | unsigned_long |


## Multiply `*` [esql-mul]

:::{image} ../../../../../images/mul.svg
:alt: Embedded
:class: text-center
:::

Multiply two numbers together. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.


**Supported types**

| lhs | rhs | result |
| --- | --- | --- |
| double | double | double |
| double | integer | double |
| double | long | double |
| integer | double | double |
| integer | integer | integer |
| integer | long | long |
| long | double | double |
| long | integer | long |
| long | long | long |
| unsigned_long | unsigned_long | unsigned_long |


## Divide `/` [esql-div]

:::{image} ../../../../../images/div.svg
:alt: Embedded
:class: text-center
:::

Divide one number by another. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

::::{note}
Division of two integer types will yield an integer result, rounding towards 0. If you need floating point division, [`Cast (::)`](../esql-functions-operators.md#esql-cast-operator) one of the arguments to a `DOUBLE`.
::::


**Supported types**

| lhs | rhs | result |
| --- | --- | --- |
| double | double | double |
| double | integer | double |
| double | long | double |
| integer | double | double |
| integer | integer | integer |
| integer | long | long |
| long | double | double |
| long | integer | long |
| long | long | long |
| unsigned_long | unsigned_long | unsigned_long |


## Modulus `%` [esql-mod]

:::{image} ../../../../../images/mod.svg
:alt: Embedded
:class: text-center
:::

Divide one number by another and return the remainder. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.


**Supported types**

| lhs | rhs | result |
| --- | --- | --- |
| double | double | double |
| double | integer | double |
| double | long | double |
| integer | double | double |
| integer | integer | integer |
| integer | long | long |
| long | double | double |
| long | integer | long |
| long | long | long |
| unsigned_long | unsigned_long | unsigned_long |

