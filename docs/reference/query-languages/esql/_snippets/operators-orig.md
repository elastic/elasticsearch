## {{esql}} operators [esql-operators]

Boolean operators for comparing against one or multiple expressions.

:::{include} lists/operators.md
:::


## Binary operators [esql-binary-operators]


## Equality [esql-binary-operators-equality]

:::{image} ../../../../images/equals.svg
:alt: Embedded
:class: text-center
:::

Check if two fields are equal. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

::::{note}
This is pushed to the underlying search index if one side of the comparison is constant and the other side is a field in the index that has both an [`index`](/reference/elasticsearch/mapping-reference/mapping-index.md) and [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md).
::::


Supported types:

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


## Inequality `!=` [_inequality]

:::{image} ../../../../images/not_equals.svg
:alt: Embedded
:class: text-center
:::

Check if two fields are unequal. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

::::{note}
This is pushed to the underlying search index if one side of the comparison is constant and the other side is a field in the index that has both an [`index`](/reference/elasticsearch/mapping-reference/mapping-index.md) and [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md).
::::


Supported types:

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


## Less than `<` [_less_than]

:::{image} ../../../../images/less_than.svg
:alt: Embedded
:class: text-center
:::

Check if one field is less than another. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

::::{note}
This is pushed to the underlying search index if one side of the comparison is constant and the other side is a field in the index that has both an [`index`](/reference/elasticsearch/mapping-reference/mapping-index.md) and [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md).
::::


Supported types:

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


## Less than or equal to `<=` [_less_than_or_equal_to]

:::{image} ../../../../images/less_than_or_equal.svg
:alt: Embedded
:class: text-center
:::

Check if one field is less than or equal to another. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

::::{note}
This is pushed to the underlying search index if one side of the comparison is constant and the other side is a field in the index that has both an [`index`](/reference/elasticsearch/mapping-reference/mapping-index.md) and [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md).
::::


Supported types:

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


## Greater than `>` [_greater_than]

:::{image} ../../../../images/greater_than.svg
:alt: Embedded
:class: text-center
:::

Check if one field is greater than another. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

::::{note}
This is pushed to the underlying search index if one side of the comparison is constant and the other side is a field in the index that has both an [`index`](/reference/elasticsearch/mapping-reference/mapping-index.md) and [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md).
::::


Supported types:

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


## Greater than or equal to `>=` [_greater_than_or_equal_to]

:::{image} ../../../../images/greater_than_or_equal.svg
:alt: Embedded
:class: text-center
:::

Check if one field is greater than or equal to another. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

::::{note}
This is pushed to the underlying search index if one side of the comparison is constant and the other side is a field in the index that has both an [`index`](/reference/elasticsearch/mapping-reference/mapping-index.md) and [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md).
::::


Supported types:

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

:::{image} ../../../../images/add.svg
:alt: Embedded
:class: text-center
:::

Add two numbers together. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

Supported types:

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


## Subtract `-` [esql-subtract]

:::{image} ../../../../images/sub.svg
:alt: Embedded
:class: text-center
:::

Subtract one number from another. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

Supported types:

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


## Multiply `*` [_multiply]

:::{image} ../../../../images/mul.svg
:alt: Embedded
:class: text-center
:::

Multiply two numbers together. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

Supported types:

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


## Divide `/` [_divide]

:::{image} ../../../../images/div.svg
:alt: Embedded
:class: text-center
:::

Divide one number by another. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

::::{note}
Division of two integer types will yield an integer result, rounding towards 0. If you need floating point division, [`Cast (::)`](../esql-functions-operators.md#esql-cast-operator) one of the arguments to a `DOUBLE`.
::::


Supported types:

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


## Modulus `%` [_modulus]

:::{image} ../../../../images/mod.svg
:alt: Embedded
:class: text-center
:::

Divide one number by another and return the remainder. If either field is [multivalued](/reference/query-languages/esql/esql-multivalued-fields.md) then the result is `null`.

Supported types:

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


## Unary operators [esql-unary-operators]

The only unary operators is negation (`-`):

:::{image} ../../../../images/neg.svg
:alt: Embedded
:class: text-center
:::

Supported types:

**Supported types**

| field | result |
| --- | --- |
| date_period | date_period |
| double | double |
| integer | integer |
| long | long |
| time_duration | time_duration |


## Logical operators [esql-logical-operators]

The following logical operators are supported:

* `AND`
* `OR`
* `NOT`


## `IS NULL` and `IS NOT NULL` predicates [esql-predicates]

For NULL comparison, use the `IS NULL` and `IS NOT NULL` predicates:

```esql
FROM employees
| WHERE birth_date IS NULL
| KEEP first_name, last_name
| SORT first_name
| LIMIT 3
```

| first_name:keyword | last_name:keyword |
| --- | --- |
| Basil | Tramer |
| Florian | Syrotiuk |
| Lucien | Rosenbaum |

```esql
FROM employees
| WHERE is_rehired IS NOT NULL
| STATS COUNT(emp_no)
```

| COUNT(emp_no):long |
| --- |
| 84 |


## `Cast (::)` [esql-cast-operator]

The `::` operator provides a convenient alternative syntax to the TO_<type> [conversion functions](../esql-functions-operators.md#esql-type-conversion-functions).

```esql
ROW ver = CONCAT(("0"::INT + 1)::STRING, ".2.3")::VERSION
```

| ver:version |
| --- |
| 1.2.3 |


## `IN` [esql-in-operator]

The `IN` operator allows testing whether a field or expression equals an element in a list of literals, fields or expressions:

```esql
ROW a = 1, b = 4, c = 3
| WHERE c-a IN (3, b / 2, a)
```

| a:integer | b:integer | c:integer |
| --- | --- | --- |
| 1 | 4 | 3 |


## `LIKE` [esql-like-operator]

Use `LIKE` to filter data based on string patterns using wildcards. `LIKE` usually acts on a field placed on the left-hand side of the operator, but it can also act on a constant (literal) expression. The right-hand side of the operator represents the pattern.

The following wildcard characters are supported:

* `*` matches zero or more characters.
* `?` matches one character.

**Supported types**

| str | pattern | result |
| --- | --- | --- |
| keyword | keyword | boolean |
| text | keyword | boolean |

```esql
FROM employees
| WHERE first_name LIKE """?b*"""
| KEEP first_name, last_name
```

| first_name:keyword | last_name:keyword |
| --- | --- |
| Ebbe | Callaway |
| Eberhardt | Terkki |

Matching the exact characters `*` and `.` will require escaping. The escape character is backslash `\`. Since also backslash is a special character in string literals, it will require further escaping.

```esql
ROW message = "foo * bar"
| WHERE message LIKE "foo \\* bar"
```

To reduce the overhead of escaping, we suggest using triple quotes strings `"""`

```esql
ROW message = "foo * bar"
| WHERE message LIKE """foo \* bar"""
```


## `RLIKE` [esql-rlike-operator]

Use `RLIKE` to filter data based on string patterns using using [regular expressions](/reference/query-languages/regexp-syntax.md). `RLIKE` usually acts on a field placed on the left-hand side of the operator, but it can also act on a constant (literal) expression. The right-hand side of the operator represents the pattern.

**Supported types**

| str | pattern | result |
| --- | --- | --- |
| keyword | keyword | boolean |
| text | keyword | boolean |

```esql
FROM employees
| WHERE first_name RLIKE """.leja.*"""
| KEEP first_name, last_name
```

| first_name:keyword | last_name:keyword |
| --- | --- |
| Alejandro | McAlpine |

Matching special characters (eg. `.`, `*`, `(`…​) will require escaping. The escape character is backslash `\`. Since also backslash is a special character in string literals, it will require further escaping.

```esql
ROW message = "foo ( bar"
| WHERE message RLIKE "foo \\( bar"
```

To reduce the overhead of escaping, we suggest using triple quotes strings `"""`

```esql
ROW message = "foo ( bar"
| WHERE message RLIKE """foo \( bar"""
```


## Search operators [esql-search-operators]

The only search operator is match (`:`).

::::{warning}
Do not use on production environments. This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


The match operator performs a [match query](/reference/query-languages/query-dsl-match-query.md) on the specified field. Returns true if the provided query matches the row.

The match operator is equivalent to the [match function](../esql-functions-operators.md#esql-match).

For using the function syntax, or adding [match query parameters](/reference/query-languages/query-dsl-match-query.md#match-field-params), you can use the [match function](../esql-functions-operators.md#esql-match).

:::{image} ../../../../images/match_operator.svg
:alt: Embedded
:class: text-center
:::

**Supported types**

| field | query | result |
| --- | --- | --- |
| boolean | boolean | boolean |
| boolean | keyword | boolean |
| date | date | boolean |
| date | keyword | boolean |
| date_nanos | date_nanos | boolean |
| date_nanos | keyword | boolean |
| double | double | boolean |
| double | integer | boolean |
| double | keyword | boolean |
| double | long | boolean |
| integer | double | boolean |
| integer | integer | boolean |
| integer | keyword | boolean |
| integer | long | boolean |
| ip | ip | boolean |
| ip | keyword | boolean |
| keyword | keyword | boolean |
| long | double | boolean |
| long | integer | boolean |
| long | keyword | boolean |
| long | long | boolean |
| text | keyword | boolean |
| unsigned_long | double | boolean |
| unsigned_long | integer | boolean |
| unsigned_long | keyword | boolean |
| unsigned_long | long | boolean |
| unsigned_long | unsigned_long | boolean |
| version | keyword | boolean |
| version | version | boolean |

```esql
FROM books
| WHERE author:"Faulkner"
| KEEP book_no, author
| SORT book_no
| LIMIT 5
```

| book_no:keyword | author:text |
| --- | --- |
| 2378 | [Carol Faulkner, Holly Byers Ochoa, Lucretia Mott] |
| 2713 | William Faulkner |
| 2847 | Colleen Faulkner |
| 2883 | William Faulkner |
| 3293 | Danny Faulkner |
