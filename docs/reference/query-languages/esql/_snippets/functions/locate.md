## `LOCATE` [esql-locate]

**Syntax**

:::{image} ../../../../../images/locate.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   An input string

`substring`
:   A substring to locate in the input string

`start`
:   The start index

**Description**

Returns an integer that indicates the position of a keyword substring within another string. Returns `0` if the substring cannot be found. Note that string positions start from `1`.

**Supported types**

| string | substring | start | result |
| --- | --- | --- | --- |
| keyword | keyword | integer | integer |
| keyword | keyword |  | integer |
| keyword | text | integer | integer |
| keyword | text |  | integer |
| text | keyword | integer | integer |
| text | keyword |  | integer |
| text | text | integer | integer |
| text | text |  | integer |

**Example**

```esql
row a = "hello"
| eval a_ll = locate(a, "ll")
```

| a:keyword | a_ll:integer |
| --- | --- |
| hello | 3 |


