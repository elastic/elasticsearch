---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions-string.html
---

# String functions [sql-functions-string]

Functions for performing string manipulation.

## `ASCII` [sql-functions-string-ascii]

```sql
ASCII(string_exp) <1>
```

**Input**:

1. string expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Returns the ASCII code value of the leftmost character of `string_exp` as an integer.

```sql
SELECT ASCII('Elastic');

ASCII('Elastic')
----------------
69
```


## `BIT_LENGTH` [sql-functions-string-bit-length]

```sql
BIT_LENGTH(string_exp) <1>
```

**Input**:

1. string expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Returns the length in bits of the `string_exp` input expression.

```sql
SELECT BIT_LENGTH('Elastic');

BIT_LENGTH('Elastic')
---------------------
56
```


## `CHAR` [sql-functions-string-char]

```sql
CHAR(code) <1>
```

**Input**:

1. integer expression between `0` and `255`. If `null`, negative, or greater than `255`, the function returns `null`.


**Output**: string

**Description**: Returns the character that has the ASCII code value specified by the numeric input.

```sql
SELECT CHAR(69);

   CHAR(69)
---------------
E
```


## `CHAR_LENGTH` [sql-functions-string-char-length]

```sql
CHAR_LENGTH(string_exp) <1>
```

**Input**:

1. string expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Returns the length in characters of the input, if the string expression is of a character data type; otherwise, returns the length in bytes of the string expression (the smallest integer not less than the number of bits divided by 8).

```sql
SELECT CHAR_LENGTH('Elastic');

CHAR_LENGTH('Elastic')
----------------------
7
```


## `CONCAT` [sql-functions-string-concat]

```sql
CONCAT(
    string_exp1, <1>
    string_exp2) <2>
```

**Input**:

1. string expression. Treats `null` as an empty string.
2. string expression. Treats `null` as an empty string.


**Output**: string

**Description**: Returns a character string that is the result of concatenating `string_exp1` to `string_exp2`.

The resulting string cannot exceed a byte length of 1 MB.

```sql
SELECT CONCAT('Elasticsearch', ' SQL');

CONCAT('Elasticsearch', ' SQL')
-------------------------------
Elasticsearch SQL
```


## `INSERT` [sql-functions-string-insert]

```sql
INSERT(
    source,      <1>
    start,       <2>
    length,      <3>
    replacement) <4>
```

**Input**:

1. string expression. If `null`, the function returns `null`.
2. integer expression. If `null`, the function returns `null`.
3. integer expression. If `null`, the function returns `null`.
4. string expression. If `null`, the function returns `null`.


**Output**: string

**Description**: Returns a string where `length` characters have been deleted from `source`, beginning at `start`, and where `replacement` has been inserted into `source`, beginning at `start`.

The resulting string cannot exceed a byte length of 1 MB.

```sql
SELECT INSERT('Elastic ', 8, 1, 'search');

INSERT('Elastic ', 8, 1, 'search')
----------------------------------
Elasticsearch
```


## `LCASE` [sql-functions-string-lcase]

```sql
LCASE(string_exp) <1>
```

**Input**:

1. string expression. If `null`, the function returns `null`.


**Output**: string

**Description**: Returns a string equal to that in `string_exp`, with all uppercase characters converted to lowercase.

```sql
SELECT LCASE('Elastic');

LCASE('Elastic')
----------------
elastic
```


## `LEFT` [sql-functions-string-left]

```sql
LEFT(
    string_exp, <1>
    count)      <2>
```

**Input**:

1. string expression. If `null`, the function returns `null`.
2. integer expression. If `null`, the function returns `null`. If `0` or negative, the function returns an empty string.


**Output**: string

**Description**: Returns the leftmost count characters of `string_exp`.

```sql
SELECT LEFT('Elastic',3);

LEFT('Elastic',3)
-----------------
Ela
```


## `LENGTH` [sql-functions-string-length]

```sql
LENGTH(string_exp) <1>
```

**Input**:

1. string expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Returns the number of characters in `string_exp`, excluding trailing blanks.

```sql
SELECT LENGTH('Elastic   ');

LENGTH('Elastic   ')
--------------------
7
```


## `LOCATE` [sql-functions-string-locate]

```sql
LOCATE(
    pattern, <1>
    source   <2>
    [, start]<3>
)
```

**Input**:

1. string expression.  If `null`, the function returns `null`.
2. string expression.  If `null`, the function returns `null`.
3. integer expression; optional. If `null`, `0`, `1`, negative, or not specified, the search starts at the first character position.


**Output**: integer

**Description**: Returns the starting position of the first occurrence of `pattern` within `source`. The optional `start` specifies the character position to start the search with. If the `pattern` is not found within `source`, the function returns `0`.

```sql
SELECT LOCATE('a', 'Elasticsearch');

LOCATE('a', 'Elasticsearch')
----------------------------
3
```

```sql
SELECT LOCATE('a', 'Elasticsearch', 5);

LOCATE('a', 'Elasticsearch', 5)
-------------------------------
10
```


## `LTRIM` [sql-functions-string-ltrim]

```sql
LTRIM(string_exp) <1>
```

**Input**:

1. string expression. If `null`, the function returns `null`.


**Output**: string

**Description**: Returns the characters of `string_exp`, with leading blanks removed.

```sql
SELECT LTRIM('   Elastic');

LTRIM('   Elastic')
-------------------
Elastic
```


## `OCTET_LENGTH` [sql-functions-string-octet-length]

```sql
OCTET_LENGTH(string_exp) <1>
```

**Input**:

1. string expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Returns the length in bytes of the `string_exp` input expression.

```sql
SELECT OCTET_LENGTH('Elastic');

OCTET_LENGTH('Elastic')
-----------------------
7
```


## `POSITION` [sql-functions-string-position]

```sql
POSITION(
    string_exp1, <1>
    string_exp2) <2>
```

**Input**:

1. string expression. If `null`, the function returns `null`.
2. string expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Returns the position of the `string_exp1` in `string_exp2`. The result is an exact numeric.

```sql
SELECT POSITION('Elastic', 'Elasticsearch');

POSITION('Elastic', 'Elasticsearch')
------------------------------------
1
```


## `REPEAT` [sql-functions-string-repeat]

```sql
REPEAT(
    string_exp, <1>
    count)      <2>
```

**Input**:

1. string expression. If `null`, the function returns `null`.
2. integer expression. If `0`, negative, or `null`, the function returns `null`.


**Output**: string

**Description**: Returns a character string composed of `string_exp` repeated `count` times.

The resulting string cannot exceed a byte length of 1 MB.

```sql
SELECT REPEAT('La', 3);

 REPEAT('La', 3)
----------------
LaLaLa
```


## `REPLACE` [sql-functions-string-replace]

```sql
REPLACE(
    source,      <1>
    pattern,     <2>
    replacement) <3>
```

**Input**:

1. string expression. If `null`, the function returns `null`.
2. string expression. If `null`, the function returns `null`.
3. string expression. If `null`, the function returns `null`.


**Output**: string

**Description**: Search `source` for occurrences of `pattern`, and replace with `replacement`.

The resulting string cannot exceed a byte length of 1 MB.

```sql
SELECT REPLACE('Elastic','El','Fant');

REPLACE('Elastic','El','Fant')
------------------------------
Fantastic
```


## `RIGHT` [sql-functions-string-right]

```sql
RIGHT(
    string_exp, <1>
    count)      <2>
```

**Input**:

1. string expression. If `null`, the function returns `null`.
2. integer expression. If `null`, the function returns `null`. If `0` or negative, the function returns an empty string.


**Output**: string

**Description**: Returns the rightmost count characters of `string_exp`.

```sql
SELECT RIGHT('Elastic',3);

RIGHT('Elastic',3)
------------------
tic
```


## `RTRIM` [sql-functions-string-rtrim]

```sql
RTRIM(string_exp) <1>
```

**Input**:

1. string expression. If `null`, the function returns `null`.


**Output**: string

**Description**: Returns the characters of `string_exp` with trailing blanks removed.

```sql
SELECT RTRIM('Elastic   ');

RTRIM('Elastic   ')
-------------------
Elastic
```


## `SPACE` [sql-functions-string-space]

```sql
SPACE(count) <1>
```

**Input**:

1. integer expression. If `null` or negative, the function returns `null`.


**Output**: string

**Description**: Returns a character string consisting of `count` spaces.

The resulting string cannot exceed a byte length of 1 MB.

```sql
SELECT SPACE(3);

   SPACE(3)
---------------
```


## `STARTS_WITH` [sql-functions-string-startswith]

```sql
STARTS_WITH(
    source,   <1>
    pattern)  <2>
```

**Input**:

1. string expression. If `null`, the function returns `null`.
2. string expression. If `null`, the function returns `null`.


**Output**: boolean value

**Description**: Returns `true` if the source expression starts with the specified pattern, `false` otherwise. The matching is case sensitive.

```sql
SELECT STARTS_WITH('Elasticsearch', 'Elastic');

STARTS_WITH('Elasticsearch', 'Elastic')
--------------------------------
true
```

```sql
SELECT STARTS_WITH('Elasticsearch', 'ELASTIC');

STARTS_WITH('Elasticsearch', 'ELASTIC')
--------------------------------
false
```


## `SUBSTRING` [sql-functions-string-substring]

```sql
SUBSTRING(
    source, <1>
    start,  <2>
    length) <3>
```

**Input**:

1. string expression. If `null`, the function returns `null`.
2. integer expression. If `null`, the function returns `null`.
3. integer expression. If `null`, the function returns `null`.


**Output**: string

**Description**: Returns a character string that is derived from `source`, beginning at the character position specified by `start` for `length` characters.

```sql
SELECT SUBSTRING('Elasticsearch', 0, 7);

SUBSTRING('Elasticsearch', 0, 7)
--------------------------------
Elastic
```


## `TRIM` [sql-functions-string-trim]

```sql
TRIM(string_exp) <1>
```

**Input**:

1. string expression. If `null`, the function returns `null`.


**Output**: string

**Description**: Returns the characters of `string_exp`, with leading and trailing blanks removed.

```sql
SELECT TRIM('   Elastic   ') AS trimmed;

trimmed
--------------
Elastic
```


## `UCASE` [sql-functions-string-ucase]

```sql
UCASE(string_exp) <1>
```

**Input**:

1. string expression. If `null`, the function returns `null`.


**Output**: string

**Description**: Returns a string equal to that of the input, with all lowercase characters converted to uppercase.

```sql
SELECT UCASE('Elastic');

UCASE('Elastic')
----------------
ELASTIC
```



