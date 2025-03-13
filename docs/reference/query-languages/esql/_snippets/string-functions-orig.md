## {{esql}} string functions [esql-string-functions]

{{esql}} supports these string functions:

:::{include} lists/string-functions.md
:::


## `BIT_LENGTH` [esql-bit_length]

**Syntax**

:::{image} ../../../../images/bit_length.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns the bit length of a string.

::::{note}
All strings are in UTF-8, so a single character can use multiple bytes.
::::


**Supported types**

| string | result |
| --- | --- |
| keyword | integer |
| text | integer |

**Example**

```esql
FROM airports
| WHERE country == "India"
| KEEP city
| EVAL fn_length = LENGTH(city), fn_bit_length = BIT_LENGTH(city)
```

| city:keyword | fn_length:integer | fn_bit_length:integer |
| --- | --- | --- |
| Agwār | 5 | 48 |
| Ahmedabad | 9 | 72 |
| Bangalore | 9 | 72 |


## `BYTE_LENGTH` [esql-byte_length]

**Syntax**

:::{image} ../../../../images/byte_length.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns the byte length of a string.

::::{note}
All strings are in UTF-8, so a single character can use multiple bytes.
::::


**Supported types**

| string | result |
| --- | --- |
| keyword | integer |
| text | integer |

**Example**

```esql
FROM airports
| WHERE country == "India"
| KEEP city
| EVAL fn_length = LENGTH(city), fn_byte_length = BYTE_LENGTH(city)
```

| city:keyword | fn_length:integer | fn_byte_length:integer |
| --- | --- | --- |
| Agwār | 5 | 6 |
| Ahmedabad | 9 | 9 |
| Bangalore | 9 | 9 |


## `CONCAT` [esql-concat]

**Syntax**

:::{image} ../../../../images/concat.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string1`
:   Strings to concatenate.

`string2`
:   Strings to concatenate.

**Description**

Concatenates two or more strings.

**Supported types**

| string1 | string2 | result |
| --- | --- | --- |
| keyword | keyword | keyword |
| keyword | text | keyword |
| text | keyword | keyword |
| text | text | keyword |

**Example**

```esql
FROM employees
| KEEP first_name, last_name
| EVAL fullname = CONCAT(first_name, " ", last_name)
```

| first_name:keyword | last_name:keyword | fullname:keyword |
| --- | --- | --- |
| Alejandro | McAlpine | Alejandro McAlpine |
| Amabile | Gomatam | Amabile Gomatam |
| Anneke | Preusig | Anneke Preusig |


## `ENDS_WITH` [esql-ends_with]

**Syntax**

:::{image} ../../../../images/ends_with.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`str`
:   String expression. If `null`, the function returns `null`.

`suffix`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns a boolean that indicates whether a keyword string ends with another string.

**Supported types**

| str | suffix | result |
| --- | --- | --- |
| keyword | keyword | boolean |
| keyword | text | boolean |
| text | keyword | boolean |
| text | text | boolean |

**Example**

```esql
FROM employees
| KEEP last_name
| EVAL ln_E = ENDS_WITH(last_name, "d")
```

| last_name:keyword | ln_E:boolean |
| --- | --- |
| Awdeh | false |
| Azuma | false |
| Baek | false |
| Bamford | true |
| Bernatsky | false |


## `FROM_BASE64` [esql-from_base64]

**Syntax**

:::{image} ../../../../images/from_base64.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   A base64 string.

**Description**

Decode a base64 string.

**Supported types**

| string | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
row a = "ZWxhc3RpYw=="
| eval d = from_base64(a)
```

| a:keyword | d:keyword |
| --- | --- |
| ZWxhc3RpYw== | elastic |


## `HASH` [esql-hash]

**Syntax**

:::{image} ../../../../images/hash.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`algorithm`
:   Hash algorithm to use.

`input`
:   Input to hash.

**Description**

Computes the hash of the input using various algorithms such as MD5, SHA, SHA-224, SHA-256, SHA-384, SHA-512.

**Supported types**

| algorithm | input | result |
| --- | --- | --- |
| keyword | keyword | keyword |
| keyword | text | keyword |
| text | keyword | keyword |
| text | text | keyword |

**Example**

```esql
FROM sample_data
| WHERE message != "Connection error"
| EVAL md5 = hash("md5", message), sha256 = hash("sha256", message)
| KEEP message, md5, sha256;
```

| message:keyword | md5:keyword | sha256:keyword |
| --- | --- | --- |
| Connected to 10.1.0.1 | abd7d1ce2bb636842a29246b3512dcae | 6d8372129ad78770f7185554dd39864749a62690216460752d6c075fa38ad85c |
| Connected to 10.1.0.2 | 8f8f1cb60832d153f5b9ec6dc828b93f | b0db24720f15857091b3c99f4c4833586d0ea3229911b8777efb8d917cf27e9a |
| Connected to 10.1.0.3 | 912b6dc13503165a15de43304bb77c78 | 75b0480188db8acc4d5cc666a51227eb2bc5b989cd8ca912609f33e0846eff57 |
| Disconnected | ef70e46fd3bbc21e3e1f0b6815e750c0 | 04dfac3671b494ad53fcd152f7a14511bfb35747278aad8ce254a0d6e4ba4718 |


## `LEFT` [esql-left]

**Syntax**

:::{image} ../../../../images/left.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   The string from which to return a substring.

`length`
:   The number of characters to return.

**Description**

Returns the substring that extracts *length* chars from *string* starting from the left.

**Supported types**

| string | length | result |
| --- | --- | --- |
| keyword | integer | keyword |
| text | integer | keyword |

**Example**

```esql
FROM employees
| KEEP last_name
| EVAL left = LEFT(last_name, 3)
| SORT last_name ASC
| LIMIT 5
```

| last_name:keyword | left:keyword |
| --- | --- |
| Awdeh | Awd |
| Azuma | Azu |
| Baek | Bae |
| Bamford | Bam |
| Bernatsky | Ber |


## `LENGTH` [esql-length]

**Syntax**

:::{image} ../../../../images/length.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns the character length of a string.

::::{note}
All strings are in UTF-8, so a single character can use multiple bytes.
::::


**Supported types**

| string | result |
| --- | --- |
| keyword | integer |
| text | integer |

**Example**

```esql
FROM airports
| WHERE country == "India"
| KEEP city
| EVAL fn_length = LENGTH(city)
```

| city:keyword | fn_length:integer |
| --- | --- |
| Agwār | 5 |
| Ahmedabad | 9 |
| Bangalore | 9 |


## `LOCATE` [esql-locate]

**Syntax**

:::{image} ../../../../images/locate.svg
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


## `LTRIM` [esql-ltrim]

**Syntax**

:::{image} ../../../../images/ltrim.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

**Description**

Removes leading whitespaces from a string.

**Supported types**

| string | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
ROW message = "   some text  ",  color = " red "
| EVAL message = LTRIM(message)
| EVAL color = LTRIM(color)
| EVAL message = CONCAT("'", message, "'")
| EVAL color = CONCAT("'", color, "'")
```

| message:keyword | color:keyword |
| --- | --- |
| 'some text  ' | 'red ' |


## `MD5` [esql-md5]

**Syntax**

:::{image} ../../../../images/md5.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`input`
:   Input to hash.

**Description**

Computes the MD5 hash of the input.

**Supported types**

| input | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
FROM sample_data
| WHERE message != "Connection error"
| EVAL md5 = md5(message)
| KEEP message, md5;
```

| message:keyword | md5:keyword |
| --- | --- |
| Connected to 10.1.0.1 | abd7d1ce2bb636842a29246b3512dcae |
| Connected to 10.1.0.2 | 8f8f1cb60832d153f5b9ec6dc828b93f |
| Connected to 10.1.0.3 | 912b6dc13503165a15de43304bb77c78 |
| Disconnected | ef70e46fd3bbc21e3e1f0b6815e750c0 |


## `REPEAT` [esql-repeat]

**Syntax**

:::{image} ../../../../images/repeat.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression.

`number`
:   Number times to repeat.

**Description**

Returns a string constructed by concatenating `string` with itself the specified `number` of times.

**Supported types**

| string | number | result |
| --- | --- | --- |
| keyword | integer | keyword |
| text | integer | keyword |

**Example**

```esql
ROW a = "Hello!"
| EVAL triple_a = REPEAT(a, 3)
```

| a:keyword | triple_a:keyword |
| --- | --- |
| Hello! | Hello!Hello!Hello! |


## `REPLACE` [esql-replace]

**Syntax**

:::{image} ../../../../images/replace.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression.

`regex`
:   Regular expression.

`newString`
:   Replacement string.

**Description**

The function substitutes in the string `str` any match of the regular expression `regex` with the replacement string `newStr`.

**Supported types**

| string | regex | newString | result |
| --- | --- | --- | --- |
| keyword | keyword | keyword | keyword |
| keyword | keyword | text | keyword |
| keyword | text | keyword | keyword |
| keyword | text | text | keyword |
| text | keyword | keyword | keyword |
| text | keyword | text | keyword |
| text | text | keyword | keyword |
| text | text | text | keyword |

**Example**

This example replaces any occurrence of the word "World" with the word "Universe":

```esql
ROW str = "Hello World"
| EVAL str = REPLACE(str, "World", "Universe")
| KEEP str
```

| str:keyword |
| --- |
| Hello Universe |


## `REVERSE` [esql-reverse]

**Syntax**

:::{image} ../../../../images/reverse.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`str`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns a new string representing the input string in reverse order.

**Supported types**

| str | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Examples**

```esql
ROW message = "Some Text" | EVAL message_reversed = REVERSE(message);
```

| message:keyword | message_reversed:keyword |
| --- | --- |
| Some Text | txeT emoS |

`REVERSE` works with unicode, too! It keeps unicode grapheme clusters together during reversal.

```esql
ROW bending_arts = "💧🪨🔥💨" | EVAL bending_arts_reversed = REVERSE(bending_arts);
```

| bending_arts:keyword | bending_arts_reversed:keyword |
| --- | --- |
| 💧🪨🔥💨 | 💨🔥🪨💧 |


## `RIGHT` [esql-right]

**Syntax**

:::{image} ../../../../images/right.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   The string from which to returns a substring.

`length`
:   The number of characters to return.

**Description**

Return the substring that extracts *length* chars from *str* starting from the right.

**Supported types**

| string | length | result |
| --- | --- | --- |
| keyword | integer | keyword |
| text | integer | keyword |

**Example**

```esql
FROM employees
| KEEP last_name
| EVAL right = RIGHT(last_name, 3)
| SORT last_name ASC
| LIMIT 5
```

| last_name:keyword | right:keyword |
| --- | --- |
| Awdeh | deh |
| Azuma | uma |
| Baek | aek |
| Bamford | ord |
| Bernatsky | sky |


## `RTRIM` [esql-rtrim]

**Syntax**

:::{image} ../../../../images/rtrim.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

**Description**

Removes trailing whitespaces from a string.

**Supported types**

| string | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
ROW message = "   some text  ",  color = " red "
| EVAL message = RTRIM(message)
| EVAL color = RTRIM(color)
| EVAL message = CONCAT("'", message, "'")
| EVAL color = CONCAT("'", color, "'")
```

| message:keyword | color:keyword |
| --- | --- |
| '   some text' | ' red' |


## `SHA1` [esql-sha1]

**Syntax**

:::{image} ../../../../images/sha1.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`input`
:   Input to hash.

**Description**

Computes the SHA1 hash of the input.

**Supported types**

| input | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
FROM sample_data
| WHERE message != "Connection error"
| EVAL sha1 = sha1(message)
| KEEP message, sha1;
```

| message:keyword | sha1:keyword |
| --- | --- |
| Connected to 10.1.0.1 | 42b85531a79088036a17759db7d2de292b92f57f |
| Connected to 10.1.0.2 | d30db445da2e9237c9718d0c7e4fb7cbbe9c2cb4 |
| Connected to 10.1.0.3 | 2733848d943809f0b10cad3e980763e88afb9853 |
| Disconnected | 771e05f27b99fd59f638f41a7a4e977b1d4691fe |


## `SHA256` [esql-sha256]

**Syntax**

:::{image} ../../../../images/sha256.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`input`
:   Input to hash.

**Description**

Computes the SHA256 hash of the input.

**Supported types**

| input | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
FROM sample_data
| WHERE message != "Connection error"
| EVAL sha256 = sha256(message)
| KEEP message, sha256;
```

| message:keyword | sha256:keyword |
| --- | --- |
| Connected to 10.1.0.1 | 6d8372129ad78770f7185554dd39864749a62690216460752d6c075fa38ad85c |
| Connected to 10.1.0.2 | b0db24720f15857091b3c99f4c4833586d0ea3229911b8777efb8d917cf27e9a |
| Connected to 10.1.0.3 | 75b0480188db8acc4d5cc666a51227eb2bc5b989cd8ca912609f33e0846eff57 |
| Disconnected | 04dfac3671b494ad53fcd152f7a14511bfb35747278aad8ce254a0d6e4ba4718 |


## `SPACE` [esql-space]

**Syntax**

:::{image} ../../../../images/space.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Number of spaces in result.

**Description**

Returns a string made of `number` spaces.

**Supported types**

| number | result |
| --- | --- |
| integer | keyword |

**Example**

```esql
ROW message = CONCAT("Hello", SPACE(1), "World!");
```

| message:keyword |
| --- |
| Hello World! |


## `SPLIT` [esql-split]

**Syntax**

:::{image} ../../../../images/split.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

`delim`
:   Delimiter. Only single byte delimiters are currently supported.

**Description**

Split a single valued string into multiple strings.

**Supported types**

| string | delim | result |
| --- | --- | --- |
| keyword | keyword | keyword |
| keyword | text | keyword |
| text | keyword | keyword |
| text | text | keyword |

**Example**

```esql
ROW words="foo;bar;baz;qux;quux;corge"
| EVAL word = SPLIT(words, ";")
```

| words:keyword | word:keyword |
| --- | --- |
| foo;bar;baz;qux;quux;corge | [foo,bar,baz,qux,quux,corge] |


## `STARTS_WITH` [esql-starts_with]

**Syntax**

:::{image} ../../../../images/starts_with.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`str`
:   String expression. If `null`, the function returns `null`.

`prefix`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns a boolean that indicates whether a keyword string starts with another string.

**Supported types**

| str | prefix | result |
| --- | --- | --- |
| keyword | keyword | boolean |
| keyword | text | boolean |
| text | keyword | boolean |
| text | text | boolean |

**Example**

```esql
FROM employees
| KEEP last_name
| EVAL ln_S = STARTS_WITH(last_name, "B")
```

| last_name:keyword | ln_S:boolean |
| --- | --- |
| Awdeh | false |
| Azuma | false |
| Baek | true |
| Bamford | true |
| Bernatsky | true |


## `SUBSTRING` [esql-substring]

**Syntax**

:::{image} ../../../../images/substring.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

`start`
:   Start position.

`length`
:   Length of the substring from the start position. Optional; if omitted, all positions after `start` are returned.

**Description**

Returns a substring of a string, specified by a start position and an optional length.

**Supported types**

| string | start | length | result |
| --- | --- | --- | --- |
| keyword | integer | integer | keyword |
| text | integer | integer | keyword |

**Examples**

This example returns the first three characters of every last name:

```esql
FROM employees
| KEEP last_name
| EVAL ln_sub = SUBSTRING(last_name, 1, 3)
```

| last_name:keyword | ln_sub:keyword |
| --- | --- |
| Awdeh | Awd |
| Azuma | Azu |
| Baek | Bae |
| Bamford | Bam |
| Bernatsky | Ber |

A negative start position is interpreted as being relative to the end of the string. This example returns the last three characters of of every last name:

```esql
FROM employees
| KEEP last_name
| EVAL ln_sub = SUBSTRING(last_name, -3, 3)
```

| last_name:keyword | ln_sub:keyword |
| --- | --- |
| Awdeh | deh |
| Azuma | uma |
| Baek | aek |
| Bamford | ord |
| Bernatsky | sky |

If length is omitted, substring returns the remainder of the string. This example returns all characters except for the first:

```esql
FROM employees
| KEEP last_name
| EVAL ln_sub = SUBSTRING(last_name, 2)
```

| last_name:keyword | ln_sub:keyword |
| --- | --- |
| Awdeh | wdeh |
| Azuma | zuma |
| Baek | aek |
| Bamford | amford |
| Bernatsky | ernatsky |


## `TO_BASE64` [esql-to_base64]

**Syntax**

:::{image} ../../../../images/to_base64.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   A string.

**Description**

Encode a string to a base64 string.

**Supported types**

| string | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
row a = "elastic"
| eval e = to_base64(a)
```

| a:keyword | e:keyword |
| --- | --- |
| elastic | ZWxhc3RpYw== |


## `TO_LOWER` [esql-to_lower]

**Syntax**

:::{image} ../../../../images/to_lower.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`str`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns a new string representing the input string converted to lower case.

**Supported types**

| str | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
ROW message = "Some Text"
| EVAL message_lower = TO_LOWER(message)
```

| message:keyword | message_lower:keyword |
| --- | --- |
| Some Text | some text |


## `TO_UPPER` [esql-to_upper]

**Syntax**

:::{image} ../../../../images/to_upper.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`str`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns a new string representing the input string converted to upper case.

**Supported types**

| str | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
ROW message = "Some Text"
| EVAL message_upper = TO_UPPER(message)
```

| message:keyword | message_upper:keyword |
| --- | --- |
| Some Text | SOME TEXT |


## `TRIM` [esql-trim]

**Syntax**

:::{image} ../../../../images/trim.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

**Description**

Removes leading and trailing whitespaces from a string.

**Supported types**

| string | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
ROW message = "   some text  ",  color = " red "
| EVAL message = TRIM(message)
| EVAL color = TRIM(color)
```

| message:s | color:s |
| --- | --- |
| some text | red |
