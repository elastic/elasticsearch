---
navigation_title: "Function reference"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/eql-function-ref.html
---

# EQL function reference [eql-function-ref]


{{es}} supports the following [EQL functions](/reference/query-languages/eql/eql-syntax.md#eql-functions).


## `add` [eql-fn-add]

Returns the sum of two provided addends.

**Example**

```eql
add(4, 5)                                           // returns 9
add(4, 0.5)                                         // returns 4.5
add(0.5, 0.25)                                      // returns 0.75
add(4, -2)                                          // returns 2
add(-2, -2)                                         // returns -4

// process.args_count = 4
add(process.args_count, 5)                          // returns 9
add(process.args_count, 0.5)                        // returns 4.5

// process.parent.args_count = 2
add(process.args_count, process.parent.args_count)  // returns 6

// null handling
add(null, 4)                                        // returns null
add(4. null)                                        // returns null
add(null, process.args_count)                       // returns null
add(process.args_count null)                        // returns null
```

**Syntax**

```txt
add(<addend>, <addend>)
```

**Parameters:**

`<addend>`
:   (Required, integer or float or `null`) Addend to add. If `null`, the function returns `null`.

    Two addends are required. No more than two addends can be provided.

    If using a field as the argument, this parameter supports only [`numeric`](/reference/elasticsearch/mapping-reference/number.md) field data types.


**Returns:** integer, float, or `null`


## `between` [eql-fn-between]

Extracts a substring that’s between a provided `left` and `right` text in a source string. Matching is case-sensitive by default.

**Example**

```eql
// file.path = "C:\\Windows\\System32\\cmd.exe"
between(file.path, "System32\\\\", ".exe")                // returns "cmd"
between(file.path, "system32\\\\", ".exe")                // returns ""
between(file.path, "workspace\\\\", ".exe")               // returns ""

// Make matching case-insensitive
between~(file.path, "system32\\\\", ".exe")               // returns "cmd"

// Greedy matching defaults to false.
between(file.path, "\\\\", "\\\\", false)                 // returns "Windows"

// Sets greedy matching to true
between(file.path, "\\\\", "\\\\", true)                  // returns "Windows\\System32"

// empty source string
between("", "System32\\\\", ".exe")                       // returns ""
between("", "", "")                                       // returns ""

// null handling
between(null, "System32\\\\", ".exe")                     // returns null
```

**Syntax**

```txt
between(<source>, <left>, <right>[, <greedy_matching>])
```

**Parameters**

`<source>`
:   (Required, string or `null`) Source string. Empty strings return an empty string (`""`), regardless of the `<left>` or `<right>` parameters. If `null`, the function returns `null`.

If using a field as the argument, this parameter supports only the following field data types:

* A type in the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) family
* [`text`](/reference/elasticsearch/mapping-reference/text.md) field with a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) sub-field


`<left>`
:   (Required, string) Text to the left of the substring to extract. This text should include whitespace.

If using a field as the argument, this parameter supports only the following field data types:

* A type in the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) family
* [`text`](/reference/elasticsearch/mapping-reference/text.md) field with a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) sub-field


`<right>`
:   (Required, string) Text to the right of the substring to extract. This text should include whitespace.

If using a field as the argument, this parameter supports only the following field data types:

* A type in the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) family
* [`text`](/reference/elasticsearch/mapping-reference/text.md) field with a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) sub-field


`<greedy_matching>`
:   (Optional, Boolean) If `true`, match the longest possible substring, similar to `.*` in regular expressions. If `false`, match the shortest possible substring, similar to `.*?` in regular expressions. Defaults to `false`.

**Returns:** string or `null`


## `cidrMatch` [eql-fn-cidrmatch]

Returns `true` if an IP address is contained in one or more provided [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing) blocks.

**Example**

```eql
// source.address = "192.168.152.12"
cidrMatch(source.address, "192.168.0.0/16")               // returns true
cidrMatch(source.address, "192.168.0.0/16", "10.0.0.0/8") // returns true
cidrMatch(source.address, "10.0.0.0/8")                   // returns false
cidrMatch(source.address, "10.0.0.0/8", "10.128.0.0/9")   // returns false

// null handling
cidrMatch(null, "10.0.0.0/8")                             // returns null
cidrMatch(source.address, null)                           // returns null
```

**Syntax**

```txt
`cidrMatch(<ip_address>, <cidr_block>[, ...])`
```

**Parameters**

`<ip_address>`
:   (Required, string or `null`) IP address. Supports [IPv4](https://en.wikipedia.org/wiki/IPv4) and [IPv6](https://en.wikipedia.org/wiki/IPv6) addresses. If `null`, the function returns `null`.

    If using a field as the argument, this parameter supports only the [`ip`](/reference/elasticsearch/mapping-reference/ip.md) field data type.


`<cidr_block>`
:   (Required, string or `null`) CIDR block you wish to search. If `null`, the function returns `null`. This parameter accepts multiple arguments.

**Returns:** boolean or `null`


## `concat` [eql-fn-concat]

Returns a concatenated string of provided values.

**Example**

```eql
concat("process is ", "regsvr32.exe")         // returns "process is regsvr32.exe"
concat("regsvr32.exe", " ", 42)               // returns "regsvr32.exe 42"
concat("regsvr32.exe", " ", 42.5)             // returns "regsvr32.exe 42.5"
concat("regsvr32.exe", " ", true)             // returns "regsvr32.exe true"
concat("regsvr32.exe")                        // returns "regsvr32.exe"

// process.name = "regsvr32.exe"
concat(process.name, " ", 42)                 // returns "regsvr32.exe 42"
concat(process.name, " ", 42.5)               // returns "regsvr32.exe 42.5"
concat("process is ", process.name)           // returns "process is regsvr32.exe"
concat(process.name, " ", true)               // returns "regsvr32.exe true"
concat(process.name)                          // returns "regsvr32.exe"

// process.arg_count = 4
concat(process.name, " ", process.arg_count)  // returns "regsvr32.exe 4"

// null handling
concat(null, "regsvr32.exe")                  // returns null
concat(process.name, null)                    // returns null
concat(null)                                  // returns null
```

**Syntax**

```txt
concat(<value>[, <value>])
```

**Parameters**

`<value>`
:   (Required) Value to concatenate. If any of the arguments are `null`, the function returns `null`. This parameter accepts multiple arguments.

    If using a field as the argument, this parameter does not support the [`text`](/reference/elasticsearch/mapping-reference/text.md) field data type.


**Returns:** string or `null`


## `divide` [eql-fn-divide]

Returns the quotient of a provided dividend and divisor.

:::::{warning}
:name: eql-divide-fn-float-rounding

If both the dividend and divisor are integers, the `divide` function *rounds down* any returned floating point numbers to the nearest integer. To avoid rounding, convert either the dividend or divisor to a float.

::::{dropdown} Example
The `process.args_count` field is a [`long`](/reference/elasticsearch/mapping-reference/number.md) integer field containing a count of process arguments.

A user might expect the following EQL query to only match events with a `process.args_count` value of `4`.

```eql
process where divide(4, process.args_count) == 1
```

However, the EQL query matches events with a `process.args_count` value of `3` or `4`.

For events with a `process.args_count` value of `3`, the `divide` function returns a floating point number of `1.333...`, which is rounded down to `1`.

To match only events with a `process.args_count` value of `4`, convert either the dividend or divisor to a float.

The following EQL query changes the integer `4` to the equivalent float `4.0`.

```eql
process where divide(4.0, process.args_count) == 1
```

::::


:::::


**Example**

```eql
divide(4, 2)                                            // returns 2
divide(4, 3)                                            // returns 1
divide(4, 3.0)                                          // returns 1.333...
divide(4, 0.5)                                          // returns 8
divide(0.5, 4)                                          // returns 0.125
divide(0.5, 0.25)                                       // returns 2.0
divide(4, -2)                                           // returns -2
divide(-4, -2)                                          // returns 2

// process.args_count = 4
divide(process.args_count, 2)                           // returns 2
divide(process.args_count, 3)                           // returns 1
divide(process.args_count, 3.0)                         // returns 1.333...
divide(12, process.args_count)                          // returns 3
divide(process.args_count, 0.5)                         // returns 8
divide(0.5, process.args_count)                         // returns 0.125

// process.parent.args_count = 2
divide(process.args_count, process.parent.args_count)   // returns 2

// null handling
divide(null, 4)                                         // returns null
divide(4, null)                                         // returns null
divide(null, process.args_count)                        // returns null
divide(process.args_count, null)                        // returns null
```

**Syntax**

```txt
divide(<dividend>, <divisor>)
```

**Parameters**

`<dividend>`
:   (Required, integer or float or `null`) Dividend to divide. If `null`, the function returns `null`.

    If using a field as the argument, this parameter supports only [`numeric`](/reference/elasticsearch/mapping-reference/number.md) field data types.


`<divisor>`
:   (Required, integer or float or `null`) Divisor to divide by. If `null`, the function returns `null`. This value cannot be zero (`0`).

    If using a field as the argument, this parameter supports only [`numeric`](/reference/elasticsearch/mapping-reference/number.md) field data types.


**Returns:** integer, float, or null


## `endsWith` [eql-fn-endswith]

Returns `true` if a source string ends with a provided substring. Matching is case-sensitive by default.

**Example**

```eql
endsWith("regsvr32.exe", ".exe")          // returns true
endsWith("regsvr32.exe", ".EXE")          // returns false
endsWith("regsvr32.exe", ".dll")          // returns false
endsWith("", "")                          // returns true

// Make matching case-insensitive
endsWith~("regsvr32.exe", ".EXE")         // returns true

// file.name = "regsvr32.exe"
endsWith(file.name, ".exe")               // returns true
endsWith(file.name, ".dll")               // returns false

// file.extension = ".exe"
endsWith("regsvr32.exe", file.extension)  // returns true
endsWith("ntdll.dll", file.name)          // returns false

// null handling
endsWith("regsvr32.exe", null)            // returns null
endsWith("", null)                        // returns null
endsWith(null, ".exe")                    // returns null
endsWith(null, null)                      // returns null
```

**Syntax**

```txt
endsWith(<source>, <substring>)
```

**Parameters**

`<source>`
:   (Required, string or `null`) Source string. If `null`, the function returns `null`.

If using a field as the argument, this parameter supports only the following field data types:

* A type in the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) family
* [`text`](/reference/elasticsearch/mapping-reference/text.md) field with a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) sub-field


`<substring>`
:   (Required, string or `null`) Substring to search for. If `null`, the function returns `null`.

If using a field as the argument, this parameter supports only the following field data types:

* A type in the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) family
* [`text`](/reference/elasticsearch/mapping-reference/text.md) field with a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) sub-field


**Returns:** boolean or `null`


## `indexOf` [eql-fn-indexof]

Returns the first position of a provided substring in a source string. Matching is case-sensitive by default.

If an optional start position is provided, this function returns the first occurrence of the substring at or after the start position.

**Example**

```eql
// url.domain = "subdomain.example.com"
indexOf(url.domain, "d")        // returns 3
indexOf(url.domain, "D")        // returns null
indexOf(url.domain, ".")        // returns 9
indexOf(url.domain, ".", 9)     // returns 9
indexOf(url.domain, ".", 10)    // returns 17
indexOf(url.domain, ".", -6)    // returns 9

// Make matching case-insensitive
indexOf~(url.domain, "D")        // returns 4

// empty strings
indexOf("", "")                 // returns 0
indexOf(url.domain, "")         // returns 0
indexOf(url.domain, "", 9)      // returns 9
indexOf(url.domain, "", 10)     // returns 10
indexOf(url.domain, "", -6)     // returns 0

// missing substrings
indexOf(url.domain, "z")        // returns null
indexOf(url.domain, "z", 9)     // returns null

// start position is higher than string length
indexOf(url.domain, ".", 30)    // returns null

// null handling
indexOf(null, ".", 9)           // returns null
indexOf(url.domain, null, 9)    // returns null
indexOf(url.domain, ".", null)  // returns null
```

**Syntax**

```txt
indexOf(<source>, <substring>[, <start_pos>])
```

**Parameters**

`<source>`
:   (Required, string or `null`) Source string. If `null`, the function returns `null`.

If using a field as the argument, this parameter supports only the following field data types:

* A type in the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) family
* [`text`](/reference/elasticsearch/mapping-reference/text.md) field with a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) sub-field


`<substring>`
:   (Required, string or `null`) Substring to search for.

If this argument is `null` or the `<source>` string does not contain this substring, the function returns `null`.

If the `<start_pos>` is positive, empty strings (`""`) return the `<start_pos>`. Otherwise, empty strings return `0`.

If using a field as the argument, this parameter supports only the following field data types:

* A type in the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) family
* [`text`](/reference/elasticsearch/mapping-reference/text.md) field with a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) sub-field


`<start_pos>`
:   (Optional, integer or `null`) Starting position for matching. The function will not return positions before this one. Defaults to `0`.

Positions are zero-indexed. Negative offsets are treated as `0`.

If this argument is `null` or higher than the length of the `<source>` string, the function returns `null`.

If using a field as the argument, this parameter supports only the following [numeric](/reference/elasticsearch/mapping-reference/number.md) field data types:

* `long`
* `integer`
* `short`
* `byte`


**Returns:** integer or `null`


## `length` [eql-fn-length]

Returns the character length of a provided string, including whitespace and punctuation.

**Example**

```eql
length("explorer.exe")         // returns 12
length("start explorer.exe")   // returns 18
length("")                     // returns 0
length(null)                   // returns null

// process.name = "regsvr32.exe"
length(process.name)           // returns 12
```

**Syntax**

```txt
length(<string>)
```

**Parameters**

`<string>`
:   (Required, string or `null`) String for which to return the character length. If `null`, the function returns `null`. Empty strings return `0`.

If using a field as the argument, this parameter supports only the following field data types:

* A type in the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) family
* [`text`](/reference/elasticsearch/mapping-reference/text.md) field with a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) sub-field


**Returns:** integer or `null`


## `modulo` [eql-fn-modulo]

Returns the remainder of the division of a provided dividend and divisor.

**Example**

```eql
modulo(10, 6)                                       // returns 4
modulo(10, 5)                                       // returns 0
modulo(10, 0.5)                                     // returns 0
modulo(10, -6)                                      // returns 4
modulo(-10, -6)                                     // returns -4

// process.args_count = 10
modulo(process.args_count, 6)                       // returns 4
modulo(process.args_count, 5)                       // returns 0
modulo(106, process.args_count)                     // returns 6
modulo(process.args_count, -6)                      // returns 4
modulo(process.args_count, 0.5)                     // returns 0

// process.parent.args_count = 6
modulo(process.args_count, process.parent.args_count)  // returns 4

// null handling
modulo(null, 5)                                     // returns null
modulo(7, null)                                     // returns null
modulo(null, process.args_count)                    // returns null
modulo(process.args_count, null)                    // returns null
```

**Syntax**

```txt
modulo(<dividend>, <divisor>)
```

**Parameters**

`<dividend>`
:   (Required, integer or float or `null`) Dividend to divide. If `null`, the function returns `null`. Floating point numbers return `0`.

    If using a field as the argument, this parameter supports only [`numeric`](/reference/elasticsearch/mapping-reference/number.md) field data types.


`<divisor>`
:   (Required, integer or float or `null`) Divisor to divide by. If `null`, the function returns `null`. Floating point numbers return `0`. This value cannot be zero (`0`).

    If using a field as the argument, this parameter supports only [`numeric`](/reference/elasticsearch/mapping-reference/number.md) field data types.


**Returns:** integer, float, or `null`


## `multiply` [eql-fn-multiply]

Returns the product of two provided factors.

**Example**

```eql
multiply(2, 2)                                           // returns 4
multiply(0.5, 2)                                         // returns 1
multiply(0.25, 2)                                        // returns 0.5
multiply(-2, 2)                                          // returns -4
multiply(-2, -2)                                         // returns 4

// process.args_count = 2
multiply(process.args_count, 2)                          // returns 4
multiply(0.5, process.args_count)                        // returns 1
multiply(0.25, process.args_count)                       // returns 0.5

// process.parent.args_count = 3
multiply(process.args_count, process.parent.args_count)  // returns 6

// null handling
multiply(null, 2)                                        // returns null
multiply(2, null)                                        // returns null
```

**Syntax**

```txt
multiply(<factor, <factor>)
```

**Parameters**

`<factor>`
:   (Required, integer or float or `null`) Factor to multiply. If `null`, the function returns `null`.

Two factors are required. No more than two factors can be provided.

If using a field as the argument, this parameter supports only [`numeric`](/reference/elasticsearch/mapping-reference/number.md) field data types.


**Returns:** integer, float, or `null`


## `number` [eql-fn-number]

Converts a string to the corresponding integer or float.

**Example**

```eql
number("1337")              // returns 1337
number("42.5")              // returns 42.5
number("deadbeef", 16)      // returns 3735928559

// integer literals beginning with "0x" are auto-detected as hexadecimal
number("0xdeadbeef")        // returns 3735928559
number("0xdeadbeef", 16)    // returns 3735928559

// "+" and "-" are supported
number("+1337")             // returns 1337
number("-1337")             // returns -1337

// surrounding whitespace is ignored
number("  1337  ")          // returns 1337

// process.pid = "1337"
number(process.pid)         // returns 1337

// null handling
number(null)                // returns null
number(null, 16)            // returns null

// strings beginning with "0x" are treated as hexadecimal (base 16),
// even if the <base_num> is explicitly null.
number("0xdeadbeef", null) // returns 3735928559

// otherwise, strings are treated as decimal (base 10)
// if the <base_num> is explicitly null.
number("1337", null)        // returns 1337
```

**Syntax**

```txt
number(<string>[, <base_num>])
```

**Parameters**

`<string>`
:   (Required, string or `null`) String to convert to an integer or float. If this value is a string, it must be one of the following:

* A string representation of an integer (e.g., `"42"`)
* A string representation of a float (e.g., `"9.5"`)
* If the `<base_num>` parameter is specified, a string containing an integer literal in the base notation (e.g., `"0xDECAFBAD"` in hexadecimal or base `16`)

Strings that begin with `0x` are auto-detected as hexadecimal and use a default `<base_num>` of `16`.

`-` and `+` are supported with no space between. Surrounding whitespace is ignored. Empty strings (`""`) are not supported.

If using a field as the argument, this parameter supports only the following field data types:

* A type in the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) family
* [`text`](/reference/elasticsearch/mapping-reference/text.md) field with a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) sub-field

If this argument is `null`, the function returns `null`.


`<base_num>`
:   (Optional, integer or `null`) Radix or base used to convert the string. If the `<string>` begins with `0x`, this parameter defaults to `16` (hexadecimal). Otherwise, it defaults to base `10`.

If this argument is explicitly `null`, the default value is used.

Fields are not supported as arguments.


**Returns:** integer or float or `null`


## `startsWith` [eql-fn-startswith]

Returns `true` if a source string begins with a provided substring. Matching is case-sensitive by default.

**Example**

```eql
startsWith("regsvr32.exe", "regsvr32")  // returns true
startsWith("regsvr32.exe", "Regsvr32")  // returns false
startsWith("regsvr32.exe", "explorer")  // returns false
startsWith("", "")                      // returns true

// Make matching case-insensitive
startsWith~("regsvr32.exe", "Regsvr32")  // returns true

// process.name = "regsvr32.exe"
startsWith(process.name, "regsvr32")    // returns true
startsWith(process.name, "explorer")    // returns false

// process.name = "regsvr32"
startsWith("regsvr32.exe", process.name) // returns true
startsWith("explorer.exe", process.name) // returns false

// null handling
startsWith("regsvr32.exe", null)        // returns null
startsWith("", null)                    // returns null
startsWith(null, "regsvr32")            // returns null
startsWith(null, null)                  // returns null
```

**Syntax**

```txt
startsWith(<source>, <substring>)
```

**Parameters**

`<source>`
:   (Required, string or `null`) Source string. If `null`, the function returns `null`.

If using a field as the argument, this parameter supports only the following field data types:

* A type in the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) family
* [`text`](/reference/elasticsearch/mapping-reference/text.md) field with a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) sub-field


`<substring>`
:   (Required, string or `null`) Substring to search for. If `null`, the function returns `null`.

If using a field as the argument, this parameter supports only the following field data types:

* A type in the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) family
* [`text`](/reference/elasticsearch/mapping-reference/text.md) field with a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) sub-field


**Returns:** boolean or `null`


## `string` [eql-fn-string]

Converts a value to a string.

**Example**

```eql
string(42)               // returns "42"
string(42.5)             // returns "42.5"
string("regsvr32.exe")   // returns "regsvr32.exe"
string(true)             // returns "true"

// null handling
string(null)             // returns null
```

**Syntax**

```txt
string(<value>)
```

**Parameters**

`<value>`
:   (Required) Value to convert to a string. If `null`, the function returns `null`.

    If using a field as the argument, this parameter does not support the [`text`](/reference/elasticsearch/mapping-reference/text.md) field data type.


**Returns:** string or `null`


## `stringContains` [eql-fn-stringcontains]

Returns `true` if a source string contains a provided substring. Matching is case-sensitive by default.

**Example**

```eql
// process.command_line = "start regsvr32.exe"
stringContains(process.command_line, "regsvr32")  // returns true
stringContains(process.command_line, "Regsvr32")  // returns false
stringContains(process.command_line, "start ")    // returns true
stringContains(process.command_line, "explorer")  // returns false

// Make matching case-insensitive
stringContains~(process.command_line, "Regsvr32")  // returns false

// process.name = "regsvr32.exe"
stringContains(command_line, process.name)        // returns true

// empty strings
stringContains("", "")                            // returns false
stringContains(process.command_line, "")          // returns false

// null handling
stringContains(null, "regsvr32")                  // returns null
stringContains(process.command_line, null)        // returns null
```

**Syntax**

```txt
stringContains(<source>, <substring>)
```

**Parameters**

`<source>`
:   (Required, string or `null`) Source string to search. If `null`, the function returns `null`.

If using a field as the argument, this parameter supports only the following field data types:

* A type in the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) family
* [`text`](/reference/elasticsearch/mapping-reference/text.md) field with a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) sub-field

    `<substring>`
    :   (Required, string or `null`) Substring to search for. If `null`, the function returns `null`.


If using a field as the argument, this parameter supports only the following field data types:

* A type in the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) family
* [`text`](/reference/elasticsearch/mapping-reference/text.md) field with a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) sub-field

**Returns:** boolean or `null`


## `substring` [eql-fn-substring]

Extracts a substring from a source string at provided start and end positions.

If no end position is provided, the function extracts the remaining string.

**Example**

```eql
substring("start regsvr32.exe", 6)        // returns "regsvr32.exe"
substring("start regsvr32.exe", 0, 5)     // returns "start"
substring("start regsvr32.exe", 6, 14)    // returns "regsvr32"
substring("start regsvr32.exe", -4)       // returns ".exe"
substring("start regsvr32.exe", -4, -1)   // returns ".ex"
```

**Syntax**

```txt
substring(<source>, <start_pos>[, <end_pos>])
```

**Parameters**

`<source>`
:   (Required, string) Source string.

`<start_pos>`
:   (Required, integer) Starting position for extraction.

If this position is higher than the `<end_pos>` position or the length of the `<source>` string, the function returns an empty string.

Positions are zero-indexed. Negative offsets are supported.


`<end_pos>`
:   (Optional, integer) Exclusive end position for extraction. If this position is not provided, the function returns the remaining string.

    Positions are zero-indexed. Negative offsets are supported.


**Returns:** string


## `subtract` [eql-fn-subtract]

Returns the difference between a provided minuend and subtrahend.

**Example**

```eql
subtract(10, 2)                                          // returns 8
subtract(10.5, 0.5)                                      // returns 10
subtract(1, 0.2)                                         // returns 0.8
subtract(-2, 4)                                          // returns -8
subtract(-2, -4)                                         // returns 8

// process.args_count = 10
subtract(process.args_count, 6)                          // returns 4
subtract(process.args_count, 5)                          // returns 5
subtract(15, process.args_count)                         // returns 5
subtract(process.args_count, 0.5)                        // returns 9.5

// process.parent.args_count = 6
subtract(process.args_count, process.parent.args_count)  // returns 4

// null handling
subtract(null, 2)                                        // returns null
subtract(2, null)                                        // returns null
```

**Syntax**

```txt
subtract(<minuend>, <subtrahend>)
```

**Parameters**

`<minuend>`
:   (Required, integer or float or `null`) Minuend to subtract from.

    If using a field as the argument, this parameter supports only [`numeric`](/reference/elasticsearch/mapping-reference/number.md) field data types.


`<subtrahend>`
:   (Optional, integer or float or `null`) Subtrahend to subtract. If `null`, the function returns `null`.

    If using a field as the argument, this parameter supports only [`numeric`](/reference/elasticsearch/mapping-reference/number.md) field data types.


**Returns:** integer, float, or `null`
