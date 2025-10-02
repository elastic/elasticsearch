---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions.html
---

# Functions and operators [sql-functions]

Elasticsearch SQL provides a comprehensive set of built-in operators and functions:

* [Operators](/reference/query-languages/sql/sql-operators.md)

    * [`Equality (=)`](/reference/query-languages/sql/sql-operators.md#sql-operators-equality)
    * [`Null safe Equality (<=>)`](/reference/query-languages/sql/sql-operators.md#sql-operators-null-safe-equality)
    * [`Inequality (<> or !=)`](/reference/query-languages/sql/sql-operators.md#sql-operators-inequality)
    * [`Comparison (<, <=, >, >=)`](/reference/query-languages/sql/sql-operators.md#sql-operators-comparison)
    * [`BETWEEN`](/reference/query-languages/sql/sql-operators.md#sql-operators-between)
    * [`IS NULL/IS NOT NULL`](/reference/query-languages/sql/sql-operators.md#sql-operators-is-null)
    * [`IN (<value1>, <value2>, ...)`](/reference/query-languages/sql/sql-operators.md#sql-operators-in)
    * [`AND`](/reference/query-languages/sql/sql-operators-logical.md#sql-operators-and)
    * [`OR`](/reference/query-languages/sql/sql-operators-logical.md#sql-operators-or)
    * [`NOT`](/reference/query-languages/sql/sql-operators-logical.md#sql-operators-not)
    * [`Add (+)`](/reference/query-languages/sql/sql-operators-math.md#sql-operators-plus)
    * [`Subtract (infix -)`](/reference/query-languages/sql/sql-operators-math.md#sql-operators-subtract)
    * [`Negate (unary -)`](/reference/query-languages/sql/sql-operators-math.md#sql-operators-negate)
    * [`Multiply (*)`](/reference/query-languages/sql/sql-operators-math.md#sql-operators-multiply)
    * [`Divide (/)`](/reference/query-languages/sql/sql-operators-math.md#sql-operators-divide)
    * [`Modulo or Remainder(%)`](/reference/query-languages/sql/sql-operators-math.md#sql-operators-remainder)
    * [`Cast (::)`](/reference/query-languages/sql/sql-operators-cast.md#sql-operators-cast-cast)

* [LIKE and RLIKE Operators](/reference/query-languages/sql/sql-like-rlike-operators.md)

    * [`LIKE`](/reference/query-languages/sql/sql-like-rlike-operators.md#sql-like-operator)
    * [`RLIKE`](/reference/query-languages/sql/sql-like-rlike-operators.md#sql-rlike-operator)

* [Aggregate Functions](/reference/query-languages/sql/sql-functions-aggs.md)

    * [`AVG`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-avg)
    * [`COUNT`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-count)
    * [`COUNT(ALL)`](sql-functions-aggs.md#sql-functions-aggs-count-all)
    * [`COUNT(DISTINCT)`](sql-functions-aggs.md#sql-functions-aggs-count-distinct)
    * [`FIRST/FIRST_VALUE`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-first)
    * [`LAST/LAST_VALUE`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-last)
    * [`MAX`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-max)
    * [`MIN`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-min)
    * [`SUM`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-sum)
    * [`KURTOSIS`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-kurtosis)
    * [`MAD`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-mad)
    * [`PERCENTILE`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-percentile)
    * [`PERCENTILE_RANK`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-percentile-rank)
    * [`SKEWNESS`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-skewness)
    * [`STDDEV_POP`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-stddev-pop)
    * [`STDDEV_SAMP`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-stddev-samp)
    * [`SUM_OF_SQUARES`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-sum-squares)
    * [`VAR_POP`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-var-pop)
    * [`VAR_SAMP`](/reference/query-languages/sql/sql-functions-aggs.md#sql-functions-aggs-var-samp)

* [Grouping Functions](/reference/query-languages/sql/sql-functions-grouping.md)

    * [`HISTOGRAM`](/reference/query-languages/sql/sql-functions-grouping.md#sql-functions-grouping-histogram)

* [Date-Time Operators](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-interval)
* [Date-Time Functions](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-current-date)

    * [`CURRENT_DATE/CURDATE`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-current-date)
    * [`CURRENT_TIME/CURTIME`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-current-time)
    * [`CURRENT_TIMESTAMP`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-current-timestamp)
    * [`DATE_ADD/DATEADD/TIMESTAMP_ADD/TIMESTAMPADD`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-add)
    * [`DATE_DIFF/DATEDIFF/TIMESTAMP_DIFF/TIMESTAMPDIFF`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-diff)
    * [`DATE_FORMAT`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-dateformat)
    * [`DATE_PARSE`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-dateparse)
    * [`DATETIME_FORMAT`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-datetimeformat)
    * [`DATETIME_PARSE`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-datetimeparse)
    * [`FORMAT`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-format)
    * [`DATE_PART/DATEPART`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-part)
    * [`DATE_TRUNC/DATETRUNC`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-trunc)
    * [`DAY_OF_MONTH/DOM/DAY`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-day)
    * [`DAY_OF_WEEK/DAYOFWEEK/DOW`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-dow)
    * [`DAY_OF_YEAR/DOY`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-doy)
    * [`DAY_NAME/DAYNAME`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-dayname)
    * [`EXTRACT`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-extract)
    * [`HOUR_OF_DAY/HOUR`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-hour)
    * [`ISO_DAY_OF_WEEK/ISODAYOFWEEK/ISODOW/IDOW`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-isodow)
    * [`ISO_WEEK_OF_YEAR/ISOWEEKOFYEAR/ISOWEEK/IWOY/IW`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-isoweek)
    * [`MINUTE_OF_DAY`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-minuteofday)
    * [`MINUTE_OF_HOUR/MINUTE`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-minute)
    * [`MONTH_OF_YEAR/MONTH`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-month)
    * [`MONTH_NAME/MONTHNAME`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-monthname)
    * [`NOW`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-now)
    * [`SECOND_OF_MINUTE/SECOND`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-second)
    * [`QUARTER`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-quarter)
    * [`TIME_PARSE`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-timeparse)
    * [`TO_CHAR`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-to_char)
    * [`TODAY`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-today)
    * [`WEEK_OF_YEAR/WEEK`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-week)
    * [`YEAR`](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-year)

* [Full-Text Search Functions](/reference/query-languages/sql/sql-functions-search.md)

    * [`MATCH`](/reference/query-languages/sql/sql-functions-search.md#sql-functions-search-match)
    * [`QUERY`](/reference/query-languages/sql/sql-functions-search.md#sql-functions-search-query)
    * [`SCORE`](/reference/query-languages/sql/sql-functions-search.md#sql-functions-search-score)

* [Mathematical Functions](/reference/query-languages/sql/sql-functions-math.md)

    * [`ABS`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-abs)
    * [`CBRT`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-cbrt)
    * [`CEIL/CEILING`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-ceil)
    * [`E`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-e)
    * [`EXP`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-exp)
    * [`EXPM1`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-expm1)
    * [`FLOOR`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-floor)
    * [`LOG`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-log)
    * [`LOG10`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-log10)
    * [`PI`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-pi)
    * [`POWER`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-power)
    * [`RANDOM/RAND`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-random)
    * [`ROUND`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-round)
    * [`SIGN/SIGNUM`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-sign)
    * [`SQRT`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-sqrt)
    * [`TRUNCATE/TRUNC`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-truncate)
    * [`ACOS`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-acos)
    * [`ASIN`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-asin)
    * [`ATAN`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-atan)
    * [`ATAN2`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-atan2)
    * [`COS`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-cos)
    * [`COSH`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-cosh)
    * [`COT`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-cot)
    * [`DEGREES`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-degrees)
    * [`RADIANS`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-radians)
    * [`SIN`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-sin)
    * [`SINH`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-sinh)
    * [`TAN`](/reference/query-languages/sql/sql-functions-math.md#sql-functions-math-tan)

* [String Functions](/reference/query-languages/sql/sql-functions-string.md)

    * [`ASCII`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-ascii)
    * [`BIT_LENGTH`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-bit-length)
    * [`CHAR`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-char)
    * [`CHAR_LENGTH`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-char-length)
    * [`CONCAT`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-concat)
    * [`INSERT`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-insert)
    * [`LCASE`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-lcase)
    * [`LEFT`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-left)
    * [`LENGTH`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-length)
    * [`LOCATE`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-locate)
    * [`LTRIM`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-ltrim)
    * [`OCTET_LENGTH`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-octet-length)
    * [`POSITION`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-position)
    * [`REPEAT`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-repeat)
    * [`REPLACE`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-replace)
    * [`RIGHT`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-right)
    * [`RTRIM`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-rtrim)
    * [`SPACE`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-space)
    * [`SUBSTRING`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-substring)
    * [`TRIM`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-trim)
    * [`UCASE`](/reference/query-languages/sql/sql-functions-string.md#sql-functions-string-ucase)

* [Type Conversion Functions](/reference/query-languages/sql/sql-functions-type-conversion.md)

    * [`CAST`](/reference/query-languages/sql/sql-functions-type-conversion.md#sql-functions-type-conversion-cast)
    * [`CONVERT`](/reference/query-languages/sql/sql-functions-type-conversion.md#sql-functions-type-conversion-convert)

* [Conditional Functions And Expressions](/reference/query-languages/sql/sql-functions-conditional.md)

    * [`CASE`](/reference/query-languages/sql/sql-functions-conditional.md#sql-functions-conditional-case)
    * [`COALESCE`](/reference/query-languages/sql/sql-functions-conditional.md#sql-functions-conditional-coalesce)
    * [`GREATEST`](/reference/query-languages/sql/sql-functions-conditional.md#sql-functions-conditional-greatest)
    * [`IFNULL`](/reference/query-languages/sql/sql-functions-conditional.md#sql-functions-conditional-ifnull)
    * [`IIF`](/reference/query-languages/sql/sql-functions-conditional.md#sql-functions-conditional-iif)
    * [`ISNULL`](/reference/query-languages/sql/sql-functions-conditional.md#sql-functions-conditional-isnull)
    * [`LEAST`](/reference/query-languages/sql/sql-functions-conditional.md#sql-functions-conditional-least)
    * [`NULLIF`](/reference/query-languages/sql/sql-functions-conditional.md#sql-functions-conditional-nullif)
    * [`NVL`](/reference/query-languages/sql/sql-functions-conditional.md#sql-functions-conditional-nvl)

* [Geo Functions](/reference/query-languages/sql/sql-functions-geo.md)

    * [`ST_AsWKT`](/reference/query-languages/sql/sql-functions-geo.md#sql-functions-geo-st-as-wkt)
    * [`ST_Distance`](/reference/query-languages/sql/sql-functions-geo.md#sql-functions-geo-st-distance)
    * [`ST_GeometryType`](/reference/query-languages/sql/sql-functions-geo.md#sql-functions-geo-st-geometrytype)
    * [`ST_WKTToSQL`](/reference/query-languages/sql/sql-functions-geo.md#sql-functions-geo-st-wkt-to-sql)
    * [`ST_X`](/reference/query-languages/sql/sql-functions-geo.md#sql-functions-geo-st-x)
    * [`ST_Y`](/reference/query-languages/sql/sql-functions-geo.md#sql-functions-geo-st-y)
    * [`ST_Z`](/reference/query-languages/sql/sql-functions-geo.md#sql-functions-geo-st-z)

* [System Functions](/reference/query-languages/sql/sql-functions-system.md)

    * [`DATABASE`](/reference/query-languages/sql/sql-functions-system.md#sql-functions-system-database)
    * [`USER`](/reference/query-languages/sql/sql-functions-system.md#sql-functions-system-user)

















