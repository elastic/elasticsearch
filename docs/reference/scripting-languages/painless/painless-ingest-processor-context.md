---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-ingest-processor-context.html
---

# Ingest processor context [painless-ingest-processor-context]

Use a Painless script in an [ingest processor](/reference/enrich-processor/script-processor.md) to modify documents upon insertion.

**Variables**

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

[`ctx['_index']`](/reference/elasticsearch/mapping-reference/mapping-index-field.md) (`String`)
:   The name of the index.

`ctx` (`Map`)
:   Contains extracted JSON in a `Map` and `List` structure for the fields that are part of the document.

**Side Effects**

[`ctx['_index']`](/reference/elasticsearch/mapping-reference/mapping-index-field.md)
:   Modify this to change the destination index for the current document.

`ctx` (`Map`)
:   Modify the values in the `Map/List` structure to add, modify, or delete the fields of a document.

**Return**

void
:   No expected return value.

**API**

Both the standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) and [Specialized Ingest API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-ingest.html) are available.

**Example**

To run this example, first follow the steps in [context examples](/reference/scripting-languages/painless/painless-context-examples.md).

The seat data contains:

* A date in the format `YYYY-MM-DD` where the second digit of both month and day is optional.
* A time in the format HH:MM* where the second digit of both hours and minutes is optional. The star (*) represents either the `String` `AM` or `PM`.

The following ingest script processes the date and time `Strings` and stores the result in a `datetime` field.

```painless
String[] dateSplit = ctx.date.splitOnToken("-");                     <1>
String year = dateSplit[0].trim();
String month = dateSplit[1].trim();

if (month.length() == 1) {                                           <2>
    month = "0" + month;
}

String day = dateSplit[2].trim();

if (day.length() == 1) {                                             <3>
    day = "0" + day;
}

boolean pm = ctx.time.substring(ctx.time.length() - 2).equals("PM"); <4>
String[] timeSplit = ctx.time.substring(0,
        ctx.time.length() - 2).splitOnToken(":");                    <5>
int hours = Integer.parseInt(timeSplit[0].trim());
int minutes = Integer.parseInt(timeSplit[1].trim());

if (pm) {                                                            <6>
    hours += 12;
}

String dts = year + "-" + month + "-" + day + "T" +
        (hours < 10 ? "0" + hours : "" + hours) + ":" +
        (minutes < 10 ? "0" + minutes : "" + minutes) +
        ":00+08:00";                                                 <7>

ZonedDateTime dt = ZonedDateTime.parse(
         dts, DateTimeFormatter.ISO_OFFSET_DATE_TIME);               <8>
ctx.datetime = dt.getLong(ChronoField.INSTANT_SECONDS)*1000L;        <9>
```

1. Uses the `splitOnToken` function to separate the date `String` from the seat data into year, month, and day `Strings`.
   NOTE : The use of the `ctx` ingest processor context variable to retrieve the data from the `date` field.

2. Appends the [string literal](/reference/scripting-languages/painless/painless-literals.md#string-literals) `"0"` value to a single digit month since the format of the seat data allows for this case.
3. Appends the [string literal](/reference/scripting-languages/painless/painless-literals.md#string-literals) `"0"` value to a single digit day since the format of the seat data allows for this case.
4. Sets the [`boolean type`](/reference/scripting-languages/painless/painless-types.md#primitive-types) [variable](/reference/scripting-languages/painless/painless-variables.md) to `true` if the time `String` is a time in the afternoon or evening.
   NOTE: The use of the `ctx` ingest processor context variable to retrieve the data from the `time` field.

5. Uses the `splitOnToken` function to separate the time `String` from the seat data into hours and minutes `Strings`.
   NOTE: The use of the `substring` method to remove the `AM` or `PM` portion of the time `String`. The use of the `ctx` ingest processor context variable to retrieve the data from the `date` field.

6. If the time `String` is an afternoon or evening value adds the [integer literal](/reference/scripting-languages/painless/painless-literals.md#integer-literals) `12` to the existing hours to move to a 24-hour based time.
7. Builds a new time `String` that is parsable using existing API methods.
8. Creates a `ZonedDateTime` [reference type](/reference/scripting-languages/painless/painless-types.md#reference-types) value by using the API method `parse` to parse the new time `String`.
9. Sets the datetime field `datetime` to the number of milliseconds retrieved from the API method `getLong`.
   NOTEL The use of the `ctx` ingest processor context variable to set the field `datetime`. Manipulate each document’s fields with the `ctx` variable as each document is indexed.




Submit the following request:

```console
PUT /_ingest/pipeline/seats
{
  "description": "update datetime for seats",
  "processors": [
    {
      "script": {
        "source": "String[] dateSplit = ctx.date.splitOnToken('-'); String year = dateSplit[0].trim(); String month = dateSplit[1].trim(); if (month.length() == 1) { month = '0' + month; } String day = dateSplit[2].trim(); if (day.length() == 1) { day = '0' + day; } boolean pm = ctx.time.substring(ctx.time.length() - 2).equals('PM'); String[] timeSplit = ctx.time.substring(0, ctx.time.length() - 2).splitOnToken(':'); int hours = Integer.parseInt(timeSplit[0].trim()); int minutes = Integer.parseInt(timeSplit[1].trim()); if (pm) { hours += 12; } String dts = year + '-' + month + '-' + day + 'T' + (hours < 10 ? '0' + hours : '' + hours) + ':' + (minutes < 10 ? '0' + minutes : '' + minutes) + ':00+08:00'; ZonedDateTime dt = ZonedDateTime.parse(dts, DateTimeFormatter.ISO_OFFSET_DATE_TIME); ctx.datetime = dt.getLong(ChronoField.INSTANT_SECONDS)*1000L;"
      }
    }
  ]
}
```

