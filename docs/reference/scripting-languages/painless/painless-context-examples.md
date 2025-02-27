---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-context-examples.html
---

# Context example data [painless-context-examples]

Complete the following steps to index the `seat` sample data into {{es}}. You can run any of the context examples against this sample data after you configure it.

Each document in the `seat` data contains the following fields:

`theatre` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md))
:   The name of the theater the play is in.

`play` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md))
:   The name of the play.

`actors` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md))
:   A list of actors in the play.

`date` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md))
:   The date of the play as a keyword.

`time` ([`keyword`](/reference/elasticsearch/mapping-reference/keyword.md))
:   The time of the play as a keyword.

`cost` ([`long`](/reference/elasticsearch/mapping-reference/number.md))
:   The cost of the ticket for the seat.

`row` ([`long`](/reference/elasticsearch/mapping-reference/number.md))
:   The row of the seat.

`number` ([`long`](/reference/elasticsearch/mapping-reference/number.md))
:   The number of the seat within a row.

`sold` ([`boolean`](/reference/elasticsearch/mapping-reference/boolean.md))
:   Whether or not the seat is sold.

`datetime` ([`date`](/reference/elasticsearch/mapping-reference/date.md))
:   The date and time of the play as a date object.

## Prerequisites [_prerequisites]

Start an [{{es}} instance](docs-content://deploy-manage/deploy/self-managed/installing-elasticsearch.md), and then access the [Console](docs-content://explore-analyze/query-filter/tools/console.md) in {{kib}}.


## Configure the `seat` sample data [_configure_the_seat_sample_data]

1. From the {{kib}} Console, create [mappings](docs-content://manage-data/data-store/mapping.md) for the sample data:

    ```console
    PUT /seats
    {
      "mappings": {
        "properties": {
          "theatre":  { "type": "keyword" },
          "play":     { "type": "keyword" },
          "actors":   { "type": "keyword" },
          "date":     { "type": "keyword" },
          "time":     { "type": "keyword" },
          "cost":     { "type": "double"  },
          "row":      { "type": "integer" },
          "number":   { "type": "integer" },
          "sold":     { "type": "boolean" },
          "datetime": { "type": "date"    }
        }
      }
    }
    ```

2. Configure a script ingest processor that parses each document as {{es}} ingests the `seat` data. The following ingest script processes the `date` and `time` fields and stores the result in a `datetime` field:

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

3. Ingest some sample data using the `seats` ingest pipeline that you defined in the previous step.

    ```console
    POST seats/_bulk?pipeline=seats&refresh=true
    {"create":{"_index":"seats","_id":"1"}}
    {"theatre":"Skyline","play":"Rent","actors":["James Holland","Krissy Smith","Joe Muir","Ryan Earns"],"date":"2021-4-1","time":"3:00PM","cost":37,"row":1,"number":7,"sold":false}
    {"create":{"_index":"seats","_id":"2"}}
    {"theatre":"Graye","play":"Rent","actors":["Dave Christmas"],"date":"2021-4-1","time":"3:00PM","cost":30,"row":3,"number":5,"sold":false}
    {"create":{"_index":"seats","_id":"3"}}
    {"theatre":"Graye","play":"Rented","actors":["Dave Christmas"],"date":"2021-4-1","time":"3:00PM","cost":33,"row":2,"number":6,"sold":false}
    {"create":{"_index":"seats","_id":"4"}}
    {"theatre":"Skyline","play":"Rented","actors":["James Holland","Krissy Smith","Joe Muir","Ryan Earns"],"date":"2021-4-1","time":"3:00PM","cost":20,"row":5,"number":2,"sold":false}
    {"create":{"_index":"seats","_id":"5"}}
    {"theatre":"Down Port","play":"Pick It Up","actors":["Joel Madigan","Jessica Brown","Baz Knight","Jo Hangum","Rachel Grass","Phoebe Miller"],"date":"2018-4-2","time":"8:00PM","cost":27.5,"row":3,"number":2,"sold":false}
    {"create":{"_index":"seats","_id":"6"}}
    {"theatre":"Down Port","play":"Harriot","actors":["Phoebe Miller","Sarah Notch","Brayden Green","Joshua Iller","Jon Hittle","Rob Kettleman","Laura Conrad","Simon Hower","Nora Blue","Mike Candlestick","Jacey Bell"],"date":"2018-8-7","time":"8:00PM","cost":30,"row":1,"number":10,"sold":false}
    {"create":{"_index":"seats","_id":"7"}}
    {"theatre":"Skyline","play":"Auntie Jo","actors":["Jo Hangum","Jon Hittle","Rob Kettleman","Laura Conrad","Simon Hower","Nora Blue"],"date":"2018-10-2","time":"5:40PM","cost":22.5,"row":7,"number":10,"sold":false}
    {"create":{"_index":"seats","_id":"8"}}
    {"theatre":"Skyline","play":"Test Run","actors":["Joe Muir","Ryan Earns","Joel Madigan","Jessica Brown"],"date":"2018-8-5","time":"7:30PM","cost":17.5,"row":11,"number":12,"sold":true}
    {"create":{"_index":"seats","_id":"9"}}
    {"theatre":"Skyline","play":"Sunnyside Down","actors":["Krissy Smith","Joe Muir","Ryan Earns","Nora Blue","Mike Candlestick","Jacey Bell"],"date":"2018-6-12","time":"4:00PM","cost":21.25,"row":8,"number":15,"sold":true}
    {"create":{"_index":"seats","_id":"10"}}
    {"theatre":"Graye","play":"Line and Single","actors":["Nora Blue","Mike Candlestick"],"date":"2018-6-5","time":"2:00PM","cost":30,"row":1,"number":2,"sold":false}
    {"create":{"_index":"seats","_id":"11"}}
    {"theatre":"Graye","play":"Hamilton","actors":["Lin-Manuel Miranda","Leslie Odom Jr."],"date":"2018-6-5","time":"2:00PM","cost":5000,"row":1,"number":20,"sold":true}
    ```



