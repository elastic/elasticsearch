---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-pagination.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Paginating through a large response [sql-pagination]

Using the example from the [previous section](sql-rest-format.md), one can continue to the next page by sending back the cursor field. In the case of CSV, TSV and TXT formats, the cursor is returned in the `Cursor` HTTP header.

```console
POST /_sql?format=json
{
  "cursor": "sDXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAEWYUpOYklQMHhRUEtld3RsNnFtYU1hQQ==:BAFmBGRhdGUBZgVsaWtlcwFzB21lc3NhZ2UBZgR1c2Vy9f///w8="
}
```
% TEST[setup:library]
% TEST[s/sDXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAEWYUpOYklQMHhRUEtld3RsNnFtYU1hQQ==:BAFmBGRhdGUBZgVsaWtlcwFzB21lc3NhZ2UBZgR1c2Vy9f\/\/\/w8=/$body.cursor/]

Which looks like:

```console-result
{
  "rows" : [
    ["Dan Simmons",        "Hyperion",             482,  "1989-05-26T00:00:00.000Z"],
    ["Iain M. Banks",      "Consider Phlebas",     471,  "1987-04-23T00:00:00.000Z"],
    ["Neal Stephenson",    "Snow Crash",           470,  "1992-06-01T00:00:00.000Z"],
    ["Frank Herbert",      "God Emperor of Dune",  454,  "1981-05-28T00:00:00.000Z"],
    ["Frank Herbert",      "Children of Dune",     408,  "1976-04-21T00:00:00.000Z"]
  ],
  "cursor" : "sDXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAEWODRMaXBUaVlRN21iTlRyWHZWYUdrdw==:BAFmBmF1dGhvcgFmBG5hbWUBZgpwYWdlX2NvdW50AWYMcmVsZWFzZV9kYXRl9f///w8="
}
```
% TESTRESPONSE[s/sDXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAEWODRMaXBUaVlRN21iTlRyWHZWYUdrdw==:BAFmBmF1dGhvcgFmBG5hbWUBZgpwYWdlX2NvdW50AWYMcmVsZWFzZV9kYXRl9f\/\/\/w8=/$body.cursor/]

Note that the `columns` object is only part of the first page.

You’ve reached the last page when there is no `cursor` returned in the results. Like Elasticsearch’s [scroll](elasticsearch://reference/elasticsearch/rest-apis/paginate-search-results.md#scroll-search-results), SQL may keep state in Elasticsearch to support the cursor. Unlike scroll, receiving the last page is enough to guarantee that the Elasticsearch state is cleared.

To clear the state earlier, use the [clear cursor API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-clear-cursor):

```console
POST /_sql/close
{
  "cursor": "sDXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAEWYUpOYklQMHhRUEtld3RsNnFtYU1hQQ==:BAFmBGRhdGUBZgVsaWtlcwFzB21lc3NhZ2UBZgR1c2Vy9f///w8="
}
```
% TEST[continued]
% TEST[s/sDXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAEWYUpOYklQMHhRUEtld3RsNnFtYU1hQQ==:BAFmBGRhdGUBZgVsaWtlcwFzB21lc3NhZ2UBZgR1c2Vy9f\/\/\/w8=/$body.cursor/]

Which will like return the

```console-result
{
  "succeeded" : true
}
```

