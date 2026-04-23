---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-rest-columnar.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Columnar results [sql-rest-columnar]

The most well known way of displaying the results of an SQL query result in general is the one where each individual record/document represents one line/row. For certain formats, Elasticsearch SQL can return the results in a columnar fashion: one row represents all the values of a certain column from the current page of results.

The following formats can be returned in columnar orientation: `json`, `yaml`, `cbor` and `smile`.

```console
POST /_sql?format=json
{
  "query": "SELECT * FROM library ORDER BY page_count DESC",
  "fetch_size": 5,
  "columnar": true
}
```
% TEST[setup:library]

Which returns:

```console-result
{
  "columns": [
    {"name": "author", "type": "text"},
    {"name": "name", "type": "text"},
    {"name": "page_count", "type": "short"},
    {"name": "release_date", "type": "datetime"}
  ],
  "values": [
    ["Peter F. Hamilton", "Vernor Vinge", "Frank Herbert", "Alastair Reynolds", "James S.A. Corey"],
    ["Pandora's Star", "A Fire Upon the Deep", "Dune", "Revelation Space", "Leviathan Wakes"],
    [768, 613, 604, 585, 561],
    ["2004-03-02T00:00:00.000Z", "1992-06-01T00:00:00.000Z", "1965-06-01T00:00:00.000Z", "2000-03-15T00:00:00.000Z", "2011-06-02T00:00:00.000Z"]
  ],
  "cursor": "sDXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAEWWWdrRlVfSS1TbDYtcW9lc1FJNmlYdw==:BAFmBmF1dGhvcgFmBG5hbWUBZgpwYWdlX2NvdW50AWYMcmVsZWFzZV9kYXRl+v///w8="
}
```
% TESTRESPONSE[s/sDXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAEWWWdrRlVfSS1TbDYtcW9lc1FJNmlYdw==:BAFmBmF1dGhvcgFmBG5hbWUBZgpwYWdlX2NvdW50AWYMcmVsZWFzZV9kYXRl\+v\/\/\/w8=/$body.cursor/]

Any subsequent calls using a `cursor` still have to contain the `columnar` parameter to preserve the orientation, meaning the initial query will not *remember* the columnar option.

```console
POST /_sql?format=json
{
  "cursor": "sDXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAEWWWdrRlVfSS1TbDYtcW9lc1FJNmlYdw==:BAFmBmF1dGhvcgFmBG5hbWUBZgpwYWdlX2NvdW50AWYMcmVsZWFzZV9kYXRl+v///w8=",
  "columnar": true
}
```
% TEST[continued]
% TEST[s/sDXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAEWWWdrRlVfSS1TbDYtcW9lc1FJNmlYdw==:BAFmBmF1dGhvcgFmBG5hbWUBZgpwYWdlX2NvdW50AWYMcmVsZWFzZV9kYXRl\+v\/\/\/w8=/$body.cursor/]

Which looks like:

```console-result
{
  "values": [
    ["Dan Simmons", "Iain M. Banks", "Neal Stephenson", "Frank Herbert", "Frank Herbert"],
    ["Hyperion", "Consider Phlebas", "Snow Crash", "God Emperor of Dune", "Children of Dune"],
    [482, 471, 470, 454, 408],
    ["1989-05-26T00:00:00.000Z", "1987-04-23T00:00:00.000Z", "1992-06-01T00:00:00.000Z", "1981-05-28T00:00:00.000Z", "1976-04-21T00:00:00.000Z"]
  ],
  "cursor": "46ToAwFzQERYRjFaWEo1UVc1a1JtVjBZMmdCQUFBQUFBQUFBQUVXWjBaNlFXbzNOV0pVY21Wa1NUZDJhV2t3V2xwblp3PT3/////DwQBZgZhdXRob3IBBHRleHQAAAFmBG5hbWUBBHRleHQAAAFmCnBhZ2VfY291bnQBBGxvbmcBAAFmDHJlbGVhc2VfZGF0ZQEIZGF0ZXRpbWUBAAEP"
}
```
% TESTRESPONSE[s/46ToAwFzQERYRjFaWEo1UVc1a1JtVjBZMmdCQUFBQUFBQUFBQUVXWjBaNlFXbzNOV0pVY21Wa1NUZDJhV2t3V2xwblp3PT3\/\/\/\/\wQBZgZhdXRob3IBBHRleHQAAAFmBG5hbWUBBHRleHQAAAFmCnBhZ2VfY291bnQBBGxvbmcBAAFmDHJlbGVhc2VfZGF0ZQEIZGF0ZXRpbWUBAAEP/$body.cursor/]
