---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-rest-format.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Response data formats [sql-rest-format]

While the textual format is nice for humans, computers prefer something more structured.

Elasticsearch SQL can return the data in the following formats which can be set either through the `format` property in the URL or by setting the `Accept` HTTP header:

::::{note}
The URL parameter takes precedence over the `Accept` HTTP header. If neither is specified then the response is returned in the same format as the request.
::::


| format | `Accept` HTTP header | Description |
| --- | --- | --- |
| Human Readable |
| `csv` | `text/csv` | [Comma-separated values](https://en.wikipedia.org/wiki/Comma-separated_values) |
| `json` | `application/json` | [JSON](https://www.json.org/) (JavaScript Object Notation) human-readable format |
| `tsv` | `text/tab-separated-values` | [Tab-separated values](https://en.wikipedia.org/wiki/Tab-separated_values) |
| `txt` | `text/plain` | CLI-like representation |
| `yaml` | `application/yaml` | [YAML](https://en.wikipedia.org/wiki/YAML) (YAML Ain’t Markup Language) human-readable format |
| Binary Formats |
| `cbor` | `application/cbor` | [Concise Binary Object Representation](https://cbor.io/) |
| `smile` | `application/smile` | [Smile](https://en.wikipedia.org/wiki/Smile_(data_interchange_format)) binary data format similar to CBOR |

The `CSV` format accepts a formatting URL query attribute, `delimiter`, which indicates which character should be used to separate the CSV values. It defaults to comma (`,`) and cannot take any of the following values: double quote (`"`), carriage-return (`\r`) and new-line (`\n`). The tab (`\t`) can also not be used, the `tsv` format needs to be used instead.

Here are some examples for the human readable formats:

## CSV [_csv]

```console
POST /_sql?format=csv
{
  "query": "SELECT * FROM library ORDER BY page_count DESC",
  "fetch_size": 5
}
```
% TEST[setup:library]

which returns:

```text
author,name,page_count,release_date
Peter F. Hamilton,Pandora's Star,768,2004-03-02T00:00:00.000Z
Vernor Vinge,A Fire Upon the Deep,613,1992-06-01T00:00:00.000Z
Frank Herbert,Dune,604,1965-06-01T00:00:00.000Z
Alastair Reynolds,Revelation Space,585,2000-03-15T00:00:00.000Z
James S.A. Corey,Leviathan Wakes,561,2011-06-02T00:00:00.000Z
```
% TESTRESPONSE[non_json]

or:

```console
POST /_sql?format=csv&delimiter=%3b
{
    "query": "SELECT * FROM library ORDER BY page_count DESC",
    "fetch_size": 5
}
```
% TEST[setup:library]

which returns:

```text
author;name;page_count;release_date
Peter F. Hamilton;Pandora's Star;768;2004-03-02T00:00:00.000Z
Vernor Vinge;A Fire Upon the Deep;613;1992-06-01T00:00:00.000Z
Frank Herbert;Dune;604;1965-06-01T00:00:00.000Z
Alastair Reynolds;Revelation Space;585;2000-03-15T00:00:00.000Z
James S.A. Corey;Leviathan Wakes;561;2011-06-02T00:00:00.000Z
```
% TESTRESPONSE[non_json]

## JSON [_json]

```console
POST /_sql?format=json
{
  "query": "SELECT * FROM library ORDER BY page_count DESC",
  "fetch_size": 5
}
```
% TEST[setup:library]

Which returns:

```console-result
{
  "columns": [
    {"name": "author",       "type": "text"},
    {"name": "name",         "type": "text"},
    {"name": "page_count",   "type": "short"},
    {"name": "release_date", "type": "datetime"}
  ],
  "rows": [
    ["Peter F. Hamilton",  "Pandora's Star",       768, "2004-03-02T00:00:00.000Z"],
    ["Vernor Vinge",       "A Fire Upon the Deep", 613, "1992-06-01T00:00:00.000Z"],
    ["Frank Herbert",      "Dune",                 604, "1965-06-01T00:00:00.000Z"],
    ["Alastair Reynolds",  "Revelation Space",     585, "2000-03-15T00:00:00.000Z"],
    ["James S.A. Corey",   "Leviathan Wakes",      561, "2011-06-02T00:00:00.000Z"]
  ],
  "cursor": "sDXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAEWWWdrRlVfSS1TbDYtcW9lc1FJNmlYdw==:BAFmBmF1dGhvcgFmBG5hbWUBZgpwYWdlX2NvdW50AWYMcmVsZWFzZV9kYXRl+v///w8="
}
```
% TESTRESPONSE[s/sDXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAEWWWdrRlVfSS1TbDYtcW9lc1FJNmlYdw==:BAFmBmF1dGhvcgFmBG5hbWUBZgpwYWdlX2NvdW50AWYMcmVsZWFzZV9kYXRl\+v\/\/\/w8=/$body.cursor/]

## TSV [_tsv]

```console
POST /_sql?format=tsv
{
  "query": "SELECT * FROM library ORDER BY page_count DESC",
  "fetch_size": 5
}
```
% TEST[setup:library]

Which returns:

```text
author	name	page_count	release_date
Peter F. Hamilton	Pandora's Star	768	2004-03-02T00:00:00.000Z
Vernor Vinge	A Fire Upon the Deep	613	1992-06-01T00:00:00.000Z
Frank Herbert	Dune	604	1965-06-01T00:00:00.000Z
Alastair Reynolds	Revelation Space	585	2000-03-15T00:00:00.000Z
James S.A. Corey	Leviathan Wakes	561	2011-06-02T00:00:00.000Z
```
% TESTRESPONSE[s/\t/ /]
% TESTRESPONSE[non_json]

## TXT [_txt]

```console
POST /_sql?format=txt
{
  "query": "SELECT * FROM library ORDER BY page_count DESC",
  "fetch_size": 5
}
```
% TEST[setup:library]

Which returns:

```text
     author      |        name        |  page_count   |      release_date
-----------------+--------------------+---------------+------------------------
Peter F. Hamilton|Pandora's Star      |768            |2004-03-02T00:00:00.000Z
Vernor Vinge     |A Fire Upon the Deep|613            |1992-06-01T00:00:00.000Z
Frank Herbert    |Dune                |604            |1965-06-01T00:00:00.000Z
Alastair Reynolds|Revelation Space    |585            |2000-03-15T00:00:00.000Z
James S.A. Corey |Leviathan Wakes     |561            |2011-06-02T00:00:00.000Z
```
% TESTRESPONSE[s/\|/\\|/ s/\+/\\+/]
% TESTRESPONSE[non_json]

## YAML [_yaml]

```console
POST /_sql?format=yaml
{
  "query": "SELECT * FROM library ORDER BY page_count DESC",
  "fetch_size": 5
}
```
% TEST[setup:library]

Which returns:

```yaml
columns:
- name: "author"
  type: "text"
- name: "name"
  type: "text"
- name: "page_count"
  type: "short"
- name: "release_date"
  type: "datetime"
rows:
- - "Peter F. Hamilton"
  - "Pandora's Star"
  - 768
  - "2004-03-02T00:00:00.000Z"
- - "Vernor Vinge"
  - "A Fire Upon the Deep"
  - 613
  - "1992-06-01T00:00:00.000Z"
- - "Frank Herbert"
  - "Dune"
  - 604
  - "1965-06-01T00:00:00.000Z"
- - "Alastair Reynolds"
  - "Revelation Space"
  - 585
  - "2000-03-15T00:00:00.000Z"
- - "James S.A. Corey"
  - "Leviathan Wakes"
  - 561
  - "2011-06-02T00:00:00.000Z"
cursor: "sDXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAEWWWdrRlVfSS1TbDYtcW9lc1FJNmlYdw==:BAFmBmF1dGhvcgFmBG5hbWUBZgpwYWdlX2NvdW50AWYMcmVsZWFzZV9kYXRl+v///w8="
```
% TESTRESPONSE[s/sDXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAEWWWdrRlVfSS1TbDYtcW9lc1FJNmlYdw==:BAFmBmF1dGhvcgFmBG5hbWUBZgpwYWdlX2NvdW50AWYMcmVsZWFzZV9kYXRl\+v\/\/\/w8=/$body.cursor/]

