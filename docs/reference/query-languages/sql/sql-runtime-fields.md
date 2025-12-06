---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-runtime-fields.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Use runtime fields [sql-runtime-fields]

Use the `runtime_mappings` parameter to extract and create [runtime fields](docs-content://manage-data/data-store/mapping/runtime-fields.md), or columns, from existing ones during a search.

The following search creates a `release_day_of_week` runtime field from `release_date` and returns it in the response.

```console
POST _sql?format=txt
{
  "runtime_mappings": {
    "release_day_of_week": {
      "type": "keyword",
      "script": """
        emit(doc['release_date'].value.dayOfWeekEnum.toString())
      """
    }
  },
  "query": """
    SELECT * FROM library WHERE page_count > 300 AND author = 'Frank Herbert'
  """
}
```
% TEST[setup:library]

The API returns:

```txt
    author     |     name      |  page_count   |      release_date      |release_day_of_week
---------------+---------------+---------------+------------------------+-------------------
Frank Herbert  |Dune           |604            |1965-06-01T00:00:00.000Z|TUESDAY
```
% TESTRESPONSE[non_json]
