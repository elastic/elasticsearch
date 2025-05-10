---
navigation_title: "Script"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-script-query.html
---

# Script query [query-dsl-script-query]


::::{note}
[Runtime fields](docs-content://manage-data/data-store/mapping/runtime-fields.md) provide a very similar feature that is more flexible. You write a script to create field values and they are available everywhere, such as [`fields`](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md), [all queries](/reference/query-languages/querydsl.md), and [aggregations](/reference/aggregations/index.md).
::::


Filters documents based on a provided [script](docs-content://explore-analyze/scripting/modules-scripting-using.md). The `script` query is typically used in a [filter context](/reference/query-languages/query-dsl/query-filter-context.md).

::::{warning}
Using scripts can result in slower search speeds. See [Scripts, caching, and search speed](docs-content://explore-analyze/scripting/scripts-search-speed.md).
::::


## Example request [script-query-ex-request]

```console
GET /_search
{
  "query": {
    "bool": {
      "filter": {
        "script": {
          "script": """
            double amount = doc['amount'].value;
            if (doc['type'].value == 'expense') {
              amount *= -1;
            }
            return amount < 10;
          """
        }
      }
    }
  }
}
```
% TEST[setup:ledger]
% TEST[s/_search/_search\?filter_path=hits.hits&sort=amount/]

You can achieve the same results in a search query by using runtime fields. Use the [`fields`](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md) parameter on the `_search` API to fetch values as part of the same query:

```console
GET /_search
{
  "runtime_mappings": {
    "amount.signed": {
      "type": "double",
      "script": """
        double amount = doc['amount'].value;
        if (doc['type'].value == 'expense') {
          amount *= -1;
        }
        emit(amount);
      """
    }
  },
  "query": {
    "bool": {
      "filter": {
        "range": {
          "amount.signed": { "lt": 10 }
        }
      }
    }
  },
  "fields": [{"field": "amount.signed"}]
}
```
% TEST[setup:ledger]
% TEST[s/_search/_search\?filter_path=hits.hits.fields&sort=amount.signed:desc/]


## Top-level parameters for `script` [script-top-level-params]

`script`
:   (Required, [script object](docs-content://explore-analyze/scripting/modules-scripting-using.md)) Contains a script to run as a query. This script must return a boolean value, `true` or `false`.


## Notes [script-query-notes]

### Custom parameters [script-query-custom-params]

Like [filters](/reference/query-languages/query-dsl/query-filter-context.md), scripts are cached for faster execution. If you frequently change the arguments of a script, we recommend you store them in the script’s `params` parameter. For example:

```console
GET /_search
{
  "query": {
    "bool": {
      "filter": {
        "script": {
          "script": {
            "source": "doc['num1'].value > params.param1",
            "lang": "painless",
            "params": {
              "param1": 5
            }
          }
        }
      }
    }
  }
}
```


### Allow expensive queries [_allow_expensive_queries_4]

Script queries will not be executed if [`search.allow_expensive_queries`](/reference/query-languages/querydsl.md#query-dsl-allow-expensive-queries) is set to false.



