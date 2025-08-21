---
navigation_title: "Terms set"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-set-query.html
---

# Terms set query [query-dsl-terms-set-query]


Returns documents that contain a minimum number of **exact** terms in a provided field.

The `terms_set` query is the same as the [`terms` query](/reference/query-languages/query-dsl/query-dsl-terms-query.md), except you can define the number of matching terms required to return a document. For example:

* A field, `programming_languages`, contains a list of known programming languages, such as `c++`, `java`, or `php` for job candidates. You can use the `terms_set` query to return documents that match at least two of these languages.
* A field, `permissions`, contains a list of possible user permissions for an application. You can use the `terms_set` query to return documents that match a subset of these permissions.

## Example request [terms-set-query-ex-request]

### Index setup [terms-set-query-ex-request-index-setup]

In most cases, you’ll need to include a [numeric](/reference/elasticsearch/mapping-reference/number.md) field mapping in your index to use the `terms_set` query. This numeric field contains the number of matching terms required to return a document.

To see how you can set up an index for the `terms_set` query, try the following example.

1. Create an index, `job-candidates`, with the following field mappings:

    * `name`, a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) field. This field contains the name of the job candidate.
    * `programming_languages`, a [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) field. This field contains programming languages known by the job candidate.
    * `required_matches`, a [numeric](/reference/elasticsearch/mapping-reference/number.md) `long` field. This field contains the number of matching terms required to return a document.

    ```console
    PUT /job-candidates
    {
      "mappings": {
        "properties": {
          "name": {
            "type": "keyword"
          },
          "programming_languages": {
            "type": "keyword"
          },
          "required_matches": {
            "type": "long"
          }
        }
      }
    }
    ```

2. Index a document with an ID of `1` and the following values:

    * `Jane Smith` in the `name` field.
    * `["c++", "java"]` in the `programming_languages` field.
    * `2` in the `required_matches` field.

    Include the `?refresh` parameter so the document is immediately available for search.

    ```console
    PUT /job-candidates/_doc/1?refresh
    {
      "name": "Jane Smith",
      "programming_languages": [ "c++", "java" ],
      "required_matches": 2
    }
    ```

3. Index another document with an ID of `2` and the following values:

    * `Jason Response` in the `name` field.
    * `["java", "php"]` in the `programming_languages` field.
    * `2` in the `required_matches` field.

    ```console
    PUT /job-candidates/_doc/2?refresh
    {
      "name": "Jason Response",
      "programming_languages": [ "java", "php" ],
      "required_matches": 2
    }
    ```


You can now use the `required_matches` field value as the number of matching terms required to return a document in the `terms_set` query.


### Example query [terms-set-query-ex-request-query]

The following search returns documents where the `programming_languages` field contains at least two of the following terms:

* `c++`
* `java`
* `php`

The `minimum_should_match_field` is `required_matches`. This means the number of matching terms required is `2`, the value of the `required_matches` field.

```console
GET /job-candidates/_search
{
  "query": {
    "terms_set": {
      "programming_languages": {
        "terms": [ "c++", "java", "php" ],
        "minimum_should_match_field": "required_matches"
      }
    }
  }
}
```



## Top-level parameters for `terms_set` [terms-set-top-level-params]

`<field>`
:   (Required, object) Field you wish to search.


## Parameters for `<field>` [terms-set-field-params]

`terms`
:   (Required, array) Array of terms you wish to find in the provided `<field>`. To return a document, a required number of terms must exactly match the field values, including whitespace and capitalization.

The required number of matching terms is defined in the `minimum_should_match`, `minimum_should_match_field` or `minimum_should_match_script` parameters. Exactly one of these parameters must be provided.


`minimum_should_match`
:   (Optional) Specification for the number of matching terms required to return a document.

For valid values, see [`minimum_should_match` parameter](/reference/query-languages/query-dsl/query-dsl-minimum-should-match.md).


`minimum_should_match_field`
:   (Optional, string) [Numeric](/reference/elasticsearch/mapping-reference/number.md) field containing the number of matching terms required to return a document.

`minimum_should_match_script`
:   (Optional, string) Custom script containing the number of matching terms required to return a document.

For parameters and valid values, see [Scripting](docs-content://explore-analyze/scripting.md).

For an example query using the `minimum_should_match_script` parameter, see [How to use the `minimum_should_match_script` parameter](#terms-set-query-script).



## Notes [terms-set-query-notes]

### How to use the `minimum_should_match_script` parameter [terms-set-query-script]

You can use `minimum_should_match_script` to define the required number of matching terms using a script. This is useful if you need to set the number of required terms dynamically.

#### Example query using `minimum_should_match_script` [terms-set-query-script-ex]

The following search returns documents where the `programming_languages` field contains at least two of the following terms:

* `c++`
* `java`
* `php`

The `source` parameter of this query indicates:

* The required number of terms to match cannot exceed `params.num_terms`, the number of terms provided in the `terms` field.
* The required number of terms to match is `2`, the value of the `required_matches` field.

```console
GET /job-candidates/_search
{
  "query": {
    "terms_set": {
      "programming_languages": {
        "terms": [ "c++", "java", "php" ],
        "minimum_should_match_script": {
          "source": "Math.min(params.num_terms, doc['required_matches'].value)"
        },
        "boost": 1.0
      }
    }
  }
}
```




