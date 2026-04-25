---
navigation_title: "Sync rules"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-sync-rules.html
---

# Connector sync rules [es-sync-rules]


Use connector sync rules to help control which documents are synced between the third-party data source and Elasticsearch. Define sync rules in the Kibana UI for each connector index, under the `Sync rules` tab for the index.

## Availability and prerequisites [es-sync-rules-availability-prerequisites]

In Elastic versions **8.8.0 and later** all connectors have support for *basic* sync rules.

Some connectors support *advanced* sync rules. Learn more in the [individual connector’s reference documentation](/reference/search-connectors/index.md).


## Types of sync rule [es-sync-rules-types]

There are two types of sync rule:

* **Basic sync rules** - these rules are represented in a table-like view. Basic sync rules are identical for all connectors.
* **Advanced sync rules** - these rules cover complex query-and-filter scenarios that cannot be expressed with basic sync rules. Advanced sync rules are defined through a *source-specific* DSL JSON snippet.

:::{image} images/filtering-rules-zero-state.png
:alt: Sync rules tab
:class: screenshot
:::


## General data filtering concepts [es-sync-rules-general-filtering]

Before discussing sync rules, it’s important to establish a basic understanding of *data filtering* concepts. The following diagram shows that data filtering can occur in several different processes/locations.

:::{image} images/filtering-general-diagram.png
:alt: Filtering
:class: screenshot
:::

In this documentation we will focus on remote and integration filtering. Sync rules can be used to modify both of these.


### Remote filtering [es-sync-rules-general-filtering-remote]

Data might be filtered at its source. We call this **remote filtering**, as the filtering process is external to Elastic.


### Integration filtering [es-sync-rules-general-filtering-integration]

**Integration filtering** acts as a bridge between the original data source and Elasticsearch. Filtering that takes place in connectors is an example of integration filtering.


### Pipeline filtering [es-sync-rules-general-filtering-pipeline]

Finally, Elasticsearch can filter data right *before persistence* using [ingest pipelines](docs-content://solutions/search/ingest-for-search.md). We will not focus on ingest pipeline filtering in this guide.

::::{note}
Currently, basic sync rules are the only way to control *integration filtering* for connectors. Remember that remote filtering extends far beyond the scope of connectors alone. For best results, collaborate with the owners and maintainers of your data source. Ensure the source data is well-organized and optimized for the query types made by the connectors.

::::



## Sync rules overview [es-sync-rules-overview]

In most cases, your data lake will contain far more data than you want to expose to end users. For example, you may want to search a product catalog, but not include vendor contact information, even if the two are co-located for business purposes.

The optimal time to filter data is *early* in the data pipeline. There are two main reasons:

* **Performance**: It’s more efficient to send a query to the backing data source than to obtain all the data and then filter it in the connector. It’s faster to send a smaller dataset over a network and to process it on the connector side.
* **Security**: Query-time filtering is applied on the data source side, so the data is not sent over the network and into the connector, which limits the exposure of your data.

In a perfect world, all filtering would be done as remote filtering.

In practice, however, this is not always possible. Some sources do not allow robust remote filtering. Others do, but require special setup (building indexes on specific fields, tweaking settings, etc.) that may require attention from other stakeholders in your organization.

With this in mind, sync rules were designed to modify both remote filtering and integration filtering. Your goal should be to do as much remote filtering as possible, but integration is a perfectly viable fall-back. By definition, remote filtering is applied before the data is obtained from a third-party source. Integration filtering is applied after the data is obtained from a third-party source, but before it is ingested into the Elasticsearch index.

::::{note}
All sync rules are applied to a given document *before* any [ingest pipelines](docs-content://solutions/search/ingest-for-search.md) are run on that document. Therefore, you can use ingest pipelines for any processing that must occur *after* integration filtering has occurred.

::::


::::{note}
If a sync rule is added, edited or removed, it will only take effect after the next full sync.

::::



## Basic sync rules [es-sync-rules-basic]

Each basic sync rules can be one of two "policies": `include` and `exclude`. `Include` rules are used to include the documents that "match" the specified condition. `Exclude` rules are used to exclude the documents that "match" the specified condition.

A "match" is determined based on a condition defined by a combination of "field", "rule", and "value".

The `Field` column should be used to define which field on a given document should be considered.

::::{note}
Only top-level fields are supported. Nested/object fields cannot be referenced with "dot notation".

::::


The following rules are available in the `Rule` column:

* `equals` - The field value is equal to the specified value.
* `starts_with` - The field value starts with the specified (string) value.
* `ends_with` - The field value ends with the specified (string) value.
* `contains` - The field value includes the specified (string) value.
* `regex` - The field value matches the specified [regular expression](https://en.wikipedia.org/wiki/Regular_expression).
* `>` - The field value is greater than the specified value.
* `<` - The field value is less than the specified value.

Finally, the `Value` column is dependent on:

* the data type in the specified "field"
* which "rule" was selected.

For example, if a value of `[A-Z]{{2}}` might make sense for a `regex` rule, but much less so for a `>` rule. Similarly, you probably wouldn’t have a value of `espresso` when operating on an `ip_address` field, but perhaps you would for a `beverage` field.


### Basic sync rules examples [es-sync-rules-basic-examples]


#### Example 1 [es-sync-rules-basic-examples-1]

Exclude all documents that have an `ID` field with the value greater than 1000.

:::{image} images/simple-rule-greater.png
:alt: Simple greater than rule
:class: screenshot
:::


#### Example 2 [es-sync-rules-basic-examples-2]

Exclude all documents that have a `state` field that matches a specified regex.

:::{image} images/simple-rule-regex.png
:alt: Simple regex rule
:class: screenshot
:::


### Performance implications [es-sync-rules-performance-implications]

* If you’re relying solely on basic sync rules in the integration filtering phase the connector will fetch **all** the data from the data source
* For data sources without automatic pagination, or similar optimizations, fetching all the data can lead to memory issues. For example, loading datasets which are too big to fit in memory at once.

::::{note}
The MongoDB connector provided by Elastic uses pagination and therefore has optimized performance. Keep in mind that custom community-built self-managed connectors may not have these performance optimizations.
::::

The following diagrams illustrate the concept of pagination. A huge data set may not fit into a connector instance’s memory. Splitting data into smaller chunks reduces the risk of out-of-memory errors.

This diagram illustrates an entire dataset being extracted at once:

:::{image} images/sync-rules-extract-all-at-once.png
:alt: Extract whole dataset at once
:class: screenshot
:::

By comparison, this diagram illustrates a paginated dataset:

:::{image} images/sync-rules-pagination.png
:alt: Pagination
:class: screenshot
:::


## Advanced sync rules [es-sync-rules-advanced]

::::{important}
Advanced sync rules overwrite any remote filtering query that could have been inferred from the basic sync rules. If an advanced sync rule is defined, any defined basic sync rules will be used exclusively for integration filtering.

::::


Advanced sync rules are only used in remote filtering. You can think of advanced sync rules as a language-agnostic way to represent queries to the data source. Therefore, these rules are highly **source-specific**.

The following connectors support advanced sync rules:

* [Confluence Online & Server](/reference/search-connectors/es-connectors-confluence.md)
* [Dropbox](/reference/search-connectors/es-connectors-dropbox.md)
* [Gmail](/reference/search-connectors/es-connectors-gmail.md)
* [GitHub](/reference/search-connectors/es-connectors-github.md)
* [Jira Online & Server](/reference/search-connectors/es-connectors-jira.md)
* [MongoDB](/reference/search-connectors/es-connectors-mongodb.md)
* [MS SQL Server](/reference/search-connectors/es-connectors-ms-sql.md)
* [MySQL](/reference/search-connectors/es-connectors-mysql.md)
* [Network Drive](/reference/search-connectors/es-connectors-network-drive.md)
* [PostgreSQL](/reference/search-connectors/es-connectors-postgresql.md)
* [S3](/reference/search-connectors/es-connectors-s3.md)
* [Salesforce](/reference/search-connectors/es-connectors-salesforce.md)
* [ServiceNow](/reference/search-connectors/es-connectors-servicenow.md)
* [SharePoint Online](/reference/search-connectors/es-connectors-sharepoint-online.md)

Each connector supporting advanced sync rules provides its own DSL to specify rules. Refer to the documentation for [each connector](/reference/search-connectors/index.md) for details.


## Combining basic and advanced sync rules [es-interplay-basic-rules-advanced-rules]

You can also use basic sync rules and advanced sync rules together to filter a data set.

The following diagram provides an overview of the order in which advanced sync rules, basic sync rules, and pipeline filtering, are applied to your documents:

:::{image} images/sync-rules-time-dimension.png
:alt: Sync Rules: What is applied when?
:class: screenshot
:::


### Example [es-example-interplay-basic-rules-advanced-rules]

In the following example we want to filter a data set containing apartments to only contain apartments with specific properties. We’ll use basic and advanced sync rules throughout the example.

A sample apartment looks like this in the `.json` format:

```js
    {
        "id": 1234,
        "bedrooms": 3,
        "price": 1500,
        "address": {
            "street": "Street 123",
            "government_area": "Area",
            "country_information": {
                "country_code": "PT",
                "country": "Portugal"
    }
  }
}
```
% NOTCONSOLE

The target data set should fulfill the following conditions:

1. Every apartment should have at least **3 bedrooms**
2. The apartments should not be more expensive than **1500 per month**
3. The apartment with id *1234* should get included without considering the first two conditions
4. Each apartment should be located in either *Portugal* or *Spain*

The first 3 conditions can be handled by basic sync rules, but we’ll need to use advanced sync rules for number 4.


#### Basic sync rules examples [es-example-interplay-basic-rules]

To create a new basic sync rule, navigate to the *Sync Rules* tab and select **Draft new sync rules**:

:::{image} images/sync-rules-draft-new-rules.png
:alt: Draft new rules
:class: screenshot
:::

Afterwards you need to press the *Save and validate draft* button to validate these rules. Note that when saved the rules will be in *draft* state. They won’t be executed in the next sync unless they are *applied*.

:::{image} images/sync-rules-save-and-validate-draft.png
:alt: Save and validate draft
:class: screenshot
:::

After a successful validation you can apply your rules so they’ll be executed in the next sync.

These following conditions can be covered by basic sync rules:

1. The apartment with id *1234* should get included without considering the first two conditions
2. Every apartment should have at least three bedrooms
3. The apartments should not be more expensive than 1000/month

:::{image} images/sync-rules-rules-fulfilling-properties.png
:alt: Save and validate draft
:class: screenshot
:::

::::{note}
Remember that order matters for basic sync rules. You may get different results for a different ordering.

::::



#### Advanced sync rules example [es-example-interplay-advanced-rules]

You want to only include apartments which are located in Portugal or Spain. We need to use advanced sync rules here, because we’re dealing with deeply nested objects.

Let’s assume that the apartment data is stored inside a MongoDB instance. For MongoDB we support [aggregation pipelines](https://www.mongodb.com/docs/manual/core/aggregation-pipeline/) in our advanced sync rules among other things. An aggregation pipeline to only select properties located in Portugal or Spain looks like this:

```js
    [
      {
        "$match": {
             "$or": [
                    {
                      "address.country_information.country": "Portugal"
                    },
                    {
                      "address.country_information.country": "Spain"
                    }
                  ]
                }
      }
    ]
```
% NOTCONSOLE

To create these advanced sync rules navigate to the sync rules creation dialog and select the *Advanced rules* tab. You can now paste your aggregation pipeline into the input field under `aggregate.pipeline`:

:::{image} images/sync-rules-paste-aggregation-pipeline.png
:alt: Paste aggregation pipeline
:class: screenshot
:::

Once validated, apply these rules. The following screenshot shows the applied sync rules, which will be executed in the next sync:

:::{image} images/sync-rules-advanced-rules-appeared.png
:alt: Advanced sync rules appeared
:class: screenshot
:::

After a successful sync you can expand the sync details to see which rules were applied:

:::{image} images/sync-rules-applied-rules-during-sync.png
:alt: Applied rules during sync
:class: screenshot
:::

::::{warning}
Active sync rules can become invalid when changed outside of the UI. Sync jobs with invalid rules will fail. One workaround is to revalidate the draft rules and override the invalid active rules.

::::
