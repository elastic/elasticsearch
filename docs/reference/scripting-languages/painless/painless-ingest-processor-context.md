---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-ingest-processor-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Ingest processor context [painless-ingest-processor-context]

Use a Painless script in an [ingest processor](/reference/enrich-processor/script-processor.md) to modify documents upon insertion.

The ingest processor context enables document transformation during the indexing process, allowing you to enrich, modify, or restructure data before it’s stored in {{es}}.  
Painless scripts run as script processors within ingest pipelines that support script execution.

Ingest pipelines consist of multiple processors that can transform documents sequentially. The script processor allows Painless scripts to access and modify document fields using the `ctx` variable during this transformation process.

For help debugging errors in this context, refer to [Debug ingest pipeline failures in Painless](docs-content://explore-analyze/scripting/painless-ingest-pipeline-failures.md).

## Processing workflow

The pipelines processing proceeds through four steps.

* Documents enter the ingest pipeline.   
* Each processor transforms the document according to its configuration.  
* Script processors execute Painless code to perform custom transformations.  
* Modified documents are indexed into {{es}}.

For more information refer to [{{es}} ingest pipelines](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md). You can also check the troubleshooting guide for help with ingest pipelines failures.

## Variables

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

[`ctx['_index']`](/reference/elasticsearch/mapping-reference/mapping-index-field.md) (`String`)
:   The name of the index.

`ctx` (`Map`)
:   Contains extracted JSON in a `Map` and `List` structure for the fields that are part of the document.

## Side Effects

[`ctx['_index']`](/reference/elasticsearch/mapping-reference/mapping-index-field.md)
:   Modify this to change the destination index for the current document.

`ctx` (`Map`)
:   Modify the values in the `Map/List` structure to add, modify, or delete the fields of a document.

## Return

void
:   No expected return value.

## API

Both the standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) and specialized [Field API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-field.html) are available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

The following example demonstrates how to use a script inside an ingest pipeline to create a new field named `custom_region_code`. This field combines the `geoip.country_iso_code` and the first two uppercase letters of `geoip.continent_name`.

```java
String country = ctx.geoip.country_iso_code; 
          
ctx.custom_region_code = country + '_' + ctx.geoip.continent_name.substring(0,2).toUpperCase();
```

The following request runs during ingestion time and uses the `ctx` object to access and modify the document fields.

```json
PUT /_ingest/pipeline/kibana_sample_data_ecommerce-custom_region_code
{
  "description": "generate region code from country and continent name for kibana_sample_data_ecommerce dataset",
  "processors": [
    {
      "script": {
        "lang": "painless",
        "source": """
          String country = ctx.geoip.country_iso_code; 
          
          ctx.custom_region_code = country + '_' + ctx.geoip.continent_name.substring(0,2).toUpperCase();
        """
      }
    }
  ]
}
```
