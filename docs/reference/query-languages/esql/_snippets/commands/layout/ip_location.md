```yaml {applies_to}
serverless: preview
stack: preview 9.5
```

The `IP_LOCATION` processing command enriches rows with geographic information based on an IP address, using a MaxMind or IPinfo database.

## Syntax

```esql
IP_LOCATION prefix = expression [WITH { option = value [, ...] }]
```

## Parameters

`prefix`
:   The prefix for the output columns. The resolved fields are available as `prefix.field_name`.

`expression`
:   An expression that evaluates to an IP address (type `ip` or a string containing an IP address).

## WITH options

`database_file`
:   The name of the IP location database to use. Default: `GeoLite2-City.mmdb`. Supported databases include GeoLite2 and GeoIP2 variants (City, Country, ASN) as well as IPinfo databases. To use a custom database, register it via the [IP geolocation database configuration API](/reference/elasticsearch/rest-apis/ip-location-database-configuration-apis.md) first.

`properties`
:   List of database property names to include in the output. Each value maps to one output column. Default: the database's default property set (for GeoLite2-City: `country_iso_code`, `country_name`, `continent_name`, `region_iso_code`, `region_name`, `city_name`, `location`). Pass `[]` to produce zero output columns. Pass a subset to reduce the output schema.

`first_only`
:   Controls multi-value input handling. When `true` (default), the first IP value is used and the rest are ignored. When `false`, multi-value input is rejected with a warning and the row produces `null` for all output columns.

## Description

:::::{note}
The first `IP_LOCATION` query triggers a one-time download of IP location databases to all nodes in the cluster. These databases are periodically updated while the cluster is running.
:::::

The `IP_LOCATION` command resolves geographic information for an IP address and adds the results as new columns prefixed with the specified `prefix` followed by a dot (`.`).

This command is the query-time equivalent of the [GeoIP ingest processor](/reference/enrich-processor/geoip-processor.md).

The output columns depend on the `database_file` and `properties` options. For the default GeoLite2-City database, the following columns are produced:

`prefix.country_iso_code`
:   The ISO code of the country (e.g., `US`, `NL`).

`prefix.country_name`
:   The country name (e.g., `United States`).

`prefix.continent_name`
:   The continent name (e.g., `North America`).

`prefix.region_iso_code`
:   The ISO code of the region (e.g., `US-FL`).

`prefix.region_name`
:   The region name (e.g., `Florida`).

`prefix.city_name`
:   The city name (e.g., `Homestead`).

`prefix.location`
:   The geographic coordinates as a `geo_point`.

If the IP address is not found in the database, all output columns are `null`.
If the database is not yet available on the node (e.g., still downloading), KEYWORD output columns contain a sentinel string and other columns are `null`, with a warning header indicating the unavailable database.

## Examples

The following example resolves geographic information for an IP address:

:::{include} ../examples/ip_location.csv-spec/basic.md
:::

To use a different database, specify it with the `database_file` option:

```esql
ROW ip = "82.170.213.79"
| IP_LOCATION g = ip WITH { "database_file": "GeoLite2-Country.mmdb" }
| KEEP g.*
```

To limit the output to specific properties:

```esql
ROW ip = "89.160.20.128"
| IP_LOCATION g = ip WITH { "properties": ["country_iso_code", "city_name"] }
| KEEP g.*
```

You can use the resolved fields in subsequent commands, for example to filter by country:

```esql
FROM web_logs
| IP_LOCATION geo = client_ip
| WHERE geo.country_iso_code == "US"
| STATS count = COUNT(*) BY geo.city_name
```
