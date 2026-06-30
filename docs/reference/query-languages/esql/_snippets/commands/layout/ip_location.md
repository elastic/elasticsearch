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
:   The name of the IP location database to use. Default: `GeoLite2-City.mmdb`. Supported databases include GeoLite2 and GeoIP2 variants (City, Country, ASN) as well as IPinfo databases. To use a custom database, register it via the [Create or update IP geolocation database configuration](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ingest-put-ip-location-database) API first.

`properties`
:   List of database property names to include in the output. Each value maps to one output column. Default: the database's default property set (for GeoLite2-City: `country_iso_code`, `country_name`, `continent_name`, `region_iso_code`, `region_name`, `city_name`, `location`). Pass a subset to reduce the output schema. The list must contain at least one property; an empty list is not supported.

`first_only`
:   Controls multi-value input handling. When `true` (default), the first IP value is used and the rest are ignored. When `false`, multi-value input is rejected with a warning and the row produces `null` for all output columns.

## Description

:::::{note}
The first `IP_LOCATION` query triggers a one-time download of IP location databases to all nodes in the cluster. These databases are periodically updated while the cluster is running.
:::::

The `IP_LOCATION` command resolves geographic information for an IP address and adds the results as new columns prefixed with the specified `prefix` followed by a dot (`.`).

This command is the query-time equivalent of the [GeoIP ingest processor](/reference/ingest-processor/geoip-processor.md).

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
If the database is not yet available on the node (e.g., still downloading), KEYWORD output columns contain the sentinel value `_ip_location_database_unavailable_<database_file>` and non-KEYWORD columns are `null`, with a warning header indicating the unavailable database. Similarly, if the database has expired, KEYWORD columns contain `_ip_location_expired_database` and non-KEYWORD columns are `null`.

## Supported databases and properties

The available `properties` depend on the `database_file`. Properties listed under **default** are included when `properties` is not specified. For a full description of each property, refer to the [GeoIP ingest processor](/reference/ingest-processor/geoip-processor.md) documentation.

**GeoLite2-City / GeoIP2-City** (default database)
:   **default:** `country_iso_code`, `country_name`, `continent_name`, `region_iso_code`, `region_name`, `city_name`, `location`
:   **also available:** `ip`, `country_in_european_union`, `continent_code`, `timezone`, `postal_code`, `accuracy_radius`, `registered_country_iso_code`, `registered_country_name`, `registered_country_in_european_union`

**GeoLite2-Country / GeoIP2-Country**
:   **default:** `continent_name`, `country_name`, `country_iso_code`
:   **also available:** `ip`, `continent_code`, `country_in_european_union`, `registered_country_iso_code`, `registered_country_name`, `registered_country_in_european_union`

**GeoLite2-ASN**
:   **default (all):** `ip`, `asn`, `organization_name`, `network`

**GeoIP2-Anonymous-IP**
:   **default:** `hosting_provider`, `tor_exit_node`, `anonymous_vpn`, `anonymous`, `public_proxy`, `residential_proxy`
:   **also available:** `ip`

**GeoIP2-Connection-Type**
:   **default:** `connection_type`
:   **also available:** `ip`

**GeoIP2-Domain**
:   **default:** `domain`
:   **also available:** `ip`

**GeoIP2-Enterprise**
:   **default:** `country_iso_code`, `country_name`, `continent_name`, `region_iso_code`, `region_name`, `city_name`, `location`
:   **also available:** `ip`, `country_confidence`, `country_in_european_union`, `continent_code`, `city_confidence`, `timezone`, `postal_code`, `postal_confidence`, `accuracy_radius`, `asn`, `organization_name`, `network`, `hosting_provider`, `tor_exit_node`, `anonymous_vpn`, `anonymous`, `public_proxy`, `residential_proxy`, `domain`, `isp`, `isp_organization_name`, `mobile_country_code`, `mobile_network_code`, `user_type`, `connection_type`, `registered_country_iso_code`, `registered_country_name`, `registered_country_in_european_union`

**GeoIP2-ISP**
:   **default (all):** `ip`, `asn`, `organization_name`, `network`, `isp`, `isp_organization_name`, `mobile_country_code`, `mobile_network_code`

Custom databases registered via the [IP geolocation database configuration](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ingest-put-ip-location-database) API have properties determined by their database type metadata.

## Examples

The following example resolves geographic information for an IP address:

:::{include} ../examples/ip_location.csv-spec/basic.md
:::

:::::{tip}
The example above uses `KEEP` to select specific columns from the default property set. An equivalent and more efficient approach is to use `properties` so that only the needed fields are resolved:

```esql
ROW ip = "2602:306:33d3:8000::3257:9652"
| IP_LOCATION g = ip WITH { "properties": ["country_iso_code", "country_name", "continent_name", "region_iso_code", "region_name", "city_name"] }
```
:::::

To use a different database, specify it with the `database_file` option:

```esql
ROW ip = "82.170.213.79"
| IP_LOCATION g = ip WITH { "database_file": "GeoLite2-Country.mmdb" }
| KEEP g.*
```

You can use the resolved fields in subsequent commands, for example to filter by country:

```esql
FROM web_logs
| IP_LOCATION geo = client_ip
| WHERE geo.country_iso_code == "US"
| STATS count = COUNT(*) BY geo.city_name
```
