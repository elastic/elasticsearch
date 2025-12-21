---
navigation_title: "IP Location"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ip-location-processor.html
---

# IP location processor [ip-location-processor]


The `ip_location` processor adds information about the geographical location of an IPv4 or IPv6 address.

$$$ip-location-automatic-updates$$$
By default, the processor uses the GeoLite2 City, GeoLite2 Country, and GeoLite2 ASN IP geolocation databases from [MaxMind](http://dev.maxmind.com/geoip/geoip2/geolite2/), shared under the CC BY-SA 4.0 license. It automatically downloads these databases if your nodes can connect to `storage.googleapis.com` domain and either:

* `ingest.geoip.downloader.eager.download` is set to true
* your cluster has at least one pipeline with a `geoip` or `ip_location` processor

{{es}} automatically downloads updates for these databases from the Elastic GeoIP endpoint: [https://geoip.elastic.co/v1/database](https://geoip.elastic.co/v1/database?elastic_geoip_service_tos=agree). To get download statistics for these updates, use the [GeoIP stats API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ingest-geo-ip-stats).

If your cluster can’t connect to the Elastic GeoIP endpoint or you want to manage your own updates, see [Manage your own IP geolocation database updates](/reference/enrich-processor/geoip-processor.md#manage-geoip-database-updates).

If you would like to have {{es}} download database files directly from Maxmind using your own provided license key, see [Create or update IP geolocation database configuration](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ingest-put-ip-location-database).

If {{es}} can’t connect to the endpoint for 30 days all updated databases will become invalid. {{es}} will stop enriching documents with ip geolocation data and will add `tags: ["_ip_location_expired_database"]` field instead.

## Using the `ip_location` Processor in a Pipeline [using-ingest-ip-location]

$$$ingest-ip-location-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field to get the IP address from for the geographical lookup. |
| `target_field` | no | ip_location | The field that will hold the geographical information looked up from the database. |
| `database_file` | no | GeoLite2-City.mmdb | The database filename referring to one of the automatically downloaded GeoLite2 databases (GeoLite2-City.mmdb, GeoLite2-Country.mmdb, or GeoLite2-ASN.mmdb), or the name of a supported database file in the `ingest-geoip` config directory, or the name of a [configured database](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ingest-get-ip-location-database) (with the `.mmdb` suffix appended). |
| `properties` | no | [`continent_name`, `country_iso_code`, `country_name`, `region_iso_code`, `region_name`, `city_name`, `location`] * | Controls what properties are added to the `target_field` based on the ip geolocation lookup. |
| `ignore_missing` | no | `false` | If `true` and `field` does not exist, the processor quietly exits without modifying the document |
| `first_only` | no | `true` | If `true` only first found ip geolocation data, will be returned, even if `field` contains array |
| `download_database_on_pipeline_creation` | no | `true` | If `true` (and if `ingest.geoip.downloader.eager.download` is `false`), the missing database is downloaded when the pipeline is created. Else, the download is triggered by when the pipeline is used as the `default_pipeline` or `final_pipeline` in an index. |

*Depends on what is available in `database_file`:

* If a GeoLite2 City or GeoIP2 City database is used, then the following fields may be added under the `target_field`: `ip`, `country_iso_code`, `country_name`, `country_in_european_union`, `registered_country_iso_code`, `registered_country_name`, `registered_country_in_european_union`, `continent_code`, `continent_name`, `region_iso_code`, `region_name`, `city_name`, `postal_code`, `timezone`, `location`, and `accuracy_radius`. The fields actually added depend on what has been found and which properties were configured in `properties`.
* If a GeoLite2 Country or GeoIP2 Country database is used, then the following fields may be added under the `target_field`: `ip`, `country_iso_code`, `country_name`, `country_in_european_union`, `registered_country_iso_code`, `registered_country_name`, `registered_country_in_european_union`, `continent_code`, and `continent_name`. The fields actually added depend on what has been found and which properties were configured in `properties`.
* If the GeoLite2 ASN database is used, then the following fields may be added under the `target_field`: `ip`, `asn`, `organization_name` and `network`. The fields actually added depend on what has been found and which properties were configured in `properties`.
* If the GeoIP2 Anonymous IP database is used, then the following fields may be added under the `target_field`: `ip`, `hosting_provider`, `tor_exit_node`, `anonymous_vpn`, `anonymous`, `public_proxy`, and `residential_proxy`. The fields actually added depend on what has been found and which properties were configured in `properties`.
* If the GeoIP2 Connection Type database is used, then the following fields may be added under the `target_field`: `ip`, and `connection_type`. The fields actually added depend on what has been found and which properties were configured in `properties`.
* If the GeoIP2 Domain database is used, then the following fields may be added under the `target_field`: `ip`, and `domain`. The fields actually added depend on what has been found and which properties were configured in `properties`.
* If the GeoIP2 ISP database is used, then the following fields may be added under the `target_field`: `ip`, `asn`, `organization_name`, `network`, `isp`, `isp_organization_name`, `mobile_country_code`, and `mobile_network_code`. The fields actually added depend on what has been found and which properties were configured in `properties`.
* If the GeoIP2 Enterprise database is used, then the following fields may be added under the `target_field`: `ip`, `country_iso_code`, `country_name`, `country_in_european_union`, `registered_country_iso_code`, `registered_country_name`, `registered_country_in_european_union`, `continent_code`, `continent_name`, `region_iso_code`, `region_name`, `city_name`, `postal_code`, `timezone`, `location`, `accuracy_radius`, `country_confidence`, `city_confidence`, `postal_confidence`, `asn`, `organization_name`, `network`, `hosting_provider`, `tor_exit_node`, `anonymous_vpn`, `anonymous`, `public_proxy`, `residential_proxy`, `domain`, `isp`, `isp_organization_name`, `mobile_country_code`, `mobile_network_code`, `user_type`, and `connection_type`. The fields actually added depend on what has been found and which properties were configured in `properties`.

Here is an example that uses the default city database and adds the geographical information to the `ip_location` field based on the `ip` field:

```console
PUT _ingest/pipeline/ip_location
{
  "description" : "Add ip geolocation info",
  "processors" : [
    {
      "ip_location" : {
        "field" : "ip"
      }
    }
  ]
}
PUT my-index-000001/_doc/my_id?pipeline=ip_location
{
  "ip": "89.160.20.128"
}
GET my-index-000001/_doc/my_id
```

Which returns:

```console-result
{
  "found": true,
  "_index": "my-index-000001",
  "_id": "my_id",
  "_version": 1,
  "_seq_no": 55,
  "_primary_term": 1,
  "_source": {
    "ip": "89.160.20.128",
    "ip_location": {
      "continent_name": "Europe",
      "country_name": "Sweden",
      "country_iso_code": "SE",
      "city_name" : "Linköping",
      "region_iso_code" : "SE-E",
      "region_name" : "Östergötland County",
      "location": { "lat": 58.4167, "lon": 15.6167 }
    }
  }
}
```
% TESTRESPONSE[s/"_seq_no": \d+/"_seq_no" : $body._seq_no/ s/"_primary_term":1/"_primary_term" : $body._primary_term/]

Here is an example that uses the default country database and adds the geographical information to the `geo` field based on the `ip` field. Note that this database is downloaded automatically. So this:

```console
PUT _ingest/pipeline/ip_location
{
  "description" : "Add ip geolocation info",
  "processors" : [
    {
      "ip_location" : {
        "field" : "ip",
        "target_field" : "geo",
        "database_file" : "GeoLite2-Country.mmdb"
      }
    }
  ]
}
PUT my-index-000001/_doc/my_id?pipeline=ip_location
{
  "ip": "89.160.20.128"
}
GET my-index-000001/_doc/my_id
```

returns this:

```console-result
{
  "found": true,
  "_index": "my-index-000001",
  "_id": "my_id",
  "_version": 1,
  "_seq_no": 65,
  "_primary_term": 1,
  "_source": {
    "ip": "89.160.20.128",
    "geo": {
      "continent_name": "Europe",
      "country_name": "Sweden",
      "country_iso_code": "SE"
    }
  }
}
```
% TESTRESPONSE[s/"_seq_no": \d+/"_seq_no" : $body._seq_no/ s/"_primary_term" : 1/"_primary_term" : $body._primary_term/]

Not all IP addresses find geo information from the database, When this occurs, no `target_field` is inserted into the document.

Here is an example of what documents will be indexed as when information for "80.231.5.0" cannot be found:

```console
PUT _ingest/pipeline/ip_location
{
  "description" : "Add ip geolocation info",
  "processors" : [
    {
      "ip_location" : {
        "field" : "ip"
      }
    }
  ]
}

PUT my-index-000001/_doc/my_id?pipeline=ip_location
{
  "ip": "80.231.5.0"
}

GET my-index-000001/_doc/my_id
```

Which returns:

```console-result
{
  "_index" : "my-index-000001",
  "_id" : "my_id",
  "_version" : 1,
  "_seq_no" : 71,
  "_primary_term": 1,
  "found" : true,
  "_source" : {
    "ip" : "80.231.5.0"
  }
}
```
% TESTRESPONSE[s/"_seq_no" : \d+/"_seq_no" : $body._seq_no/ s/"_primary_term" : 1/"_primary_term" : $body._primary_term/]


