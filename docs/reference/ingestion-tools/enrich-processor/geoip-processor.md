---
navigation_title: "GeoIP"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/geoip-processor.html
---

# GeoIP processor [geoip-processor]


The `geoip` processor adds information about the geographical location of an IPv4 or IPv6 address.

$$$geoip-automatic-updates$$$
By default, the processor uses the GeoLite2 City, GeoLite2 Country, and GeoLite2 ASN IP geolocation databases from [MaxMind](http://dev.maxmind.com/geoip/geoip2/geolite2/), shared under the CC BY-SA 4.0 license. It automatically downloads these databases if your nodes can connect to `storage.googleapis.com` domain and either:

* `ingest.geoip.downloader.eager.download` is set to true
* your cluster has at least one pipeline with a `geoip` or `ip_location` processor

{{es}} automatically downloads updates for these databases from the Elastic GeoIP endpoint: [https://geoip.elastic.co/v1/database](https://geoip.elastic.co/v1/database?elastic_geoip_service_tos=agree). To get download statistics for these updates, use the [GeoIP stats API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ingest-geo-ip-stats).

If your cluster can’t connect to the Elastic GeoIP endpoint or you want to manage your own updates, see [Manage your own IP geolocation database updates](#manage-geoip-database-updates).

If you would like to have {{es}} download database files directly from Maxmind using your own provided license key, see [Create or update IP geolocation database configuration](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ingest-put-ip-location-database).

If {{es}} can’t connect to the endpoint for 30 days all updated databases will become invalid. {{es}} will stop enriching documents with ip geolocation data and will add `tags: ["_geoip_expired_database"]` field instead.

## Using the `geoip` Processor in a Pipeline [using-ingest-geoip]

$$$ingest-geoip-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field to get the IP address from for the geographical lookup. |
| `target_field` | no | geoip | The field that will hold the geographical information looked up from the database. |
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

Here is an example that uses the default city database and adds the geographical information to the `geoip` field based on the `ip` field:

```console
PUT _ingest/pipeline/geoip
{
  "description" : "Add ip geolocation info",
  "processors" : [
    {
      "geoip" : {
        "field" : "ip"
      }
    }
  ]
}
PUT my-index-000001/_doc/my_id?pipeline=geoip
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
    "geoip": {
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
PUT _ingest/pipeline/geoip
{
  "description" : "Add ip geolocation info",
  "processors" : [
    {
      "geoip" : {
        "field" : "ip",
        "target_field" : "geo",
        "database_file" : "GeoLite2-Country.mmdb"
      }
    }
  ]
}
PUT my-index-000001/_doc/my_id?pipeline=geoip
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
PUT _ingest/pipeline/geoip
{
  "description" : "Add ip geolocation info",
  "processors" : [
    {
      "geoip" : {
        "field" : "ip"
      }
    }
  ]
}

PUT my-index-000001/_doc/my_id?pipeline=geoip
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

### Recognizing Location as a Geopoint [ingest-geoip-mappings-note]

Although this processor enriches your document with a `location` field containing the estimated latitude and longitude of the IP address, this field will not be indexed as a [`geo_point`](/reference/elasticsearch/mapping-reference/geo-point.md) type in Elasticsearch without explicitly defining it as such in the mapping.

You can use the following mapping for the example index above:

```console
PUT my_ip_locations
{
  "mappings": {
    "properties": {
      "geoip": {
        "properties": {
          "location": { "type": "geo_point" }
        }
      }
    }
  }
}
```



## Manage your own IP geolocation database updates [manage-geoip-database-updates]

If you can’t [automatically update](#geoip-automatic-updates) your IP geolocation databases from the Elastic endpoint, you have a few other options:

* [Use a proxy endpoint](#use-proxy-geoip-endpoint)
* [Use a custom endpoint](#use-custom-geoip-endpoint)
* [Manually update your IP geolocation databases](#manually-update-geoip-databases)

$$$use-proxy-geoip-endpoint$$$
**Use a proxy endpoint**

If you can’t connect directly to the Elastic GeoIP endpoint, consider setting up a secure proxy. You can then specify the proxy endpoint URL in the [`ingest.geoip.downloader.endpoint`](#ingest-geoip-downloader-endpoint) setting of each node’s `elasticsearch.yml` file.

In a strict setup the following domains may need to be added to the allowed domains list:

* `geoip.elastic.co`
* `storage.googleapis.com`

$$$use-custom-geoip-endpoint$$$
**Use a custom endpoint**

You can create a service that mimics the Elastic GeoIP endpoint. You can then get automatic updates from this service.

1. Download your `.mmdb` database files from the [MaxMind site](http://dev.maxmind.com/geoip/geoip2/geolite2).
2. Copy your database files to a single directory.
3. From your {{es}} directory, run:

    ```sh
    ./bin/elasticsearch-geoip -s my/source/dir [-t target/directory]
    ```

4. Serve the static database files from your directory. For example, you can use Docker to serve the files from an nginx server:

    ```sh
    docker run -v my/source/dir:/usr/share/nginx/html:ro nginx
    ```

5. Specify the service’s endpoint URL in the [`ingest.geoip.downloader.endpoint`](#ingest-geoip-downloader-endpoint) setting of each node’s `elasticsearch.yml` file.

    By default, {{es}} checks the endpoint for updates every three days. To use another polling interval, use the [cluster update settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings) to set [`ingest.geoip.downloader.poll.interval`](#ingest-geoip-downloader-poll-interval).


$$$manually-update-geoip-databases$$$
**Manually update your IP geolocation databases**

1. Use the [cluster update settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings) to set `ingest.geoip.downloader.enabled` to `false`. This disables automatic updates that may overwrite your database changes. This also deletes all downloaded databases.
2. Download your `.mmdb` database files from the [MaxMind site](http://dev.maxmind.com/geoip/geoip2/geolite2).

    You can also use custom city, country, and ASN `.mmdb` files. These files must be uncompressed. The type (city, country, or ASN) will be pulled from the file metadata, so the filename does not matter.

3. On {{ess}} deployments upload database using a [custom bundle](/reference/elasticsearch-plugins/cloud/ec-custom-bundles.md).
4. On self-managed deployments copy the database files to `$ES_CONFIG/ingest-geoip`.
5. In your `geoip` processors, configure the `database_file` parameter to use a custom database file.

### Node Settings [ingest-geoip-settings]

The `geoip` processor supports the following setting:

`ingest.geoip.cache_size`
:   The maximum number of results that should be cached. Defaults to `1000`.

Note that these settings are node settings and apply to all `geoip` and `ip_location` processors, i.e. there is a single cache for all such processors.


### Cluster settings [geoip-cluster-settings]

$$$ingest-geoip-downloader-enabled$$$

`ingest.geoip.downloader.enabled`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), Boolean) If `true`, {{es}} automatically downloads and manages updates for IP geolocation databases from the `ingest.geoip.downloader.endpoint`. If `false`, {{es}} does not download updates and deletes all downloaded databases. Defaults to `true`.

$$$ingest-geoip-downloader-eager-download$$$

`ingest.geoip.downloader.eager.download`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), Boolean) If `true`, {{es}} downloads IP geolocation databases immediately, regardless of whether a pipeline exists with a geoip processor. If `false`, {{es}} only begins downloading the databases if a pipeline with a geoip processor exists or is added. Defaults to `false`.

$$$ingest-geoip-downloader-endpoint$$$

`ingest.geoip.downloader.endpoint`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) Endpoint URL used to download updates for IP geolocation databases. For example, `https://myDomain.com/overview.json`. Defaults to `https://geoip.elastic.co/v1/database`. {{es}} stores downloaded database files in each node’s [temporary directory](docs-content://deploy-manage/deploy/self-managed/important-settings-configuration.md#es-tmpdir) at `$ES_TMPDIR/geoip-databases/<node_id>`. Note that {{es}} will make a GET request to `${ingest.geoip.downloader.endpoint}?elastic_geoip_service_tos=agree`, expecting the list of metadata about databases typically found in `overview.json`.

The downloader uses the JDK’s builtin cacerts. If you’re using a custom endpoint, add the custom https endpoint cacert(s) to the JDK’s truststore.

$$$ingest-geoip-downloader-poll-interval$$$

`ingest.geoip.downloader.poll.interval`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) How often {{es}} checks for IP geolocation database updates at the `ingest.geoip.downloader.endpoint`. Must be greater than `1d` (one day). Defaults to `3d` (three days).
