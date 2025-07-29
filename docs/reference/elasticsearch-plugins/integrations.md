---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/integrations.html
---

# Integrations [integrations]

Integrations are not plugins, but are external tools or modules that make it easier to work with Elasticsearch.


## CMS integrations [cms-integrations]


### Supported by the community: [_supported_by_the_community]

* [ElasticPress](https://wordpress.org/plugins/elasticpress/): Elasticsearch WordPress Plugin
* [Tiki Wiki CMS Groupware](https://doc.tiki.org/Elasticsearch): Tiki has native support for Elasticsearch. This provides faster & better search (facets, etc), along with some Natural Language Processing features (ex.: More like this)
* [XWiki Next Generation Wiki](https://extensions.xwiki.org/xwiki/bin/view/Extension/Elastic+Search+Macro/): XWiki has an Elasticsearch and Kibana macro allowing to run Elasticsearch queries and display the results in XWiki pages using XWiki’s scripting language as well as include Kibana Widgets in XWiki pages


### Supported by Elastic: [_supported_by_elastic]

* [Logstash output to Elasticsearch](logstash-docs-md://lsr//plugins-outputs-elasticsearch.md): The Logstash `elasticsearch` output plugin.
* [Elasticsearch input to Logstash](logstash-docs-md://lsr/plugins-inputs-elasticsearch.md) The Logstash `elasticsearch` input plugin.
* [Elasticsearch event filtering in Logstash](logstash-docs-md://lsr/plugins-filters-elasticsearch.md) The Logstash `elasticsearch` filter plugin.
* [Elasticsearch bulk codec](logstash-docs-md://lsr//plugins-codecs-es_bulk.md) The Logstash `es_bulk` plugin decodes the Elasticsearch bulk format into individual events.


### Supported by the community: [_supported_by_the_community_2]

* [Ingest processor template](https://github.com/spinscale/cookiecutter-elasticsearch-ingest-processor): A template for creating new ingest processors.
* [Kafka Standalone Consumer (Indexer)](https://github.com/BigDataDevs/kafka-elasticsearch-consumer): Kafka Standalone Consumer [Indexer] will read messages from Kafka in batches, processes(as implemented) and bulk-indexes them into Elasticsearch. Flexible and scalable. More documentation in above GitHub repo’s Wiki.
* [Scrutineer](https://github.com/Aconex/scrutineer): A high performance consistency checker to compare what you’ve indexed with your source of truth content (e.g. DB)
* [FS Crawler](https://github.com/dadoonet/fscrawler): The File System (FS) crawler allows to index documents (PDF, Open Office…​) from your local file system and over SSH. (by David Pilato)
* [Elasticsearch Evolution](https://github.com/senacor/elasticsearch-evolution): A library to migrate elasticsearch mappings.
* [PGSync](https://pgsync.com): A tool for syncing data from Postgres to Elasticsearch.


## Deployment [deployment]


### Supported by the community: [_supported_by_the_community_3]

* [Ansible](https://github.com/elastic/ansible-elasticsearch): Ansible playbook for Elasticsearch.
* [Puppet](https://github.com/elastic/puppet-elasticsearch): Elasticsearch puppet module.
* [Chef](https://github.com/elastic/cookbook-elasticsearch): Chef cookbook for Elasticsearch


## Framework integrations [framework-integrations]


### Supported by the community: [_supported_by_the_community_4]

* [Apache Camel Integration](https://camel.apache.org/components/2.x/elasticsearch-component.md): An Apache camel component to integrate Elasticsearch
* [Catmandu](https://metacpan.org/pod/Catmandu::Store::ElasticSearch): An Elasticsearch backend for the Catmandu framework.
* [FOSElasticaBundle](https://github.com/FriendsOfSymfony/FOSElasticaBundle): Symfony2 Bundle wrapping Elastica.
* [Grails](https://plugins.grails.org/plugin/puneetbehl/elasticsearch): Elasticsearch Grails plugin.
* [Hibernate Search](https://hibernate.org/search/) Integration with Hibernate ORM, from the Hibernate team. Automatic synchronization of write operations, yet exposes full Elasticsearch capabilities for queries. Can return either Elasticsearch native or re-map queries back into managed entities loaded within transactions from the reference database.
* [Spring Data Elasticsearch](https://github.com/spring-projects/spring-data-elasticsearch): Spring Data implementation for Elasticsearch
* [Spring Elasticsearch](https://github.com/dadoonet/spring-elasticsearch): Spring Factory for Elasticsearch
* [Zeebe](https://zeebe.io): An Elasticsearch exporter acts as a bridge between Zeebe and Elasticsearch
* [Apache Pulsar](https://pulsar.apache.org/docs/en/io-elasticsearch): The Elasticsearch Sink Connector is used to pull messages from Pulsar topics and persist the messages to an index.
* [Micronaut Elasticsearch Integration](https://micronaut-projects.github.io/micronaut-elasticsearch/latest/guide/index.html): Integration of Micronaut with Elasticsearch
* [Apache StreamPipes](https://streampipes.apache.org): StreamPipes is a framework that enables users to work with IoT data sources.
* [Apache MetaModel](https://metamodel.apache.org/): Providing a common interface for discovery, exploration of metadata and querying of different types of data sources.
* [Micrometer](https://micrometer.io): Vendor-neutral application metrics facade. Think SLF4j, but for metrics.


## Hadoop integrations [hadoop-integrations]


### Supported by Elastic: [_supported_by_elastic_2]

* [es-hadoop](https://www.elastic.co/elasticsearch/hadoop): Elasticsearch real-time search and analytics natively integrated with Hadoop. Supports Map/Reduce, Cascading, Apache Hive, Apache Pig, Apache Spark and Apache Storm.


### Supported by the community: [_supported_by_the_community_5]

* [Garmadon](https://github.com/criteo/garmadon): Garmadon is a solution for Hadoop Cluster realtime introspection.


## Health and Performance Monitoring [monitoring-integrations]


### Supported by the community: [_supported_by_the_community_6]

* [SPM for Elasticsearch](https://sematext.com/spm/index.html): Performance monitoring with live charts showing cluster and node stats, integrated alerts, email reports, etc.
* [Zabbix monitoring template](https://www.zabbix.com/integrations/elasticsearch): Monitor the performance and status of your {{es}} nodes and cluster with Zabbix and receive events information.


## Other integrations [other-integrations]


### Supported by the community: [_supported_by_the_community_7]

* [Wireshark](https://www.wireshark.org/): Protocol dissection for HTTP and the transport protocol
* [ItemsAPI](https://www.itemsapi.com/): Search backend for mobile and web
