---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-azure-classic-usage.html
---

# Azure Virtual Machine discovery [discovery-azure-classic-usage]

Azure VM discovery allows to use the Azure APIs to perform automatic discovery. Here is a simple sample configuration:

```yaml
cloud:
    azure:
        management:
             subscription.id: XXX-XXX-XXX-XXX
             cloud.service.name: es-demo-app
             keystore:
                   path: /path/to/azurekeystore.pkcs12
                   password: WHATEVER
                   type: pkcs12

discovery:
    seed_providers: azure
```

::::{admonition} Binding the network host
:class: important

The keystore file must be placed in a directory accessible by Elasticsearch like the `config` directory.

It’s important to define `network.host` as by default it’s bound to `localhost`.

You can use [core network host settings](/reference/elasticsearch/configuration-reference/networking-settings.md). For example `_en0_`.

::::


## How to start (short story) [discovery-azure-classic-short]

* Create Azure instances
* Install Elasticsearch
* Install Azure plugin
* Modify `elasticsearch.yml` file
* Start Elasticsearch


## Azure credential API settings [discovery-azure-classic-settings]

The following are a list of settings that can further control the credential API:

`cloud.azure.management.keystore.path`
:   /path/to/keystore

`cloud.azure.management.keystore.type`
:   `pkcs12`, `jceks` or `jks`. Defaults to `pkcs12`.

`cloud.azure.management.keystore.password`
:   your_password for the keystore

`cloud.azure.management.subscription.id`
:   your_azure_subscription_id

`cloud.azure.management.cloud.service.name`
:   your_azure_cloud_service_name. This is the cloud service name/DNS but without the `cloudapp.net` part. So if the DNS name is `abc.cloudapp.net` then the `cloud.service.name` to use is just `abc`.


## Advanced settings [discovery-azure-classic-settings-advanced]

The following are a list of settings that can further control the discovery:

`discovery.azure.host.type`
:   Either `public_ip` or `private_ip` (default). Azure discovery will use the one you set to ping other nodes.

`discovery.azure.endpoint.name`
:   When using `public_ip` this setting is used to identify the endpoint name used to forward requests to Elasticsearch (aka transport port name). Defaults to `elasticsearch`. In Azure management console, you could define an endpoint `elasticsearch` forwarding for example requests on public IP on port 8100 to the virtual machine on port 9300.

`discovery.azure.deployment.name`
:   Deployment name if any. Defaults to the value set with `cloud.azure.management.cloud.service.name`.

`discovery.azure.deployment.slot`
:   Either `staging` or `production` (default).

For example:

```yaml
discovery:
    type: azure
    azure:
        host:
            type: private_ip
        endpoint:
            name: elasticsearch
        deployment:
            name: your_azure_cloud_service_name
            slot: production
```


