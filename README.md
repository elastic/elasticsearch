Azure Cloud Plugin for ElasticSearch
====================================

The Azure Cloud plugin allows to use Azure API for the unicast discovery mechanism.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-cloud-azure/1.0.0`.

    -----------------------------------------
    | Azure Cloud Plugin | ElasticSearch    |
    -----------------------------------------
    | master             | 0.90 -> master   |
    -----------------------------------------
    | 1.0.0              | 0.20.6           |
    -----------------------------------------


Azure Virtual Machine Discovery
-------------------------------

Azure VM discovery allows to use the azure APIs to perform automatic discovery (similar to multicast in non hostile
multicast environments). Here is a simple sample configuration:

```
    cloud:
        azure:
            private_key: /path/to/private.key
            certificate: /path/to/azure.certficate
            password: your_password_for_pk
            subscription_id: your_azure_subscription_id
    discovery:
            type: azure
```

How to start
------------

Short story:

    * Create Azure instances
    * Open 9300 port
    * Install Elasticsearch
    * Install Azure plugin
    * Modify `elasticsearch.yml` file
    * Start Elasticsearch

Long story:

    See [tutorial](http://www.elasticsearch.org/tutorials/2013/04/02/elasticsearch-on-azure/).

License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2012 Shay Banon and ElasticSearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
