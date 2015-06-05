Stempel (Polish) Analysis for Elasticsearch
==================================

The Stempel (Polish) Analysis plugin integrates Lucene stempel (polish) analysis module into elasticsearch.

In order to install the plugin, simply run: 

```sh
bin/plugin install elasticsearch/elasticsearch-analysis-stempel/2.4.3
```

| elasticsearch |  Stempel Analysis Plugin  |   Docs     |  
|---------------|-----------------------|------------|
| master        |  Build from source    | See below  |
| es-1.x        |  Build from source    | [2.6.0-SNAPSHOT](https://github.com/elastic/elasticsearch-analysis-stempel/tree/es-1.x/#version-260-snapshot-for-elasticsearch-1x)  |
| es-1.5        |  2.5.0                | [2.5.0](https://github.com/elastic/elasticsearch-analysis-stempel/tree/v2.5.0/#version-250-for-elasticsearch-15)                  |
|    es-1.4              |     2.4.3         | [2.4.3](https://github.com/elasticsearch/elasticsearch-analysis-stempel/tree/v2.4.3/#version-243-for-elasticsearch-14)                  |
| < 1.4.5       |  2.4.2                | [2.4.2](https://github.com/elastic/elasticsearch-analysis-stempel/tree/v2.4.2/#version-242-for-elasticsearch-14)                  |
| < 1.4.3       |  2.4.1                | [2.4.1](https://github.com/elastic/elasticsearch-analysis-stempel/tree/v2.4.1/#version-241-for-elasticsearch-14)                  |
| es-1.3        |  2.3.0                | [2.3.0](https://github.com/elastic/elasticsearch-analysis-stempel/tree/v2.3.0/#stempel-polish-analysis-for-elasticsearch)  |
| es-1.2        |  2.2.0                | [2.2.0](https://github.com/elastic/elasticsearch-analysis-stempel/tree/v2.2.0/#stempel-polish-analysis-for-elasticsearch)  |
| es-1.1        |  2.1.0                | [2.1.0](https://github.com/elastic/elasticsearch-analysis-stempel/tree/v2.1.0/#stempel-polish-analysis-for-elasticsearch)  |
| es-1.0        |  2.0.0                | [2.0.0](https://github.com/elastic/elasticsearch-analysis-stempel/tree/v2.0.0/#stempel-polish-analysis-for-elasticsearch)  |
| es-0.90       |  1.13.0               | [1.13.0](https://github.com/elastic/elasticsearch-analysis-stempel/tree/v1.13.0/#stempel-polish-analysis-for-elasticsearch)  |

To build a `SNAPSHOT` version, you need to build it with Maven:

```bash
mvn clean install
plugin --install analysis-stempel \
       --url file:target/releases/elasticsearch-analysis-stempel-X.X.X-SNAPSHOT.zip
```

Stempel Plugin
-----------------

The plugin includes the `polish` analyzer and `polish_stem` token filter.

License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2014 Elasticsearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
