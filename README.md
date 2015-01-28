elasticsearch-license
=====================

Elasticsearch Licensing core, tools and plugin

## Core

Contains core data structures, utilities used by **Licensor** and **Plugin**.

See `core/` and `core-shaded/`

## Licensor

Contains a collection of tools to generate key-pairs, licenses and validate licenses.

See `licensor/`

see [wiki] (https://github.com/elasticsearch/elasticsearch-license/wiki) for documentation on
[Licensing Tools Usage & Reference] (https://github.com/elasticsearch/elasticsearch-license/wiki/License-Tools-Usage-&-Reference)

## Plugin

**NOTE**: The license plugin has to be packaged with the right public key when being deployed to public repositories in maven
or uploaded to s3. Use `-Dkeys.path=<PATH_TO_KEY_DIR>` with maven command to package the plugin with a specified key.

See `plugin/`

see [Getting Started] (https://github.com/elasticsearch/elasticsearch-license/blob/master/docs/getting-started.asciidoc) to install license plugin.

see [Licensing REST APIs] (https://github.com/elasticsearch/elasticsearch-license/blob/master/docs/license.asciidoc)
to use the license plugin from an elasticsearch deployment.

see [wiki] (https://github.com/elasticsearch/elasticsearch-license/wiki) for documentation on
 - [License Plugin Consumer Interface] (https://github.com/elasticsearch/elasticsearch-license/wiki/License---Consumer-Interface)
 - [License Plugin Release Process] (https://github.com/elasticsearch/elasticsearch-license/wiki/Plugin-Release-Process)
 - [License Plugin Design] (https://github.com/elasticsearch/elasticsearch-license/wiki/License-Plugin--Design)
