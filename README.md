elasticsearch-license
=====================

Internal Elasticsearch Licensing Plugin


## Licensing REST APIs [PUBLIC]

Licensing REST APIs enable users to:

 - register signed license(s) to their deployment provided by elasticsearch
 - view current status of signed and one-time-trial licenses effective in their deployment

see [Licensing REST APIs] (https://github.com/elasticsearch/elasticsearch-license/blob/es_integration/docs/public/license.asciidoc)

## License Tools [PRIVATE]

License tools are a collection of command-line tools to generate key-pair and signed license(s). It also provides
a tool to verify issued signed license(s) and generate effective license file from a collection of issued license files.

see [License Tools Usage & Reference] (https://github.com/elasticsearch/elasticsearch-license/tree/es_integration/docs/private/license-tools.asciidoc)

## Licensing Consumer Interface [PRIVATE]

Licensing Consumer Interface defines how consumer plugins should enforce licensing on a feature in question using the license-plugin.

see [Licensing Consumer Interface] (https://github.com/elasticsearch/elasticsearch-license/tree/es_integration/docs/private/license-consumer-interface.asciidoc)

## Licensing Plugin Design [PRIVATE]

see [Licensing Plugin Design] (https://github.com/elasticsearch/elasticsearch-license/blob/es_integration/docs/private/license-plugin-design.asciidoc)

## Licensing Test Plan [PRIVATE]

see [Licensing Test Plan] (https://github.com/elasticsearch/elasticsearch-license/blob/es_integration/docs/private/license-plugin-guarantees.asciidoc)