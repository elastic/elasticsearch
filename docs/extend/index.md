---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/plugin-authors.html
---

# Create Elasticsearch plugins [plugin-authors]

{{es}} plugins are modular bits of code that add functionality to {{es}}. Plugins are written in Java and implement Java interfaces that are defined in the source code. Plugins are composed of JAR files and metadata files, compressed in a single zip file.

There are two ways to create a plugin:

[Creating text analysis plugins with the stable plugin API](/extend/creating-stable-plugins.md)
:   Text analysis plugins can be developed against the stable plugin API to provide {{es}} with custom Lucene analyzers, token filters, character filters, and tokenizers.

[Creating classic plugins](/extend/creating-classic-plugins.md)
:   Other plugins can be developed against the classic plugin API to provide custom authentication, authorization, or scoring mechanisms, and more.



