## Logstash Bridge

This package contains bridge functionality to ensure that Logstash's Elastic Integration plugin
has access to the minimal subset of Elasticsearch to perform its functions without relying on
other Elasticsearch internals.

If a change is introduced in a separate Elasticsearch project that causes this project to fail,
please consult with members of @elastic/logstash to chart a path forward.
