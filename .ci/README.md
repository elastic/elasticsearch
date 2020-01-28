CI configuration for Elasticsearch
==================================

Layout and Syntax
-----------------

CI is run by Jenkins at [elasticsearch-ci](https://elasticsearch-ci.elastic.co/).
Jobs live in the [jobs.t](jobs.t) directory, these are defined in YML using a syntax 
simmilar to [JJB](https://elasticsearch-ci.elastic.co/view/Elasticsearch%20master/).
Macros are not allowed, and each job must be defined in its own file.
Merging of the default configuration is customized so unlike in standard JJB,
it recurses into YML objects. 
Further (internal) documentation  on the setup 
[is available](https://github.com/elastic/infra/blob/master/flavortown/jjbb/README.md) 
.

