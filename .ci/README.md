CI configuration for Elasticsearch
==================================

Layout and Syntax
-----------------

CI is run by Jenkins at [elasticsearch-ci](https://elasticsearch-ci.elastic.co/).
Jobs live in the [jobs.t](jobs.t) directory, these are defined in YML using a syntax 
simmilar to [JJB](https://elasticsearch-ci.elastic.co/view/Elasticsearch%20master/).
Macros are not allowed, and each job must be defined in it's own file.
Merging of the default configuration is customized so unlike in standard JJB,
it recurses into YML objects. 


