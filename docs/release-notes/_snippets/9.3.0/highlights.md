## 9.3.0 [elasticsearch-9.3.0-highlights]

::::{dropdown} New Cloud Connect UI for self-managed installations.
Adds Cloud Connect functionality to Kibana, which allows you to use cloud solutions like AutoOps and Elastic Inference Service in your self-managed Elasticsearch clusters.

::::

**Mapping**

::::{dropdown} Enable doc_values skippers.
Doc_values skippers add a sparse index to doc_values fields, allowing efficient querying and filtering on a field without having to build a separate BKD or terms index.  These are now enabled automatically on any field configured with  and  if the index setting  is set to  (default , or  for TSDB indexes). TSDB indexes now default to using skippers in place of indexes for their , , and   fields, greatly reducing their on-disk footprint.  To disable skippers in TSDB indexes, set  to .

For more information, check [#138723](https://github.com/elastic/elasticsearch/pull/138723).

::::
