#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0 and the Server Side Public License, v 1; you may not use this file except
# in compliance with, at your election, the Elastic License 2.0 or the Server
# Side Public License, v 1.
#

# Demonstrates a weighted RRF implementation using the Script Reranker.
# The weights are passed in as params to the script: [0.7, 0.3]
curl -X GET -u elastic:password "localhost:9200/demo/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "retriever": {
    "script_rank": {
      "retrievers": [
        {
          "standard": {"query": {"term": {"kw": {"value": "two"}}}}
        },
        {
          "knn": {"field": "v", "query_vector": [9, 9], "k": 5, "num_candidates": 10}
        }
      ],
      "window_size": 10,
      "script": {
        "source": "def results = [:];for (int retrieverNum = 0; retrieverNum < inputs.size(); ++retrieverNum) {    def retrieverResult = inputs[retrieverNum];    int rank = retrieverResult.size();    for (ScriptRankDoc scriptRankDoc : retrieverResult) {        ScoreDoc scoreDoc = scriptRankDoc.scoreDoc();        results.compute(new RankKey(scoreDoc.doc, scoreDoc.shardIndex), (key, value) -> {            def v = value;            if (v == null) {                v = new ScoreDoc(scoreDoc.doc, 0f, scoreDoc.shardIndex);            }            v.score += params.weights[retrieverNum] * (1.0 / (params.k + rank));            return v;        });        --rank;    }}def output = new ArrayList(results.values());output.sort((ScoreDoc sd1, ScoreDoc sd2) -> { return sd1.score < sd2.score ? 1 : -1; });return output;",
        "params": {
          "weights": [0.7, 0.3],
          "k": 20
        }
      }
    }
  },
  "_source": false,
  "fields": ["kw", "v"]
}
'
