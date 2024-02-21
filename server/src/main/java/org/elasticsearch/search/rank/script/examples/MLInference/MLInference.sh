#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0 and the Server Side Public License, v 1; you may not use this file except
# in compliance with, at your election, the Elastic License 2.0 or the Server
# Side Public License, v 1.
#


# Demonstrates re-ranking using ELSER text-expansion on the top 10 docs returned by a Bool query.
curl -X GET -u elastic:password "localhost:9200/demo/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "retriever": {
    "script_rank": {
      "retrievers": [
        {
          "standard": {
            "query": {
              "bool": {
                "should": [
                  {"term": {"kw": {"value": "one"}}},
                  {"term": {"kw": {"value": "two"}}},
                  {"term": {"kw": {"value": "three"}}}
                ]
              }
            }
          }
        }
      ],
      "window_size": 10,
      "queries":[
        {
          "text_expansion":{
            "content_embedding":{
               "model_id":".elser_model_2",
               "model_text":"apache lucene"
            }
          }
        }
      ],
      "script": {
          "source": "StringBuilder sb = new StringBuilder(); List output = []; for (ScriptRankDoc scriptRankDoc : inputs[0]) { sb.append(scriptRankDoc.queryScores()[0]); sb.append(\"|\"); float newScore = scriptRankDoc.queryScores()[0]; output.add(new ScoreDoc(scriptRankDoc.scoreDoc().doc, newScore, scriptRankDoc.scoreDoc().shardIndex)); sb.append(output.get(output.size() - 1)); sb.append(\"|\");} /*if (output != null) throw new IllegalArgumentException(sb.toString());*/ output.sort((ScoreDoc sd1, ScoreDoc sd2) -> { return sd1.score < sd2.score ? 1 : -1; }); return output;"
      }
    }
  },
  "_source": false,
  "fields": ["kw", "v"]
}
'
