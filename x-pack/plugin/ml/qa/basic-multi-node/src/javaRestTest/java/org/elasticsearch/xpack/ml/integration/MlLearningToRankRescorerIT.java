/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class MlLearningToRankRescorerIT extends ESRestTestCase {

    private static final String MODEL_ID = "basic-ltr-model";
    private static final String INDEX_NAME = "store";

    @Before
    public void setupModelAndData() throws IOException {
        putLearningToRankModel(MODEL_ID, """
            {
              "description": "super complex model for tests",
              "inference_config": {
                "learning_to_rank": {
                  "feature_extractors": [
                    {
                      "query_extractor": {
                        "feature_name": "cost",
                        "query": {"script_score": {"query": {"match_all":{}}, "script": {"source": "return doc['cost'].value;"}}}
                      }
                    },
                    {
                      "query_extractor": {
                        "feature_name": "type_tv",
                        "query": {"constant_score": {"filter": {"term": { "product": "TV" }}, "boost": 1.0}}
                      }
                    },
                    {
                      "query_extractor": {
                        "feature_name": "type_vcr",
                        "query": {"constant_score": {"filter": {"term": { "product": "VCR" }}, "boost": 1.0}}
                      }
                    },
                    {
                      "query_extractor": {
                        "feature_name": "type_laptop",
                        "query": {"constant_score": {"filter": {"term": { "product": "Laptop" }}, "boost": 1.0}}
                      }
                    },
                    {
                        "query_extractor": {
                            "feature_name": "two",
                            "query": { "script_score": { "query": { "match_all": {} }, "script": { "source": "return 2.0;" } } }
                        }
                    },
                    {
                        "query_extractor": {
                            "feature_name": "product_bm25",
                            "query": { "term": { "product": "{{keyword}}" } }
                        }
                    }
                  ]
                }
              },
              "definition": {
                "trained_model": {
                  "ensemble": {
                    "feature_names": ["cost", "type_tv", "type_vcr", "type_laptop", "two", "product_bm25"],
                    "target_type": "regression",
                    "trained_models": [
                    {
                      "tree": {
                        "feature_names": [
                          "cost"
                        ],
                        "tree_structure": [
                        {
                          "node_index": 0,
                          "split_feature": 0,
                          "split_gain": 12,
                          "threshold": 400,
                          "decision_type": "lte",
                          "default_left": true,
                          "left_child": 1,
                          "right_child": 2
                        },
                        {
                          "node_index": 1,
                          "leaf_value": 5.0
                        },
                        {
                          "node_index": 2,
                          "leaf_value": 2.0
                        }
                        ],
                        "target_type": "regression"
                      }
                    },
                    {
                      "tree": {
                        "feature_names": [
                          "type_tv"
                        ],
                        "tree_structure": [
                        {
                          "node_index": 0,
                          "split_feature": 0,
                          "split_gain": 12,
                          "threshold": 1,
                          "decision_type": "lt",
                          "default_left": true,
                          "left_child": 1,
                          "right_child": 2
                        },
                        {
                          "node_index": 1,
                          "leaf_value": 1.0
                        },
                        {
                          "node_index": 2,
                          "leaf_value": 12.0
                        }
                        ],
                        "target_type": "regression"
                      }
                    },
                     {
                      "tree": {
                        "feature_names": [
                          "two"
                        ],
                        "tree_structure": [
                        {
                          "node_index": 0,
                          "split_feature": 0,
                          "split_gain": 12,
                          "threshold": 1,
                          "decision_type": "lt",
                          "default_left": true,
                          "left_child": 1,
                          "right_child": 2
                        },
                        {
                          "node_index": 1,
                          "leaf_value": 1.0
                        },
                        {
                          "node_index": 2,
                          "leaf_value": 2.0
                        }
                        ],
                        "target_type": "regression"
                      }
                    },
                     {
                      "tree": {
                        "feature_names": [
                          "product_bm25"
                        ],
                        "tree_structure": [
                        {
                          "node_index": 0,
                          "split_feature": 0,
                          "split_gain": 12,
                          "threshold": 1,
                          "decision_type": "lt",
                          "default_left": true,
                          "left_child": 1,
                          "right_child": 2
                        },
                        {
                          "node_index": 1,
                          "leaf_value": 1.0
                        },
                        {
                          "node_index": 2,
                          "leaf_value": 4.0
                        }
                        ],
                        "target_type": "regression"
                      }
                    }
                    ]
                  }
                }
              }
            }
            """);
        createIndex(INDEX_NAME, Settings.builder().put("number_of_shards", randomIntBetween(1, 3)).build(), """
            "properties":{
                "product":{ "type": "keyword" },
                "cost":{ "type": "integer" }
            }""");
        indexData("{ \"product\": \"TV\", \"cost\": 300 }");
        indexData("{ \"product\": \"TV\", \"cost\": 400 }");
        indexData("{ \"product\": \"VCR\", \"cost\": 150 }");
        indexData("{ \"product\": \"VCR\", \"cost\": 180 }");
        indexData("{ \"product\": \"Laptop\", \"cost\": 15000 }");
        refreshAllIndices();
    }

    @SuppressWarnings("unchecked")
    public void testLtrSimple() throws Exception {
        Response searchResponse = search("""
            {
                "query": {
                    "match": { "product": { "query": "TV" } }
                },
                "rescore": {
                    "window_size": 10,
                    "learning_to_rank": {
                        "model_id": "basic-ltr-model"
                    }
                }
            }""");

        Map<String, Object> response = responseAsMap(searchResponse);
        assertThat((List<Double>) XContentMapValues.extractValue("hits.hits._score", response), contains(20.0, 20.0));
    }

    @SuppressWarnings("unchecked")
    public void testLtrSimpleDFS() throws Exception {
        Response searchResponse = searchDfs("""
            {
                "query": {
                    "match": { "product": { "query": "TV" } }
                },
                "rescore": {
                    "window_size": 10,
                    "learning_to_rank": {
                        "model_id": "basic-ltr-model",
                        "params": { "keyword": "TV" }
                    }
                }
            }""");

        Map<String, Object> response = responseAsMap(searchResponse);
        assertThat(response.toString(), (List<Double>) XContentMapValues.extractValue("hits.hits._score", response), contains(20.0, 20.0));

        searchResponse = searchDfs("""
            {
                "rescore": {
                    "window_size": 10,
                    "learning_to_rank": {
                        "model_id": "basic-ltr-model",
                        "params": { "keyword": "TV" }
                    }
                }
            }""");

        response = responseAsMap(searchResponse);
        assertThat(
            response.toString(),
            (List<Double>) XContentMapValues.extractValue("hits.hits._score", response),
            contains(20.0, 20.0, 9.0, 9.0, 6.0)
        );
    }

    @SuppressWarnings("unchecked")
    public void testLtrSimpleEmpty() throws Exception {
        Response searchResponse = search("""
            {
                "query": {
                    "term": { "product": "computer" }
                },
                "rescore": {
                    "window_size": 10,
                    "learning_to_rank": {
                        "model_id": "basic-ltr-model"
                    }
                }
            }""");

        Map<String, Object> response = responseAsMap(searchResponse);
        assertThat((List<Double>) XContentMapValues.extractValue("hits.hits._score", response), empty());
    }

    @SuppressWarnings("unchecked")
    public void testLtrEmptyDFS() throws Exception {
        Response searchResponse = searchDfs("""
            {
                "query": {
                    "match": { "product": { "query": "computer"} }
                },
                "rescore": {
                    "window_size": 10,
                    "learning_to_rank": {
                        "model_id": "basic-ltr-model"
                    }
                }
            }""");

        Map<String, Object> response = responseAsMap(searchResponse);
        assertThat(response.toString(), (List<Double>) XContentMapValues.extractValue("hits.hits._score", response), empty());
    }

    @SuppressWarnings("unchecked")
    public void testLtrCanMatch() throws Exception {
        Response searchResponse = searchCanMatch("""
            {
                "query": {
                    "match": { "product": { "query": "TV" } }
                },
                "rescore": {
                    "window_size": 10,
                    "learning_to_rank": {
                        "model_id": "basic-ltr-model"
                    }
                }
            }""", false);

        Map<String, Object> response = responseAsMap(searchResponse);
        assertThat(response.toString(), (List<Double>) XContentMapValues.extractValue("hits.hits._score", response), contains(20.0, 20.0));

        searchResponse = searchCanMatch("""
            {
                "query": {
                    "match": { "product": { "query": "TV" } }
                },
                "rescore": {
                    "window_size": 10,
                    "learning_to_rank": {
                        "model_id": "basic-ltr-model"
                    }
                }

            }""", true);

        response = responseAsMap(searchResponse);
        assertThat(response.toString(), (List<Double>) XContentMapValues.extractValue("hits.hits._score", response), contains(20.0, 20.0));
    }

    @SuppressWarnings("unchecked")
    public void testModelCacheIsFlushedOnModelChange() throws IOException {
        String searchBody = """
            {
                "rescore": {
                    "window_size": 10,
                    "learning_to_rank": {
                        "model_id": "basic-ltr-model"
                    }
                }
            }""";

        Response searchResponse = searchDfs(searchBody);
        Map<String, Object> response = responseAsMap(searchResponse);
        assertThat(
            response.toString(),
            (List<Double>) XContentMapValues.extractValue("hits.hits._score", response),
            contains(20.0, 20.0, 9.0, 9.0, 6.0)
        );

        deleteLearningToRankModel(MODEL_ID);
        putLearningToRankModel(MODEL_ID, """
            {
              "inference_config": {
                "learning_to_rank": {
                  "feature_extractors": [
                    {
                      "query_extractor": {
                        "feature_name": "cost",
                        "query": {
                          "script_score": {
                            "query": { "match_all": {} },
                            "script": { "source": "return doc[\\"cost\\"].value" }
                          }
                        }
                      }
                    }
                  ]
                }
              },
              "definition": {
                "trained_model": {
                  "ensemble": {
                    "feature_names": ["cost"],
                    "target_type": "regression",
                    "trained_models": [
                      {
                        "tree": {
                          "feature_names": ["cost"],
                          "tree_structure": [
                            {
                              "node_index": 0,
                              "split_feature": 0,
                              "split_gain": 12,
                              "threshold": 1000,
                              "decision_type": "lt",
                              "default_left": true,
                              "left_child": 1,
                              "right_child": 2
                            },
                            {
                              "node_index": 1,
                              "leaf_value": 1.0
                            },
                            {
                              "node_index": 2,
                              "leaf_value": 10
                            }
                          ],
                          "target_type": "regression"
                        }
                      }
                    ]
                  }
                }
              }
            }
            """);

        searchResponse = searchDfs(searchBody);
        response = responseAsMap(searchResponse);
        assertThat(
            response.toString(),
            (List<Double>) XContentMapValues.extractValue("hits.hits._score", response),
            contains(10.0, 1.0, 1.0, 1.0, 1.0)
        );
    }

    private void indexData(String data) throws IOException {
        Request request = new Request("POST", INDEX_NAME + "/_doc");
        request.setJsonEntity(data);
        client().performRequest(request);
    }

    private Response search(String searchBody) throws IOException {
        Request request = new Request("POST", INDEX_NAME + "/_search?request_cache=false");
        request.setJsonEntity(searchBody);
        return client().performRequest(request);
    }

    private Response searchDfs(String searchBody) throws IOException {
        Request request = new Request("POST", INDEX_NAME + "/_search?search_type=dfs_query_then_fetch&request_cache=false");
        request.setJsonEntity(searchBody);
        return client().performRequest(request);
    }

    private Response searchCanMatch(String searchBody, boolean dfs) throws IOException {
        Request request = dfs
            ? new Request("POST", INDEX_NAME + "/_search?search_type=dfs_query_then_fetch&request_cache=false&pre_filter_shard_size=1")
            : new Request("POST", INDEX_NAME + "/_search?request_cache=false&pre_filter_shard_size=1");
        request.setJsonEntity(searchBody);
        return client().performRequest(request);
    }

    private void deleteLearningToRankModel(String modelId) throws IOException {
        Request model = new Request("DELETE", "_ml/trained_models/" + modelId);
        assertThat(client().performRequest(model).getStatusLine().getStatusCode(), equalTo(200));
    }

    private void putLearningToRankModel(String modelId, String body) throws IOException {
        Request model = new Request("PUT", "_ml/trained_models/" + modelId);
        model.setJsonEntity(body);
        assertThat(client().performRequest(model).getStatusLine().getStatusCode(), equalTo(200));
    }
}
