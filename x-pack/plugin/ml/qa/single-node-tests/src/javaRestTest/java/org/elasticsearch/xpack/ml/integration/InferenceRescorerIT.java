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
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class InferenceRescorerIT extends InferenceTestCase {

    private static final String MODEL_ID = "ltr-model";
    private static final String INDEX_NAME = "store";

    @Before
    public void setupModelAndData() throws IOException {
        putRegressionModel(MODEL_ID, """
            {
               "description": "super complex model for tests",
               "input": {"field_names": ["cost", "product"]},
               "inference_config": {
                 "learn_to_rank": {
                   "feature_extractors": [{
                     "query_extractor": {
                       "feature_name": "two",
                       "query": {"script_score": {"query": {"match_all":{}}, "script": {"source": "return 2.0;"}}}
                     }
                   }]
                 }
               },
               "definition": {
                 "preprocessors" : [{
                   "one_hot_encoding": {
                     "field": "product",
                     "hot_map": {
                       "TV": "type_tv",
                       "VCR": "type_vcr",
                       "Laptop": "type_laptop"
                     }
                   }
                 }],
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
             }""");
        createIndex(INDEX_NAME, Settings.EMPTY, """
            "properties":{
             "product":{"type": "keyword"},
             "cost":{"type": "integer"}}""");
        indexData("{ \"product\": \"TV\", \"cost\": 300}");
        indexData("{ \"product\": \"TV\", \"cost\": 400}");
        indexData("{ \"product\": \"TV\", \"cost\": 600}");
        indexData("{ \"product\": \"VCR\", \"cost\": 15}");
        indexData("{ \"product\": \"VCR\", \"cost\": 350}");
        indexData("{ \"product\": \"VCR\", \"cost\": 580}");
        indexData("{ \"product\": \"Laptop\", \"cost\": 100}");
        indexData("{ \"product\": \"Laptop\", \"cost\": 300}");
        indexData("{ \"product\": \"Laptop\", \"cost\": 500}");
        adminClient().performRequest(new Request("POST", INDEX_NAME + "/_refresh"));
    }

    public void testInferenceRescore() throws Exception {
        Request request = new Request("GET", "store/_search?size=3&error_trace");
        request.setJsonEntity("""
            {
              "rescore": {
                "window_size": 10,
                "inference": { "model_id": "ltr-model" }
              }
            }""");
        assertHitScores(client().performRequest(request), List.of(20.0, 20.0, 17.0));
        request.setJsonEntity(
            """
                {
                  "query": {"term": {"product": "Laptop"}},
                  "rescore": {
                    "window_size": 10,
                    "inference": {
                      "model_id": "ltr-model",
                      "inference_config": {
                        "learn_to_rank": {
                          "feature_extractors":[{
                            "query_extractor": {"feature_name": "product_bm25", "query": {"term": {"product": "Laptop"}}}
                          }]
                        }
                      }
                    }
                  }
                }"""
        );
        assertHitScores(client().performRequest(request), List.of(12.0, 12.0, 9.0));
        request.setJsonEntity("""
            {
              "query": {"term": {"product": "Laptop"}},
              "rescore": {
                "window_size": 10,
                "inference": { "model_id": "ltr-model"}
              }
            }""");
        assertHitScores(client().performRequest(request), List.of(9.0, 9.0, 6.0));
    }

    public void testInferenceRescoreSmallWindow() throws Exception {
        Request request = new Request("GET", "store/_search?size=5");
        request.setJsonEntity("""
            {
              "rescore": {
                "window_size": 2,
                "inference": { "model_id": "ltr-model" }
              }
            }""");
        assertHitScores(client().performRequest(request), List.of(20.0, 20.0, 1.0, 1.0, 1.0));
    }

    public void testInferenceRescorerWithChainedRescorers() throws IOException {
        Request request = new Request("GET", "store/_search?size=5");
        request.setJsonEntity("""
            {
               "rescore": [
               {
                 "window_size": 4,
                 "query": { "rescore_query":{ "script_score": {"query": {"match_all": {}}, "script": {"source": "return 4"}}}}
               },
               {
                 "window_size": 3,
                 "inference": { "model_id": "ltr-model" }
               },
               {
                 "window_size": 2,
                 "query": { "rescore_query": { "script_score": {"query": {"match_all": {}}, "script": {"source": "return 20"}}}}
               }
              ]
             }""");
        assertHitScores(client().performRequest(request), List.of(40.0, 40.0, 17.0, 5.0, 1.0));
    }

    private void indexData(String data) throws IOException {
        Request request = new Request("POST", INDEX_NAME + "/_doc");
        request.setJsonEntity(data);
        client().performRequest(request);
    }

    @SuppressWarnings("unchecked")
    private static void assertHitScores(Response response, List<Double> expectedScores) throws IOException {
        assertThat((List<Double>) XContentMapValues.extractValue("hits.hits._score", responseAsMap(response)), equalTo(expectedScores));
    }
}
