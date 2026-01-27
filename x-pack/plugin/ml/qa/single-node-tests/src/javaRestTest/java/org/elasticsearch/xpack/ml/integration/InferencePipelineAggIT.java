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

import static org.hamcrest.Matchers.contains;

public class InferencePipelineAggIT extends InferenceTestCase {

    private static final String MODEL_ID = "a-complex-regression-model";
    private static final String INDEX_NAME = "store";

    @Before
    public void setupModelAndData() throws IOException {
        putRegressionModel(MODEL_ID, """
            {
                        "description": "super complex model for tests",
                        "input": {"field_names": ["avg_cost", "item"]},
                        "inference_config": {
                          "regression": {
                            "results_field": "regression-value",
                            "num_top_feature_importance_values": 2
                          }
                        },
                        "definition": {
                          "preprocessors" : [{
                            "one_hot_encoding": {
                              "field": "product_type",
                              "hot_map": {
                                "TV": "type_tv",
                                "VCR": "type_vcr",
                                "Laptop": "type_laptop"
                              }
                            }
                          }],
                          "trained_model": {
                            "ensemble": {
                              "feature_names": [],
                              "target_type": "regression",
                              "trained_models": [
                              {
                                "tree": {
                                  "feature_names": [
                                    "avg_cost", "type_tv", "type_vcr", "type_laptop"
                                  ],
                                  "tree_structure": [
                                  {
                                    "node_index": 0,
                                    "split_feature": 0,
                                    "split_gain": 12,
                                    "threshold": 38,
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
                              }
                              ]
                            }
                          }
                        }
                      }""");
        createIndex(INDEX_NAME, Settings.EMPTY, """
            "properties":{
             "product":{"type": "keyword"},
             "cost":{"type": "integer"},
             "time": {"type": "date"}}""");
        indexData("{ \"product\": \"TV\", \"cost\": 300, \"time\": 1587501233000 }");
        indexData("{ \"product\": \"TV\", \"cost\": 400, \"time\": 1587501233000}");
        indexData("{ \"product\": \"VCR\", \"cost\": 150, \"time\": 1587501233000 }");
        indexData("{ \"product\": \"VCR\", \"cost\": 180, \"time\": 1587501233000 }");
        indexData("{ \"product\": \"Laptop\", \"cost\": 15000, \"time\": 1587501233000 }");
        adminClient().performRequest(new Request("POST", INDEX_NAME + "/_refresh"));
    }

    private void indexData(String data) throws IOException {
        Request request = new Request("POST", INDEX_NAME + "/_doc");
        request.setJsonEntity(data);
        client().performRequest(request);
    }

    private Response search(String searchBody) throws IOException {
        Request request = new Request("POST", INDEX_NAME + "/_search");
        request.setJsonEntity(searchBody);
        return client().performRequest(request);
    }

    @SuppressWarnings("unchecked")
    public void testPipelineRegressionSimple() throws Exception {
        Response searchResponse = search("""
            {
                        "size": 0,
                        "aggs": {
                          "good": {
                            "terms": {
                              "field": "product",
                              "size": 10
                            },
                            "aggs": {
                              "avg_cost_agg": {
                                "avg": {
                                  "field": "cost"
                                }
                              },
                              "regression_agg": {
                                "inference": {
                                  "model_id": "a-complex-regression-model",
                                  "inference_config": {
                                    "regression": {
                                      "results_field": "value"
                                    }
                                  },
                                  "buckets_path": {
                                    "avg_cost": "avg_cost_agg"
                                  }
                                }
                              }
                            }
                          }
                        }
                      }""");
        assertThat(
            (List<Double>) XContentMapValues.extractValue("aggregations.good.buckets.regression_agg.value", responseAsMap(searchResponse)),
            contains(2.0, 2.0, 2.0)
        );
    }

    @SuppressWarnings("unchecked")
    public void testPipelineAggReferencingSingleBucket() throws Exception {
        Response searchResponse = search("""
            {
                          "size": 0,
                          "query": {
                            "match_all": {}
                          },
                          "aggs": {
                            "date_histo": {
                              "date_histogram": {
                                "field": "time",
                                "fixed_interval": "1d"
                              },
                              "aggs": {
                                "good": {
                                  "terms": {
                                    "field": "product",
                                    "size": 10
                                  },
                                  "aggs": {
                                    "avg_cost_agg": {
                                      "avg": {
                                        "field": "cost"
                                      }
                                    }
                                  }
                                },
                                "regression_agg": {
                                  "inference": {
                                    "model_id": "a-complex-regression-model",
                                    "buckets_path": {
                                      "avg_cost": "good['TV']>avg_cost_agg",
                                      "product_type": "good['TV']"
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }""");
        assertThat(
            (List<Double>) XContentMapValues.extractValue(
                "aggregations.date_histo.buckets.regression_agg.value",
                responseAsMap(searchResponse)
            ),
            contains(2.0)
        );
    }

    @SuppressWarnings("unchecked")
    public void testAllFieldsMissingWarning() throws IOException {
        Response searchResponse = search("""
            {
                        "size": 0,
                        "query": { "match_all" : { } },
                        "aggs": {
                          "good": {
                            "terms": {
                              "field": "product",
                              "size": 10
                            },
                            "aggs": {
                              "avg_cost_agg": {
                                "avg": {
                                  "field": "cost"
                                }
                              },
                              "regression_agg" : {
                                "inference": {
                                  "model_id": "a-complex-regression-model",
                                  "buckets_path": {
                                    "cost" : "avg_cost_agg"
                                  }
                                }
                              }
                            }
                          }
                        }
                      }""");
        assertThat(
            (List<String>) XContentMapValues.extractValue(
                "aggregations.good.buckets.regression_agg.warning",
                responseAsMap(searchResponse)
            ),
            contains(
                "Model [a-complex-regression-model] could not be inferred as all fields were missing",
                "Model [a-complex-regression-model] could not be inferred as all fields were missing",
                "Model [a-complex-regression-model] could not be inferred as all fields were missing"
            )
        );
    }

}
