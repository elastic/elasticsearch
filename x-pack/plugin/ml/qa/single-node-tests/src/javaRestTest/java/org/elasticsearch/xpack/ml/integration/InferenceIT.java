/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Strings;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class InferenceIT extends InferenceTestCase {

    private static final String MODEL_ID = "a-complex-regression-model";

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
    }

    @SuppressWarnings("unchecked")
    public void testInference() throws Exception {
        Response response = infer("a-complex-regression-model", "{\"item\": \"TV\", \"avg_cost\": 300}");
        assertThat(
            (List<Double>) XContentMapValues.extractValue("inference_results.regression-value", responseAsMap(response)),
            equalTo(List.of(2.0))
        );
    }

    @SuppressWarnings("unchecked")
    public void testAllFieldsMissingWarning() throws IOException {
        Response response = infer("a-complex-regression-model", "{}");
        assertThat(
            (List<String>) XContentMapValues.extractValue("inference_results.warning", responseAsMap(response)),
            equalTo(List.of("Model [a-complex-regression-model] could not be inferred as all fields were missing"))
        );
    }

    private Response infer(String modelId, String body) throws IOException {
        Request request = new Request("POST", "/_ml/trained_models/" + modelId + "/_infer");
        request.setJsonEntity(Strings.format("""
            {  "docs": [%s] }
            """, body));
        return client().performRequest(request);
    }

}
