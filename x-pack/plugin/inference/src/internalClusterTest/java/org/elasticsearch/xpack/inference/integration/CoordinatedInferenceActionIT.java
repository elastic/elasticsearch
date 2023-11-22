/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.notNullValue;

public class CoordinatedInferenceActionIT extends ESIntegTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        // entries.addAll(new MlDataFrameAnalysisNamedXContentProvider().getNamedXContentParsers());
        return new NamedXContentRegistry(entries);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InferencePlugin.class, MachineLearning.class, TestInferenceServicePlugin.class);
    }

    // TODO is this necessary?
    @Override
    protected Function<Client, Client> getClientWrapper() {
        final Map<String, String> headers = Map.of(
            "Authorization",
            basicAuthHeaderValue("x_pack_rest_user", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        // we need to wrap node clients because we do not specify a user for nodes and all requests will use the system
        // user. This is ok for internal n2n stuff but the test framework does other things like wiping indices, repositories, etc
        // that the system user cannot do. so we wrap the node client with a user that can do these things since the client() calls
        // return a node client
        return client -> client.filterWithHeader(headers);
    }

    public void testMultipleModels() throws IOException {
        // createDfaModel("dfa_regression");
        // createInferenceServiceModel("inference_service_model");
    }

    private void createDfaModel(String modelId) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, REGRESSION_CONFIG)) {
            var dfaModelConfig = PutTrainedModelAction.Request.parseRequest(modelId, false, false, parser);
            var putResponse = client().execute(PutTrainedModelAction.INSTANCE, dfaModelConfig).actionGet();
            assertThat(putResponse.getResponse(), notNullValue());
        }
    }

    private void createInferenceServiceModel(String modelId) {
        MockInferenceServiceIT.putMockService(client(), modelId, "mock_service", TaskType.SPARSE_EMBEDDING);
    }

    // Copied from {@link org.elasticsearch.xpack.ml.integration.InferenceIT}
    // TODO it should be possible to share this code rather than copying it
    private static final String REGRESSION_DEFINITION = """
        {  "preprocessors": [
            {
              "one_hot_encoding": {
                "field": "col1",
                "hot_map": {
                  "male": "col1_male",
                  "female": "col1_female"
                }
              }
            },
            {
              "target_mean_encoding": {
                "field": "col2",
                "feature_name": "col2_encoded",
                "target_map": {
                  "S": 5.0,
                  "M": 10.0,
                  "L": 20
                },
                "default_value": 5.0
              }
            },
            {
              "frequency_encoding": {
                "field": "col3",
                "feature_name": "col3_encoded",
                "frequency_map": {
                  "none": 0.75,
                  "true": 0.10,
                  "false": 0.15
                }
              }
            }
          ],
          "trained_model": {
            "ensemble": {
              "feature_names": [
                "col1_male",
                "col1_female",
                "col2_encoded",
                "col3_encoded",
                "col4"
              ],
              "aggregate_output": {
                "weighted_sum": {
                  "weights": [
                    0.5,
                    0.5
                  ]
                }
              },
              "target_type": "regression",
              "trained_models": [
                {
                  "tree": {
                    "feature_names": [
                      "col1_male",
                      "col1_female",
                      "col4"
                    ],
                    "tree_structure": [
                      {
                        "node_index": 0,
                        "split_feature": 0,
                        "split_gain": 12.0,
                        "threshold": 10.0,
                        "decision_type": "lte",
                        "number_samples": 300,
                        "default_left": true,
                        "left_child": 1,
                        "right_child": 2
                      },
                      {
                        "node_index": 1,
                        "number_samples": 100,
                        "leaf_value": 1
                      },
                      {
                        "node_index": 2,
                        "number_samples": 200,
                        "leaf_value": 2
                      }
                    ],
                    "target_type": "regression"
                  }
                },
                {
                  "tree": {
                    "feature_names": [
                      "col2_encoded",
                      "col3_encoded",
                      "col4"
                    ],
                    "tree_structure": [
                      {
                        "node_index": 0,
                        "split_feature": 0,
                        "split_gain": 12.0,
                        "threshold": 10.0,
                        "decision_type": "lte",
                        "default_left": true,
                        "number_samples": 150,
                        "left_child": 1,
                        "right_child": 2
                      },
                      {
                        "node_index": 1,
                        "number_samples": 50,
                        "leaf_value": 1
                      },
                      {
                        "node_index": 2,
                        "number_samples": 100,
                        "leaf_value": 2
                      }
                    ],
                    "target_type": "regression"
                  }
                }
              ]
            }
          }
        }""";

    public static final String REGRESSION_CONFIG = Strings.format("""
        {
            "input": {
                "field_names": [
                    "col1",
                    "col2",
                    "col3",
                    "col4"
                ]
            },
            "description": "test model for regression",
            "inference_config": {
                "regression": {}
            },
            "definition": %s
        }""", REGRESSION_DEFINITION);
}
