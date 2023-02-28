/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinitionTests;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class InferenceIT extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE_SUPER_USER = UsernamePasswordToken.basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE_SUPER_USER).build();
    }

    @After
    public void cleanUpData() throws Exception {
        new MlRestTestStateCleaner(logger, adminClient()).resetFeatures();
    }

    @SuppressWarnings("unchecked")
    public void testInferRegressionModel() throws Exception {
        String regressionModelId = "test_regression";
        putModel(regressionModelId, REGRESSION_CONFIG);

        int numRequests = randomIntBetween(1, 10);
        List<String> docs = new ArrayList<>(numRequests);
        for (int i = 0; i < numRequests; i++) {
            docs.add(generateSourceDoc());
        }

        Request inferRequest = new Request("POST", "_ml/trained_models/" + regressionModelId + "/_infer");
        inferRequest.setJsonEntity(Strings.format("""
            {
              "docs": [%s]
            }
            """, String.join(",", docs)));
        Response response = client().performRequest(inferRequest);
        Map<String, Object> responseMap = responseAsMap(response);
        List<Double> predictions = (List<Double>) XContentMapValues.extractValue("inference_results.predicted_value", responseMap);
        assertThat(predictions, hasSize(numRequests));

        putModelAlias("foo_regression", regressionModelId);

        inferRequest = new Request("POST", "_ml/trained_models/foo_regression/_infer");
        inferRequest.setJsonEntity(Strings.format("""
            {
              "docs": [%s]
            }
            """, String.join(",", docs)));
        response = client().performRequest(inferRequest);
        responseMap = responseAsMap(response);
        predictions = (List<Double>) XContentMapValues.extractValue("inference_results.predicted_value", responseMap);
        assertThat(predictions, hasSize(numRequests));
    }

    @SuppressWarnings("unchecked")
    public void testInferClassificationModel() throws Exception {
        String classificationModelId = "test_classification";
        putModel(classificationModelId, CLASSIFICATION_CONFIG);

        int numRequests = randomIntBetween(1, 10);
        List<String> docs = new ArrayList<>(numRequests);
        for (int i = 0; i < numRequests; i++) {
            docs.add(generateSourceDoc());
        }

        Request inferRequest = new Request("POST", "_ml/trained_models/" + classificationModelId + "/_infer");
        inferRequest.setJsonEntity(Strings.format("""
            {
              "docs": [%s]
            }
            """, String.join(",", docs)));
        Response response = client().performRequest(inferRequest);
        Map<String, Object> responseMap = responseAsMap(response);
        List<Double> predictions = (List<Double>) XContentMapValues.extractValue("inference_results.predicted_value", responseMap);
        assertThat(predictions, hasSize(numRequests));

        putModelAlias("foo_classification", classificationModelId);

        inferRequest = new Request("POST", "_ml/trained_models/foo_classification/_infer");
        inferRequest.setJsonEntity(Strings.format("""
            {
              "docs": [%s]
            }
            """, String.join(",", docs)));
        response = client().performRequest(inferRequest);
        responseMap = responseAsMap(response);
        predictions = (List<Double>) XContentMapValues.extractValue("inference_results.predicted_value", responseMap);
        assertThat(predictions, hasSize(numRequests));
    }

    @SuppressWarnings("unchecked")
    public void testInferLangIdent() throws Exception {
        Request inferRequest = new Request("POST", "_ml/trained_models/lang_ident_model_1/_infer");
        inferRequest.setJsonEntity("""
            {
              "docs": [{"text": "this is some plain text."}]
            }
            """);
        Response response = client().performRequest(inferRequest);
        Map<String, Object> responseMap = responseAsMap(response);
        List<String> predictions = (List<String>) XContentMapValues.extractValue("inference_results.predicted_value", responseMap);
        assertThat(predictions.get(0), equalTo("en"));
    }

    private String generateSourceDoc() throws IOException {
        Map<String, Object> m = Map.of(
            "col1",
            randomFrom("female", "male"),
            "col2",
            randomFrom("S", "M", "L", "XL"),
            "col3",
            randomFrom("true", "false", "none", "other"),
            "col4",
            randomIntBetween(0, 10)
        );
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(m)) {
            return XContentHelper.convertToJson(BytesReference.bytes(xContentBuilder), false, XContentType.JSON);
        }
    }

    private void putModelAlias(String modelAlias, String newModel) throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + newModel + "/model_aliases/" + modelAlias + "?reassign=true");
        client().performRequest(request);
    }

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

    private static final String REGRESSION_CONFIG = Strings.format("""
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

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

    private static final String CLASSIFICATION_CONFIG = Strings.format("""
        {
          "input": {
            "field_names": [ "col1", "col2", "col3", "col4" ]
          },
          "description": "test model for classification",
          "default_field_map": {
            "col_1_alias": "col1"
          },
          "inference_config": {
            "classification": {}
          },
          "definition": %s
        }""", InferenceDefinitionTests.getClassificationDefinition(false));

    private void putModel(String modelId, String modelConfiguration) throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + modelId);
        request.setJsonEntity(modelConfiguration);
        client().performRequest(request);
    }

}
