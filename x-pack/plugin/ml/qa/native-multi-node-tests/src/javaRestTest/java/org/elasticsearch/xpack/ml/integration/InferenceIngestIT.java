/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ExternalTestCluster;
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
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * This is a {@link ESRestTestCase} because the cleanup code in {@link ExternalTestCluster#ensureEstimatedStats()} causes problems
 * Specifically, ensuring the accounting breaker has been reset.
 * It has to do with `_simulate` not anything really to do with the ML code
 */
public class InferenceIngestIT extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE_SUPER_USER = UsernamePasswordToken.basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    @Before
    public void setup() throws Exception {
        Request loggingSettings = new Request("PUT", "_cluster/settings");
        loggingSettings.setJsonEntity("""
            {"persistent" : {
                    "logger.org.elasticsearch.xpack.ml.inference" : "TRACE"
                }}""");
        client().performRequest(loggingSettings);
        client().performRequest(new Request("GET", "/_cluster/health?wait_for_status=green&timeout=30s"));
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE_SUPER_USER).build();
    }

    @After
    public void cleanUpData() throws Exception {
        new MlRestTestStateCleaner(logger, adminClient()).resetFeatures();
        Request loggingSettings = new Request("PUT", "_cluster/settings");
        loggingSettings.setJsonEntity("""
            {"persistent" : {
                    "logger.org.elasticsearch.xpack.ml.inference" : null
                }}""");
        client().performRequest(loggingSettings);
    }

    public void testPathologicalPipelineCreationAndDeletion() throws Exception {
        String classificationModelId = "test_pathological_classification";
        putModel(classificationModelId, CLASSIFICATION_CONFIG);

        String regressionModelId = "test_pathological_regression";
        putModel(regressionModelId, REGRESSION_CONFIG);

        for (int i = 0; i < 10; i++) {
            client().performRequest(
                putPipeline("simple_classification_pipeline", pipelineDefinition(classificationModelId, "classification"))
            );
            client().performRequest(indexRequest("index_for_inference_test", "simple_classification_pipeline", generateSourceDoc()));
            client().performRequest(new Request("DELETE", "_ingest/pipeline/simple_classification_pipeline"));

            client().performRequest(putPipeline("simple_regression_pipeline", pipelineDefinition(regressionModelId, "regression")));
            client().performRequest(indexRequest("index_for_inference_test", "simple_regression_pipeline", generateSourceDoc()));
            client().performRequest(new Request("DELETE", "_ingest/pipeline/simple_regression_pipeline"));
        }
        client().performRequest(new Request("POST", "index_for_inference_test/_refresh"));

        Response searchResponse = client().performRequest(
            searchRequest(
                "index_for_inference_test",
                QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery("ml.inference.regression.predicted_value"))
            )
        );
        assertThat(EntityUtils.toString(searchResponse.getEntity()), containsString("\"value\":10"));

        searchResponse = client().performRequest(
            searchRequest(
                "index_for_inference_test",
                QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery("ml.inference.classification.predicted_value"))
            )
        );

        assertThat(EntityUtils.toString(searchResponse.getEntity()), containsString("\"value\":10"));
        assertBusy(() -> {
            try {
                assertStatsWithCacheMisses(classificationModelId, 10);
                assertStatsWithCacheMisses(regressionModelId, 10);
            } catch (ResponseException ex) {
                // this could just mean shard failures.
                fail(ex.getMessage());
            }
        }, 30, TimeUnit.SECONDS);
    }

    public void testPipelineIngest() throws Exception {
        String classificationModelId = "test_classification";
        putModel(classificationModelId, CLASSIFICATION_CONFIG);

        String regressionModelId = "test_regression";
        putModel(regressionModelId, REGRESSION_CONFIG);

        client().performRequest(putPipeline("simple_classification_pipeline", pipelineDefinition(classificationModelId, "classification")));
        client().performRequest(putPipeline("simple_regression_pipeline", pipelineDefinition(regressionModelId, "regression")));

        for (int i = 0; i < 10; i++) {
            client().performRequest(indexRequest("index_for_inference_test", "simple_classification_pipeline", generateSourceDoc()));
            client().performRequest(indexRequest("index_for_inference_test", "simple_regression_pipeline", generateSourceDoc()));
        }

        for (int i = 0; i < 5; i++) {
            client().performRequest(indexRequest("index_for_inference_test", "simple_regression_pipeline", generateSourceDoc()));
        }

        client().performRequest(new Request("DELETE", "_ingest/pipeline/simple_regression_pipeline"));
        client().performRequest(new Request("DELETE", "_ingest/pipeline/simple_classification_pipeline"));

        client().performRequest(new Request("POST", "index_for_inference_test/_refresh"));

        Response searchResponse = client().performRequest(
            searchRequest(
                "index_for_inference_test",
                QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery("ml.inference.regression.predicted_value"))
            )
        );
        assertThat(EntityUtils.toString(searchResponse.getEntity()), containsString("\"value\":15"));

        searchResponse = client().performRequest(
            searchRequest(
                "index_for_inference_test",
                QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery("ml.inference.classification.predicted_value"))
            )
        );

        assertThat(EntityUtils.toString(searchResponse.getEntity()), containsString("\"value\":10"));

        assertBusy(() -> {
            try {
                assertStatsWithCacheMisses(classificationModelId, 10);
                assertStatsWithCacheMisses(regressionModelId, 15);
            } catch (ResponseException ex) {
                // this could just mean shard failures.
                fail(ex.getMessage());
            }
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void testPipelineIngestWithModelAliases() throws Exception {
        String regressionModelId = "test_regression_1";
        putModel(regressionModelId, REGRESSION_CONFIG);
        String regressionModelId2 = "test_regression_2";
        putModel(regressionModelId2, REGRESSION_CONFIG);
        String modelAlias = "test_regression";
        putModelAlias(modelAlias, regressionModelId);

        client().performRequest(putPipeline("simple_regression_pipeline", pipelineDefinition(modelAlias, "regression")));

        for (int i = 0; i < 10; i++) {
            client().performRequest(indexRequest("index_for_inference_test", "simple_regression_pipeline", generateSourceDoc()));
        }
        putModelAlias(modelAlias, regressionModelId2);
        // Need to assert busy as loading the model and then switching the model alias can take time
        assertBusy(() -> {
            String source = """
                {
                  "docs": [
                    {"_source": {
                      "col1": "female",
                      "col2": "M",
                      "col3": "none",
                      "col4": 10
                    }}]
                }""";
            Request request = new Request("POST", "_ingest/pipeline/simple_regression_pipeline/_simulate");
            request.setJsonEntity(source);
            Response response = client().performRequest(request);
            String responseString = EntityUtils.toString(response.getEntity());
            assertThat(responseString, containsString("\"model_id\":\"test_regression_2\""));
        }, 30, TimeUnit.SECONDS);

        for (int i = 0; i < 10; i++) {
            client().performRequest(indexRequest("index_for_inference_test", "simple_regression_pipeline", generateSourceDoc()));
        }

        client().performRequest(new Request("DELETE", "_ingest/pipeline/simple_regression_pipeline"));

        client().performRequest(new Request("POST", "index_for_inference_test/_refresh"));

        Response searchResponse = client().performRequest(
            searchRequest(
                "index_for_inference_test",
                QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery("ml.inference.regression.predicted_value"))
            )
        );
        // Verify we have 20 documents that contain a predicted value for regression
        assertThat(EntityUtils.toString(searchResponse.getEntity()), containsString("\"value\":20"));

        // Since this is a multi-node cluster, the model could be loaded and cached on one ingest node but not the other
        // Consequently, we should only verify that some of the documents refer to the first regression model
        // and some refer to the second.
        searchResponse = client().performRequest(
            searchRequest(
                "index_for_inference_test",
                QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("ml.inference.regression.model_id.keyword", regressionModelId))
            )
        );
        assertThat(EntityUtils.toString(searchResponse.getEntity()), not(containsString("\"value\":0")));

        searchResponse = client().performRequest(
            searchRequest(
                "index_for_inference_test",
                QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("ml.inference.regression.model_id.keyword", regressionModelId2))
            )
        );
        assertThat(EntityUtils.toString(searchResponse.getEntity()), not(containsString("\"value\":0")));

        assertBusy(() -> {
            try {
                Response response = client().performRequest(new Request("GET", "_ml/trained_models/" + modelAlias + "/_stats"));
                var responseMap = entityAsMap(response);
                assertThat((List<?>) responseMap.get("trained_model_stats"), hasSize(1));
                var stats = ((List<Map<String, Object>>) responseMap.get("trained_model_stats")).get(0);
                assertThat(stats.get("model_id"), equalTo(regressionModelId2));
                assertThat(stats.get("inference_stats"), is(notNullValue()));
            } catch (ResponseException ex) {
                // this could just mean shard failures.
                fail(ex.getMessage());
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void assertStatsWithCacheMisses(String modelId, int inferenceCount) throws IOException {
        Response statsResponse = client().performRequest(new Request("GET", "_ml/trained_models/" + modelId + "/_stats"));
        var responseMap = entityAsMap(statsResponse);
        assertThat((List<?>) responseMap.get("trained_model_stats"), hasSize(1));
        var stats = ((List<Map<String, Object>>) responseMap.get("trained_model_stats")).get(0);
        assertThat(stats.get("inference_stats"), is(notNullValue()));
        assertThat(
            stats.toString(),
            (Integer) XContentMapValues.extractValue("inference_stats.inference_count", stats),
            equalTo(inferenceCount)
        );
        assertThat(stats.toString(), (Integer) XContentMapValues.extractValue("inference_stats.cache_miss_count", stats), greaterThan(0));
    }

    public void testSimulate() throws IOException {
        String classificationModelId = "test_classification_simulate";
        putModel(classificationModelId, CLASSIFICATION_CONFIG);

        String regressionModelId = "test_regression_simulate";
        putModel(regressionModelId, REGRESSION_CONFIG);

        String source = """
            {
              "pipeline": {
                "processors": [
                  {
                    "inference": {
                      "target_field": "ml.classification",
                      "inference_config": {
                        "classification": {
                          "num_top_classes": 0,
                          "top_classes_results_field": "result_class_prob",
                          "num_top_feature_importance_values": 2
                        }
                      },
                      "model_id": "%s",
                      "field_map": {
                        "col1": "col1",
                        "col2": "col2",
                        "col3": "col3",
                        "col4": "col4"
                      }
                    }
                  },
                  {
                    "inference": {
                      "target_field": "ml.regression",
                      "model_id": "%s",
                      "inference_config": {
                        "regression": {}
                      },
                      "field_map": {
                        "col1": "col1",
                        "col2": "col2",
                        "col3": "col3",
                        "col4": "col4"
                      }
                    }
                  }
                ]
              },
              "docs": [
                {
                  "_source": {
                    "col1": "female",
                    "col2": "M",
                    "col3": "none",
                    "col4": 10
                  }
                }
              ]
            }
            """.formatted(classificationModelId, regressionModelId);

        Response response = client().performRequest(simulateRequest(source));
        String responseString = EntityUtils.toString(response.getEntity());
        assertThat(responseString, containsString("\"prediction_probability\":1.0"));
        assertThat(responseString, containsString("\"prediction_score\":1.0"));
        assertThat(responseString, containsString("\"predicted_value\":\"second\""));
        assertThat(responseString, containsString("\"predicted_value\":1.0"));
        assertThat(responseString, containsString("\"feature_name\":\"col1\""));
        assertThat(responseString, containsString("\"feature_name\":\"col2\""));
        assertThat(responseString, containsString("\"importance\":0.944"));
        assertThat(responseString, containsString("\"importance\":0.19999"));

        String sourceWithMissingModel = """
            {
              "pipeline": {
                "processors": [
                  {
                    "inference": {
                      "model_id": "test_classification_missing",
                      "inference_config": {"classification":{}},
                      "field_map": {
                        "col1": "col1",
                        "col2": "col2",
                        "col3": "col3",
                        "col4": "col4"
                      }
                    }
                  }
                ]
              },
              "docs": [
                {"_source": {
                  "col1": "female",
                  "col2": "M",
                  "col3": "none",
                  "col4": 10
                }}]
            }""";

        response = client().performRequest(simulateRequest(sourceWithMissingModel));
        responseString = EntityUtils.toString(response.getEntity());

        assertThat(responseString, containsString("Could not find trained model [test_classification_missing]"));
    }

    public void testSimulateWithDefaultMappedField() throws IOException {
        String classificationModelId = "test_classification_default_mapped_field";
        putModel(classificationModelId, CLASSIFICATION_CONFIG);
        String source = """
            {
              "pipeline": {
                "processors": [
                  {
                    "inference": {
                      "target_field": "ml.classification",
                      "inference_config": {
                        "classification": {
                          "num_top_classes": 2,
                          "top_classes_results_field": "result_class_prob",
                          "num_top_feature_importance_values": 2
                        }
                      },
                      "model_id": "%s",
                      "field_map": {}
                    }
                  }
                ]
              },
              "docs": [
                {
                  "_source": {
                    "col_1_alias": "female",
                    "col2": "M",
                    "col3": "none",
                    "col4": 10
                  }
                }
              ]
            }""".formatted(classificationModelId);

        Response response = client().performRequest(simulateRequest(source));
        String responseString = EntityUtils.toString(response.getEntity());
        assertThat(responseString, containsString("\"predicted_value\":\"second\""));
        assertThat(responseString, containsString("\"feature_name\":\"col1\""));
        assertThat(responseString, containsString("\"feature_name\":\"col2\""));
        assertThat(responseString, containsString("\"importance\":0.944"));
        assertThat(responseString, containsString("\"importance\":0.19999"));
    }

    public void testSimulateLangIdent() throws IOException {
        String source = """
            {
              "pipeline": {
                "processors": [
                  {
                    "inference": {
                      "inference_config": {"classification":{}},
                      "model_id": "lang_ident_model_1",
                      "field_map": {}
                    }
                  }
                ]
              },
              "docs": [
                {"_source": {
                  "text": "this is some plain text."
                }}]
            }""";

        Response response = client().performRequest(simulateRequest(source));
        assertThat(EntityUtils.toString(response.getEntity()), containsString("\"predicted_value\":\"en\""));
    }

    public void testSimulateLangIdentForeach() throws IOException {
        String source = """
            {  "pipeline": {
                "description": "detect text lang",
                "processors": [
                  {
                    "foreach": {
                      "field": "greetings",
                      "processor": {
                        "inference": {
                          "model_id": "lang_ident_model_1",
                          "inference_config": {
                            "classification": {
                              "num_top_classes": 5
                            }
                          },
                          "field_map": {
                            "_ingest._value.text": "text"
                          }
                        }
                      }
                    }
                  }
                ]
              },
              "docs": [
                {
                  "_source": {
                    "greetings": [
                      {
                        "text": " a backup credit card by visiting your billing preferences page or visit the adwords help"
                      },
                      {
                        "text": " 개별적으로 리포트 액세스 권한을 부여할 수 있습니다 액세스 권한 부여사용자에게 프로필 리포트에 "
                      }
                    ]
                  }
                }
              ]
            }""";
        Response response = client().performRequest(simulateRequest(source));
        String stringResponse = EntityUtils.toString(response.getEntity());
        assertThat(stringResponse, containsString("\"predicted_value\":\"en\""));
        assertThat(stringResponse, containsString("\"predicted_value\":\"ko\""));
    }

    static Request simulateRequest(String jsonEntity) {
        Request request = new Request("POST", "_ingest/pipeline/_simulate?error_trace=true");
        request.setJsonEntity(jsonEntity);
        return request;
    }

    private static Request indexRequest(String index, String pipeline, Map<String, Object> doc) throws IOException {
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(doc)) {
            return indexRequest(
                index,
                pipeline,
                XContentHelper.convertToJson(BytesReference.bytes(xContentBuilder), false, XContentType.JSON)
            );
        }
    }

    private static Request indexRequest(String index, String pipeline, String doc) {
        Request request = new Request("POST", index + "/_doc?pipeline=" + pipeline);
        request.setJsonEntity(doc);
        return request;
    }

    static Request putPipeline(String pipelineId, String pipelineDefinition) {
        Request request = new Request("PUT", "_ingest/pipeline/" + pipelineId);
        request.setJsonEntity(pipelineDefinition);
        return request;
    }

    private static Request searchRequest(String index, QueryBuilder queryBuilder) throws IOException {
        BytesReference reference = XContentHelper.toXContent(queryBuilder, XContentType.JSON, false);
        String queryJson = XContentHelper.convertToJson(reference, false, XContentType.JSON);
        String json = "{\"query\": " + queryJson + "}";
        Request request = new Request("GET", index + "/_search?track_total_hits=true");
        request.setJsonEntity(json);
        return request;
    }

    private Map<String, Object> generateSourceDoc() {
        return new HashMap<>() {
            {
                put("col1", randomFrom("female", "male"));
                put("col2", randomFrom("S", "M", "L", "XL"));
                put("col3", randomFrom("true", "false", "none", "other"));
                put("col4", randomIntBetween(0, 10));
            }
        };
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

    private static final String REGRESSION_CONFIG = """
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
        }""".formatted(REGRESSION_DEFINITION);

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

    private static final String CLASSIFICATION_CONFIG = """
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
        }""".formatted(InferenceDefinitionTests.getClassificationDefinition(false));

    private static String pipelineDefinition(String modelId, String inferenceConfig) {
        return """
            {
              "processors": [
                {
                  "inference": {
                    "model_id": "%s",
                    "tag": "%s",
                    "inference_config": {
                      "%s": {}
                    },
                    "field_map": {
                      "col1": "col1",
                      "col2": "col2",
                      "col3": "col3",
                      "col4": "col4"
                    }
                  }
                }
              ]
            }""".formatted(modelId, inferenceConfig, inferenceConfig);
    }

    private void putModel(String modelId, String modelConfiguration) throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + modelId);
        request.setJsonEntity(modelConfiguration);
        client().performRequest(request);
    }

    private void putModelAlias(String modelAlias, String newModel) throws IOException {
        Request request = new Request("PUT", "_ml/trained_models/" + newModel + "/model_aliases/" + modelAlias + "?reassign=true");
        client().performRequest(request);
    }

}
