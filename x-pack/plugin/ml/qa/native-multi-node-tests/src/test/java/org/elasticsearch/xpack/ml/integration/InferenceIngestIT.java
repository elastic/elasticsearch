/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ExternalTestCluster;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.CoreMatchers.containsString;

/**
 * This is a {@link ESRestTestCase} because the cleanup code in {@link ExternalTestCluster#ensureEstimatedStats()} causes problems
 * Specifically, ensuring the accounting breaker has been reset.
 * It has to do with `_simulate` not anything really to do with the ML code
 */
public class InferenceIngestIT extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE_SUPER_USER =
        basicAuthHeaderValue("x_pack_rest_user", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);

    @Before
    public void createBothModels() throws Exception {
        Request request = new Request("PUT", "_ml/inference/test_classification");
        request.setJsonEntity(CLASSIFICATION_CONFIG);
        client().performRequest(request);

        request = new Request("PUT", "_ml/inference/test_regression");
        request.setJsonEntity(REGRESSION_CONFIG);
        client().performRequest(request);
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE_SUPER_USER).build();
    }

    @After
    public void cleanUpData() throws Exception {
        new MlRestTestStateCleaner(logger, adminClient()).clearMlMetadata();
        ESRestTestCase.waitForPendingTasks(adminClient());
        client().performRequest(new Request("DELETE", "_ml/inference/test_classification"));
        client().performRequest(new Request("DELETE", "_ml/inference/test_regression"));
    }

    public void testPipelineCreationAndDeletion() throws Exception {

        for (int i = 0; i < 10; i++) {
            client().performRequest(putPipeline("simple_classification_pipeline", CLASSIFICATION_PIPELINE));
            client().performRequest(indexRequest("index_for_inference_test", "simple_classification_pipeline", generateSourceDoc()));
            client().performRequest(new Request("DELETE", "_ingest/pipeline/simple_classification_pipeline"));

            client().performRequest(putPipeline("simple_regression_pipeline", REGRESSION_PIPELINE));
            client().performRequest(indexRequest("index_for_inference_test", "simple_regression_pipeline", generateSourceDoc()));
            client().performRequest(new Request("DELETE", "_ingest/pipeline/simple_regression_pipeline"));
        }

        client().performRequest(putPipeline("simple_classification_pipeline", CLASSIFICATION_PIPELINE));
        client().performRequest(putPipeline("simple_regression_pipeline", REGRESSION_PIPELINE));

        for (int i = 0; i < 10; i++) {
            client().performRequest(indexRequest("index_for_inference_test", "simple_classification_pipeline", generateSourceDoc()));
            client().performRequest(indexRequest("index_for_inference_test", "simple_regression_pipeline", generateSourceDoc()));
        }

        client().performRequest(new Request("DELETE", "_ingest/pipeline/simple_regression_pipeline"));
        client().performRequest(new Request("DELETE", "_ingest/pipeline/simple_classification_pipeline"));

        client().performRequest(new Request("POST", "index_for_inference_test/_refresh"));


        Response searchResponse = client().performRequest(searchRequest("index_for_inference_test",
            QueryBuilders.boolQuery()
                .filter(
                    QueryBuilders.existsQuery("ml.inference.regression.predicted_value"))));
        assertThat(EntityUtils.toString(searchResponse.getEntity()), containsString("\"value\":20"));

        searchResponse = client().performRequest(searchRequest("index_for_inference_test",
            QueryBuilders.boolQuery()
                .filter(
                    QueryBuilders.existsQuery("ml.inference.classification.predicted_value"))));

        assertThat(EntityUtils.toString(searchResponse.getEntity()), containsString("\"value\":20"));
    }

    public void testSimulate() throws IOException {
        String source = "{\n" +
            "  \"pipeline\": {\n" +
            "    \"processors\": [\n" +
            "      {\n" +
            "        \"inference\": {\n" +
            "          \"target_field\": \"ml.classification\",\n" +
            "          \"inference_config\": {\"classification\": " +
            "                {\"num_top_classes\":2, \"top_classes_results_field\": \"result_class_prob\"}},\n" +
            "          \"model_id\": \"test_classification\",\n" +
            "          \"field_mappings\": {\n" +
            "            \"col1\": \"col1\",\n" +
            "            \"col2\": \"col2\",\n" +
            "            \"col3\": \"col3\",\n" +
            "            \"col4\": \"col4\"\n" +
            "          }\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"inference\": {\n" +
            "          \"target_field\": \"ml.regression\",\n" +
            "          \"model_id\": \"test_regression\",\n" +
            "          \"inference_config\": {\"regression\":{}},\n" +
            "          \"field_mappings\": {\n" +
            "            \"col1\": \"col1\",\n" +
            "            \"col2\": \"col2\",\n" +
            "            \"col3\": \"col3\",\n" +
            "            \"col4\": \"col4\"\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    ]\n" +
            "  },\n" +
            "  \"docs\": [\n" +
            "    {\"_source\": {\n" +
            "      \"col1\": \"female\",\n" +
            "      \"col2\": \"M\",\n" +
            "      \"col3\": \"none\",\n" +
            "      \"col4\": 10\n" +
            "    }}]\n" +
            "}";

        Response response = client().performRequest(simulateRequest(source));
        String responseString = EntityUtils.toString(response.getEntity());
        assertThat(responseString, containsString("\"predicted_value\":\"second\""));
        assertThat(responseString, containsString("\"predicted_value\":1.0"));

        String sourceWithMissingModel = "{\n" +
            "  \"pipeline\": {\n" +
            "    \"processors\": [\n" +
            "      {\n" +
            "        \"inference\": {\n" +
            "          \"model_id\": \"test_classification_missing\",\n" +
            "          \"inference_config\": {\"classification\":{}},\n" +
            "          \"field_mappings\": {\n" +
            "            \"col1\": \"col1\",\n" +
            "            \"col2\": \"col2\",\n" +
            "            \"col3\": \"col3\",\n" +
            "            \"col4\": \"col4\"\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    ]\n" +
            "  },\n" +
            "  \"docs\": [\n" +
            "    {\"_source\": {\n" +
            "      \"col1\": \"female\",\n" +
            "      \"col2\": \"M\",\n" +
            "      \"col3\": \"none\",\n" +
            "      \"col4\": 10\n" +
            "    }}]\n" +
            "}";

        response = client().performRequest(simulateRequest(sourceWithMissingModel));
        responseString = EntityUtils.toString(response.getEntity());

        assertThat(responseString, containsString("Could not find trained model [test_classification_missing]"));
    }

    public void testSimulateLangIdent() throws IOException {
        String source = "{\n" +
            "  \"pipeline\": {\n" +
            "    \"processors\": [\n" +
            "      {\n" +
            "        \"inference\": {\n" +
            "          \"inference_config\": {\"classification\":{}},\n" +
            "          \"model_id\": \"lang_ident_model_1\",\n" +
            "          \"field_mappings\": {}\n" +
            "        }\n" +
            "      }\n" +
            "    ]\n" +
            "  },\n" +
            "  \"docs\": [\n" +
            "    {\"_source\": {\n" +
            "      \"text\": \"this is some plain text.\"\n" +
            "    }}]\n" +
            "}";

        Response response = client().performRequest(simulateRequest(source));
        assertThat(EntityUtils.toString(response.getEntity()), containsString("\"predicted_value\":\"en\""));
    }

    private static Request simulateRequest(String jsonEntity) {
        Request request = new Request("POST", "_ingest/pipeline/_simulate");
        request.setJsonEntity(jsonEntity);
        return request;
    }

    private static Request indexRequest(String index, String pipeline, Map<String, Object> doc) throws IOException {
        try(XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(doc)) {
            return indexRequest(index,
                pipeline,
                XContentHelper.convertToJson(BytesReference.bytes(xContentBuilder), false, XContentType.JSON));
        }
    }

    private static Request indexRequest(String index, String pipeline, String doc) {
        Request request = new Request("POST", index + "/_doc?pipeline=" + pipeline);
        request.setJsonEntity(doc);
        return request;
    }

    private static Request putPipeline(String pipelineId, String pipelineDefinition) {
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
        return new HashMap<>(){{
            put("col1", randomFrom("female", "male"));
            put("col2", randomFrom("S", "M", "L", "XL"));
            put("col3", randomFrom("true", "false", "none", "other"));
            put("col4", randomIntBetween(0, 10));
        }};
    }

    private static final String REGRESSION_DEFINITION = "{" +
        "  \"preprocessors\": [\n" +
        "    {\n" +
        "      \"one_hot_encoding\": {\n" +
        "        \"field\": \"col1\",\n" +
        "        \"hot_map\": {\n" +
        "          \"male\": \"col1_male\",\n" +
        "          \"female\": \"col1_female\"\n" +
        "        }\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"target_mean_encoding\": {\n" +
        "        \"field\": \"col2\",\n" +
        "        \"feature_name\": \"col2_encoded\",\n" +
        "        \"target_map\": {\n" +
        "          \"S\": 5.0,\n" +
        "          \"M\": 10.0,\n" +
        "          \"L\": 20\n" +
        "        },\n" +
        "        \"default_value\": 5.0\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"frequency_encoding\": {\n" +
        "        \"field\": \"col3\",\n" +
        "        \"feature_name\": \"col3_encoded\",\n" +
        "        \"frequency_map\": {\n" +
        "          \"none\": 0.75,\n" +
        "          \"true\": 0.10,\n" +
        "          \"false\": 0.15\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  ],\n" +
        "  \"trained_model\": {\n" +
        "    \"ensemble\": {\n" +
        "      \"feature_names\": [\n" +
        "        \"col1_male\",\n" +
        "        \"col1_female\",\n" +
        "        \"col2_encoded\",\n" +
        "        \"col3_encoded\",\n" +
        "        \"col4\"\n" +
        "      ],\n" +
        "      \"aggregate_output\": {\n" +
        "        \"weighted_sum\": {\n" +
        "          \"weights\": [\n" +
        "            0.5,\n" +
        "            0.5\n" +
        "          ]\n" +
        "        }\n" +
        "      },\n" +
        "      \"target_type\": \"regression\",\n" +
        "      \"trained_models\": [\n" +
        "        {\n" +
        "          \"tree\": {\n" +
        "            \"feature_names\": [\n" +
        "              \"col1_male\",\n" +
        "              \"col1_female\",\n" +
        "              \"col4\"\n" +
        "            ],\n" +
        "            \"tree_structure\": [\n" +
        "              {\n" +
        "                \"node_index\": 0,\n" +
        "                \"split_feature\": 0,\n" +
        "                \"split_gain\": 12.0,\n" +
        "                \"threshold\": 10.0,\n" +
        "                \"decision_type\": \"lte\",\n" +
        "                \"default_left\": true,\n" +
        "                \"left_child\": 1,\n" +
        "                \"right_child\": 2\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 1,\n" +
        "                \"leaf_value\": 1\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 2,\n" +
        "                \"leaf_value\": 2\n" +
        "              }\n" +
        "            ],\n" +
        "            \"target_type\": \"regression\"\n" +
        "          }\n" +
        "        },\n" +
        "        {\n" +
        "          \"tree\": {\n" +
        "            \"feature_names\": [\n" +
        "              \"col2_encoded\",\n" +
        "              \"col3_encoded\",\n" +
        "              \"col4\"\n" +
        "            ],\n" +
        "            \"tree_structure\": [\n" +
        "              {\n" +
        "                \"node_index\": 0,\n" +
        "                \"split_feature\": 0,\n" +
        "                \"split_gain\": 12.0,\n" +
        "                \"threshold\": 10.0,\n" +
        "                \"decision_type\": \"lte\",\n" +
        "                \"default_left\": true,\n" +
        "                \"left_child\": 1,\n" +
        "                \"right_child\": 2\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 1,\n" +
        "                \"leaf_value\": 1\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 2,\n" +
        "                \"leaf_value\": 2\n" +
        "              }\n" +
        "            ],\n" +
        "            \"target_type\": \"regression\"\n" +
        "          }\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  }\n" +
        "}";

    private static final String REGRESSION_CONFIG = "{" +
        "  \"input\":{\"field_names\":[\"col1\",\"col2\",\"col3\",\"col4\"]}," +
        "  \"description\": \"test model for regression\",\n" +
        "  \"definition\": " + REGRESSION_DEFINITION +
        "}";

    private static final String CLASSIFICATION_DEFINITION = "{" +
        "  \"preprocessors\": [\n" +
        "    {\n" +
        "      \"one_hot_encoding\": {\n" +
        "        \"field\": \"col1\",\n" +
        "        \"hot_map\": {\n" +
        "          \"male\": \"col1_male\",\n" +
        "          \"female\": \"col1_female\"\n" +
        "        }\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"target_mean_encoding\": {\n" +
        "        \"field\": \"col2\",\n" +
        "        \"feature_name\": \"col2_encoded\",\n" +
        "        \"target_map\": {\n" +
        "          \"S\": 5.0,\n" +
        "          \"M\": 10.0,\n" +
        "          \"L\": 20\n" +
        "        },\n" +
        "        \"default_value\": 5.0\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"frequency_encoding\": {\n" +
        "        \"field\": \"col3\",\n" +
        "        \"feature_name\": \"col3_encoded\",\n" +
        "        \"frequency_map\": {\n" +
        "          \"none\": 0.75,\n" +
        "          \"true\": 0.10,\n" +
        "          \"false\": 0.15\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  ],\n" +
        "  \"trained_model\": {\n" +
        "    \"ensemble\": {\n" +
        "      \"feature_names\": [\n" +
        "        \"col1_male\",\n" +
        "        \"col1_female\",\n" +
        "        \"col2_encoded\",\n" +
        "        \"col3_encoded\",\n" +
        "        \"col4\"\n" +
        "      ],\n" +
        "      \"aggregate_output\": {\n" +
        "        \"weighted_mode\": {\n" +
        "          \"num_classes\": \"2\",\n" +
        "          \"weights\": [\n" +
        "            0.5,\n" +
        "            0.5\n" +
        "          ]\n" +
        "        }\n" +
        "      },\n" +
        "      \"target_type\": \"classification\",\n" +
        "      \"classification_labels\": [\"first\", \"second\"],\n" +
        "      \"trained_models\": [\n" +
        "        {\n" +
        "          \"tree\": {\n" +
        "            \"feature_names\": [\n" +
        "              \"col1_male\",\n" +
        "              \"col1_female\",\n" +
        "              \"col4\"\n" +
        "            ],\n" +
        "            \"tree_structure\": [\n" +
        "              {\n" +
        "                \"node_index\": 0,\n" +
        "                \"split_feature\": 0,\n" +
        "                \"split_gain\": 12.0,\n" +
        "                \"threshold\": 10.0,\n" +
        "                \"decision_type\": \"lte\",\n" +
        "                \"default_left\": true,\n" +
        "                \"left_child\": 1,\n" +
        "                \"right_child\": 2\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 1,\n" +
        "                \"leaf_value\": 1\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 2,\n" +
        "                \"leaf_value\": 0\n" +
        "              }\n" +
        "            ],\n" +
        "            \"target_type\": \"regression\"\n" +
        "          }\n" +
        "        },\n" +
        "        {\n" +
        "          \"tree\": {\n" +
        "            \"feature_names\": [\n" +
        "              \"col2_encoded\",\n" +
        "              \"col3_encoded\",\n" +
        "              \"col4\"\n" +
        "            ],\n" +
        "            \"tree_structure\": [\n" +
        "              {\n" +
        "                \"node_index\": 0,\n" +
        "                \"split_feature\": 0,\n" +
        "                \"split_gain\": 12.0,\n" +
        "                \"threshold\": 10.0,\n" +
        "                \"decision_type\": \"lte\",\n" +
        "                \"default_left\": true,\n" +
        "                \"left_child\": 1,\n" +
        "                \"right_child\": 2\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 1,\n" +
        "                \"leaf_value\": 1\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 2,\n" +
        "                \"leaf_value\": 0\n" +
        "              }\n" +
        "            ],\n" +
        "            \"target_type\": \"regression\"\n" +
        "          }\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  }\n" +
        "}";

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

    private static final String CLASSIFICATION_CONFIG = "" +
        "{\n" +
        "  \"input\":{\"field_names\":[\"col1\",\"col2\",\"col3\",\"col4\"]}," +
        "  \"description\": \"test model for classification\",\n" +
        "  \"definition\": " + CLASSIFICATION_DEFINITION +
        "}";

    private static final String CLASSIFICATION_PIPELINE = "{" +
        "    \"processors\": [\n" +
        "      {\n" +
        "        \"inference\": {\n" +
        "          \"model_id\": \"test_classification\",\n" +
        "          \"tag\": \"classification\",\n" +
        "          \"inference_config\": {\"classification\": {}},\n" +
        "          \"field_mappings\": {\n" +
        "            \"col1\": \"col1\",\n" +
        "            \"col2\": \"col2\",\n" +
        "            \"col3\": \"col3\",\n" +
        "            \"col4\": \"col4\"\n" +
        "          }\n" +
        "        }\n" +
        "      }]}\n";

    private static final String REGRESSION_PIPELINE = "{" +
        "    \"processors\": [\n" +
        "      {\n" +
        "        \"inference\": {\n" +
        "          \"model_id\": \"test_regression\",\n" +
        "          \"tag\": \"regression\",\n" +
        "          \"inference_config\": {\"regression\": {}},\n" +
        "          \"field_mappings\": {\n" +
        "            \"col1\": \"col1\",\n" +
        "            \"col2\": \"col2\",\n" +
        "            \"col3\": \"col3\",\n" +
        "            \"col4\": \"col4\"\n" +
        "          }\n" +
        "        }\n" +
        "      }]}\n";

}
