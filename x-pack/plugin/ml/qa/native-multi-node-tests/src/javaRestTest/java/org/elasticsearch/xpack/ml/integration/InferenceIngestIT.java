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
import org.elasticsearch.client.ml.GetTrainedModelsStatsResponse;
import org.elasticsearch.client.ml.inference.TrainedModelStats;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ExternalTestCluster;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinitionTests;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
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

    private static final String BASIC_AUTH_VALUE_SUPER_USER =
        UsernamePasswordToken.basicAuthHeaderValue("x_pack_rest_user", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);

    @Before
    public void setup() throws Exception {
        Request loggingSettings = new Request("PUT", "_cluster/settings");
        loggingSettings.setJsonEntity("" +
            "{" +
            "\"transient\" : {\n" +
            "        \"logger.org.elasticsearch.xpack.ml.inference\" : \"TRACE\"\n" +
            "    }" +
            "}");
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
        loggingSettings.setJsonEntity("" +
            "{" +
            "\"transient\" : {\n" +
            "        \"logger.org.elasticsearch.xpack.ml.inference\" : null\n" +
            "    }" +
            "}");
        client().performRequest(loggingSettings);
    }

    public void testPathologicalPipelineCreationAndDeletion() throws Exception {
        String classificationModelId = "test_pathological_classification";
        putModel(classificationModelId, CLASSIFICATION_CONFIG);

        String regressionModelId = "test_pathological_regression";
        putModel(regressionModelId, REGRESSION_CONFIG);

        for (int i = 0; i < 10; i++) {
            client().performRequest(putPipeline("simple_classification_pipeline",
                pipelineDefinition(classificationModelId, "classification")));
            client().performRequest(indexRequest("index_for_inference_test", "simple_classification_pipeline", generateSourceDoc()));
            client().performRequest(new Request("DELETE", "_ingest/pipeline/simple_classification_pipeline"));

            client().performRequest(putPipeline("simple_regression_pipeline", pipelineDefinition(regressionModelId, "regression")));
            client().performRequest(indexRequest("index_for_inference_test", "simple_regression_pipeline", generateSourceDoc()));
            client().performRequest(new Request("DELETE", "_ingest/pipeline/simple_regression_pipeline"));
        }
        client().performRequest(new Request("POST", "index_for_inference_test/_refresh"));

        Response searchResponse = client().performRequest(searchRequest("index_for_inference_test",
            QueryBuilders.boolQuery()
                .filter(
                    QueryBuilders.existsQuery("ml.inference.regression.predicted_value"))));
        assertThat(EntityUtils.toString(searchResponse.getEntity()), containsString("\"value\":10"));

        searchResponse = client().performRequest(searchRequest("index_for_inference_test",
            QueryBuilders.boolQuery()
                .filter(
                    QueryBuilders.existsQuery("ml.inference.classification.predicted_value"))));

        assertThat(EntityUtils.toString(searchResponse.getEntity()), containsString("\"value\":10"));
        assertBusy(() -> {
            try {
                assertStatsWithCacheMisses(classificationModelId, 10L);
                assertStatsWithCacheMisses(regressionModelId, 10L);
            } catch (ResponseException ex) {
                //this could just mean shard failures.
                fail(ex.getMessage());
            }
        }, 30, TimeUnit.SECONDS);
    }

    public void testPipelineIngest() throws Exception {
        String classificationModelId = "test_classification";
        putModel(classificationModelId, CLASSIFICATION_CONFIG);

        String regressionModelId = "test_regression";
        putModel(regressionModelId, REGRESSION_CONFIG);

        client().performRequest(putPipeline("simple_classification_pipeline",
            pipelineDefinition(classificationModelId, "classification")));
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

        Response searchResponse = client().performRequest(searchRequest("index_for_inference_test",
            QueryBuilders.boolQuery()
                .filter(
                    QueryBuilders.existsQuery("ml.inference.regression.predicted_value"))));
        assertThat(EntityUtils.toString(searchResponse.getEntity()), containsString("\"value\":15"));

        searchResponse = client().performRequest(searchRequest("index_for_inference_test",
            QueryBuilders.boolQuery()
                .filter(
                    QueryBuilders.existsQuery("ml.inference.classification.predicted_value"))));

        assertThat(EntityUtils.toString(searchResponse.getEntity()), containsString("\"value\":10"));

        assertBusy(() -> {
            try {
                assertStatsWithCacheMisses(classificationModelId, 10L);
                assertStatsWithCacheMisses(regressionModelId, 15L);
            } catch (ResponseException ex) {
                //this could just mean shard failures.
                fail(ex.getMessage());
            }
        }, 30, TimeUnit.SECONDS);
    }

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
            String source = "{\n" +
                "  \"docs\": [\n" +
                "    {\"_source\": {\n" +
                "      \"col1\": \"female\",\n" +
                "      \"col2\": \"M\",\n" +
                "      \"col3\": \"none\",\n" +
                "      \"col4\": 10\n" +
                "    }}]\n" +
                "}";
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

        Response searchResponse = client().performRequest(searchRequest("index_for_inference_test",
            QueryBuilders.boolQuery()
                .filter(
                    QueryBuilders.existsQuery("ml.inference.regression.predicted_value"))));
        // Verify we have 20 documents that contain a predicted value for regression
        assertThat(EntityUtils.toString(searchResponse.getEntity()), containsString("\"value\":20"));


        // Since this is a multi-node cluster, the model could be loaded and cached on one ingest node but not the other
        // Consequently, we should only verify that some of the documents refer to the first regression model
        // and some refer to the second.
        searchResponse = client().performRequest(searchRequest("index_for_inference_test",
            QueryBuilders.boolQuery()
                .filter(
                    QueryBuilders.termQuery("ml.inference.regression.model_id.keyword", regressionModelId))));
        assertThat(EntityUtils.toString(searchResponse.getEntity()), not(containsString("\"value\":0")));

        searchResponse = client().performRequest(searchRequest("index_for_inference_test",
            QueryBuilders.boolQuery()
                .filter(
                    QueryBuilders.termQuery("ml.inference.regression.model_id.keyword", regressionModelId2))));
        assertThat(EntityUtils.toString(searchResponse.getEntity()), not(containsString("\"value\":0")));

        assertBusy(() -> {
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, client().performRequest(new Request("GET",
                "_ml/trained_models/" + modelAlias + "/_stats")).getEntity().getContent())) {
                GetTrainedModelsStatsResponse response = GetTrainedModelsStatsResponse.fromXContent(parser);
                assertThat(response.toString(), response.getTrainedModelStats(), hasSize(1));
                TrainedModelStats trainedModelStats = response.getTrainedModelStats().get(0);
                assertThat(trainedModelStats.getModelId(), equalTo(regressionModelId2));
                assertThat(trainedModelStats.getInferenceStats(), is(notNullValue()));
            } catch (ResponseException ex) {
                //this could just mean shard failures.
                fail(ex.getMessage());
            }
        });
    }

    public void assertStatsWithCacheMisses(String modelId, long inferenceCount) throws IOException {
        Response statsResponse = client().performRequest(new Request("GET",
            "_ml/trained_models/" + modelId + "/_stats"));
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, statsResponse.getEntity().getContent())) {
            GetTrainedModelsStatsResponse response = GetTrainedModelsStatsResponse.fromXContent(parser);
            assertThat(response.getTrainedModelStats(), hasSize(1));
            TrainedModelStats trainedModelStats = response.getTrainedModelStats().get(0);
            assertThat(trainedModelStats.getInferenceStats(), is(notNullValue()));
            assertThat(trainedModelStats.getInferenceStats().getInferenceCount(), equalTo(inferenceCount));
            assertThat(trainedModelStats.getInferenceStats().getCacheMissCount(), greaterThan(0L));
        }
    }

    public void testSimulate() throws IOException {
        String classificationModelId = "test_classification_simulate";
        putModel(classificationModelId, CLASSIFICATION_CONFIG);

        String regressionModelId = "test_regression_simulate";
        putModel(regressionModelId, REGRESSION_CONFIG);

        String source = "{\n" +
            "  \"pipeline\": {\n" +
            "    \"processors\": [\n" +
            "      {\n" +
            "        \"inference\": {\n" +
            "          \"target_field\": \"ml.classification\",\n" +
            "          \"inference_config\": {\"classification\": " +
            "                {\"num_top_classes\":0, " +
            "                \"top_classes_results_field\": \"result_class_prob\"," +
            "                \"num_top_feature_importance_values\": 2" +
            "          }},\n" +
            "          \"model_id\": \"" + classificationModelId + "\",\n" +
            "          \"field_map\": {\n" +
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
            "          \"model_id\": \"" + regressionModelId + "\",\n" +
            "          \"inference_config\": {\"regression\":{}},\n" +
            "          \"field_map\": {\n" +
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
        assertThat(responseString, containsString("\"prediction_probability\":1.0"));
        assertThat(responseString, containsString("\"prediction_score\":1.0"));
        assertThat(responseString, containsString("\"predicted_value\":\"second\""));
        assertThat(responseString, containsString("\"predicted_value\":1.0"));
        assertThat(responseString, containsString("\"feature_name\":\"col1\""));
        assertThat(responseString, containsString("\"feature_name\":\"col2\""));
        assertThat(responseString, containsString("\"importance\":0.944"));
        assertThat(responseString, containsString("\"importance\":0.19999"));

        String sourceWithMissingModel = "{\n" +
            "  \"pipeline\": {\n" +
            "    \"processors\": [\n" +
            "      {\n" +
            "        \"inference\": {\n" +
            "          \"model_id\": \"test_classification_missing\",\n" +
            "          \"inference_config\": {\"classification\":{}},\n" +
            "          \"field_map\": {\n" +
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

    public void testSimulateWithDefaultMappedField() throws IOException {
        String classificationModelId = "test_classification_default_mapped_field";
        putModel(classificationModelId, CLASSIFICATION_CONFIG);
        String source = "{\n" +
            "  \"pipeline\": {\n" +
            "    \"processors\": [\n" +
            "      {\n" +
            "        \"inference\": {\n" +
            "          \"target_field\": \"ml.classification\",\n" +
            "          \"inference_config\": {\"classification\": " +
            "                {\"num_top_classes\":2, " +
            "                \"top_classes_results_field\": \"result_class_prob\"," +
            "                \"num_top_feature_importance_values\": 2" +
            "          }},\n" +
            "          \"model_id\": \"" + classificationModelId + "\",\n" +
            "          \"field_map\": {}\n" +
            "        }\n" +
            "      }\n"+
            "    ]\n" +
            "  },\n" +
            "  \"docs\": [\n" +
            "    {\"_source\": {\n" +
            "      \"col_1_alias\": \"female\",\n" +
            "      \"col2\": \"M\",\n" +
            "      \"col3\": \"none\",\n" +
            "      \"col4\": 10\n" +
            "    }}]\n" +
            "}";

        Response response = client().performRequest(simulateRequest(source));
        String responseString = EntityUtils.toString(response.getEntity());
        assertThat(responseString, containsString("\"predicted_value\":\"second\""));
        assertThat(responseString, containsString("\"feature_name\":\"col1\""));
        assertThat(responseString, containsString("\"feature_name\":\"col2\""));
        assertThat(responseString, containsString("\"importance\":0.944"));
        assertThat(responseString, containsString("\"importance\":0.19999"));
    }

    public void testSimulateLangIdent() throws IOException {
        String source = "{\n" +
            "  \"pipeline\": {\n" +
            "    \"processors\": [\n" +
            "      {\n" +
            "        \"inference\": {\n" +
            "          \"inference_config\": {\"classification\":{}},\n" +
            "          \"model_id\": \"lang_ident_model_1\",\n" +
            "          \"field_map\": {}\n" +
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

    public void testSimulateLangIdentForeach() throws IOException {
        String source = "{" +
            "  \"pipeline\": {\n" +
            "    \"description\": \"detect text lang\",\n" +
            "    \"processors\": [\n" +
            "      {\n" +
            "        \"foreach\": {\n" +
            "          \"field\": \"greetings\",\n" +
            "          \"processor\": {\n" +
            "            \"inference\": {\n" +
            "              \"model_id\": \"lang_ident_model_1\",\n" +
            "              \"inference_config\": {\n" +
            "                \"classification\": {\n" +
            "                  \"num_top_classes\": 5\n" +
            "                }\n" +
            "              },\n" +
            "              \"field_map\": {\n" +
            "                \"_ingest._value.text\": \"text\"\n" +
            "              }\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    ]\n" +
            "  },\n" +
            "  \"docs\": [\n" +
            "    {\n" +
            "      \"_source\": {\n" +
            "        \"greetings\": [\n" +
            "          {\n" +
            "            \"text\": \" a backup credit card by visiting your billing preferences page or visit the adwords help\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"text\": \" 개별적으로 리포트 액세스 권한을 부여할 수 있습니다 액세스 권한 부여사용자에게 프로필 리포트에 \"\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";
        Response response = client().performRequest(simulateRequest(source));
        String stringResponse = EntityUtils.toString(response.getEntity());
        assertThat(stringResponse, containsString("\"predicted_value\":\"en\""));
        assertThat(stringResponse, containsString("\"predicted_value\":\"ko\""));
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
        "                \"number_samples\": 300,\n" +
        "                \"default_left\": true,\n" +
        "                \"left_child\": 1,\n" +
        "                \"right_child\": 2\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 1,\n" +
        "                \"number_samples\": 100,\n" +
        "                \"leaf_value\": 1\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 2,\n" +
        "                \"number_samples\": 200,\n" +
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
        "                \"number_samples\": 150,\n" +
        "                \"left_child\": 1,\n" +
        "                \"right_child\": 2\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 1,\n" +
        "                \"number_samples\": 50,\n" +
        "                \"leaf_value\": 1\n" +
        "              },\n" +
        "              {\n" +
        "                \"node_index\": 2,\n" +
        "                \"number_samples\": 100,\n" +
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
        "  \"inference_config\": {\"regression\": {}},\n" +
        "  \"definition\": " + REGRESSION_DEFINITION +
        "}";

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

    private static final String CLASSIFICATION_CONFIG = "" +
        "{\n" +
        "  \"input\":{\"field_names\":[\"col1\",\"col2\",\"col3\",\"col4\"]}," +
        "  \"description\": \"test model for classification\",\n" +
        "  \"default_field_map\": {\"col_1_alias\": \"col1\"},\n" +
        "  \"inference_config\": {\"classification\": {}},\n" +
        "  \"definition\": " + InferenceDefinitionTests.getClassificationDefinition(false) +
        "}";

    private static String pipelineDefinition(String modelId, String inferenceConfig) {
        return "{" +
            "    \"processors\": [\n" +
            "      {\n" +
            "        \"inference\": {\n" +
            "          \"model_id\": \"" + modelId + "\",\n" +
            "          \"tag\": \""+ inferenceConfig + "\",\n" +
            "          \"inference_config\": {\"" + inferenceConfig + "\": {}},\n" +
            "          \"field_map\": {\n" +
            "            \"col1\": \"col1\",\n" +
            "            \"col2\": \"col2\",\n" +
            "            \"col3\": \"col3\",\n" +
            "            \"col4\": \"col4\"\n" +
            "          }\n" +
            "        }\n" +
            "      }]}\n";
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
