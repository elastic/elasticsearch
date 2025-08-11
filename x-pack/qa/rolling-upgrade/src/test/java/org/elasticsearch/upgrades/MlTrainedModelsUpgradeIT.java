/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class MlTrainedModelsUpgradeIT extends AbstractUpgradeTestCase {

    static final String BOOLEAN_FIELD = "boolean-field";
    static final String NUMERICAL_FIELD = "numerical-field";
    static final String DISCRETE_NUMERICAL_FIELD = "discrete-numerical-field";
    static final String KEYWORD_FIELD = "keyword-field";
    static final List<Boolean> BOOLEAN_FIELD_VALUES = org.elasticsearch.core.List.of(false, true);
    static final List<Double> NUMERICAL_FIELD_VALUES = org.elasticsearch.core.List.of(1.0, 2.0);
    static final List<Integer> DISCRETE_NUMERICAL_FIELD_VALUES = org.elasticsearch.core.List.of(10, 20);
    static final List<String> KEYWORD_FIELD_VALUES = org.elasticsearch.core.List.of("cat", "dog");
    static final String INDEX_NAME = "created_index";

    @BeforeClass
    public static void maybeSkip() {
        assumeFalse("Skip ML tests on unsupported glibc versions", SKIP_ML_TESTS);
    }

    @Override
    protected Collection<String> templatesToWaitFor() {
        // We shouldn't wait for ML templates during the upgrade - production won't
        if (CLUSTER_TYPE != ClusterType.OLD) {
            return super.templatesToWaitFor();
        }
        return Stream.concat(XPackRestTestConstants.ML_POST_V7120_TEMPLATES.stream(), super.templatesToWaitFor().stream())
            .collect(Collectors.toSet());
    }

    public void testTrainedModelInference() throws Exception {
        assumeTrue("We should only test if old cluster is after trained models we GA", UPGRADE_FROM_VERSION.after(Version.V_7_13_0));
        switch (CLUSTER_TYPE) {
            case OLD:
                createIndex(INDEX_NAME);
                indexData(INDEX_NAME, 1000);
                createAndRunClassificationJob();
                createAndRunRegressionJob();
                List<String> oldModels = getTrainedModels();
                createPipelines(oldModels);
                testInfer(oldModels);
                break;
            case MIXED:
            case UPGRADED:
                ensureHealth(".ml-inference-*,.ml-config*", (request -> {
                    request.addParameter("wait_for_status", "yellow");
                    request.addParameter("timeout", "70s");
                }));
                List<String> modelIds = getTrainedModels();
                // Test that stats are serializable and can be gathered
                getTrainedModelStats();
                // Verify that the pipelines still work and inference is possible
                testInfer(modelIds);
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    void createPipelines(List<String> modelIds) throws Exception {
        for (String modelId : modelIds) {
            createPipeline(modelId, modelId.startsWith("classification") ? "classification" : "regression", modelId);
        }
    }

    @SuppressWarnings("unchecked")
    List<String> getTrainedModels() throws Exception {
        return ((List<Map<String, Object>>) entityAsMap(client().performRequest(new Request("GET", "_ml/trained_models/_all"))).get(
            "trained_model_configs"
        )).stream()
            .map(m -> m.get("model_id").toString())
            .filter(s -> s.startsWith("classification") || s.startsWith("regression"))
            .collect(Collectors.toList());
    }

    void getTrainedModelStats() throws Exception {
        client().performRequest(new Request("GET", "_ml/trained_models/_all/_stats"));
    }

    void testInfer(List<String> modelIds) throws Exception {
        for (String modelId : modelIds) {
            Request simulate = new Request("POST", "/_ingest/pipeline/" + modelId + "/_simulate");
            simulate.setJsonEntity(
                ""
                    + "{\n"
                    + "  \"docs\": [\n"
                    + "    {\n"
                    + "      \"_index\": \"index\",\n"
                    + "      \"_id\": \"id\",\n"
                    + "      \"_source\": "
                    + String.format(
                        Locale.ROOT,
                        "{\"%s\":%s,\"%s\":%f,\"%s\":%d,\"%s\":\"%s\"}",
                        BOOLEAN_FIELD,
                        BOOLEAN_FIELD_VALUES.get(0),
                        NUMERICAL_FIELD,
                        NUMERICAL_FIELD_VALUES.get(0),
                        DISCRETE_NUMERICAL_FIELD,
                        DISCRETE_NUMERICAL_FIELD_VALUES.get(0),
                        KEYWORD_FIELD,
                        KEYWORD_FIELD_VALUES.get(0)
                    )
                    + "    },\n"
                    + "    {\n"
                    + "      \"_index\": \"index\",\n"
                    + "      \"_id\": \"id\",\n"
                    + "      \"_source\": "
                    + String.format(
                        Locale.ROOT,
                        "{\"%s\":%s,\"%s\":%f,\"%s\":%d,\"%s\":\"%s\"}",
                        BOOLEAN_FIELD,
                        BOOLEAN_FIELD_VALUES.get(1),
                        NUMERICAL_FIELD,
                        NUMERICAL_FIELD_VALUES.get(1),
                        DISCRETE_NUMERICAL_FIELD,
                        DISCRETE_NUMERICAL_FIELD_VALUES.get(1),
                        KEYWORD_FIELD,
                        KEYWORD_FIELD_VALUES.get(1)
                    )
                    + "    }\n"
                    + "  ]\n"
                    + "}"
                    + ""
            );
            Response response = client().performRequest(simulate);
            String value = EntityUtils.toString(response.getEntity());
            assertThat(value, containsString(",\"model_id\":\"" + modelId + "\""));
            if (modelId.startsWith("classification")) {
                assertThat(value, containsString("prediction_score"));
            } else {
                assertThat(value, containsString("numerical-field_prediction"));
            }
        }
    }

    void createAndRunRegressionJob() throws Exception {
        String config = "{\n"
            + "      \"source\" : {\n"
            + "        \"index\" : [\n"
            + "          \""
            + INDEX_NAME
            + "\"\n"
            + "        ]\n"
            + "      },\n"
            + "      \"dest\" : {\n"
            + "        \"index\" : \"regression\"\n"
            + "      },\n"
            + "      \"analysis\" : {\n"
            + "        \"regression\" : {\n"
            + "          \"dependent_variable\" : \""
            + NUMERICAL_FIELD
            + "\"\n"
            + "        }\n"
            + "      },\n"
            + "      \"model_memory_limit\" : \"18mb\"\n"
            + "    }";
        putAndStartDFAAndWaitForFinish(config, "regression");
    }

    void createAndRunClassificationJob() throws Exception {
        String config = ""
            + "{\n"
            + "      \"source\" : {\n"
            + "        \"index\" : [\n"
            + "          \""
            + INDEX_NAME
            + "\"\n"
            + "        ]\n"
            + "      },\n"
            + "      \"dest\" : {\n"
            + "        \"index\" : \"classification\"\n"
            + "      },\n"
            + "      \"analysis\" : {\n"
            + "        \"classification\" : {\n"
            + "          \"dependent_variable\" : \""
            + KEYWORD_FIELD
            + "\"\n"
            + "        }\n"
            + "      },\n"
            + "      \"model_memory_limit\" : \"18mb\"\n"
            + "    }";
        putAndStartDFAAndWaitForFinish(config, "classification");
    }

    @SuppressWarnings("unchecked")
    void putAndStartDFAAndWaitForFinish(String config, String id) throws Exception {
        Request putRequest = new Request("PUT", "_ml/data_frame/analytics/" + id);
        putRequest.setJsonEntity(config);
        client().performRequest(putRequest);
        client().performRequest(new Request("POST", "_ml/data_frame/analytics/" + id + "/_start"));
        assertBusy(() -> {
            Map<String, Object> state = ((List<Map<String, Object>>) entityAsMap(
                client().performRequest(new Request("GET", "_ml/data_frame/analytics/" + id + "/_stats"))
            ).get("data_frame_analytics")).get(0);
            assertThat(state.get("state"), equalTo("stopped"));
        }, 1, TimeUnit.MINUTES);
    }

    void createPipeline(String id, String modelType, String modelId) throws Exception {
        String body = ""
            + "{\n"
            + "  \"processors\": [\n"
            + "    {\n"
            + "      \"inference\": {\n"
            + "        \"model_id\": \""
            + modelId
            + "\",\n"
            + "        \"inference_config\": {\""
            + modelType
            + "\": {}},\n"
            + "        \"field_map\": {}\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";
        Request putRequest = new Request("PUT", "_ingest/pipeline/" + id);
        putRequest.setJsonEntity(body);
        client().performRequest(putRequest);
    }

    void createIndex(String index) throws IOException {
        String mapping = ""
            + "      \"properties\": {\n"
            + "        \""
            + BOOLEAN_FIELD
            + "\": {\n"
            + "          \"type\": \"boolean\"\n"
            + "        },"
            + "        \""
            + NUMERICAL_FIELD
            + "\": {\n"
            + "          \"type\": \"double\"\n"
            + "        },"
            + "        \""
            + DISCRETE_NUMERICAL_FIELD
            + "\": {\n"
            + "          \"type\": \"integer\"\n"
            + "        },"
            + "        \""
            + KEYWORD_FIELD
            + "\": {\n"
            + "          \"type\": \"keyword\"\n"
            + "        }"
            + "    }";
        createIndex(index, Settings.EMPTY, mapping);
    }

    void indexData(String sourceIndex, int numTrainingRows) throws IOException {
        List<String> bulkRequests = new ArrayList<>();
        for (int i = 0; i < numTrainingRows; i++) {
            bulkRequests.add(
                String.format(
                    Locale.ROOT,
                    "{\"index\":{}}\n{\"%s\":%s,\"%s\":%f,\"%s\":%d,\"%s\":\"%s\"}",
                    BOOLEAN_FIELD,
                    BOOLEAN_FIELD_VALUES.get(i % BOOLEAN_FIELD_VALUES.size()),
                    NUMERICAL_FIELD,
                    NUMERICAL_FIELD_VALUES.get(i % NUMERICAL_FIELD_VALUES.size()),
                    DISCRETE_NUMERICAL_FIELD,
                    DISCRETE_NUMERICAL_FIELD_VALUES.get(i % DISCRETE_NUMERICAL_FIELD_VALUES.size()),
                    KEYWORD_FIELD,
                    KEYWORD_FIELD_VALUES.get(i % KEYWORD_FIELD_VALUES.size())
                )
            );
        }
        Request bulkRequest = new Request("POST", sourceIndex + "/_bulk?refresh=true");
        bulkRequest.setJsonEntity(String.join("\n", bulkRequests) + "\n");
        client().performRequest(bulkRequest);
    }

}
