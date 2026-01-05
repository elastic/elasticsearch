/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class HuggingFaceServiceUpgradeIT extends InferenceUpgradeTestCase {

    // TODO: replace with proper test features
    private static final String HF_EMBEDDINGS_TEST_FEATURE = "gte_v8.12.0";
    private static final String HF_ELSER_TEST_FEATURE = "gte_v8.12.0";

    private static MockWebServer embeddingsServer;
    private static MockWebServer elserServer;

    public HuggingFaceServiceUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @BeforeClass
    public static void startWebServer() throws IOException {
        embeddingsServer = new MockWebServer();
        embeddingsServer.start();

        elserServer = new MockWebServer();
        elserServer.start();
    }

    @AfterClass
    public static void shutdown() {
        embeddingsServer.close();
        elserServer.close();
    }

    @SuppressWarnings("unchecked")
    public void testHFEmbeddings() throws IOException {
        var embeddingsSupported = oldClusterHasFeature(HF_EMBEDDINGS_TEST_FEATURE);
        String oldClusterEndpointIdentifier = oldClusterHasFeature(MODELS_RENAMED_TO_ENDPOINTS_FEATURE) ? "endpoints" : "models";
        assumeTrue("Hugging Face embedding service supported", embeddingsSupported);

        final String oldClusterId = "old-cluster-embeddings";
        final String upgradedClusterId = "upgraded-cluster-embeddings";

        var testTaskType = TaskType.TEXT_EMBEDDING;

        if (isOldCluster()) {
            // queue a response as PUT will call the service
            embeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));
            put(oldClusterId, embeddingConfig(getUrl(embeddingsServer)), testTaskType);

            var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterId).get(oldClusterEndpointIdentifier);
            assertThat(configs, hasSize(1));

            assertEmbeddingInference(oldClusterId);
        } else if (isMixedCluster()) {
            var configs = getConfigsWithBreakingChangeHandling(testTaskType, oldClusterId);

            assertEquals("hugging_face", configs.get(0).get("service"));

            assertEmbeddingInference(oldClusterId);
        } else if (isUpgradedCluster()) {
            // check old cluster model
            var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterId).get("endpoints");
            assertEquals("hugging_face", configs.get(0).get("service"));

            // Inference on old cluster model
            assertEmbeddingInference(oldClusterId);

            embeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));
            put(upgradedClusterId, embeddingConfig(getUrl(embeddingsServer)), testTaskType);

            configs = (List<Map<String, Object>>) get(testTaskType, upgradedClusterId).get("endpoints");
            assertThat(configs, hasSize(1));

            assertEmbeddingInference(upgradedClusterId);

            delete(oldClusterId);
            delete(upgradedClusterId);
        }
    }

    void assertEmbeddingInference(String inferenceId) throws IOException {
        embeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));
        var inferenceMap = inference(inferenceId, TaskType.TEXT_EMBEDDING, "some text");
        assertThat(inferenceMap.entrySet(), not(empty()));
    }

    @SuppressWarnings("unchecked")
    public void testElser() throws IOException {
        var supported = oldClusterHasFeature(HF_ELSER_TEST_FEATURE);
        String old_cluster_endpoint_identifier = oldClusterHasFeature(MODELS_RENAMED_TO_ENDPOINTS_FEATURE) ? "endpoints" : "models";
        assumeTrue("HF elser service supported", supported);

        final String oldClusterId = "old-cluster-elser";
        final String upgradedClusterId = "upgraded-cluster-elser";

        var testTaskType = TaskType.SPARSE_EMBEDDING;

        if (isOldCluster()) {
            elserServer.enqueue(new MockResponse().setResponseCode(200).setBody(elserResponse()));
            put(oldClusterId, elserConfig(getUrl(elserServer)), testTaskType);
            var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterId).get(old_cluster_endpoint_identifier);
            assertThat(configs, hasSize(1));

            assertElser(oldClusterId);
        } else if (isMixedCluster()) {
            var configs = getConfigsWithBreakingChangeHandling(testTaskType, oldClusterId);
            assertEquals("hugging_face", configs.get(0).get("service"));
            assertElser(oldClusterId);
        } else if (isUpgradedCluster()) {
            // check old cluster model
            var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterId).get("endpoints");
            assertEquals("hugging_face", configs.get(0).get("service"));
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            assertThat(taskSettings, anyOf(nullValue(), anEmptyMap()));

            assertElser(oldClusterId);

            // New endpoint
            elserServer.enqueue(new MockResponse().setResponseCode(200).setBody(elserResponse()));
            put(upgradedClusterId, elserConfig(getUrl(elserServer)), testTaskType);
            configs = (List<Map<String, Object>>) get(upgradedClusterId).get("endpoints");
            assertThat(configs, hasSize(1));

            assertElser(upgradedClusterId);

            delete(oldClusterId);
            delete(upgradedClusterId);
        }
    }

    private void assertElser(String inferenceId) throws IOException {
        elserServer.enqueue(new MockResponse().setResponseCode(200).setBody(elserResponse()));
        var inferenceMap = inference(inferenceId, TaskType.SPARSE_EMBEDDING, "some text");
        assertThat(inferenceMap.entrySet(), not(empty()));
    }

    static String embeddingConfig(String url) {
        return Strings.format("""
            {
                "service": "hugging_face",
                "service_settings": {
                    "url": "%s",
                    "api_key": "XXXX"
                }
            }
            """, url);
    }

    private String embeddingResponse() {
        return """
            [
                  [
                      0.014539449,
                      -0.015288644
                  ]
            ]
            """;
    }

    static String elserConfig(String url) {
        return Strings.format("""
            {
                "service": "hugging_face",
                "service_settings": {
                    "api_key": "XXXX",
                    "url": "%s"
                }
            }
            """, url);
    }

    static String elserResponse() {
        return """
            [
                {
                    ".": 0.133155956864357,
                    "the": 0.6747211217880249
                }
            ]
            """;
    }

}
