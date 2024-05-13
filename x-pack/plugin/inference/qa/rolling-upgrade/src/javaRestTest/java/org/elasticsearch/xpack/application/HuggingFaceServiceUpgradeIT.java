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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class HuggingFaceServiceUpgradeIT extends InferenceUpgradeTestCase {

    private static final String HF_EMBEDDINGS_ADDED = "8.12.0";
    private static final String HF_ELSER_ADDED = "8.12.0";

    private static MockWebServer embeddingsServer;
    private static MockWebServer elserServer;

    public HuggingFaceServiceUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    // @BeforeClass
    public static void startWebServer() throws IOException {
        embeddingsServer = new MockWebServer();
        embeddingsServer.start();

        elserServer = new MockWebServer();
        elserServer.start();
    }

    // @AfterClass for the awaits fix
    public static void shutdown() {
        embeddingsServer.close();
        elserServer.close();
    }

    @SuppressWarnings("unchecked")
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/107887")
    public void testHFEmbeddings() throws IOException {
        var embeddingsSupported = getOldClusterTestVersion().onOrAfter(HF_EMBEDDINGS_ADDED);
        assumeTrue("Hugging Face embedding service added in " + HF_EMBEDDINGS_ADDED, embeddingsSupported);

        final String oldClusterId = "old-cluster-embeddings";
        final String upgradedClusterId = "upgraded-cluster-embeddings";

        if (isOldCluster()) {
            // queue a response as PUT will call the service
            embeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));
            put(oldClusterId, embeddingConfig(getUrl(embeddingsServer)), TaskType.TEXT_EMBEDDING);

            var configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, oldClusterId).get("endpoints");
            assertThat(configs, hasSize(1));

            assertEmbeddingInference(oldClusterId);
        } else if (isMixedCluster()) {
            var configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, oldClusterId).get("endpoints");
            assertEquals("hugging_face", configs.get(0).get("service"));

            assertEmbeddingInference(oldClusterId);
        } else if (isUpgradedCluster()) {
            // check old cluster model
            var configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, oldClusterId).get("endpoints");
            assertEquals("hugging_face", configs.get(0).get("service"));

            // Inference on old cluster model
            assertEmbeddingInference(oldClusterId);

            embeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));
            put(upgradedClusterId, embeddingConfig(getUrl(embeddingsServer)), TaskType.TEXT_EMBEDDING);

            configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, upgradedClusterId).get("endpoints");
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
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/107887")
    public void testElser() throws IOException {
        var supported = getOldClusterTestVersion().onOrAfter(HF_ELSER_ADDED);
        assumeTrue("HF elser service added in " + HF_ELSER_ADDED, supported);

        final String oldClusterId = "old-cluster-elser";
        final String upgradedClusterId = "upgraded-cluster-elser";

        if (isOldCluster()) {
            put(oldClusterId, elserConfig(getUrl(elserServer)), TaskType.SPARSE_EMBEDDING);
            var configs = (List<Map<String, Object>>) get(TaskType.SPARSE_EMBEDDING, oldClusterId).get("endpoints");
            assertThat(configs, hasSize(1));

            assertElser(oldClusterId);
        } else if (isMixedCluster()) {
            var configs = (List<Map<String, Object>>) get(TaskType.SPARSE_EMBEDDING, oldClusterId).get("endpoints");
            assertEquals("hugging_face", configs.get(0).get("service"));
            assertElser(oldClusterId);
        } else if (isUpgradedCluster()) {
            // check old cluster model
            var configs = (List<Map<String, Object>>) get(TaskType.SPARSE_EMBEDDING, oldClusterId).get("endpoints");
            assertEquals("hugging_face", configs.get(0).get("service"));
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            assertThat(taskSettings.keySet(), empty());

            assertElser(oldClusterId);

            // New endpoint
            put(upgradedClusterId, elserConfig(getUrl(elserServer)), TaskType.SPARSE_EMBEDDING);
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

    private String embeddingConfig(String url) {
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

    private String elserConfig(String url) {
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

    private String elserResponse() {
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
