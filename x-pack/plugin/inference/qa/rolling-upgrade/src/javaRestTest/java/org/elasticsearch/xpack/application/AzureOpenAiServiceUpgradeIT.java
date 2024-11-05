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

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class AzureOpenAiServiceUpgradeIT extends InferenceUpgradeTestCase {

    private static final String OPEN_AI_AZURE_EMBEDDINGS_ADDED = "8.14.0";

    private static MockWebServer openAiEmbeddingsServer;

    public AzureOpenAiServiceUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @BeforeClass
    public static void startWebServer() throws IOException {
        openAiEmbeddingsServer = new MockWebServer();
        openAiEmbeddingsServer.start();
    }

    @AfterClass
    public static void shutdown() {
        openAiEmbeddingsServer.close();
    }

    @SuppressWarnings("unchecked")
    @AwaitsFix(bugUrl = "Cannot set the URL in the tests")
    public void testOpenAiEmbeddings() throws IOException {
        var openAiEmbeddingsSupported = getOldClusterTestVersion().onOrAfter(OPEN_AI_AZURE_EMBEDDINGS_ADDED);
        // `gte_v` indicates that the cluster version is Greater Than or Equal to MODELS_RENAMED_TO_ENDPOINTS
        String oldClusterEndpointIdentifier = oldClusterHasFeature("gte_v" + MODELS_RENAMED_TO_ENDPOINTS) ? "endpoints" : "models";
        assumeTrue("Azure OpenAI embedding service added in " + OPEN_AI_AZURE_EMBEDDINGS_ADDED, openAiEmbeddingsSupported);

        final String oldClusterId = "old-cluster-embeddings";
        final String upgradedClusterId = "upgraded-cluster-embeddings";

        var testTaskType = TaskType.TEXT_EMBEDDING;

        if (isOldCluster()) {
            // queue a response as PUT will call the service
            openAiEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(OpenAiServiceUpgradeIT.embeddingResponse()));
            put(oldClusterId, embeddingConfig(getUrl(openAiEmbeddingsServer)), testTaskType);

            var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterId).get(oldClusterEndpointIdentifier);
            assertThat(configs, hasSize(1));
        } else if (isMixedCluster()) {
            var configs = getConfigsWithBreakingChangeHandling(testTaskType, oldClusterId);
            assertEquals("azureopenai", configs.get(0).get("service"));

            assertEmbeddingInference(oldClusterId);
        } else if (isUpgradedCluster()) {
            // check old cluster model
            var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterId).get("endpoints");
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");

            // Inference on old cluster model
            assertEmbeddingInference(oldClusterId);

            openAiEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(OpenAiServiceUpgradeIT.embeddingResponse()));
            put(upgradedClusterId, embeddingConfig(getUrl(openAiEmbeddingsServer)), testTaskType);

            configs = (List<Map<String, Object>>) get(testTaskType, upgradedClusterId).get("endpoints");
            assertThat(configs, hasSize(1));

            // Inference on the new config
            assertEmbeddingInference(upgradedClusterId);

            delete(oldClusterId);
            delete(upgradedClusterId);
        }
    }

    void assertEmbeddingInference(String inferenceId) throws IOException {
        openAiEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(OpenAiServiceUpgradeIT.embeddingResponse()));
        var inferenceMap = inference(inferenceId, TaskType.TEXT_EMBEDDING, "some text");
        assertThat(inferenceMap.entrySet(), not(empty()));
    }

    private String embeddingConfig(String url) {
        return Strings.format("""
            {
                "service": "azureopenai",
                "service_settings": {
                    "api_key": "XXXX",
                    "url": "%s",
                    "resource_name": "resource_name",
                    "deployment_id": "deployment_id",
                    "api_version": "2024-02-01"
                }
            }
            """, url);
    }

}
