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
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingType;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

public class CohereServiceUpgradeIT extends InferenceUpgradeTestCase {

    // TODO: replace with proper test features
    private static final String COHERE_EMBEDDINGS_ADDED_TEST_FEATURE = "gte_v8.13.0";
    private static final String COHERE_RERANK_ADDED_TEST_FEATURE = "gte_v8.14.0";

    private static MockWebServer cohereEmbeddingsServer;
    private static MockWebServer cohereRerankServer;

    public CohereServiceUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @BeforeClass
    public static void startWebServer() throws IOException {
        cohereEmbeddingsServer = new MockWebServer();
        cohereEmbeddingsServer.start();

        cohereRerankServer = new MockWebServer();
        cohereRerankServer.start();
    }

    @AfterClass
    public static void shutdown() {
        cohereEmbeddingsServer.close();
        cohereRerankServer.close();
    }

    @SuppressWarnings("unchecked")
    public void testCohereEmbeddings() throws IOException {
        var embeddingsSupported = oldClusterHasFeature(COHERE_EMBEDDINGS_ADDED_TEST_FEATURE);
        String oldClusterEndpointIdentifier = oldClusterHasFeature(MODELS_RENAMED_TO_ENDPOINTS_FEATURE) ? "endpoints" : "models";
        assumeTrue("Cohere embedding service supported", embeddingsSupported);

        final String oldClusterIdInt8 = "old-cluster-embeddings-int8";
        final String oldClusterIdFloat = "old-cluster-embeddings-float";

        var testTaskType = TaskType.TEXT_EMBEDDING;

        if (isOldCluster()) {
            // queue a response as PUT will call the service
            cohereEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseByte()));
            put(oldClusterIdInt8, embeddingConfigInt8(getUrl(cohereEmbeddingsServer)), testTaskType);
            // float model
            cohereEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseFloat()));
            put(oldClusterIdFloat, embeddingConfigFloat(getUrl(cohereEmbeddingsServer)), testTaskType);

            {
                var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterIdInt8).get(oldClusterEndpointIdentifier);
                assertThat(configs, hasSize(1));
                assertEquals("cohere", configs.get(0).get("service"));
                var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
                assertThat(serviceSettings, hasEntry("model_id", "embed-english-light-v3.0"));
                var embeddingType = serviceSettings.get("embedding_type");
                // An upgraded node will report the embedding type as byte, the old node int8
                assertThat(embeddingType, Matchers.is(oneOf("int8", "byte")));
                assertEmbeddingInference(oldClusterIdInt8, CohereEmbeddingType.BYTE);
            }
            {
                var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterIdFloat).get(oldClusterEndpointIdentifier);
                assertThat(configs, hasSize(1));
                assertEquals("cohere", configs.get(0).get("service"));
                var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
                assertThat(serviceSettings, hasEntry("model_id", "embed-english-light-v3.0"));
                assertThat(serviceSettings, hasEntry("embedding_type", "float"));
                assertEmbeddingInference(oldClusterIdFloat, CohereEmbeddingType.FLOAT);
            }
        } else if (isMixedCluster()) {
            {
                var configs = getConfigsWithBreakingChangeHandling(testTaskType, oldClusterIdInt8);
                assertEquals("cohere", configs.get(0).get("service"));
                var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
                assertThat(serviceSettings, hasEntry("model_id", "embed-english-light-v3.0"));
                var embeddingType = serviceSettings.get("embedding_type");
                // An upgraded node will report the embedding type as byte, an old node int8
                assertThat(embeddingType, Matchers.is(oneOf("int8", "byte")));
                assertEmbeddingInference(oldClusterIdInt8, CohereEmbeddingType.BYTE);
            }
            {
                var configs = getConfigsWithBreakingChangeHandling(testTaskType, oldClusterIdFloat);
                assertEquals("cohere", configs.get(0).get("service"));
                var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
                assertThat(serviceSettings, hasEntry("model_id", "embed-english-light-v3.0"));
                assertThat(serviceSettings, hasEntry("embedding_type", "float"));
                assertEmbeddingInference(oldClusterIdFloat, CohereEmbeddingType.FLOAT);
            }
        } else if (isUpgradedCluster()) {
            // check old cluster model
            var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterIdInt8).get("endpoints");
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            assertThat(serviceSettings, hasEntry("model_id", "embed-english-light-v3.0"));
            assertThat(serviceSettings, hasEntry("embedding_type", "byte"));
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            assertThat(taskSettings, anyOf(nullValue(), anEmptyMap()));

            // Inference on old cluster models
            assertEmbeddingInference(oldClusterIdInt8, CohereEmbeddingType.BYTE);
            assertEmbeddingInference(oldClusterIdFloat, CohereEmbeddingType.FLOAT);

            {
                final String upgradedClusterIdByte = "upgraded-cluster-embeddings-byte";

                cohereEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseByte()));
                put(upgradedClusterIdByte, embeddingConfigByte(getUrl(cohereEmbeddingsServer)), testTaskType);

                configs = (List<Map<String, Object>>) get(testTaskType, upgradedClusterIdByte).get("endpoints");
                serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
                assertThat(serviceSettings, hasEntry("embedding_type", "byte"));

                assertEmbeddingInference(upgradedClusterIdByte, CohereEmbeddingType.BYTE);
                delete(upgradedClusterIdByte);
            }
            {
                final String upgradedClusterIdInt8 = "upgraded-cluster-embeddings-int8";

                cohereEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseByte()));
                put(upgradedClusterIdInt8, embeddingConfigInt8(getUrl(cohereEmbeddingsServer)), testTaskType);

                configs = (List<Map<String, Object>>) get(testTaskType, upgradedClusterIdInt8).get("endpoints");
                serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
                assertThat(serviceSettings, hasEntry("embedding_type", "byte")); // int8 rewritten to byte

                assertEmbeddingInference(upgradedClusterIdInt8, CohereEmbeddingType.INT8);
                delete(upgradedClusterIdInt8);
            }
            {
                final String upgradedClusterIdFloat = "upgraded-cluster-embeddings-float";
                cohereEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseFloat()));
                put(upgradedClusterIdFloat, embeddingConfigFloat(getUrl(cohereEmbeddingsServer)), testTaskType);

                configs = (List<Map<String, Object>>) get(testTaskType, upgradedClusterIdFloat).get("endpoints");
                serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
                assertThat(serviceSettings, hasEntry("embedding_type", "float"));

                assertEmbeddingInference(upgradedClusterIdFloat, CohereEmbeddingType.FLOAT);
                delete(upgradedClusterIdFloat);
            }

            delete(oldClusterIdFloat);
            delete(oldClusterIdInt8);
        }
    }

    void assertEmbeddingInference(String inferenceId, CohereEmbeddingType type) throws IOException {
        switch (type) {
            case INT8:
            case BYTE:
                cohereEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseByte()));
                break;
            case FLOAT:
                cohereEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseFloat()));
        }

        var inferenceMap = inference(inferenceId, TaskType.TEXT_EMBEDDING, "some text");
        assertThat(inferenceMap.entrySet(), not(empty()));
    }

    @SuppressWarnings("unchecked")
    public void testRerank() throws IOException {
        var rerankSupported = oldClusterHasFeature(COHERE_RERANK_ADDED_TEST_FEATURE);
        String old_cluster_endpoint_identifier = oldClusterHasFeature(MODELS_RENAMED_TO_ENDPOINTS_FEATURE) ? "endpoints" : "models";
        assumeTrue("Cohere rerank service supported", rerankSupported);

        final String oldClusterId = "old-cluster-rerank";
        final String upgradedClusterId = "upgraded-cluster-rerank";

        var testTaskType = TaskType.RERANK;

        if (isOldCluster()) {
            cohereRerankServer.enqueue(new MockResponse().setResponseCode(200).setBody(rerankResponse()));
            put(oldClusterId, rerankConfig(getUrl(cohereRerankServer)), testTaskType);
            var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterId).get(old_cluster_endpoint_identifier);
            assertThat(configs, hasSize(1));

            assertRerank(oldClusterId);
        } else if (isMixedCluster()) {
            var configs = getConfigsWithBreakingChangeHandling(testTaskType, oldClusterId);

            assertEquals("cohere", configs.get(0).get("service"));
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            assertThat(serviceSettings, hasEntry("model_id", "rerank-english-v3.0"));
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            assertThat(taskSettings, hasEntry("top_n", 3));

            assertRerank(oldClusterId);

        } else if (isUpgradedCluster()) {
            // check old cluster model
            var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterId).get("endpoints");
            assertEquals("cohere", configs.get(0).get("service"));
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            assertThat(serviceSettings, hasEntry("model_id", "rerank-english-v3.0"));
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            assertThat(taskSettings, hasEntry("top_n", 3));

            assertRerank(oldClusterId);

            // New endpoint
            cohereRerankServer.enqueue(new MockResponse().setResponseCode(200).setBody(rerankResponse()));
            put(upgradedClusterId, rerankConfig(getUrl(cohereRerankServer)), testTaskType);
            configs = (List<Map<String, Object>>) get(upgradedClusterId).get("endpoints");
            assertThat(configs, hasSize(1));

            assertRerank(upgradedClusterId);

            delete(oldClusterId);
            delete(upgradedClusterId);
        }
    }

    private void assertRerank(String inferenceId) throws IOException {
        cohereRerankServer.enqueue(new MockResponse().setResponseCode(200).setBody(rerankResponse()));
        var inferenceMap = rerank(
            inferenceId,
            List.of("luke", "like", "leia", "chewy", "r2d2", "star", "wars"),
            "star wars main character"
        );
        assertThat(inferenceMap.entrySet(), not(empty()));
    }

    private String embeddingConfigByte(String url) {
        return embeddingConfigTemplate(url, "byte");
    }

    private String embeddingConfigInt8(String url) {
        return embeddingConfigTemplate(url, "int8");
    }

    private String embeddingConfigFloat(String url) {
        return embeddingConfigTemplate(url, "float");
    }

    private String embeddingConfigTemplate(String url, String embeddingType) {
        return Strings.format("""
            {
                "service": "cohere",
                "service_settings": {
                    "url": "%s",
                    "api_key": "XXXX",
                    "model_id": "embed-english-light-v3.0",
                    "embedding_type": "%s"
                }
            }
            """, url, embeddingType);
    }

    private String embeddingResponseByte() {
        return """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": [
                    [
                        12,
                        56
                    ]
                ],
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_bytes"
            }
            """;
    }

    private String embeddingResponseFloat() {
        return """
            {
                "id": "3198467e-399f-4d4a-aa2c-58af93bd6dc4",
                "texts": [
                    "hello"
                ],
                "embeddings": [
                    [
                        -0.0018434525,
                        0.01777649
                    ]
                ],
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "input_tokens": 1
                    }
                },
                "response_type": "embeddings_floats"
            }
            """;
    }

    private String rerankConfig(String url) {
        return Strings.format("""
            {
                "service": "cohere",
                "service_settings": {
                    "api_key": "XXXX",
                    "model_id": "rerank-english-v3.0",
                    "url": "%s"
                },
                "task_settings": {
                    "return_documents": false,
                    "top_n": 3
                }
            }
            """, url);
    }

    private String rerankResponse() {
        return """
            {
                "index": "d0760819-5a73-4d58-b163-3956d3648b62",
                "results": [
                    {
                        "index": 2,
                        "relevance_score": 0.98005307
                    },
                    {
                        "index": 3,
                        "relevance_score": 0.27904198
                    },
                    {
                        "index": 0,
                        "relevance_score": 0.10194652
                    }
                ],
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "search_units": 1
                    }
                }
            }
            """;
    }

}
