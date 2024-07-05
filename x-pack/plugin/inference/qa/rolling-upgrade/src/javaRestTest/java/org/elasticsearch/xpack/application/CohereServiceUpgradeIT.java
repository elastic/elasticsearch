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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;

public class CohereServiceUpgradeIT extends InferenceUpgradeTestCase {

    private static final String COHERE_EMBEDDINGS_ADDED = "8.13.0";
    private static final String COHERE_RERANK_ADDED = "8.14.0";
    private static final String BYTE_ALIAS_FOR_INT8_ADDED = "8.14.0";

    private static MockWebServer cohereEmbeddingsServer;
    private static MockWebServer cohereRerankServer;

    public CohereServiceUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    // @BeforeClass
    public static void startWebServer() throws IOException {
        cohereEmbeddingsServer = new MockWebServer();
        cohereEmbeddingsServer.start();

        cohereRerankServer = new MockWebServer();
        cohereRerankServer.start();
    }

    // @AfterClass // for the awaitsfix
    public static void shutdown() {
        cohereEmbeddingsServer.close();
        cohereRerankServer.close();
    }

    @SuppressWarnings("unchecked")
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/107887")
    public void testCohereEmbeddings() throws IOException {
        var embeddingsSupported = getOldClusterTestVersion().onOrAfter(COHERE_EMBEDDINGS_ADDED);
        assumeTrue("Cohere embedding service added in " + COHERE_EMBEDDINGS_ADDED, embeddingsSupported);

        final String oldClusterIdInt8 = "old-cluster-embeddings-int8";
        final String oldClusterIdFloat = "old-cluster-embeddings-float";

        if (isOldCluster()) {
            // queue a response as PUT will call the service
            cohereEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseByte()));
            put(oldClusterIdInt8, embeddingConfigInt8(getUrl(cohereEmbeddingsServer)), TaskType.TEXT_EMBEDDING);
            // float model
            cohereEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseFloat()));
            put(oldClusterIdFloat, embeddingConfigFloat(getUrl(cohereEmbeddingsServer)), TaskType.TEXT_EMBEDDING);

            var configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, oldClusterIdInt8).get("endpoints");
            assertThat(configs, hasSize(1));
            assertEquals("cohere", configs.get(0).get("service"));
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            assertThat(serviceSettings, hasEntry("model_id", "embed-english-light-v3.0"));
            var embeddingType = serviceSettings.get("embedding_type");
            // An upgraded node will report the embedding type as byte, the old node int8
            assertThat(embeddingType, Matchers.is(oneOf("int8", "byte")));

            assertEmbeddingInference(oldClusterIdInt8, CohereEmbeddingType.BYTE);
            assertEmbeddingInference(oldClusterIdFloat, CohereEmbeddingType.FLOAT);
        } else if (isMixedCluster()) {
            var configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, oldClusterIdInt8).get("endpoints");
            assertEquals("cohere", configs.get(0).get("service"));
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            assertThat(serviceSettings, hasEntry("model_id", "embed-english-light-v3.0"));
            var embeddingType = serviceSettings.get("embedding_type");
            // An upgraded node will report the embedding type as byte, an old node int8
            assertThat(embeddingType, Matchers.is(oneOf("int8", "byte")));

            configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, oldClusterIdFloat).get("endpoints");
            serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            assertThat(serviceSettings, hasEntry("embedding_type", "float"));

            assertEmbeddingInference(oldClusterIdInt8, CohereEmbeddingType.BYTE);
            assertEmbeddingInference(oldClusterIdFloat, CohereEmbeddingType.FLOAT);
        } else if (isUpgradedCluster()) {
            // check old cluster model
            var configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, oldClusterIdInt8).get("endpoints");
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            assertThat(serviceSettings, hasEntry("model_id", "embed-english-light-v3.0"));
            assertThat(serviceSettings, hasEntry("embedding_type", "byte"));
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            assertThat(taskSettings.keySet(), empty());

            // Inference on old cluster models
            assertEmbeddingInference(oldClusterIdInt8, CohereEmbeddingType.BYTE);
            assertEmbeddingInference(oldClusterIdFloat, CohereEmbeddingType.FLOAT);

            {
                final String upgradedClusterIdByte = "upgraded-cluster-embeddings-byte";

                cohereEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseByte()));
                put(upgradedClusterIdByte, embeddingConfigByte(getUrl(cohereEmbeddingsServer)), TaskType.TEXT_EMBEDDING);

                configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, upgradedClusterIdByte).get("endpoints");
                serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
                assertThat(serviceSettings, hasEntry("embedding_type", "byte"));

                assertEmbeddingInference(upgradedClusterIdByte, CohereEmbeddingType.BYTE);
                delete(upgradedClusterIdByte);
            }
            {
                final String upgradedClusterIdInt8 = "upgraded-cluster-embeddings-int8";

                cohereEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseByte()));
                put(upgradedClusterIdInt8, embeddingConfigInt8(getUrl(cohereEmbeddingsServer)), TaskType.TEXT_EMBEDDING);

                configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, upgradedClusterIdInt8).get("endpoints");
                serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
                assertThat(serviceSettings, hasEntry("embedding_type", "byte")); // int8 rewritten to byte

                assertEmbeddingInference(upgradedClusterIdInt8, CohereEmbeddingType.INT8);
                delete(upgradedClusterIdInt8);
            }
            {
                final String upgradedClusterIdFloat = "upgraded-cluster-embeddings-float";
                cohereEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseFloat()));
                put(upgradedClusterIdFloat, embeddingConfigFloat(getUrl(cohereEmbeddingsServer)), TaskType.TEXT_EMBEDDING);

                configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, upgradedClusterIdFloat).get("endpoints");
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
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/107887")
    public void testRerank() throws IOException {
        var rerankSupported = getOldClusterTestVersion().onOrAfter(COHERE_RERANK_ADDED);
        assumeTrue("Cohere rerank service added in " + COHERE_RERANK_ADDED, rerankSupported);

        final String oldClusterId = "old-cluster-rerank";
        final String upgradedClusterId = "upgraded-cluster-rerank";

        if (isOldCluster()) {
            put(oldClusterId, rerankConfig(getUrl(cohereRerankServer)), TaskType.RERANK);
            var configs = (List<Map<String, Object>>) get(TaskType.RERANK, oldClusterId).get("endpoints");
            assertThat(configs, hasSize(1));

            assertRerank(oldClusterId);
        } else if (isMixedCluster()) {
            var configs = (List<Map<String, Object>>) get(TaskType.RERANK, oldClusterId).get("endpoints");
            assertEquals("cohere", configs.get(0).get("service"));
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            assertThat(serviceSettings, hasEntry("model_id", "rerank-english-v3.0"));
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            assertThat(taskSettings, hasEntry("top_n", 3));

            assertRerank(oldClusterId);

        } else if (isUpgradedCluster()) {
            // check old cluster model
            var configs = (List<Map<String, Object>>) get(TaskType.RERANK, oldClusterId).get("endpoints");
            assertEquals("cohere", configs.get(0).get("service"));
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            assertThat(serviceSettings, hasEntry("model_id", "rerank-english-v3.0"));
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            assertThat(taskSettings, hasEntry("top_n", 3));

            assertRerank(oldClusterId);

            // New endpoint
            put(upgradedClusterId, rerankConfig(getUrl(cohereRerankServer)), TaskType.RERANK);
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
