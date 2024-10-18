/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.qa.mixed;

import org.elasticsearch.Version;
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

import static org.elasticsearch.xpack.inference.qa.mixed.MixedClusterSpecTestCase.bwcVersion;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;

public class CohereServiceMixedIT extends BaseMixedTestCase {

    private static final String COHERE_EMBEDDINGS_ADDED = "8.13.0";
    private static final String COHERE_RERANK_ADDED = "8.14.0";
    private static final String COHERE_EMBEDDINGS_CHUNKING_SETTINGS_ADDED = "8.16.0";
    private static final String BYTE_ALIAS_FOR_INT8_ADDED = "8.14.0";
    private static final String MINIMUM_SUPPORTED_VERSION = "8.15.0";

    private static MockWebServer cohereEmbeddingsServer;
    private static MockWebServer cohereRerankServer;

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
        var embeddingsSupported = bwcVersion.onOrAfter(Version.fromString(COHERE_EMBEDDINGS_ADDED));
        assumeTrue("Cohere embedding service added in " + COHERE_EMBEDDINGS_ADDED, embeddingsSupported);
        assumeTrue(
            "Cohere service requires at least " + MINIMUM_SUPPORTED_VERSION,
            bwcVersion.onOrAfter(Version.fromString(MINIMUM_SUPPORTED_VERSION))
        );

        final String inferenceIdInt8 = "mixed-cluster-cohere-embeddings-int8";
        final String inferenceIdFloat = "mixed-cluster-cohere-embeddings-float";

        try {
            // queue a response as PUT will call the service
            cohereEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseByte()));
            put(inferenceIdInt8, embeddingConfigInt8(getUrl(cohereEmbeddingsServer)), TaskType.TEXT_EMBEDDING);

            // float model
            cohereEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseFloat()));
            put(inferenceIdFloat, embeddingConfigFloat(getUrl(cohereEmbeddingsServer)), TaskType.TEXT_EMBEDDING);
        } catch (Exception e) {
            if (bwcVersion.before(Version.fromString(COHERE_EMBEDDINGS_CHUNKING_SETTINGS_ADDED))) {
                // Chunking settings were added in 8.16.0. if the version is before that, an exception will be thrown if the index mapping
                // was created based on a mapping from an old node
                assertThat(
                    e.getMessage(),
                    containsString(
                        "One or more nodes in your cluster does not support chunking_settings. "
                            + "Please update all nodes in your cluster to the latest version to use chunking_settings."
                    )
                );
                return;
            }
        }

        var configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, inferenceIdInt8).get("endpoints");
        assertEquals("cohere", configs.get(0).get("service"));
        var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
        assertThat(serviceSettings, hasEntry("model_id", "embed-english-light-v3.0"));
        var embeddingType = serviceSettings.get("embedding_type");
        // An upgraded node will report the embedding type as byte, an old node int8
        assertThat(embeddingType, Matchers.is(oneOf("int8", "byte")));

        configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, inferenceIdFloat).get("endpoints");
        serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
        assertThat(serviceSettings, hasEntry("embedding_type", "float"));

        assertEmbeddingInference(inferenceIdInt8, CohereEmbeddingType.BYTE);
        assertEmbeddingInference(inferenceIdFloat, CohereEmbeddingType.FLOAT);

        delete(inferenceIdFloat);
        delete(inferenceIdInt8);

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
        var rerankSupported = bwcVersion.onOrAfter(Version.fromString(COHERE_RERANK_ADDED));
        assumeTrue("Cohere rerank service added in " + COHERE_RERANK_ADDED, rerankSupported);
        assumeTrue(
            "Cohere service requires at least " + MINIMUM_SUPPORTED_VERSION,
            bwcVersion.onOrAfter(Version.fromString(MINIMUM_SUPPORTED_VERSION))
        );

        final String inferenceId = "mixed-cluster-rerank";

        put(inferenceId, rerankConfig(getUrl(cohereRerankServer)), TaskType.RERANK);
        assertRerank(inferenceId);

        var configs = (List<Map<String, Object>>) get(TaskType.RERANK, inferenceId).get("endpoints");
        assertThat(configs, hasSize(1));
        assertEquals("cohere", configs.get(0).get("service"));
        var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
        assertThat(serviceSettings, hasEntry("model_id", "rerank-english-v3.0"));
        var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
        assertThat(taskSettings, hasEntry("top_n", 3));

        assertRerank(inferenceId);

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
