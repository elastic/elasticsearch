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
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class OpenAIServiceMixedIT extends BaseMixedTestCase {

    private static final String OPEN_AI_EMBEDDINGS_ADDED = "8.12.0";
    private static final String OPEN_AI_EMBEDDINGS_MODEL_SETTING_MOVED = "8.13.0";
    private static final String OPEN_AI_EMBEDDINGS_CHUNKING_SETTINGS_ADDED = "8.16.0";
    private static final String OPEN_AI_COMPLETIONS_ADDED = "8.14.0";
    private static final String MINIMUM_SUPPORTED_VERSION = "8.15.0";

    private static MockWebServer openAiEmbeddingsServer;
    private static MockWebServer openAiChatCompletionsServer;

    @BeforeClass
    public static void startWebServer() throws IOException {
        openAiEmbeddingsServer = new MockWebServer();
        openAiEmbeddingsServer.start();

        openAiChatCompletionsServer = new MockWebServer();
        openAiChatCompletionsServer.start();
    }

    @AfterClass
    public static void shutdown() {
        openAiEmbeddingsServer.close();
        openAiChatCompletionsServer.close();
    }

    @AwaitsFix(bugUrl = "Backport #112074 to 8.16")
    @SuppressWarnings("unchecked")
    public void testOpenAiEmbeddings() throws IOException {
        var openAiEmbeddingsSupported = bwcVersion.onOrAfter(Version.fromString(OPEN_AI_EMBEDDINGS_ADDED));
        assumeTrue("OpenAI embedding service added in " + OPEN_AI_EMBEDDINGS_ADDED, openAiEmbeddingsSupported);
        assumeTrue(
            "OpenAI service requires at least " + MINIMUM_SUPPORTED_VERSION,
            bwcVersion.onOrAfter(Version.fromString(MINIMUM_SUPPORTED_VERSION))
        );

        final String inferenceId = "mixed-cluster-embeddings";

        String inferenceConfig = oldClusterVersionCompatibleEmbeddingConfig();
        // queue a response as PUT will call the service
        openAiEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));

        try {
            put(inferenceId, inferenceConfig, TaskType.TEXT_EMBEDDING);
        } catch (Exception e) {
            if (getOldClusterTestVersion().before(OPEN_AI_EMBEDDINGS_CHUNKING_SETTINGS_ADDED)) {
                // Chunking settings were added in 8.16.0. if the version is before that, an exception will be thrown if the index mapping
                // was created based on a mapping from an old node
                assertThat(
                    e.getMessage(),
                    containsString(
                        "One or more nodes in your cluster does not support chunking_settings. "
                            + "Please update all nodes in your cluster to use chunking_settings."
                    )
                );
                return;
            }
        }

        var configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, inferenceId).get("endpoints");
        assertThat(configs, hasSize(1));
        assertEquals("openai", configs.get(0).get("service"));
        var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
        var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
        var modelIdFound = serviceSettings.containsKey("model_id") || taskSettings.containsKey("model_id");
        assertTrue("model_id not found in config: " + configs.toString(), modelIdFound);

        assertEmbeddingInference(inferenceId);
    }

    void assertEmbeddingInference(String inferenceId) throws IOException {
        openAiEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));
        var inferenceMap = inference(inferenceId, TaskType.TEXT_EMBEDDING, "some text");
        assertThat(inferenceMap.entrySet(), not(empty()));
    }

    @SuppressWarnings("unchecked")
    public void testOpenAiCompletions() throws IOException {
        var openAiEmbeddingsSupported = bwcVersion.onOrAfter(Version.fromString(OPEN_AI_EMBEDDINGS_ADDED));
        assumeTrue("OpenAI completions service added in " + OPEN_AI_COMPLETIONS_ADDED, openAiEmbeddingsSupported);
        assumeTrue(
            "OpenAI service requires at least " + MINIMUM_SUPPORTED_VERSION,
            bwcVersion.onOrAfter(Version.fromString(MINIMUM_SUPPORTED_VERSION))
        );

        final String inferenceId = "mixed-cluster-completions";
        final String upgradedClusterId = "upgraded-cluster-completions";

        // queue a response as PUT will call the service
        openAiChatCompletionsServer.enqueue(new MockResponse().setResponseCode(200).setBody(chatCompletionsResponse()));
        put(inferenceId, chatCompletionsConfig(getUrl(openAiChatCompletionsServer)), TaskType.COMPLETION);

        var configsMap = get(TaskType.COMPLETION, inferenceId);
        var configs = (List<Map<String, Object>>) configsMap.get("endpoints");
        assertThat(configs, hasSize(1));
        assertEquals("openai", configs.get(0).get("service"));
        var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
        assertThat(serviceSettings, hasEntry("model_id", "gpt-4"));
        var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
        assertThat(taskSettings, anyOf(nullValue(), anEmptyMap()));

        assertCompletionInference(inferenceId);
    }

    void assertCompletionInference(String inferenceId) throws IOException {
        openAiChatCompletionsServer.enqueue(new MockResponse().setResponseCode(200).setBody(chatCompletionsResponse()));
        var inferenceMap = inference(inferenceId, TaskType.COMPLETION, "some text");
        assertThat(inferenceMap.entrySet(), not(empty()));
    }

    private String oldClusterVersionCompatibleEmbeddingConfig() {
        if (getOldClusterTestVersion().before(OPEN_AI_EMBEDDINGS_MODEL_SETTING_MOVED)) {
            return embeddingConfigWithModelInTaskSettings(getUrl(openAiEmbeddingsServer));
        } else {
            return embeddingConfigWithModelInServiceSettings(getUrl(openAiEmbeddingsServer));
        }
    }

    protected static org.elasticsearch.test.cluster.util.Version getOldClusterTestVersion() {
        return org.elasticsearch.test.cluster.util.Version.fromString(bwcVersion.toString());
    }

    private String embeddingConfigWithModelInTaskSettings(String url) {
        return Strings.format("""
            {
                "service": "openai",
                "service_settings": {
                    "api_key": "XXXX",
                    "url": "%s"
                },
                "task_settings": {
                   "model": "text-embedding-ada-002"
                }
            }
            """, url);
    }

    static String embeddingConfigWithModelInServiceSettings(String url) {
        return Strings.format("""
            {
                "service": "openai",
                "service_settings": {
                    "api_key": "XXXX",
                    "url": "%s",
                    "model_id": "text-embedding-ada-002"
                }
            }
            """, url);
    }

    private String chatCompletionsConfig(String url) {
        return Strings.format("""
            {
                "service": "openai",
                "service_settings": {
                    "api_key": "XXXX",
                    "url": "%s",
                    "model_id": "gpt-4"
                }
            }
            """, url);
    }

    static String embeddingResponse() {
        return """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          0.0123,
                          -0.0123
                      ]
                  }
              ],
              "model": "text-embedding-ada-002",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;
    }

    private String chatCompletionsResponse() {
        return """
            {
              "id": "some-id",
              "object": "chat.completion",
              "created": 1705397787,
              "model": "gpt-3.5-turbo-0613",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "some content"
                  },
                  "logprobs": null,
                  "finish_reason": "stop"
                }
              ],
              "usage": {
                "prompt_tokens": 46,
                "completion_tokens": 39,
                "total_tokens": 85
              },
              "system_fingerprint": null
            }
            """;
    }

}
