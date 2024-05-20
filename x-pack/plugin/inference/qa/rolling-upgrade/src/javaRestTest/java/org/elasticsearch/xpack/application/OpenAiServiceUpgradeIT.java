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
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class OpenAiServiceUpgradeIT extends InferenceUpgradeTestCase {

    private static final String OPEN_AI_EMBEDDINGS_ADDED = "8.12.0";
    private static final String OPEN_AI_EMBEDDINGS_MODEL_SETTING_MOVED = "8.13.0";
    private static final String OPEN_AI_COMPLETIONS_ADDED = "8.14.0";

    private static MockWebServer openAiEmbeddingsServer;
    private static MockWebServer openAiChatCompletionsServer;

    public OpenAiServiceUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    // @BeforeClass
    public static void startWebServer() throws IOException {
        openAiEmbeddingsServer = new MockWebServer();
        openAiEmbeddingsServer.start();

        openAiChatCompletionsServer = new MockWebServer();
        openAiChatCompletionsServer.start();
    }

    // @AfterClass for the awaits fix
    public static void shutdown() {
        openAiEmbeddingsServer.close();
        openAiChatCompletionsServer.close();
    }

    @SuppressWarnings("unchecked")
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/107887")
    public void testOpenAiEmbeddings() throws IOException {
        var openAiEmbeddingsSupported = getOldClusterTestVersion().onOrAfter(OPEN_AI_EMBEDDINGS_ADDED);
        assumeTrue("OpenAI embedding service added in " + OPEN_AI_EMBEDDINGS_ADDED, openAiEmbeddingsSupported);

        final String oldClusterId = "old-cluster-embeddings";
        final String upgradedClusterId = "upgraded-cluster-embeddings";

        if (isOldCluster()) {
            String inferenceConfig = oldClusterVersionCompatibleEmbeddingConfig();
            // queue a response as PUT will call the service
            openAiEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));
            put(oldClusterId, inferenceConfig, TaskType.TEXT_EMBEDDING);

            var configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, oldClusterId).get("endpoints");
            assertThat(configs, hasSize(1));

            assertEmbeddingInference(oldClusterId);
        } else if (isMixedCluster()) {
            var configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, oldClusterId).get("endpoints");
            assertEquals("openai", configs.get(0).get("service"));
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            var modelIdFound = serviceSettings.containsKey("model_id") || taskSettings.containsKey("model_id");
            assertTrue("model_id not found in config: " + configs.toString(), modelIdFound);

            assertEmbeddingInference(oldClusterId);
        } else if (isUpgradedCluster()) {
            // check old cluster model
            var configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, oldClusterId).get("endpoints");
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            // model id is moved to service settings
            assertThat(serviceSettings, hasEntry("model_id", "text-embedding-ada-002"));
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            assertThat(taskSettings.keySet(), empty());

            // Inference on old cluster model
            assertEmbeddingInference(oldClusterId);

            String inferenceConfig = embeddingConfigWithModelInServiceSettings(getUrl(openAiEmbeddingsServer));
            openAiEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));
            put(upgradedClusterId, inferenceConfig, TaskType.TEXT_EMBEDDING);

            configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, upgradedClusterId).get("endpoints");
            assertThat(configs, hasSize(1));

            assertEmbeddingInference(upgradedClusterId);

            delete(oldClusterId);
            delete(upgradedClusterId);
        }
    }

    void assertEmbeddingInference(String inferenceId) throws IOException {
        openAiEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));
        var inferenceMap = inference(inferenceId, TaskType.TEXT_EMBEDDING, "some text");
        assertThat(inferenceMap.entrySet(), not(empty()));
    }

    @SuppressWarnings("unchecked")
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/107887")
    public void testOpenAiCompletions() throws IOException {
        var openAiEmbeddingsSupported = getOldClusterTestVersion().onOrAfter(OPEN_AI_COMPLETIONS_ADDED);
        assumeTrue("OpenAI completions service added in " + OPEN_AI_COMPLETIONS_ADDED, openAiEmbeddingsSupported);

        final String oldClusterId = "old-cluster-completions";
        final String upgradedClusterId = "upgraded-cluster-completions";

        if (isOldCluster()) {
            put(oldClusterId, chatCompletionsConfig(getUrl(openAiChatCompletionsServer)), TaskType.COMPLETION);

            var configs = (List<Map<String, Object>>) get(TaskType.COMPLETION, oldClusterId).get("endpoints");
            assertThat(configs, hasSize(1));

            assertCompletionInference(oldClusterId);
        } else if (isMixedCluster()) {
            var configs = (List<Map<String, Object>>) get(TaskType.COMPLETION, oldClusterId).get("endpoints");
            assertEquals("openai", configs.get(0).get("service"));
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            assertThat(serviceSettings, hasEntry("model_id", "gpt-4"));
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            assertThat(taskSettings.keySet(), empty());

            assertCompletionInference(oldClusterId);
        } else if (isUpgradedCluster()) {
            // check old cluster model
            var configs = (List<Map<String, Object>>) get(TaskType.COMPLETION, oldClusterId).get("endpoints");
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            assertThat(serviceSettings, hasEntry("model_id", "gpt-4"));
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            assertThat(taskSettings.keySet(), empty());

            assertCompletionInference(oldClusterId);

            put(upgradedClusterId, chatCompletionsConfig(getUrl(openAiChatCompletionsServer)), TaskType.COMPLETION);
            configs = (List<Map<String, Object>>) get(TaskType.COMPLETION, upgradedClusterId).get("endpoints");
            assertThat(configs, hasSize(1));

            // Inference on the new config
            assertCompletionInference(upgradedClusterId);

            delete(oldClusterId);
            delete(upgradedClusterId);
        }
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
