/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file has been contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.TaskType;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.assertStatusOkOrCreated;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class InferenceGetServicesIT extends BaseMockEISAuthServerTest {

    @BeforeClass
    public static void init() {
        // Ensure the mock EIS server has an authorized response ready
        mockEISServer.enqueueAuthorizeAllModelsResponse();
    }

    public void testGetServicesWithoutTaskType() throws IOException {
        assertThat(
            allProviders(),
            containsInAnyOrder(
                List.of(
                    "alibabacloud-ai-search",
                    "amazonbedrock",
                    "anthropic",
                    "azureaistudio",
                    "azureopenai",
                    "cohere",
                    "custom",
                    "deepseek",
                    "elastic",
                    "elasticsearch",
                    "googleaistudio",
                    "googlevertexai",
                    "hugging_face",
                    "jinaai",
                    "mistral",
                    "openai",
                    "streaming_completion_test_service",
                    "completion_test_service",
                    "test_reranking_service",
                    "test_service",
                    "text_embedding_test_service",
                    "voyageai",
                    "watsonxai",
                    "amazon_sagemaker"
                ).toArray()
            )
        );
    }

    private Iterable<String> allProviders() throws IOException {
        return providers(getAllServices());
    }

    @SuppressWarnings("unchecked")
    private Iterable<String> providers(List<Object> services) {
        return services.stream().map(service -> {
            var serviceConfig = (Map<String, Object>) service;
            return (String) serviceConfig.get("service");
        }).toList();
    }

    public void testGetServicesWithTextEmbeddingTaskType() throws IOException {
        List<Object> services = getServices(TaskType.TEXT_EMBEDDING);
        assertThat(services.size(), equalTo(18));

        assertThat(
            providersFor(TaskType.TEXT_EMBEDDING),
            containsInAnyOrder(
                List.of(
                    "alibabacloud-ai-search",
                    "amazonbedrock",
                    "amazon_sagemaker",
                    "azureaistudio",
                    "azureopenai",
                    "cohere",
                    "custom",
                    "elastic",
                    "elasticsearch",
                    "googleaistudio",
                    "googlevertexai",
                    "hugging_face",
                    "jinaai",
                    "mistral",
                    "openai",
                    "text_embedding_test_service",
                    "voyageai",
                    "watsonxai"
                ).toArray()
            )
        );
    }

    private Iterable<String> providersFor(TaskType taskType) throws IOException {
        return providers(getServices(taskType));
    }

    public void testGetServicesWithRerankTaskType() throws IOException {
        assertThat(
            providersFor(TaskType.RERANK),
            containsInAnyOrder(
                List.of(
                    "alibabacloud-ai-search",
                    "cohere",
                    "custom",
                    "elasticsearch",
                    "googlevertexai",
                    "jinaai",
                    "test_reranking_service",
                    "voyageai",
                    "hugging_face",
                    "amazon_sagemaker",
                    "elastic"
                ).toArray()
            )
        );
    }

    public void testGetServicesWithCompletionTaskType() throws IOException {
        assertThat(
            providersFor(TaskType.COMPLETION),
            containsInAnyOrder(
                List.of(
                    "alibabacloud-ai-search",
                    "amazonbedrock",
                    "anthropic",
                    "azureaistudio",
                    "azureopenai",
                    "cohere",
                    "custom",
                    "deepseek",
                    "googleaistudio",
                    "openai",
                    "streaming_completion_test_service",
                    "completion_test_service",
                    "hugging_face",
                    "amazon_sagemaker",
                    "mistral"
                ).toArray()
            )
        );
    }

    public void testGetServicesWithChatCompletionTaskType() throws IOException {
        assertThat(
            providersFor(TaskType.CHAT_COMPLETION),
            containsInAnyOrder(
                List.of(
                    "deepseek",
                    "elastic",
                    "openai",
                    "streaming_completion_test_service",
                    "hugging_face",
                    "amazon_sagemaker",
                    "googlevertexai",
                    "mistral"
                ).toArray()
            )
        );
    }

    public void testGetServicesWithSparseEmbeddingTaskType() throws IOException {
        assertThat(
            providersFor(TaskType.SPARSE_EMBEDDING),
            containsInAnyOrder(
                List.of(
                    "alibabacloud-ai-search",
                    "custom",
                    "elastic",
                    "elasticsearch",
                    "hugging_face",
                    "streaming_completion_test_service",
                    "test_service",
                    "amazon_sagemaker"
                ).toArray()
            )
        );
    }

    private List<Object> getAllServices() throws IOException {
        var endpoint = Strings.format("_inference/_services");
        return getInternalAsList(endpoint);
    }

    private List<Object> getServices(TaskType taskType) throws IOException {
        var endpoint = Strings.format("_inference/_services/%s", taskType);
        return getInternalAsList(endpoint);
    }

    private List<Object> getInternalAsList(String endpoint) throws IOException {
        var request = new Request("GET", endpoint);
        var response = client().performRequest(request);
        assertStatusOkOrCreated(response);
        return entityAsList(response);
    }
}
