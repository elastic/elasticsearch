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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.assertStatusOkOrCreated;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class InferenceGetServicesIT extends BaseMockEISAuthServerTest {

    public void testGetServicesWithoutTaskType() throws IOException {
        List<Object> services = getAllServices();
        assertThat(services.size(), equalTo(23));

        var providers = providers(services);

        assertThat(
            providers,
            containsInAnyOrder(
                List.of(
                    "alibabacloud-ai-search",
                    "amazonbedrock",
                    "anthropic",
                    "azureaistudio",
                    "azureopenai",
                    "cohere",
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
                    "sagemaker"
                ).toArray()
            )
        );
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
        assertThat(services.size(), equalTo(16));

        var providers = providers(services);

        assertThat(
            providers,
            containsInAnyOrder(
                List.of(
                    "alibabacloud-ai-search",
                    "amazonbedrock",
                    "azureaistudio",
                    "azureopenai",
                    "cohere",
                    "elasticsearch",
                    "googleaistudio",
                    "googlevertexai",
                    "hugging_face",
                    "jinaai",
                    "mistral",
                    "openai",
                    "text_embedding_test_service",
                    "voyageai",
                    "watsonxai",
                    "sagemaker"
                ).toArray()
            )
        );
    }

    public void testGetServicesWithRerankTaskType() throws IOException {
        List<Object> services = getServices(TaskType.RERANK);
        assertThat(services.size(), equalTo(7));

        var providers = providers(services);

        assertThat(
            providers,
            containsInAnyOrder(
                List.of(
                    "alibabacloud-ai-search",
                    "cohere",
                    "elasticsearch",
                    "googlevertexai",
                    "jinaai",
                    "test_reranking_service",
                    "voyageai"
                ).toArray()
            )
        );
    }

    public void testGetServicesWithCompletionTaskType() throws IOException {
        List<Object> services = getServices(TaskType.COMPLETION);
        assertThat(services.size(), equalTo(12));

        var providers = providers(services);

        assertThat(
            providers,
            containsInAnyOrder(
                List.of(
                    "alibabacloud-ai-search",
                    "amazonbedrock",
                    "anthropic",
                    "azureaistudio",
                    "azureopenai",
                    "cohere",
                    "deepseek",
                    "googleaistudio",
                    "openai",
                    "streaming_completion_test_service",
                    "completion_test_service",
                    "hugging_face"
                ).toArray()
            )
        );
    }

    public void testGetServicesWithChatCompletionTaskType() throws IOException {
        List<Object> services = getServices(TaskType.CHAT_COMPLETION);
        assertThat(services.size(), equalTo(5));

        var providers = providers(services);

        assertThat(
            providers,
            containsInAnyOrder(List.of("deepseek", "elastic", "openai", "streaming_completion_test_service", "hugging_face").toArray())
        );
    }

    public void testGetServicesWithSparseEmbeddingTaskType() throws IOException {
        List<Object> services = getServices(TaskType.SPARSE_EMBEDDING);
        assertThat(services.size(), equalTo(6));

        var providers = providers(services);

        assertThat(
            providers,
            containsInAnyOrder(
                List.of(
                    "alibabacloud-ai-search",
                    "elastic",
                    "elasticsearch",
                    "hugging_face",
                    "streaming_completion_test_service",
                    "test_service"
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
