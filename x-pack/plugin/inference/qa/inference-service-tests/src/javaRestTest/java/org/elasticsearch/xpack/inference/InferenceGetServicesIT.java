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
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.assertStatusOkOrCreated;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class InferenceGetServicesIT extends BaseMockEISAuthServerTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Ensure the mock EIS server has an authorized response ready before each test because each test will
        // use the services API which makes a call to EIS
        mockEISServer.enqueueAuthorizeAllModelsResponse();
    }

    /**
     * This is done before the class because I've run into issues where another class that extends {@link BaseMockEISAuthServerTest}
     * results in an authorization response not being queued up for the new Elasticsearch Node in time. When the node starts up, it
     * retrieves authorization. If the request isn't queued up when that happens the tests will fail. From my testing locally it seems
     * like the base class's static functionality to queue a response is only done once and not for each subclass.
     *
     * My understanding is that the @Before will be run after the node starts up and wouldn't be sufficient to handle
     * this scenario. That is why this needs to be @BeforeClass.
     */
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
                    "ai21",
                    "alibabacloud-ai-search",
                    "amazonbedrock",
                    "anthropic",
                    "azureaistudio",
                    "azureopenai",
                    "cohere",
                    "contextualai",
                    "deepseek",
                    "elastic",
                    "elasticsearch",
                    "googleaistudio",
                    "googlevertexai",
                    "hugging_face",
                    "jinaai",
                    "llama",
                    "mistral",
                    "openai",
                    "openshift_ai",
                    "streaming_completion_test_service",
                    "completion_test_service",
                    "test_reranking_service",
                    "test_service",
                    "alternate_sparse_embedding_test_service",
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
                    "elastic",
                    "elasticsearch",
                    "googleaistudio",
                    "googlevertexai",
                    "hugging_face",
                    "jinaai",
                    "llama",
                    "mistral",
                    "openai",
                    "openshift_ai",
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
                    "azureaistudio",
                    "cohere",
                    "contextualai",
                    "elasticsearch",
                    "googlevertexai",
                    "jinaai",
                    "openshift_ai",
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
                    "ai21",
                    "llama",
                    "alibabacloud-ai-search",
                    "amazonbedrock",
                    "anthropic",
                    "azureaistudio",
                    "azureopenai",
                    "cohere",
                    "deepseek",
                    "googleaistudio",
                    "googlevertexai",
                    "openai",
                    "openshift_ai",
                    "streaming_completion_test_service",
                    "completion_test_service",
                    "hugging_face",
                    "amazon_sagemaker",
                    "mistral",
                    "watsonxai"
                ).toArray()
            )
        );
    }

    public void testGetServicesWithChatCompletionTaskType() throws IOException {
        assertThat(
            providersFor(TaskType.CHAT_COMPLETION),
            containsInAnyOrder(
                List.of(
                    "ai21",
                    "llama",
                    "deepseek",
                    "elastic",
                    "openai",
                    "openshift_ai",
                    "streaming_completion_test_service",
                    "hugging_face",
                    "amazon_sagemaker",
                    "googlevertexai",
                    "mistral",
                    "watsonxai"
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
                    "elastic",
                    "elasticsearch",
                    "hugging_face",
                    "streaming_completion_test_service",
                    "test_service",
                    "alternate_sparse_embedding_test_service",
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
