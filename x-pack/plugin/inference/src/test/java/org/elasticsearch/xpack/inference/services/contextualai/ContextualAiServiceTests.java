/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai;

import org.apache.hc.core5.http.HttpHeaders;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.services.InferenceServiceTestCase;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankModel;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankModelTests;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.InferenceStringTests.createRandomUsingDataTypes;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_API_KEY;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_INFERENCE_ENTITY_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_RATE_LIMIT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContextualAiServiceTests extends InferenceServiceTestCase {

    public void testRerankInfer() throws IOException {
        var responseJson = """
            {
                "results": [
                    {
                        "index": 1,
                        "relevance_score": 0.94
                    },
                    {
                        "index": 0,
                        "relevance_score": 0.78
                    }
                ]
            }
            """;

        try (var service = createInferenceService()) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var instruction = randomAlphaOfLengthOrNull(8);
            var modelId = randomAlphaOfLength(8);
            var model = ContextualAiRerankModelTests.createModel(getUrl(webServer), TEST_API_KEY, modelId, null, instruction);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

            var inputOne = "abc";
            var inputTwo = "def";
            var query = "some query";
            var topN = randomNonNegativeIntOrNull();
            var returnDocuments = randomOptionalBoolean();
            var request = new RerankRequest(
                List.of(new InferenceString(DataType.TEXT, inputOne), new InferenceString(DataType.TEXT, inputTwo)),
                new InferenceString(DataType.TEXT, query),
                topN,
                returnDocuments,
                new HashMap<>()
            );
            service.rerankInfer(model, request, null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(result, CoreMatchers.instanceOf(RankedDocsResults.class));

            var rankedDocsResults = (RankedDocsResults) result;
            var rankedDocs = rankedDocsResults.getRankedDocs();
            assertThat(rankedDocs.size(), is(2));
            assertThat(rankedDocs.get(0).relevanceScore(), is(0.94F));
            assertThat(rankedDocs.get(0).index(), is(1));
            assertThat(rankedDocs.get(1).relevanceScore(), is(0.78F));
            assertThat(rankedDocs.get(1).index(), is(0));

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().getFirst().getUri().getQuery());
            assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(
                webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION),
                CoreMatchers.is(Strings.format("Bearer %s", TEST_API_KEY))
            );

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            Map<String, Object> expectedRequestMap = new HashMap<>(
                Map.of("query", query, "documents", List.of(inputOne, inputTwo), "model", modelId)
            );
            if (topN != null) {
                expectedRequestMap.put("top_n", topN);
            }
            if (instruction != null) {
                expectedRequestMap.put("instruction", instruction);
            }
            assertThat(requestMap, is(expectedRequestMap));
        }
    }

    public void testRerankInfer_ThrowsError_WithNonTextQuery() throws IOException {
        var textInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.of(DataType.TEXT)));
        var nonTextQuery = createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT)));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(textInputs, nonTextQuery);
    }

    public void testRerankInfer_ThrowsError_WithNonTextInputs() throws IOException {
        var nonTextInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT))));
        var textQuery = createRandomUsingDataTypes(EnumSet.of(DataType.TEXT));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(nonTextInputs, textQuery);
    }

    public void testRerankInfer_ThrowsError_WithNonTextInputsAndQuery() throws IOException {
        var nonTextInputs = randomList(1, 5, () -> createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT))));
        var nonTextQuery = createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT)));
        testRerankInfer_ThrowsError_WithNonTextInputOrQuery(nonTextInputs, nonTextQuery);
    }

    private void testRerankInfer_ThrowsError_WithNonTextInputOrQuery(List<InferenceString> inputs, InferenceString query)
        throws IOException {
        var model = mock(ContextualAiRerankModel.class);
        when(model.getTaskType()).thenReturn(TaskType.RERANK);

        try (var service = createInferenceService()) {
            TestPlainActionFuture<InferenceServiceResults> listener = new TestPlainActionFuture<>();

            service.rerankInfer(model, new RerankRequest(inputs, query, null, null, new HashMap<>()), null, listener);

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(thrownException.status(), is(RestStatus.BAD_REQUEST));
            assertThat(
                thrownException.getMessage(),
                is("The contextualai service does not support rerank with non-text inputs or queries")
            );
        }
    }

    @Override
    public InferenceService createInferenceService() {
        return new ContextualAiService(
            HttpRequestSenderTests.createSenderFactory(threadPool, clientManager),
            createWithEmptySettings(threadPool),
            mockClusterServiceEmpty()
        );
    }

    @Override
    public EnumSet<TaskType> expectedStreamingTasks() {
        return EnumSet.noneOf(TaskType.class);
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(rerankingInferenceService.rerankerWindowSize(TEST_MODEL_ID), is(ContextualAiService.DEFAULT_RERANKER_WINDOW_SIZE_WORDS));
    }

    public void testBuildModelFromConfigAndSecrets_Rerank() throws IOException, URISyntaxException {
        var model = new ContextualAiRerankModel(
            TEST_INFERENCE_ENTITY_ID,
            new ContextualAiRerankServiceSettings(
                new ContextualAiServiceSettings.CommonSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))
            ),
            ContextualAiRerankTaskSettings.EMPTY_SETTINGS,
            new DefaultSecretSettings(new SecureString(TEST_API_KEY.toCharArray()))
        );
        validateModelBuilding(model);
    }

    public void testBuildModelFromConfigAndSecrets_UnsupportedTaskType() throws IOException {
        var modelConfigurations = new ModelConfigurations(
            TEST_INFERENCE_ENTITY_ID,
            TaskType.CHAT_COMPLETION,
            ContextualAiService.NAME,
            mock(ServiceSettings.class)
        );
        try (var inferenceService = createInferenceService()) {
            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> inferenceService.buildModelFromConfigAndSecrets(modelConfigurations, mock(ModelSecrets.class))
            );
            assertThat(
                thrownException.getMessage(),
                CoreMatchers.is(
                    Strings.format("The [%s] service does not support task type [%s]", ContextualAiService.NAME, TaskType.CHAT_COMPLETION)
                )
            );
        }
    }

    private void validateModelBuilding(Model model) throws IOException {
        try (var inferenceService = createInferenceService()) {
            var resultModel = inferenceService.buildModelFromConfigAndSecrets(model.getConfigurations(), model.getSecrets());
            assertThat(resultModel, CoreMatchers.is(model));
        }
    }
}
