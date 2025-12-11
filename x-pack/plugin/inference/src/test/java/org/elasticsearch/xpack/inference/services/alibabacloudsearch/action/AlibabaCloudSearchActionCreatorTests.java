/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResultsTests;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResultsTests;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.AlibabaCloudSearchUtils;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank.AlibabaCloudSearchRerankModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.core.inference.results.RankedDocsResultsTests.buildExpectationRerank;
import static org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResultsTests.buildExpectationSparseEmbeddings;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class AlibabaCloudSearchActionCreatorTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private Sender sender;
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws IOException {
        sender = mock(Sender.class);
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testExecute_withTextEmbeddingsAction_Success() {
        float[] values = { 0.1111111f, 0.2222222f, 0.3333333f };
        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(3);
            listener.onResponse(new DenseEmbeddingFloatResults(List.of(new DenseEmbeddingFloatResults.Embedding(values))));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());
        var action = createTextEmbeddingsAction();

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new EmbeddingsInput(List.of(randomAlphaOfLength(10)), null), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var result = listener.actionGet(TIMEOUT);
        assertThat(result.asMap(), is(buildExpectationFloat(List.of(values))));
    }

    public void testExecute_withTextEmbeddingsAction_ListenerThrowsElasticsearchException_WhenSenderThrowsElasticsearchException() {
        doThrow(new ElasticsearchException("error")).when(sender).send(any(), any(), any(), any());
        var action = createTextEmbeddingsAction();

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new EmbeddingsInput(List.of(randomAlphaOfLength(10)), null), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("error"));
    }

    public void testExecute_withTextEmbeddingsAction_ListenerThrowsInternalServerError_WhenSenderThrowsException() {
        doThrow(new RuntimeException("error")).when(sender).send(any(), any(), any(), any());
        var action = createTextEmbeddingsAction();

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new EmbeddingsInput(List.of(randomAlphaOfLength(10)), null), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("Failed to send AlibabaCloud Search text embeddings request. Cause: error"));
    }

    public void testExecute_withSparseEmbeddingsAction_Success() {
        String token = "token";
        float weight = 0.1111111f;
        boolean isTruncated = false;
        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(3);
            listener.onResponse(
                new SparseEmbeddingResults(
                    List.of(new SparseEmbeddingResults.Embedding(List.of(new WeightedToken(token, weight)), isTruncated))
                )
            );

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());
        var action = createSparseEmbeddingsAction();

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new EmbeddingsInput(List.of(randomAlphaOfLength(10)), null), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var result = listener.actionGet(TIMEOUT);
        assertThat(
            result.asMap(),
            is(
                buildExpectationSparseEmbeddings(
                    List.of(new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of(token, weight), isTruncated))
                )
            )
        );
    }

    public void testExecute_withSparseEmbeddingsAction_ListenerThrowsElasticsearchException_WhenSenderThrowsElasticsearchException() {
        doThrow(new ElasticsearchException("error")).when(sender).send(any(), any(), any(), any());
        var action = createSparseEmbeddingsAction();

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new EmbeddingsInput(List.of(randomAlphaOfLength(10)), null), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("error"));
    }

    public void testExecute_withSparseEmbeddingsAction_ListenerThrowsInternalServerError_WhenSenderThrowsException() {
        doThrow(new RuntimeException("error")).when(sender).send(any(), any(), any(), any());
        var action = createSparseEmbeddingsAction();

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new EmbeddingsInput(List.of(randomAlphaOfLength(10)), null), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("Failed to send AlibabaCloud Search sparse embeddings request. Cause: error"));
    }

    public void testExecute_withRerankAction_Success() {
        int index = 0;
        float relevanceScore = 0.1111111f;
        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(3);
            listener.onResponse(new RankedDocsResults(List.of(new RankedDocsResults.RankedDoc(index, relevanceScore, null))));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());
        var action = createRerankAction();

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new QueryAndDocsInputs("query", List.of(randomAlphaOfLength(10))),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var result = listener.actionGet(TIMEOUT);
        assertThat(
            result.asMap(),
            is(
                buildExpectationRerank(
                    List.of(new RankedDocsResultsTests.RerankExpectation(Map.of("index", index, "relevance_score", relevanceScore)))
                )
            )
        );
    }

    public void testExecute_withRerankAction_ListenerThrowsElasticsearchException_WhenSenderThrowsElasticsearchException() {
        doThrow(new ElasticsearchException("error")).when(sender).send(any(), any(), any(), any());
        var action = createRerankAction();

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new QueryAndDocsInputs("query", List.of(randomAlphaOfLength(10))),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("error"));
    }

    public void testExecute_withRerankAction_ListenerThrowsInternalServerError_WhenSenderThrowsException() {
        doThrow(new RuntimeException("error")).when(sender).send(any(), any(), any(), any());
        var action = createRerankAction();

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new QueryAndDocsInputs("query", List.of(randomAlphaOfLength(10))),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("Failed to send AlibabaCloud Search rerank request. Cause: error"));
    }

    public void testExecute_withCompletionAction_Success() {
        var resultString = randomAlphaOfLength(100);
        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(3);
            listener.onResponse(new ChatCompletionResults(List.of(new ChatCompletionResults.Result(resultString))));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());
        var action = createCompletionAction();

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of(randomAlphaOfLength(10))), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var result = listener.actionGet(TIMEOUT);
        assertThat(result.asMap(), is(buildExpectationCompletion(List.of(resultString))));
    }

    public void testExecute_withCompletionAction_ListenerThrowsElasticsearchException_WhenSenderThrowsElasticsearchException() {
        doThrow(new ElasticsearchException("error")).when(sender).send(any(), any(), any(), any());
        var action = createCompletionAction();

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of(randomAlphaOfLength(10))), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("error"));
    }

    public void testExecute_withCompletionAction_ListenerThrowsInternalServerError_WhenSenderThrowsException() {
        doThrow(new RuntimeException("error")).when(sender).send(any(), any(), any(), any());
        var action = createCompletionAction();

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of(randomAlphaOfLength(10))), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("Failed to send AlibabaCloud Search completion request. Cause: error"));
    }

    private ExecutableAction createTextEmbeddingsAction() {
        var serviceComponents = ServiceComponentsTests.createWithEmptySettings(threadPool);
        AlibabaCloudSearchEmbeddingsModel embeddingsModel = new AlibabaCloudSearchEmbeddingsModel(
            "text_embedding_test",
            TaskType.TEXT_EMBEDDING,
            AlibabaCloudSearchUtils.SERVICE_NAME,
            AlibabaCloudSearchCompletionServiceSettingsTests.getServiceSettingsMap("text_embedding_test", "host", "default"),
            null,
            null,
            DefaultSecretSettingsTests.getSecretSettingsMap("secret"),
            null
        );
        var actionCreator = new AlibabaCloudSearchActionCreator(sender, serviceComponents);
        return actionCreator.create(embeddingsModel, Map.of());
    }

    private ExecutableAction createSparseEmbeddingsAction() {
        var serviceComponents = ServiceComponentsTests.createWithEmptySettings(threadPool);
        AlibabaCloudSearchSparseModel sparseModel = new AlibabaCloudSearchSparseModel(
            "sparse_embedding_test",
            TaskType.SPARSE_EMBEDDING,
            AlibabaCloudSearchUtils.SERVICE_NAME,
            AlibabaCloudSearchCompletionServiceSettingsTests.getServiceSettingsMap("sparse_embedding_test", "host", "default"),
            null,
            null,
            DefaultSecretSettingsTests.getSecretSettingsMap("secret"),
            null
        );
        var actionCreator = new AlibabaCloudSearchActionCreator(sender, serviceComponents);
        return actionCreator.create(sparseModel, Map.of());
    }

    private ExecutableAction createRerankAction() {
        var serviceComponents = ServiceComponentsTests.createWithEmptySettings(threadPool);
        AlibabaCloudSearchRerankModel rerankModel = new AlibabaCloudSearchRerankModel(
            "rerank_test",
            TaskType.RERANK,
            AlibabaCloudSearchUtils.SERVICE_NAME,
            AlibabaCloudSearchCompletionServiceSettingsTests.getServiceSettingsMap("rerank_test", "host", "default"),
            null,
            DefaultSecretSettingsTests.getSecretSettingsMap("secret"),
            null
        );
        var actionCreator = new AlibabaCloudSearchActionCreator(sender, serviceComponents);
        return actionCreator.create(rerankModel, Map.of());
    }

    private ExecutableAction createCompletionAction() {
        var serviceComponents = ServiceComponentsTests.createWithEmptySettings(threadPool);
        AlibabaCloudSearchCompletionModel completionModel = new AlibabaCloudSearchCompletionModel(
            "completion_test",
            TaskType.COMPLETION,
            AlibabaCloudSearchUtils.SERVICE_NAME,
            AlibabaCloudSearchCompletionServiceSettingsTests.getServiceSettingsMap("completion_test", "host", "default"),
            null,
            DefaultSecretSettingsTests.getSecretSettingsMap("secret"),
            null
        );
        var actionCreator = new AlibabaCloudSearchActionCreator(sender, serviceComponents);
        return actionCreator.create(completionModel, Map.of());
    }
}
