/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.AlwaysRetryingResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceRequestManager;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModel;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModelTests.createModel;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class HuggingFaceActionTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String URL = "http://localhost:12345";
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = createThreadPool(inferenceUtilityPool());
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderThrows() {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(URL, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of("abc"), null, InputTypeTests.randomWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("failed"));
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled() {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(3);
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());

        var action = createAction(URL, sender, "inferenceEntityId");

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of("abc"), null, InputTypeTests.randomWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(format("Failed to send Hugging Face test action request from inference entity id [%s]. Cause: failed", "inferenceEntityId"))
        );
    }

    public void testExecute_ThrowsException() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(URL, sender, "inferenceEntityId");

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of("abc"), null, InputTypeTests.randomWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(format("Failed to send Hugging Face test action request from inference entity id [%s]. Cause: failed", "inferenceEntityId"))
        );
    }

    private ExecutableAction createAction(String url, Sender sender) {
        var model = createModel(url, "secret");
        return createAction(model, sender);
    }

    private ExecutableAction createAction(HuggingFaceElserModel model, Sender sender) {
        var requestCreator = HuggingFaceRequestManager.of(
            model,
            new AlwaysRetryingResponseHandler("test", (result) -> null),
            TruncatorTests.createTruncator(),
            threadPool
        );
        var errorMessage = format(
            "Failed to send Hugging Face %s request from inference entity id [%s]",
            "test action",
            model.getInferenceEntityId()
        );

        return new SenderExecutableAction(sender, requestCreator, errorMessage);
    }

    private ExecutableAction createAction(String url, Sender sender, String modelId) {
        var model = createModel(url, "secret", modelId);
        return createAction(model, sender);
    }
}
