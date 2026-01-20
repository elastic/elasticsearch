/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankModelTests;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class MixedbreadActionCreatorTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final QueryAndDocsInputs QUERY_AND_DOCS_INPUTS = new QueryAndDocsInputs(
        "popular name",
        List.of("Luke"),
        false,
        3,
        false
    );
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
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

    public void testExecute_ThrowsURISyntaxException_ForInvalidUrl() throws IOException {
        try (var sender = mock(Sender.class)) {
            var thrownException = expectThrows(
                IllegalArgumentException.class,
                () -> createAction("model", "secret", "^^", null, null, sender)
            );
            MatcherAssert.assertThat(thrownException.getMessage(), containsString("unable to parse url [^^]"));
        }
    }

    public void testExecute_ThrowsElasticsearchException() {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction("model", "secret", getUrl(webServer), null, null, sender);
        ElasticsearchException thrownException = executeActionWithException(action);

        MatcherAssert.assertThat(thrownException.getMessage(), is("failed"));
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled() {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            ActionListener<HttpResult> listener = invocation.getArgument(3);
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());

        var action = createAction("model", "secret", getUrl(webServer), null, null, sender);
        ElasticsearchException thrownException = executeActionWithException(action);

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is("Failed to send Mixedbread rerank request from inference entity id [model]. Cause: failed")
        );
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled_WhenUrlIsNull() {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            ActionListener<HttpResult> listener = invocation.getArgument(3);
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());

        var action = createAction("model", "secret", null, null, null, sender);
        ElasticsearchException thrownException = executeActionWithException(action);

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is("Failed to send Mixedbread rerank request from inference entity id [model]. Cause: failed")
        );
    }

    public void testExecute_ThrowsException() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction("model", "secret", getUrl(webServer), null, null, sender);
        ElasticsearchException thrownException = executeActionWithException(action);

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is("Failed to send Mixedbread rerank request from inference entity id [model]. Cause: failed")
        );
    }

    public void testExecute_ThrowsExceptionWithNullUrl() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction("model", "secret", null, null, null, sender);
        var thrownException = executeActionWithException(action);

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is("Failed to send Mixedbread rerank request from inference entity id [model]. Cause: failed")
        );
    }

    private static ElasticsearchException executeActionWithException(ExecutableAction action) {
        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(QUERY_AND_DOCS_INPUTS, InferenceAction.Request.DEFAULT_TIMEOUT, listener);
        return expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
    }

    private ExecutableAction createAction(
        String modelName,
        String apiKey,
        String url,
        Integer topN,
        Boolean returnDocuments,
        Sender sender
    ) {
        var actionCreator = new MixedbreadActionCreator(sender, createWithEmptySettings(threadPool));
        var model = MixedbreadRerankModelTests.createModel(modelName, apiKey, url, topN, returnDocuments);
        return actionCreator.create(model, null);
    }
}
