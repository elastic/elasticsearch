/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.huggingface;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderFactory;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModel;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserSecretSettings;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserServiceSettings;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.external.http.Utils.mockClusterServiceEmpty;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class HuggingFaceElserActionTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    @SuppressWarnings("unchecked")
    public void testExecute_ReturnsSuccessfulResponse() throws IOException {
        var senderFactory = new HttpRequestSenderFactory(threadPool, clientManager, mockClusterServiceEmpty(), Settings.EMPTY);

        try (var sender = senderFactory.createSender("test_service")) {
            sender.start();

            String responseJson = """
                [
                    {
                        ".": 0.133155956864357
                    }
                ]
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var action = createAction(getUrl(webServer), sender);

            PlainActionFuture<List<? extends InferenceResults>> listener = new PlainActionFuture<>();
            action.execute(List.of("abc"), listener);

            var result = listener.actionGet(TIMEOUT).get(0);

            assertThat(result.asMap(), is(Map.of(DEFAULT_RESULTS_FIELD, Map.of(".", 0.13315596f))));
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(1));
            assertThat(requestMap.get("inputs"), instanceOf(List.class));
            var inputList = (List<String>) requestMap.get("inputs");
            assertThat(inputList, contains("abc"));
        }
    }

    public void testExecute_ThrowsURISyntaxException_ForInvalidUrl() throws IOException {
        try (var sender = mock(Sender.class)) {
            var thrownException = expectThrows(IllegalArgumentException.class, () -> createAction("^^", sender));
            assertThat(thrownException.getMessage(), is("unable to parse url [^^]"));
        }
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderThrows() {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException("failed")).when(sender).send(any(), any());

        var action = createAction(getUrl(webServer), sender);

        PlainActionFuture<List<? extends InferenceResults>> listener = new PlainActionFuture<>();
        action.execute(List.of("abc"), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("failed"));
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled() {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[1];
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any());

        var action = createAction(getUrl(webServer), sender);

        PlainActionFuture<List<? extends InferenceResults>> listener = new PlainActionFuture<>();
        action.execute(List.of("abc"), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is(format("Failed to send ELSER Hugging Face request to [%s]", getUrl(webServer))));
    }

    public void testExecute_ThrowsException() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any());

        var action = createAction(getUrl(webServer), sender);

        PlainActionFuture<List<? extends InferenceResults>> listener = new PlainActionFuture<>();
        action.execute(List.of("abc"), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is(format("Failed to send ELSER Hugging Face request to [%s]", getUrl(webServer))));
    }

    private HuggingFaceElserAction createAction(String url, Sender sender) {
        var model = new HuggingFaceElserModel(
            "id",
            TaskType.SPARSE_EMBEDDING,
            "service",
            new HuggingFaceElserServiceSettings(url),
            new HuggingFaceElserSecretSettings(new SecureString("secret".toCharArray()))
        );

        return new HuggingFaceElserAction(sender, model, new ServiceComponents(threadPool, mock(ThrottlerManager.class), Settings.EMPTY));
    }
}
