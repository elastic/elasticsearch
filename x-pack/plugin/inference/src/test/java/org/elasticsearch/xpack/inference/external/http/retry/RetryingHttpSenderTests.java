/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.HttpRequestTests;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.UnknownHostException;

import static org.elasticsearch.xpack.inference.external.http.retry.RetrySettingsTests.createDefaultRetrySettings;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RetryingHttpSenderTests extends ESTestCase {
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    private DeterministicTaskQueue taskQueue;

    @Before
    public void init() throws Exception {
        taskQueue = new DeterministicTaskQueue();
    }

    public void testSend_CallsSenderAgain_AfterValidateResponseThrowsAnException() throws IOException {
        var httpClient = mock(HttpClient.class);
        var httpResponse = mockHttpResponse();

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onResponse(new HttpResult(httpResponse, new byte[0]));

            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        var inferenceResults = mock(InferenceServiceResults.class);
        Answer<InferenceServiceResults> answer = (invocation) -> inferenceResults;

        var handler = mock(ResponseHandler.class);
        doThrow(new RetryException(true, "failed")).doNothing().when(handler).validateResponse(any(), any(), any(), any());
        // Mockito.thenReturn() does not compile when returning a
        // bounded wild card list, thenAnswer must be used instead.
        when(handler.parseResult(any(), any())).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), HttpClientContext.create(), () -> false, handler, listener), 1);

        assertThat(listener.actionGet(TIMEOUT), is(inferenceResults));
        verify(httpClient, times(2)).send(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
    }

    public void testSend_CallsSenderAgain_WhenAFailureStatusCodeIsReturned() throws IOException {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(300).thenReturn(200);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        var httpClient = mock(HttpClient.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onResponse(new HttpResult(httpResponse, new byte[] { 'a' }));

            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        var inferenceResults = mock(InferenceServiceResults.class);

        var handler = new AlwaysRetryingResponseHandler("test", result -> inferenceResults);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), HttpClientContext.create(), () -> false, handler, listener), 1);

        assertThat(listener.actionGet(TIMEOUT), is(inferenceResults));
        verify(httpClient, times(2)).send(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
    }

    public void testSend_CallsSenderAgain_WhenParsingFailsOnce() throws IOException {
        var httpClient = mock(HttpClient.class);
        var httpResponse = mockHttpResponse();

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onResponse(new HttpResult(httpResponse, new byte[] { 'a' }));

            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        var inferenceResults = mock(InferenceServiceResults.class);
        Answer<InferenceServiceResults> answer = (invocation) -> inferenceResults;

        var handler = mock(ResponseHandler.class);
        when(handler.parseResult(any(), any())).thenThrow(new RetryException(true, "failed")).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), HttpClientContext.create(), () -> false, handler, listener), 1);

        assertThat(listener.actionGet(TIMEOUT), is(inferenceResults));
        verify(httpClient, times(2)).send(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
    }

    public void testSend_DoesNotCallSenderAgain_WhenParsingFailsWithNonRetryableException() throws IOException {
        var httpClient = mock(HttpClient.class);
        var httpResponse = mockHttpResponse();

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onResponse(new HttpResult(httpResponse, new byte[] { 'a' }));

            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        var inferenceResults = mock(InferenceServiceResults.class);
        Answer<InferenceServiceResults> answer = (invocation) -> inferenceResults;

        var handler = mock(ResponseHandler.class);
        when(handler.parseResult(any(), any())).thenThrow(new IllegalStateException("failed")).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), HttpClientContext.create(), () -> false, handler, listener), 0);

        var thrownException = expectThrows(IllegalStateException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("failed"));

        verify(httpClient, times(1)).send(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
    }

    public void testSend_CallsSenderAgain_WhenHttpResultListenerCallsOnFailureOnce() throws IOException {
        var httpClient = mock(HttpClient.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onFailure(new RetryException(true, "failed"));

            return Void.TYPE;
        }).doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onResponse(new HttpResult(mock(HttpResponse.class), new byte[] { 'a' }));

            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        var inferenceResults = mock(InferenceServiceResults.class);
        Answer<InferenceServiceResults> answer = (invocation) -> inferenceResults;

        var handler = mock(ResponseHandler.class);
        when(handler.parseResult(any(), any())).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), HttpClientContext.create(), () -> false, handler, listener), 1);

        assertThat(listener.actionGet(TIMEOUT), is(inferenceResults));
        verify(httpClient, times(2)).send(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
    }

    public void testSend_CallsSenderAgain_WhenHttpResultListenerCallsOnFailureOnce_WithContentTooLargeException() throws IOException {
        var httpClient = mock(HttpClient.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onFailure(new ContentTooLargeException(new IllegalStateException("failed")));

            return Void.TYPE;
        }).doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onResponse(new HttpResult(mock(HttpResponse.class), new byte[] { 'a' }));

            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        var inferenceResults = mock(InferenceServiceResults.class);
        Answer<InferenceServiceResults> answer = (invocation) -> inferenceResults;

        var handler = mock(ResponseHandler.class);
        when(handler.parseResult(any(), any())).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), HttpClientContext.create(), () -> false, handler, listener), 1);

        assertThat(listener.actionGet(TIMEOUT), is(inferenceResults));
        verify(httpClient, times(2)).send(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
    }

    public void testSend_CallsSenderAgain_WhenHttpResultListenerCallsOnFailureOnceWithConnectionClosedException() throws IOException {
        var httpClient = mock(HttpClient.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onFailure(new ConnectionClosedException("failed"));

            return Void.TYPE;
        }).doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onResponse(new HttpResult(mock(HttpResponse.class), new byte[] { 'a' }));

            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        var inferenceResults = mock(InferenceServiceResults.class);
        Answer<InferenceServiceResults> answer = (invocation) -> inferenceResults;

        var handler = mock(ResponseHandler.class);
        when(handler.parseResult(any(), any())).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), HttpClientContext.create(), () -> false, handler, listener), 1);

        assertThat(listener.actionGet(TIMEOUT), is(inferenceResults));
        verify(httpClient, times(2)).send(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
    }

    public void testSend_ReturnsFailure_WhenHttpResultListenerCallsOnFailureOnceWithUnknownHostException() throws IOException {
        var httpClient = mock(HttpClient.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onFailure(new UnknownHostException("failed"));

            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        var inferenceResults = mock(InferenceServiceResults.class);
        Answer<InferenceServiceResults> answer = (invocation) -> inferenceResults;

        var handler = mock(ResponseHandler.class);
        when(handler.parseResult(any(), any())).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), HttpClientContext.create(), () -> false, handler, listener), 0);

        var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("Invalid host [null], please check that the URL is correct."));
        verify(httpClient, times(1)).send(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
    }

    public void testSend_ReturnsElasticsearchExceptionFailure_WhenTheHttpClientThrowsAnIllegalStateException() throws IOException {
        var httpClient = mock(HttpClient.class);

        doAnswer(invocation -> { throw new IllegalStateException("failed"); }).when(httpClient).send(any(), any(), any());

        var inferenceResults = mock(InferenceServiceResults.class);
        Answer<InferenceServiceResults> answer = (invocation) -> inferenceResults;

        var handler = mock(ResponseHandler.class);
        when(handler.parseResult(any(), any())).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(
            () -> retrier.send(mock(Logger.class), mockRequest("id"), HttpClientContext.create(), () -> false, handler, listener),
            0
        );

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("Http client failed to send request from inference entity id [id]"));
        verify(httpClient, times(1)).send(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
    }

    public void testSend_ReturnsFailure_WhenValidateResponseThrowsAnException_AfterOneRetry() throws IOException {
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(mock(StatusLine.class));

        var sender = mock(HttpClient.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onResponse(new HttpResult(httpResponse, new byte[0]));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any());

        var inferenceResults = mock(InferenceServiceResults.class);
        Answer<InferenceServiceResults> answer = (invocation) -> inferenceResults;

        var handler = mock(ResponseHandler.class);
        doThrow(new RetryException(true, "failed")).doThrow(new IllegalStateException("failed again"))
            .when(handler)
            .validateResponse(any(), any(), any(), any());
        when(handler.parseResult(any(), any())).thenAnswer(answer);

        var retrier = createRetrier(sender);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), HttpClientContext.create(), () -> false, handler, listener), 1);

        var thrownException = expectThrows(IllegalStateException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("failed again"));
        assertThat(thrownException.getSuppressed().length, is(1));
        assertThat(thrownException.getSuppressed()[0].getMessage(), is("failed"));

        verify(sender, times(2)).send(any(), any(), any());
        verifyNoMoreInteractions(sender);
    }

    public void testSend_ReturnsFailure_WhenValidateResponseThrowsAnElasticsearchException_AfterOneRetry() throws IOException {
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(mock(StatusLine.class));

        var httpClient = mock(HttpClient.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onResponse(new HttpResult(httpResponse, new byte[0]));

            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        var inferenceResults = mock(InferenceServiceResults.class);
        Answer<InferenceServiceResults> answer = (invocation) -> inferenceResults;

        var handler = mock(ResponseHandler.class);
        doThrow(new RetryException(true, "failed")).doThrow(new RetryException(false, "failed again"))
            .when(handler)
            .validateResponse(any(), any(), any(), any());
        when(handler.parseResult(any(), any())).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), HttpClientContext.create(), () -> false, handler, listener), 1);

        var thrownException = expectThrows(RetryException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("failed again"));
        assertThat(thrownException.getSuppressed().length, is(1));
        assertThat(thrownException.getSuppressed()[0].getMessage(), is("failed"));
        verify(httpClient, times(2)).send(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
    }

    public void testSend_ReturnsFailure_WhenHttpResultsListenerCallsOnFailure_AfterOneRetry() throws IOException {
        var httpClient = mock(HttpClient.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onFailure(new RetryException(true, "failed"));

            return Void.TYPE;
        }).doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onFailure(new RetryException(false, "failed again"));

            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        var handler = mock(ResponseHandler.class);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), HttpClientContext.create(), () -> false, handler, listener), 1);

        var thrownException = expectThrows(RetryException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("failed again"));
        assertThat(thrownException.getSuppressed().length, is(1));
        assertThat(thrownException.getSuppressed()[0].getMessage(), is("failed"));
        verify(httpClient, times(2)).send(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
    }

    public void testSend_ReturnsFailure_WhenHttpResultsListenerCallsOnFailure_WithNonRetryableException() throws IOException {
        var httpClient = mock(HttpClient.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        var handler = mock(ResponseHandler.class);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), HttpClientContext.create(), () -> false, handler, listener), 0);

        var thrownException = expectThrows(IllegalStateException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("failed"));
        assertThat(thrownException.getSuppressed().length, is(0));
        verify(httpClient, times(1)).send(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
    }

    private static HttpResponse mockHttpResponse() {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(200);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        return httpResponse;
    }

    private void executeTasks(Runnable runnable, int retries) {
        taskQueue.scheduleNow(runnable);
        // Execute the task scheduled from the line above
        taskQueue.runAllRunnableTasks();

        for (int i = 0; i < retries; i++) {
            // set the timing correctly to get ready to run the next task
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }
    }

    private static Request mockRequest() {
        return mockRequest("inferenceEntityId");
    }

    private static Request mockRequest(String inferenceEntityId) {
        var request = mock(Request.class);
        when(request.truncate()).thenReturn(request);
        when(request.createHttpRequest()).thenReturn(HttpRequestTests.createMock(inferenceEntityId));
        when(request.getInferenceEntityId()).thenReturn(inferenceEntityId);

        return request;
    }

    private RetryingHttpSender createRetrier(HttpClient httpClient) {
        return new RetryingHttpSender(
            httpClient,
            mock(ThrottlerManager.class),
            createDefaultRetrySettings(),
            taskQueue.getThreadPool(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }
}
