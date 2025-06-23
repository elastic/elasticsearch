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
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.StreamingHttpResult;
import org.elasticsearch.xpack.inference.external.request.HttpRequestTests;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.Before;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.inference.external.http.retry.RetrySettingsTests.createDefaultRetrySettings;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
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
        doThrow(new RetryException(true, "failed")).doNothing().when(handler).validateResponse(any(), any(), any(), any(), anyBoolean());
        // Mockito.thenReturn() does not compile when returning a
        // bounded wild card list, thenAnswer must be used instead.
        when(handler.parseResult(any(Request.class), any(HttpResult.class))).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), () -> false, handler, listener), 1);

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
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), () -> false, handler, listener), 1);

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
        when(handler.parseResult(any(Request.class), any(HttpResult.class))).thenThrow(new RetryException(true, "failed"))
            .thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), () -> false, handler, listener), 1);

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
        when(handler.parseResult(any(Request.class), any(HttpResult.class))).thenThrow(new IllegalStateException("failed"))
            .thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), () -> false, handler, listener), 0);

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
        when(handler.parseResult(any(Request.class), any(HttpResult.class))).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), () -> false, handler, listener), 1);

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
        when(handler.parseResult(any(Request.class), any(HttpResult.class))).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), () -> false, handler, listener), 1);

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
        when(handler.parseResult(any(Request.class), any(HttpResult.class))).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), () -> false, handler, listener), 1);

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
        when(handler.parseResult(any(Request.class), any(HttpResult.class))).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), () -> false, handler, listener), 0);

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
        when(handler.parseResult(any(Request.class), any(HttpResult.class))).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest("id"), () -> false, handler, listener), 0);

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
            .validateResponse(any(), any(), any(), any(), anyBoolean());
        when(handler.parseResult(any(Request.class), any(HttpResult.class))).thenAnswer(answer);

        var retrier = createRetrier(sender);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), () -> false, handler, listener), 1);

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
            .validateResponse(any(), any(), any(), any(), anyBoolean());
        when(handler.parseResult(any(Request.class), any(HttpResult.class))).thenAnswer(answer);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), () -> false, handler, listener), 1);

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
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), () -> false, handler, listener), 1);

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
        executeTasks(() -> retrier.send(mock(Logger.class), mockRequest(), () -> false, handler, listener), 0);

        var thrownException = expectThrows(IllegalStateException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("failed"));
        assertThat(thrownException.getSuppressed().length, is(0));
        verify(httpClient, times(1)).send(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
    }

    public void testStreamSuccess() throws IOException {
        var httpClient = mock(HttpClient.class);
        StreamingHttpResult streamingHttpResult = new StreamingHttpResult(mockHttpResponse(), randomPublisher());
        doAnswer(ans -> {
            ActionListener<StreamingHttpResult> listener = ans.getArgument(2);
            listener.onResponse(streamingHttpResult);
            return null;
        }).when(httpClient).stream(any(), any(), any());

        var retrier = createRetrier(httpClient);

        ActionListener<InferenceServiceResults> listener = mock();
        var request = mockRequest();
        when(request.isStreaming()).thenReturn(true);
        var responseHandler = mock(ResponseHandler.class);
        when(responseHandler.canHandleStreamingResponses()).thenReturn(true);
        executeTasks(() -> retrier.send(mock(Logger.class), request, () -> false, responseHandler, listener), 0);

        verify(httpClient, times(1)).stream(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
        verify(responseHandler, times(1)).parseResult(any(), ArgumentMatchers.<Flow.Publisher<HttpResult>>any());
    }

    private Flow.Publisher<byte[]> randomPublisher() {
        var calls = new AtomicInteger(randomIntBetween(1, 4));
        return subscriber -> {
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    if (calls.getAndDecrement() > 0) {
                        subscriber.onNext(randomByteArrayOfLength(3));
                    } else {
                        subscriber.onComplete();
                    }
                }

                @Override
                public void cancel() {

                }
            });
        };
    }

    public void testStream_ResponseHandlerDoesNotHandleStreams() throws IOException {
        var httpClient = mock(HttpClient.class);
        doAnswer(ans -> {
            ActionListener<HttpResult> listener = ans.getArgument(2);
            listener.onResponse(new HttpResult(mock(), new byte[0]));
            return null;
        }).when(httpClient).send(any(), any(), any());

        var expectedResponse = mock(InferenceServiceResults.class);

        var retrier = createRetrier(httpClient);

        var listener = new PlainActionFuture<InferenceServiceResults>();
        var request = mockRequest();
        when(request.isStreaming()).thenReturn(true);
        var responseHandler = mock(ResponseHandler.class);
        when(responseHandler.parseResult(any(Request.class), any(HttpResult.class))).thenReturn(expectedResponse);
        when(responseHandler.canHandleStreamingResponses()).thenReturn(false);
        executeTasks(() -> retrier.send(mock(Logger.class), request, () -> false, responseHandler, listener), 0);

        var actualResponse = listener.actionGet(TIMEOUT);

        verify(httpClient, times(1)).send(any(), any(), any());
        verifyNoMoreInteractions(httpClient);
        assertThat(actualResponse, sameInstance(expectedResponse));
    }

    public void testSend_DoesNotRetryIndefinitely() throws IOException {
        var threadPool = new TestThreadPool(getTestName());
        try {

            var httpClient = mock(HttpClient.class);

            doAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
                // respond with a retryable exception
                listener.onFailure(new ConnectionClosedException("failed"));

                return Void.TYPE;
            }).when(httpClient).send(any(), any(), any());

            var handler = mock(ResponseHandler.class);

            var retrier = new RetryingHttpSender(
                httpClient,
                mock(ThrottlerManager.class),
                createDefaultRetrySettings(),
                threadPool,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );

            var listener = new PlainActionFuture<InferenceServiceResults>();
            retrier.send(mock(Logger.class), mockRequest(), () -> false, handler, listener);

            // Assert that the retrying sender stopped after max retires even though the exception is retryable
            var thrownException = expectThrows(UncategorizedExecutionException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(thrownException.getCause(), instanceOf(ConnectionClosedException.class));
            assertThat(thrownException.getMessage(), is("Failed execution"));
            assertThat(thrownException.getSuppressed().length, is(0));
            verify(httpClient, times(RetryingHttpSender.MAX_RETIES)).send(any(), any(), any());
            verifyNoMoreInteractions(httpClient);
        } finally {
            terminate(threadPool);
        }
    }

    public void testStream_DoesNotRetryIndefinitely() throws IOException {
        var threadPool = new TestThreadPool(getTestName());
        try {
            var httpClient = mock(HttpClient.class);
            doAnswer(ans -> {
                ActionListener<StreamingHttpResult> listener = ans.getArgument(2);
                listener.onFailure(new ConnectionClosedException("failed"));
                return null;
            }).when(httpClient).stream(any(), any(), any());

            var handler = mock(ResponseHandler.class);
            when(handler.canHandleStreamingResponses()).thenReturn(true);

            var retrier = new RetryingHttpSender(
                httpClient,
                mock(ThrottlerManager.class),
                createDefaultRetrySettings(),
                threadPool,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );

            var listener = new PlainActionFuture<InferenceServiceResults>();
            var request = mockRequest();
            when(request.isStreaming()).thenReturn(true);
            retrier.send(mock(Logger.class), request, () -> false, handler, listener);

            // Assert that the retrying sender stopped after max retires even though the exception is retryable
            var thrownException = expectThrows(UncategorizedExecutionException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(thrownException.getCause(), instanceOf(ConnectionClosedException.class));
            assertThat(thrownException.getMessage(), is("Failed execution"));
            assertThat(thrownException.getSuppressed().length, is(0));
            verify(httpClient, times(RetryingHttpSender.MAX_RETIES)).stream(any(), any(), any());
            verifyNoMoreInteractions(httpClient);
        } finally {
            terminate(threadPool);
        }
    }

    public void testSend_DoesNotRetryIndefinitely_WithAlwaysRetryingResponseHandler() throws IOException {
        var threadPool = new TestThreadPool(getTestName());
        try {

            var httpClient = mock(HttpClient.class);

            doAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
                listener.onFailure(new ConnectionClosedException("failed"));

                return Void.TYPE;
            }).when(httpClient).send(any(), any(), any());

            // This handler will always tell the sender to retry
            var handler = createRetryingResponseHandler();

            var retrier = new RetryingHttpSender(
                httpClient,
                mock(ThrottlerManager.class),
                createDefaultRetrySettings(),
                threadPool,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );

            var listener = new PlainActionFuture<InferenceServiceResults>();
            retrier.send(mock(Logger.class), mockRequest(), () -> false, handler, listener);

            // Assert that the retrying sender stopped after max retires
            var thrownException = expectThrows(UncategorizedExecutionException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(thrownException.getCause(), instanceOf(ConnectionClosedException.class));
            assertThat(thrownException.getMessage(), is("Failed execution"));
            assertThat(thrownException.getSuppressed().length, is(0));
            verify(httpClient, times(RetryingHttpSender.MAX_RETIES)).send(any(), any(), any());
            verifyNoMoreInteractions(httpClient);
        } finally {
            terminate(threadPool);
        }
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

    private ResponseHandler createRetryingResponseHandler() {
        // Returns a response handler that wants to retry.
        // Does not need to handle parsing as it should only be used
        // testing failed requests
        return new ResponseHandler() {
            @Override
            public void validateResponse(
                ThrottlerManager throttlerManager,
                Logger logger,
                Request request,
                HttpResult result,
                boolean checkForErrorObject
            ) throws RetryException {
                throw new RetryException(true, new IOException("response handler validate failed as designed"));
            }

            @Override
            public InferenceServiceResults parseResult(Request request, HttpResult result) throws RetryException {
                throw new RetryException(true, new IOException("response handler parse failed as designed"));
            }

            @Override
            public String getRequestType() {
                return "foo";
            }

            @Override
            public boolean canHandleStreamingResponses() {
                return false;
            }
        };
    }
}
