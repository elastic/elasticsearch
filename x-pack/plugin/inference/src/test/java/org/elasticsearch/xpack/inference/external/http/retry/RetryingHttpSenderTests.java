/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RetryingHttpSenderTests extends ESTestCase {
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    public void testSend_CallsSenderAgain_AfterValidateResponseThrowsAnException() throws IOException {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[1];
            listener.onResponse(new HttpResult(mock(HttpResponse.class), new byte[0]));

            return Void.TYPE;
        }).when(sender).send(any(), any());

        var inferenceResults = mock(InferenceResults.class);

        var handler = mock(ResponseHandler.class);
        doThrow(new IllegalStateException("failed")).doNothing().when(handler).validateResponse(any(), any(), any(), any());
        when(handler.parseResult(any())).thenReturn(inferenceResults);

        var retrier = new RetryingHttpSender(
            sender,
            mock(ThrottlerManager.class),
            mock(Logger.class),
            new RetrySettings(1, TimeValue.timeValueNanos(1))
        );

        var listener = new PlainActionFuture<InferenceResults>();
        retrier.send(mock(HttpRequestBase.class), handler, listener);

        assertThat(listener.actionGet(TIMEOUT), is(inferenceResults));
        verify(sender, times(2)).send(any(), any());
    }

    public void testSend_CallsSenderAgain_WhenAFailureStatusCodeIsReturned() throws IOException {
        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(300).thenReturn(200);

        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[1];
            listener.onResponse(new HttpResult(httpResponse, new byte[] { 'a' }));

            return Void.TYPE;
        }).when(sender).send(any(), any());

        var inferenceResults = mock(InferenceResults.class);

        var handler = new DefaultResponseHandler("test") {
            @Override
            public InferenceResults parseResult(HttpResult result) throws IOException {
                return inferenceResults;
            }
        };

        var retrier = new RetryingHttpSender(
            sender,
            mock(ThrottlerManager.class),
            mock(Logger.class),
            new RetrySettings(1, TimeValue.timeValueNanos(1))
        );

        var listener = new PlainActionFuture<InferenceResults>();
        retrier.send(mock(HttpRequestBase.class), handler, listener);

        assertThat(listener.actionGet(TIMEOUT), is(inferenceResults));
        verify(sender, times(2)).send(any(), any());
    }

    public void testSend_CallsSenderAgain_WhenParsingFailsOnce() throws IOException {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[1];
            listener.onResponse(new HttpResult(mock(HttpResponse.class), new byte[] { 'a' }));

            return Void.TYPE;
        }).when(sender).send(any(), any());

        var inferenceResults = mock(InferenceResults.class);

        var handler = mock(ResponseHandler.class);
        when(handler.parseResult(any())).thenThrow(new IOException("failed")).thenReturn(inferenceResults);

        var retrier = new RetryingHttpSender(
            sender,
            mock(ThrottlerManager.class),
            mock(Logger.class),
            new RetrySettings(1, TimeValue.timeValueNanos(1))
        );

        var listener = new PlainActionFuture<InferenceResults>();
        retrier.send(mock(HttpRequestBase.class), handler, listener);

        assertThat(listener.actionGet(TIMEOUT), is(inferenceResults));
        verify(sender, times(2)).send(any(), any());
    }

    public void testSend_CallsSenderAgain_WhenHttpResultListenerCallsOnFailureOnce() throws IOException {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[1];
            listener.onFailure(new ElasticsearchException("failed"));

            return Void.TYPE;
        }).doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[1];
            listener.onResponse(new HttpResult(mock(HttpResponse.class), new byte[] { 'a' }));

            return Void.TYPE;
        }).when(sender).send(any(), any());

        var inferenceResults = mock(InferenceResults.class);

        var handler = mock(ResponseHandler.class);
        when(handler.parseResult(any())).thenReturn(inferenceResults);

        var retrier = new RetryingHttpSender(
            sender,
            mock(ThrottlerManager.class),
            mock(Logger.class),
            new RetrySettings(1, TimeValue.timeValueNanos(1))
        );

        var listener = new PlainActionFuture<InferenceResults>();
        retrier.send(mock(HttpRequestBase.class), handler, listener);

        assertThat(listener.actionGet(TIMEOUT), is(inferenceResults));
        verify(sender, times(2)).send(any(), any());
    }

    public void testSend_ReturnsFailure_WhenValidateResponseThrowsAnException_AfterOneRetry() throws IOException {
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(mock(StatusLine.class));

        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[1];
            listener.onResponse(new HttpResult(httpResponse, new byte[0]));

            return Void.TYPE;
        }).when(sender).send(any(), any());

        var inferenceResults = mock(InferenceResults.class);

        var handler = mock(ResponseHandler.class);
        doThrow(new IllegalStateException("failed")).when(handler).validateResponse(any(), any(), any(), any());
        when(handler.parseResult(any())).thenReturn(inferenceResults);

        var retrier = new RetryingHttpSender(
            sender,
            mock(ThrottlerManager.class),
            mock(Logger.class),
            new RetrySettings(1, TimeValue.timeValueNanos(1))
        );

        var listener = new PlainActionFuture<InferenceResults>();
        retrier.send(mock(HttpRequestBase.class), handler, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        // The return exception is for a failure of retries
        assertThat(thrownException.getMessage(), is("Failed sending request [null] after [1] retries"));

        // The next level exception will be an Elasticsearch exception because we wrap anything that isn't already an ElasticsearchException
        assertThat(thrownException.getCause(), instanceOf(ElasticsearchException.class));
        assertThat(thrownException.getCause().getMessage(), is("Failed to process request [null]"));

        // The final level is the original exception
        assertThat(thrownException.getCause().getCause(), instanceOf(IllegalStateException.class));
        assertThat(thrownException.getCause().getCause().getMessage(), is("failed"));
        verify(sender, times(2)).send(any(), any());
    }

    public void testSend_ReturnsFailure_WhenValidateResponseThrowsAnElasticsearchException_AfterOneRetry() throws IOException {
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(mock(StatusLine.class));

        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[1];
            listener.onResponse(new HttpResult(httpResponse, new byte[0]));

            return Void.TYPE;
        }).when(sender).send(any(), any());

        var inferenceResults = mock(InferenceResults.class);

        var handler = mock(ResponseHandler.class);
        doThrow(new ElasticsearchException("failed")).when(handler).validateResponse(any(), any(), any(), any());
        when(handler.parseResult(any())).thenReturn(inferenceResults);

        var retrier = new RetryingHttpSender(
            sender,
            mock(ThrottlerManager.class),
            mock(Logger.class),
            new RetrySettings(1, TimeValue.timeValueNanos(1))
        );

        var listener = new PlainActionFuture<InferenceResults>();
        retrier.send(mock(HttpRequestBase.class), handler, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        // The return exception is for a failure of retries
        assertThat(thrownException.getMessage(), is("Failed sending request [null] after [1] retries"));

        // The final level is the original exception
        assertThat(thrownException.getCause(), instanceOf(ElasticsearchException.class));
        assertThat(thrownException.getCause().getMessage(), is("failed"));
        verify(sender, times(2)).send(any(), any());
    }

    public void testSend_ReturnsFailure_WhenHttpResultsListenerCallsOnFailure_AfterOneRetry() {
        var httpResponse = mock(HttpResponse.class);
        when(httpResponse.getStatusLine()).thenReturn(mock(StatusLine.class));

        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[1];
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any());

        var handler = mock(ResponseHandler.class);

        var retrier = new RetryingHttpSender(
            sender,
            mock(ThrottlerManager.class),
            mock(Logger.class),
            new RetrySettings(1, TimeValue.timeValueNanos(1))
        );

        var listener = new PlainActionFuture<InferenceResults>();
        retrier.send(mock(HttpRequestBase.class), handler, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        // The return exception is for a failure of retries
        assertThat(thrownException.getMessage(), is("Failed sending request [null] after [1] retries"));

        // The final level is the original exception
        assertThat(thrownException.getCause(), instanceOf(IllegalStateException.class));
        assertThat(thrownException.getCause().getMessage(), is("failed"));
        verify(sender, times(2)).send(any(), any());
    }

    // TODO add a test to ensure the right log message is getting written for an IOException vs other exceptions
}
