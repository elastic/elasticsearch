/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.Flow;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamingResponseHandlerTests extends ESTestCase {
    @Mock
    private HttpResponse response;
    @Mock
    private ThrottlerManager throttlerManager;
    @Mock
    private Logger logger;
    @Mock
    private Request request;
    @Mock
    private ResponseHandler responseHandler;
    @Mock
    private ActionListener<InferenceServiceResults> listener;
    @Mock
    private Flow.Subscriber<HttpResult> downstreamSubscriber;
    @InjectMocks
    private StreamingResponseHandler streamingResponseHandler;
    private AutoCloseable mocks;
    private HttpResult item;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mocks = MockitoAnnotations.openMocks(this);
        item = new HttpResult(response, new byte[0]);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        mocks.close();
    }

    public void testOnSubscribeCallsRequest() {
        var subscription = mock(Flow.Subscription.class);
        streamingResponseHandler.onSubscribe(subscription);
        verify(subscription, only()).request(1L);
    }

    public void testResponseHandlerFailureIsForwardedToListener() {
        var upstreamSubscription = mock(Flow.Subscription.class);
        streamingResponseHandler.onSubscribe(upstreamSubscription);
        var expectedException = new RetryException(true, "ah");
        doThrow(expectedException).when(responseHandler).validateResponse(any(), any(), any(), any());

        var statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(404);
        when(statusLine.getReasonPhrase()).thenReturn("not found");
        when(response.getStatusLine()).thenReturn(statusLine);

        streamingResponseHandler.onNext(item);

        verify(listener, only()).onFailure(expectedException);
        verify(upstreamSubscription, times(1)).cancel();
    }

    public void testSuccessfulResponseCallsListener() {
        var upstreamSubscription = upstreamWithListenerCalled();

        verify(listener, only()).onResponse(any());
        verify(upstreamSubscription, never()).cancel();
    }

    private Flow.Subscription upstreamWithListenerCalled() {
        var upstreamSubscription = mock(Flow.Subscription.class);
        streamingResponseHandler.onSubscribe(upstreamSubscription);
        var inferenceServiceResults = mock(InferenceServiceResults.class);

        doAnswer(ans -> {
            Flow.Publisher<HttpResult> publisher = ans.getArgument(2);
            publisher.subscribe(downstreamSubscriber);
            return inferenceServiceResults;
        }).when(responseHandler).parseResult(any(), any(), any());

        streamingResponseHandler.onNext(item);
        return upstreamSubscription;
    }

    public void testOnNextOnlyCallsListenerOnce() {
        upstreamWithListenerCalled();

        streamingResponseHandler.onNext(item);

        verify(listener, times(1)).onResponse(any());
        verify(listener, never()).onFailure(any());
    }

    public void testSecondOnNextCallsDownstream() {
        upstreamWithListenerCalled();

        streamingResponseHandler.onNext(item);

        verify(downstreamSubscriber, times(1)).onNext(item);
    }

    public void testCompleteForwardsComplete() {
        upstreamWithListenerCalled();

        streamingResponseHandler.onComplete();

        verify(downstreamSubscriber, times(1)).onSubscribe(any());
        verify(downstreamSubscriber, times(1)).onComplete();
    }

    public void testErrorForwardsError() {
        var expectedError = new RetryException(false, "ah");
        upstreamWithListenerCalled();

        streamingResponseHandler.onError(expectedError);

        verify(downstreamSubscriber, times(1)).onSubscribe(any());
        verify(downstreamSubscriber, times(1)).onError(same(expectedError));
    }

    public void testSubscriptionForwardsRequest() {
        var upstreamSubscription = upstreamWithListenerCalled();

        var downstream = ArgumentCaptor.forClass(Flow.Subscription.class);
        verify(downstreamSubscriber, times(1)).onSubscribe(downstream.capture());
        var downstreamSubscription = downstream.getValue();

        var requestCount = randomIntBetween(2, 200);
        downstreamSubscription.request(requestCount);
        verify(upstreamSubscription, times(1)).request(requestCount);
    }
}
