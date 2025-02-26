/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DelegatingProcessorTests extends ESTestCase {

    public static <T, R> R onNext(DelegatingProcessor<T, R> processor, T item) {
        var response = new AtomicReference<R>();
        var error = new AtomicReference<Throwable>();

        processor.onSubscribe(mock());

        Flow.Subscriber<R> downstream = mock();

        doAnswer(ans -> {
            response.set(ans.getArgument(0));
            return null;
        }).when(downstream).onNext(any());
        doAnswer(ans -> {
            error.set(ans.getArgument(0));
            return null;
        }).when(downstream).onError(any());

        processor.subscribe(downstream);

        processor.onNext(item);
        assertThat("onError should not be called", error.get(), nullValue());
        assertThat("Response from processor was null", response.get(), notNullValue());
        return response.get();
    }

    public static <T, R> Throwable onError(DelegatingProcessor<T, R> processor, T item) {
        var response = new AtomicReference<Throwable>();

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        Flow.Subscriber<R> downstream = mock();
        doAnswer(ans -> {
            response.set(ans.getArgument(0));
            return null;
        }).when(downstream).onError(any());
        processor.subscribe(downstream);

        processor.onNext(item);
        assertThat("Error from processor was null", response.get(), notNullValue());
        verify(upstream, times(1)).cancel();
        return response.get();
    }

    public void testRequestBeforeOnSubscribe() {
        var processor = delegatingProcessor();
        var expectedRequestCount = randomLongBetween(2, 100);

        Flow.Subscriber<String> downstream = mock();
        processor.subscribe(downstream);

        var subscription = ArgumentCaptor.forClass(Flow.Subscription.class);
        verify(downstream, times(1)).onSubscribe(subscription.capture());
        subscription.getValue().request(expectedRequestCount);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);
        verify(upstream, times(1)).request(eq(expectedRequestCount));
    }

    public void testRequestAfterOnSubscribe() {
        var processor = delegatingProcessor();
        var expectedRequestCount = randomLongBetween(2, 100);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);
        verify(upstream, never()).request(anyInt());

        Flow.Subscriber<String> downstream = mock();
        processor.subscribe(downstream);

        var subscription = ArgumentCaptor.forClass(Flow.Subscription.class);
        verify(downstream, times(1)).onSubscribe(subscription.capture());

        subscription.getValue().request(expectedRequestCount);
        verify(upstream, times(1)).request(eq(expectedRequestCount));
    }

    public void testOnNextAfterCancelDoesNotForwardItem() {
        var expectedItem = "hello";

        var processor = delegatingProcessor();
        processor.onSubscribe(mock());

        Flow.Subscriber<String> downstream = mock();
        doAnswer(ans -> {
            Flow.Subscription sub = ans.getArgument(0);
            sub.cancel();
            return null;
        }).when(downstream).onSubscribe(any());
        processor.subscribe(downstream);

        processor.onNext(expectedItem);

        verify(downstream, never()).onNext(any());
    }

    public void testCancelForwardsToUpstream() {
        var processor = delegatingProcessor();
        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        Flow.Subscriber<String> downstream = mock();
        doAnswer(ans -> {
            Flow.Subscription sub = ans.getArgument(0);
            sub.cancel();
            return null;
        }).when(downstream).onSubscribe(any());
        processor.subscribe(downstream);

        verify(upstream, times(1)).cancel();
    }

    public void testRequestForwardsToUpstream() {
        var expectedRequestCount = randomLongBetween(2, 20);
        var processor = delegatingProcessor();
        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        Flow.Subscriber<String> downstream = mock();
        doAnswer(ans -> {
            Flow.Subscription sub = ans.getArgument(0);
            sub.request(expectedRequestCount);
            return null;
        }).when(downstream).onSubscribe(any());
        processor.subscribe(downstream);

        verify(upstream, times(1)).request(expectedRequestCount);
    }

    public void testOnErrorBeforeSubscriptionThrowsException() {
        assertThrows(IllegalStateException.class, () -> delegatingProcessor().onError(new NullPointerException()));
    }

    public void testOnError() {
        var expectedException = new IllegalStateException("hello");

        var processor = delegatingProcessor();

        Flow.Subscriber<String> downstream = mock();
        processor.subscribe(downstream);

        processor.onError(expectedException);

        verify(downstream, times(1)).onError(eq(expectedException));
    }

    public void testOnCompleteBeforeSubscriptionInvokesOnComplete() {
        var processor = delegatingProcessor();

        Flow.Subscriber<String> downstream = mock();
        doAnswer(ans -> {
            Flow.Subscription sub = ans.getArgument(0);
            sub.request(1);
            return null;
        }).when(downstream).onSubscribe(any());

        processor.onComplete();
        verify(downstream, times(0)).onComplete();

        processor.subscribe(downstream);
        verify(downstream, times(1)).onComplete();
    }

    public void testOnComplete() {
        var processor = delegatingProcessor();

        Flow.Subscriber<String> downstream = mock();
        processor.subscribe(downstream);
        processor.onComplete();

        verify(downstream, times(1)).onComplete();
    }

    public void testSubscriberOnlyAllowsOnePublisher() {
        var publisher1 = delegatingProcessor();
        var publisher2 = delegatingProcessor();
        var subscriber1 = spy(delegatingProcessor());

        publisher1.subscribe(subscriber1);
        verify(subscriber1, times(1)).onSubscribe(any());

        // verify we cannot reuse subscribers
        assertThrows(IllegalStateException.class, () -> publisher2.subscribe(subscriber1));

        // verify publisher resets its subscriber
        var subscriber2 = spy(delegatingProcessor());
        publisher2.subscribe(subscriber2);
        verify(subscriber2, times(1)).onSubscribe(any());
    }

    private DelegatingProcessor<String, String> delegatingProcessor() {
        return new DelegatingProcessor<>() {
            @Override
            public void next(String item) {
                downstream().onNext(item);
            }
        };
    }
}
