/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentFactory;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.elasticsearch.test.ESTestCase.fail;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.CoreMatchers.is;

/**
 * Helper to chain together assertions for streaming {@link InferenceServiceResults}.
 */
public record InferenceEventsAssertion(Iterator<String> events, Throwable error, boolean isComplete, int iterations) {

    public static InferenceEventsAssertion assertThat(InferenceServiceResults results) throws Exception {
        return TestSubscriber.subscribeAndWait(results.publisher()).toAssertion();
    }

    public InferenceEventsAssertion hasFinishedStream() {
        MatcherAssert.assertThat(
            "Expected publisher to eventually call onComplete, but it stopped after [" + iterations + "] iterations.",
            isComplete
        );
        return this;
    }

    public InferenceEventsAssertion hasNoErrors() {
        if (error != null) {
            fail(error, "Expected no errors from stream.");
        }
        return this;
    }

    public InferenceEventsAssertion hasError() {
        MatcherAssert.assertThat("Expected error from stream.", error, Matchers.notNullValue());
        return this;
    }

    public InferenceEventsAssertion hasErrorWithStatusCode(int statusCode) {
        hasError();
        Throwable t = error;
        while (t != null) {
            if (t instanceof ElasticsearchStatusException statusException) {
                MatcherAssert.assertThat(statusException.status().getStatus(), Matchers.equalTo(statusCode));
                return this;
            }
            t = t.getCause();
        }
        fail(error, "Expected an underlying ElasticsearchStatusException.");
        return this;
    }

    public InferenceEventsAssertion hasErrorContaining(String message) {
        hasError();
        Throwable t = error;
        while (t != null) {
            if (t.getMessage() != null && t.getMessage().contains(message)) {
                return this;
            }
            t = t.getCause();
        }
        fail(error, "Expected exception to contain string: " + message);
        return this;
    }

    public InferenceEventsAssertion hasErrorMatching(CheckedConsumer<Throwable, ?> matcher) {
        hasError();
        try {
            matcher.accept(error);
        } catch (Exception e) {
            fail(e);
        }
        return this;
    }

    public InferenceEventsAssertion hasEvents(String... events) {
        Arrays.stream(events).forEach(this::hasEvent);
        return this;
    }

    public InferenceEventsAssertion hasNoEvents() {
        MatcherAssert.assertThat("Expected no items processed by the subscriber.", iterations, Matchers.is(0));
        return this;
    }

    public InferenceEventsAssertion hasEvent(String event) {
        MatcherAssert.assertThat(
            "Subscriber returned [" + iterations + "] results, but we expect at least one more.",
            this.events.hasNext(),
            Matchers.is(true)
        );
        MatcherAssert.assertThat(this.events.next(), Matchers.equalTo(event));
        return this;
    }

    private static class TestSubscriber<T extends ChunkedToXContent> implements Flow.Subscriber<T> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicInteger infiniteLoopCheck = new AtomicInteger(0);
        private final Stream.Builder<String> events = Stream.builder();
        private Throwable error;
        private boolean isComplete;
        private Flow.Subscription subscription;

        private static <T extends ChunkedToXContent> TestSubscriber<T> subscribeAndWait(Flow.Publisher<T> publisher) throws Exception {
            var testSubscriber = new TestSubscriber<T>();
            publisher.subscribe(testSubscriber);
            // the subscriber will initiate response handling on another thread, so we need to wait for that thread to finish
            try {
                MatcherAssert.assertThat(
                    "Timed out waiting for publisher or mock web server to finish.  Collected ["
                        + testSubscriber.infiniteLoopCheck.get()
                        + "] items.",
                    testSubscriber.latch.await(10, TimeUnit.SECONDS),
                    is(true)
                );
            } catch (Exception e) {
                // the test is about to fail, but stop the mock server from responding anyway
                testSubscriber.subscription.cancel();
                throw e;
            }
            return testSubscriber;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(T item) {
            if (infiniteLoopCheck.incrementAndGet() > 10) {
                subscription.cancel();
                latch.countDown();
                return;
            }

            try {
                events.add(toJsonString(item));
                subscription.request(1);
            } catch (IOException e) {
                onError(e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            error = throwable;
            isComplete = true;
            latch.countDown();
        }

        @Override
        public void onComplete() {
            isComplete = true;
            latch.countDown();
        }

        private String toJsonString(ChunkedToXContent chunkedToXContent) throws IOException {
            try (var builder = XContentFactory.jsonBuilder()) {
                chunkedToXContent.toXContentChunked(EMPTY_PARAMS).forEachRemaining(xContent -> {
                    try {
                        xContent.toXContent(builder, EMPTY_PARAMS);
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                });
                return XContentHelper.convertToJson(BytesReference.bytes(builder), false, builder.contentType());
            }
        }

        private InferenceEventsAssertion toAssertion() {
            return new InferenceEventsAssertion(events.build().iterator(), error, isComplete, infiniteLoopCheck.get());
        }
    }
}
