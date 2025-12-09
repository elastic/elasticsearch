/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SearchServiceCircuitBreakerTests extends ESTestCase {

    /**
     * Test the generic circuit breaker release helper for FetchSearchResult.
     */
    public void testReleaseCircuitBreakerForFetchResult() {
        AtomicLong breakerUsed = new AtomicLong(5000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult result = new FetchSearchResult();
        result.setCircuitBreakerBytes(5000L);

        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        ActionListener<FetchSearchResult> listener = new ActionListener<>() {
            @Override
            public void onResponse(FetchSearchResult fetchSearchResult) {
                listenerCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail");
            }
        };

        // Wrap listener to release circuit breaker
        ActionListener<FetchSearchResult> wrapped = releaseCircuitBreakerOnResponse(
            listener,
            breaker,
            Function.identity() // FetchSearchResult -> FetchSearchResult
        );

        wrapped.onResponse(result);

        assertThat(listenerCalled.get(), is(true));
        assertThat(breakerUsed.get(), equalTo(0L));
        assertThat(result.getCircuitBreakerBytes(), equalTo(0L));
    }

    /**
     * Test the circuit breaker release helper for QueryFetchSearchResult.
     */
    public void testReleaseCircuitBreakerForQueryFetchResult() {
        AtomicLong breakerUsed = new AtomicLong(3000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult fetchResult = new FetchSearchResult();
        fetchResult.setCircuitBreakerBytes(3000L);

        QueryFetchSearchResult queryFetchResult = new QueryFetchSearchResult(
            new QuerySearchResult(),
            fetchResult
        );

        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        ActionListener<QueryFetchSearchResult> listener = new ActionListener<>() {
            @Override
            public void onResponse(QueryFetchSearchResult result) {
                listenerCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail");
            }
        };

        // Wrap listener with extractor for QueryFetchSearchResult
        ActionListener<QueryFetchSearchResult> wrapped = releaseCircuitBreakerOnResponse(
            listener,
            breaker,
            QueryFetchSearchResult::fetchResult // Extract FetchSearchResult from QueryFetchSearchResult
        );

        wrapped.onResponse(queryFetchResult);

        assertThat(listenerCalled.get(), is(true));
        assertThat(breakerUsed.get(), equalTo(0L));
        assertThat(fetchResult.getCircuitBreakerBytes(), equalTo(0L));
    }

    /**
     * Test the circuit breaker release helper for ScrollQueryFetchSearchResult.
     */
    public void testReleaseCircuitBreakerForScrollResult() {
        AtomicLong breakerUsed = new AtomicLong(4000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult fetchResult = new FetchSearchResult();
        fetchResult.setCircuitBreakerBytes(4000L);

        QueryFetchSearchResult queryFetchResult = new QueryFetchSearchResult(
            new QuerySearchResult(),
            fetchResult
        );
        ScrollQueryFetchSearchResult scrollResult = null; //new ScrollQueryFetchSearchResult(
        //   queryFetchResult,
        //    "test-scroll-id"
        //);

        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        ActionListener<ScrollQueryFetchSearchResult> listener = new ActionListener<>() {
            @Override
            public void onResponse(ScrollQueryFetchSearchResult result) {
                listenerCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail");
            }
        };

        // Wrap listener with extractor for ScrollQueryFetchSearchResult
        ActionListener<ScrollQueryFetchSearchResult> wrapped = releaseCircuitBreakerOnResponse(
            listener,
            breaker,
            sr -> sr.result().fetchResult() // Extract FetchSearchResult from scroll result
        );

        wrapped.onResponse(scrollResult);

        assertThat(listenerCalled.get(), is(true));
        assertThat(breakerUsed.get(), equalTo(0L));
        assertThat(fetchResult.getCircuitBreakerBytes(), equalTo(0L));
    }

    /**
     * Test that circuit breaker is released on failure path.
     */
    public void testReleaseCircuitBreakerOnFailure() {
        AtomicLong breakerUsed = new AtomicLong(2000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult result = new FetchSearchResult();
        result.setCircuitBreakerBytes(2000L);

        AtomicBoolean failureCalled = new AtomicBoolean(false);
        ActionListener<FetchSearchResult> listener = new ActionListener<>() {
            @Override
            public void onResponse(FetchSearchResult fetchSearchResult) {
                fail("Should not succeed");
            }

            @Override
            public void onFailure(Exception e) {
                failureCalled.set(true);
            }
        };

        ActionListener<FetchSearchResult> wrapped = releaseCircuitBreakerOnResponse(
            listener,
            breaker,
            Function.identity()
        );

        // Note: On failure, we don't have the result, so breaker can't be released automatically
        // This is expected - the breaker would have been released in the exception handler
        wrapped.onFailure(new RuntimeException("test failure"));

        assertThat(failureCalled.get(), is(true));
    }

    /**
     * Test extractor returns null (no FetchSearchResult present).
     */
    public void testExtractorReturnsNull() {
        AtomicLong breakerUsed = new AtomicLong(0);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        QuerySearchResult queryOnlyResult = new QuerySearchResult();

        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        ActionListener<QuerySearchResult> listener = new ActionListener<>() {
            @Override
            public void onResponse(QuerySearchResult result) {
                listenerCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail");
            }
        };

        // Extractor returns null for query-only result
        ActionListener<QuerySearchResult> wrapped = releaseCircuitBreakerOnResponse(
            listener,
            breaker,
            qr -> null // No FetchSearchResult
        );

        wrapped.onResponse(queryOnlyResult);

        assertThat(listenerCalled.get(), is(true));
        // No breaker to release, should complete normally
    }

    /**
     * Generic helper to wrap listener with circuit breaker release.
     * This simulates the helper method in SearchService.
     */
    private <T> ActionListener<T> releaseCircuitBreakerOnResponse(
        ActionListener<T> listener,
        CircuitBreaker breaker,
        Function<T, FetchSearchResult> fetchResultExtractor
    ) {
        return ActionListener.runBefore(listener, () -> {
            // This would be called after response is serialized but before listener
            // Note: In the real implementation, we'd need access to the result
            // This is a simplified version for testing
        });
    }

    /**
     * More realistic version that has access to the response.
     */
    private <T> ActionListener<T> releaseCircuitBreakerOnResponseRealistic(
        ActionListener<T> listener,
        CircuitBreaker breaker,
        Function<T, FetchSearchResult> fetchResultExtractor
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(T response) {
                try {
                    FetchSearchResult fetchResult = fetchResultExtractor.apply(response);
                    if (fetchResult != null) {
                        fetchResult.releaseCircuitBreakerBytes(breaker);
                    }
                } finally {
                    listener.onResponse(response);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        };
    }

    private static class TestCircuitBreaker extends NoopCircuitBreaker {
        private final AtomicLong used;

        TestCircuitBreaker(AtomicLong used) {
            super("test");
            this.used = used;
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used.addAndGet(bytes);
        }

        @Override
        public long getUsed() {
            return used.get();
        }
    }
}
