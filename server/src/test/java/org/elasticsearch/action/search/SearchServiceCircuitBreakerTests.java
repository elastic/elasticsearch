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
import org.elasticsearch.common.breaker.CircuitBreakingException;
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

/**
 * Unit tests for circuit breaker release logic in SearchService.
 * Tests the generic helper method that releases circuit breaker bytes
 * after search results are sent to the coordinator.
 */
public class SearchServiceCircuitBreakerTests extends ESTestCase {

    /**
     * Test the generic circuit breaker release helper for FetchSearchResult.
     */
    public void testReleaseCircuitBreakerForFetchResult() {
        AtomicLong breakerUsed = new AtomicLong(5000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        FetchSearchResult result = new FetchSearchResult();
        result.setCircuitBreakerBytes(5000L);

        fetchSearchResultListener(
            successCalled,
            failureCalled,
            breaker
        ).onResponse(result);

        assertThat(successCalled.get(), is(true));
        assertThat(failureCalled.get(), is(false));
        assertThat(breakerUsed.get(), equalTo(0L));
        assertThat(result.getCircuitBreakerBytes(), equalTo(0L));
    }

    /**
     * Test the circuit breaker release helper for QueryFetchSearchResult.
     */
    public void testReleaseCircuitBreakerForQueryFetchResult() {
        AtomicLong breakerUsed = new AtomicLong(3000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        FetchSearchResult fetchResult = new FetchSearchResult();
        fetchResult.setCircuitBreakerBytes(3000L);

        QueryFetchSearchResult queryFetchResult = new QueryFetchSearchResult(
            new QuerySearchResult(),
            fetchResult
        );

        queryFetchSearchResultListener(
            successCalled,
            failureCalled,
            breaker
        ).onResponse(queryFetchResult);

        assertThat(successCalled.get(), is(true));
        assertThat(failureCalled.get(), is(false));
        assertThat(breakerUsed.get(), equalTo(0L));
        assertThat(fetchResult.getCircuitBreakerBytes(), equalTo(0L));
    }

    /**
     * Test the circuit breaker release helper for ScrollQueryFetchSearchResult.
     */
    public void testReleaseCircuitBreakerForScrollResult() {
        AtomicLong breakerUsed = new AtomicLong(4000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        FetchSearchResult fetchResult = new FetchSearchResult();
        fetchResult.setCircuitBreakerBytes(4000L);

        QueryFetchSearchResult queryFetchResult = new QueryFetchSearchResult(
            new QuerySearchResult(),
            fetchResult
        );
        ScrollQueryFetchSearchResult scrollResult = new ScrollQueryFetchSearchResult(
            queryFetchResult,
            null
        );

        scrollQueryFetchSearchResultListener(
            successCalled,
            failureCalled,
            breaker
        ).onResponse(scrollResult);

        assertThat(successCalled.get(), is(true));
        assertThat(failureCalled.get(), is(false));
        assertThat(breakerUsed.get(), equalTo(0L));
        assertThat(fetchResult.getCircuitBreakerBytes(), equalTo(0L));
    }

    /**
     * Test that circuit breaker release is safe on failure path.
     * On failure, there's no result to extract, so nothing to release.
     */
    public void testReleaseCircuitBreakerOnFailure() {
        AtomicLong breakerUsed = new AtomicLong(0);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        fetchSearchResultListener(
            successCalled,
            failureCalled,
            breaker
        ).onFailure(new RuntimeException("test failure"));

        assertThat(successCalled.get(), is(false));
        assertThat(failureCalled.get(), is(true));
        assertThat(breakerUsed.get(), equalTo(0L));
    }

    /**
     * Test extractor returns null (no FetchSearchResult present).
     */
    public void testExtractorReturnsNull() {
        AtomicLong breakerUsed = new AtomicLong(0);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        querySearchResultListener(
            successCalled,
            failureCalled,
            breaker
        ).onResponse( new QuerySearchResult());

        assertThat(successCalled.get(), is(true));
        assertThat(failureCalled.get(), is(false));
        // No breaker to release, should complete normally
    }


    /**
     * Test multiple releases are safe (idempotent).
     */
    public void testMultipleReleasesAreIdempotent() {
        AtomicLong breakerUsed = new AtomicLong(2000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult result = new FetchSearchResult();
        result.setCircuitBreakerBytes(2000L);

        // First release
        result.releaseCircuitBreakerBytes(breaker);
        assertThat(breakerUsed.get(), equalTo(0L));
        assertThat(result.getCircuitBreakerBytes(), equalTo(0L));

        // Next release - should be no-op
        result.releaseCircuitBreakerBytes(breaker);
        assertThat(breakerUsed.get(), equalTo(0L));
        assertThat(result.getCircuitBreakerBytes(), equalTo(0L));
    }

    /**
     * Test with large allocation.
     */
    public void testLargeAllocation() {
        long largeBytes = randomLongBetween(1_000_000, 10_000_000);
        AtomicLong breakerUsed = new AtomicLong(largeBytes);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        FetchSearchResult result = new FetchSearchResult();
        result.setCircuitBreakerBytes(largeBytes);

        fetchSearchResultListener(
            successCalled,
            failureCalled,
            breaker
        ).onResponse(result);

        assertThat(successCalled.get(), is(true));
        assertThat(breakerUsed.get(), equalTo(0L));
        assertThat(result.getCircuitBreakerBytes(), equalTo(0L));
    }

    /**
     * Test multiple fetch results with different circuit breaker bytes.
     */
    public void testMultipleFetchResults() {
        AtomicLong breakerUsed = new AtomicLong(6000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult result1 = new FetchSearchResult();
        FetchSearchResult result2 = new FetchSearchResult();
        FetchSearchResult result3 = new FetchSearchResult();

        result1.setCircuitBreakerBytes(1000L);
        result2.setCircuitBreakerBytes(2000L);
        result3.setCircuitBreakerBytes(3000L);

        result1.releaseCircuitBreakerBytes(breaker);
        assertThat(breakerUsed.get(), equalTo(5000L));

        result2.releaseCircuitBreakerBytes(breaker);
        assertThat(breakerUsed.get(), equalTo(3000L));

        result3.releaseCircuitBreakerBytes(breaker);
        assertThat(breakerUsed.get(), equalTo(0L));
    }


    /**
     * Create a listener that tracks if it was called.
     */
    private <T> ActionListener<T> trackingListener(
        AtomicBoolean successCalled,
        AtomicBoolean failureCalled
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(T result) {
                successCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                failureCalled.set(true);
            }
        };
    }

    /**
     * Wrap a listener with circuit breaker release.
     */
    private <T> ActionListener<T> withCircuitBreakerRelease(
        ActionListener<T> listener,
        CircuitBreaker breaker,
        Function<T, FetchSearchResult> fetchResultExtractor
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(T response) {
                try {
                    listener.onResponse(response);
                } finally {
                    FetchSearchResult fetchResult = fetchResultExtractor.apply(response);
                    if (fetchResult != null) {
                        fetchResult.releaseCircuitBreakerBytes(breaker);
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        };
    }

    private ActionListener<QuerySearchResult> querySearchResultListener(
        AtomicBoolean successCalled,
        AtomicBoolean failureCalled,
        CircuitBreaker breaker
    ) {
        return withCircuitBreakerRelease(
            trackingListener(successCalled, failureCalled),
            breaker,
            qr -> null
        );
    }

    private ActionListener<FetchSearchResult> fetchSearchResultListener(
        AtomicBoolean successCalled,
        AtomicBoolean failureCalled,
        CircuitBreaker breaker
    ) {
        return withCircuitBreakerRelease(
            trackingListener(successCalled, failureCalled),
            breaker,
            Function.identity()
        );
    }

    private ActionListener<QueryFetchSearchResult> queryFetchSearchResultListener(
        AtomicBoolean successCalled,
        AtomicBoolean failureCalled,
        CircuitBreaker breaker
    ) {
        return withCircuitBreakerRelease(
            trackingListener(successCalled, failureCalled),
            breaker,
            QueryFetchSearchResult::fetchResult
        );
    }

    private ActionListener<ScrollQueryFetchSearchResult> scrollQueryFetchSearchResultListener(
        AtomicBoolean successCalled,
        AtomicBoolean failureCalled,
        CircuitBreaker breaker
    ) {
        return withCircuitBreakerRelease(
            trackingListener(successCalled, failureCalled),
            breaker,
            sr -> sr.result().fetchResult()
        );
    }

    /**
     * CB implementation for testing that tracks used bytes.
     */
    private static class TestCircuitBreaker extends NoopCircuitBreaker {
        private final AtomicLong used;

        TestCircuitBreaker(AtomicLong used) {
            super("test");
            this.used = used;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            used.addAndGet(bytes);
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
