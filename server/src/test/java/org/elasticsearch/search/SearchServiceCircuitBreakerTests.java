/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search;

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

import static org.elasticsearch.search.SearchService.releaseCircuitBreakerOnResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for circuit breaker release logic in SearchService.
 * Tests the generic helper method that releases circuit breaker bytes
 * after search results are sent to the coordinator.
 */
public class SearchServiceCircuitBreakerTests extends ESTestCase {

    public void testReleaseCircuitBreakerForFetchResult() {
        AtomicLong breakerUsed = new AtomicLong(5000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setSearchHitsSizeBytes(5000L, breaker);

            fetchSearchResultListener(successCalled, failureCalled).onResponse(result);

            assertThat(successCalled.get(), is(true));
            assertThat(failureCalled.get(), is(false));
            assertThat(breakerUsed.get(), equalTo(0L));
            assertThat(result.getSearchHitsSizeBytes(), equalTo(0L));
        } finally {
            result.decRef();
        }
    }

    public void testReleaseCircuitBreakerForQueryFetchResult() {
        AtomicLong breakerUsed = new AtomicLong(3000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        FetchSearchResult fetchResult = new FetchSearchResult();
        QueryFetchSearchResult queryFetchResult = null;
        try {
            fetchResult.setSearchHitsSizeBytes(3000L, breaker);

            queryFetchResult = new QueryFetchSearchResult(new QuerySearchResult(), fetchResult);

            queryFetchSearchResultListener(successCalled, failureCalled).onResponse(queryFetchResult);

            assertThat(successCalled.get(), is(true));
            assertThat(failureCalled.get(), is(false));
            assertThat(breakerUsed.get(), equalTo(0L));
            assertThat(fetchResult.getSearchHitsSizeBytes(), equalTo(0L));
        } finally {
            if (queryFetchResult != null) {
                queryFetchResult.decRef();
            } else {
                fetchResult.decRef();
            }
        }
    }

    public void testReleaseCircuitBreakerForScrollResult() {
        AtomicLong breakerUsed = new AtomicLong(4000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        FetchSearchResult fetchResult = new FetchSearchResult();
        ScrollQueryFetchSearchResult scrollResult = null;
        try {
            fetchResult.setSearchHitsSizeBytes(4000L, breaker);

            QueryFetchSearchResult queryFetchResult = new QueryFetchSearchResult(new QuerySearchResult(), fetchResult);
            scrollResult = new ScrollQueryFetchSearchResult(queryFetchResult, null);

            scrollQueryFetchSearchResultListener(successCalled, failureCalled).onResponse(scrollResult);

            assertThat(successCalled.get(), is(true));
            assertThat(failureCalled.get(), is(false));
            assertThat(breakerUsed.get(), equalTo(0L));
            assertThat(fetchResult.getSearchHitsSizeBytes(), equalTo(0L));
        } finally {
            if (scrollResult != null) {
                scrollResult.decRef();
            } else {
                fetchResult.decRef();
            }
        }
    }

    public void testReleaseCircuitBreakerOnFailure() {
        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        fetchSearchResultListener(successCalled, failureCalled).onFailure(new RuntimeException("test failure"));

        assertThat(successCalled.get(), is(false));
        assertThat(failureCalled.get(), is(true));
    }

    public void testThrowingResponseHandlerStillReleases() {
        AtomicLong breakerUsed = new AtomicLong(3000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        ActionListener<FetchSearchResult> listener = releaseCircuitBreakerOnResponse(new ActionListener<>() {
            @Override
            public void onResponse(FetchSearchResult response) {
                throw new IllegalStateException("response handler failed");
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("should not be reached", e);
            }
        }, Function.identity());

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setSearchHitsSizeBytes(3000L, breaker);

            // The wrapper releases from a finally, so a throwing handler must not strand the charge, and the
            // exception must still propagate so wrapFailureListener can free the reader context.
            expectThrows(IllegalStateException.class, () -> listener.onResponse(result));
            assertThat(breakerUsed.get(), equalTo(0L));
        } finally {
            result.decRef();
        }
    }

    public void testExtractorReturnsNull() {
        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        querySearchResultListener(successCalled, failureCalled).onResponse(new QuerySearchResult());

        assertThat(successCalled.get(), is(true));
        assertThat(failureCalled.get(), is(false));
        // No breaker to release, should complete normally
    }

    public void testMultipleReleasesAreIdempotent() {
        AtomicLong breakerUsed = new AtomicLong(2000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setSearchHitsSizeBytes(2000L, breaker);

            // First release
            result.releaseCircuitBreakerBytes();
            assertThat(breakerUsed.get(), equalTo(0L));
            assertThat(result.getSearchHitsSizeBytes(), equalTo(0L));

            // Next release - should be no-op
            result.releaseCircuitBreakerBytes();
            assertThat(breakerUsed.get(), equalTo(0L));
            assertThat(result.getSearchHitsSizeBytes(), equalTo(0L));
        } finally {
            result.decRef();
        }
    }

    public void testLargeAllocation() {
        long largeBytes = randomLongBetween(1_000_000, 10_000_000);
        AtomicLong breakerUsed = new AtomicLong(largeBytes);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setSearchHitsSizeBytes(largeBytes, breaker);

            fetchSearchResultListener(successCalled, failureCalled).onResponse(result);

            assertThat(successCalled.get(), is(true));
            assertThat(breakerUsed.get(), equalTo(0L));
            assertThat(result.getSearchHitsSizeBytes(), equalTo(0L));
        } finally {
            result.decRef();
        }
    }

    public void testMultipleFetchResults() {
        AtomicLong breakerUsed = new AtomicLong(6000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult result1 = new FetchSearchResult();
        FetchSearchResult result2 = new FetchSearchResult();
        FetchSearchResult result3 = new FetchSearchResult();

        try {
            result1.setSearchHitsSizeBytes(1000L, breaker);
            result2.setSearchHitsSizeBytes(2000L, breaker);
            result3.setSearchHitsSizeBytes(3000L, breaker);

            result1.releaseCircuitBreakerBytes();
            assertThat(breakerUsed.get(), equalTo(5000L));

            result2.releaseCircuitBreakerBytes();
            assertThat(breakerUsed.get(), equalTo(3000L));

            result3.releaseCircuitBreakerBytes();
            assertThat(breakerUsed.get(), equalTo(0L));
        } finally {
            result1.decRef();
            result2.decRef();
            result3.decRef();
        }
    }

    public void testReleaseOnDeallocateWhenNeverReleasedExplicitly() {
        AtomicLong breakerUsed = new AtomicLong(7000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setSearchHitsSizeBytes(7000L, breaker);
            assertThat(breakerUsed.get(), equalTo(7000L));
        } finally {
            // Dropped without an explicit release; the charge is owned by the result, so deallocate gives it back.
            result.decRef();
        }

        assertThat(breakerUsed.get(), equalTo(0L));
        assertThat(result.getSearchHitsSizeBytes(), equalTo(0L));
    }

    public void testChargeHeldWhileReferencesRemain() {
        AtomicLong breakerUsed = new AtomicLong(1500);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setSearchHitsSizeBytes(1500L, breaker);
            result.incRef();

            // Still reachable by the second holder, so the bytes stay charged.
            result.decRef();
            assertThat(breakerUsed.get(), equalTo(1500L));
        } finally {
            result.decRef();
        }

        assertThat(breakerUsed.get(), equalTo(0L));
    }

    public void testExplicitReleaseThenDeallocateDoesNotDoubleRelease() {
        AtomicLong breakerUsed = new AtomicLong(2500);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setSearchHitsSizeBytes(2500L, breaker);

            result.releaseCircuitBreakerBytes();
            assertThat(breakerUsed.get(), equalTo(0L));
        } finally {
            // A second return here would drive the breaker negative.
            result.decRef();
        }

        assertThat(breakerUsed.get(), equalTo(0L));
    }

    public void testChargeGoesBackToTheBreakerItWasChargedTo() {
        AtomicLong firstUsed = new AtomicLong(800);
        AtomicLong secondUsed = new AtomicLong(500);
        CircuitBreaker first = new TestCircuitBreaker(firstUsed);
        CircuitBreaker second = new TestCircuitBreaker(secondUsed);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setSearchHitsSizeBytes(800L, first);
            result.releaseCircuitBreakerBytes();
            assertThat(firstUsed.get(), equalTo(0L));
            assertThat(secondUsed.get(), equalTo(500L));

            // Charging again against a different breaker must return there, not to the one used before.
            result.setSearchHitsSizeBytes(500L, second);
            result.releaseCircuitBreakerBytes();
            assertThat(secondUsed.get(), equalTo(0L));
            assertThat(firstUsed.get(), equalTo(0L));
        } finally {
            result.decRef();
        }
    }

    public void testZeroChargeRecordsNothing() {
        FetchSearchResult result = new FetchSearchResult();
        try {
            // A zero charge records nothing, so it returns before it needs a breaker to give anything back to.
            result.setSearchHitsSizeBytes(0L, null);
            assertThat(result.getSearchHitsSizeBytes(), equalTo(0L));

            result.releaseCircuitBreakerBytes();
        } finally {
            result.decRef();
        }
    }

    public void testOverwritingAnOutstandingChargeTripsAssertion() {
        AtomicLong breakerUsed = new AtomicLong(1200);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setSearchHitsSizeBytes(1200L, breaker);
            expectThrows(AssertionError.class, () -> result.setSearchHitsSizeBytes(400L, breaker));
        } finally {
            result.decRef();
        }
    }

    /**
     * Create a listener that tracks if it was called.
     */
    private <T> ActionListener<T> trackingListener(AtomicBoolean successCalled, AtomicBoolean failureCalled) {
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

    private ActionListener<QuerySearchResult> querySearchResultListener(AtomicBoolean successCalled, AtomicBoolean failureCalled) {
        return releaseCircuitBreakerOnResponse(trackingListener(successCalled, failureCalled), qr -> null);
    }

    private ActionListener<FetchSearchResult> fetchSearchResultListener(AtomicBoolean successCalled, AtomicBoolean failureCalled) {
        return releaseCircuitBreakerOnResponse(trackingListener(successCalled, failureCalled), Function.identity());
    }

    private ActionListener<QueryFetchSearchResult> queryFetchSearchResultListener(
        AtomicBoolean successCalled,
        AtomicBoolean failureCalled
    ) {
        return releaseCircuitBreakerOnResponse(trackingListener(successCalled, failureCalled), QueryFetchSearchResult::fetchResult);
    }

    private ActionListener<ScrollQueryFetchSearchResult> scrollQueryFetchSearchResultListener(
        AtomicBoolean successCalled,
        AtomicBoolean failureCalled
    ) {
        return releaseCircuitBreakerOnResponse(trackingListener(successCalled, failureCalled), sr -> sr.result().fetchResult());
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
