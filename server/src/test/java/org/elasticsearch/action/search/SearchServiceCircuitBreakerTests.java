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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
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
 * Unit tests for circuit breaker release logic used by SearchService and SearchTransportService.
 * <p>
 * For transport paths (fetch, scroll-fetch), the circuit-breaker reservation is atomically
 * extracted right after serialization inside {@code SearchTransportService.asBytesResponse} and
 * attached to the ref-counted serialized bytes. The actual release happens when the refcount
 * drops to zero — i.e. after Netty completes the write.
 * <p>
 * For the query phase (which may produce a {@code QueryFetchSearchResult} consumed locally by
 * {@code SearchQueryThenFetchAsyncAction}), the circuit breaker is released after the listener
 * consumes the response via {@code SearchService.releaseCircuitBreakerOnResponse}.
 */
public class SearchServiceCircuitBreakerTests extends ESTestCase {

    public void testReleaseCircuitBreakerForFetchResult() {
        AtomicLong breakerUsed = new AtomicLong(5000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setSearchHitsSizeBytes(5000L);

            fetchSearchResultListener(successCalled, failureCalled, breaker).onResponse(result);

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
            fetchResult.setSearchHitsSizeBytes(3000L);

            queryFetchResult = new QueryFetchSearchResult(new QuerySearchResult(), fetchResult);

            queryFetchSearchResultListener(successCalled, failureCalled, breaker).onResponse(queryFetchResult);

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
            fetchResult.setSearchHitsSizeBytes(4000L);

            QueryFetchSearchResult queryFetchResult = new QueryFetchSearchResult(new QuerySearchResult(), fetchResult);
            scrollResult = new ScrollQueryFetchSearchResult(queryFetchResult, null);

            scrollQueryFetchSearchResultListener(successCalled, failureCalled, breaker).onResponse(scrollResult);

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
        AtomicLong breakerUsed = new AtomicLong(0);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        fetchSearchResultListener(successCalled, failureCalled, breaker).onFailure(new RuntimeException("test failure"));

        assertThat(successCalled.get(), is(false));
        assertThat(failureCalled.get(), is(true));
        assertThat(breakerUsed.get(), equalTo(0L));
    }

    public void testExtractorReturnsNull() {
        AtomicLong breakerUsed = new AtomicLong(0);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        AtomicBoolean successCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);

        QuerySearchResult result = new QuerySearchResult();
        querySearchResultListener(successCalled, failureCalled, breaker).onResponse(result);

        assertThat(successCalled.get(), is(true));
        assertThat(failureCalled.get(), is(false));
    }

    public void testMultipleReleasesAreIdempotent() {
        AtomicLong breakerUsed = new AtomicLong(2000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setSearchHitsSizeBytes(2000L);

            result.releaseCircuitBreakerBytes(breaker);
            assertThat(breakerUsed.get(), equalTo(0L));
            assertThat(result.getSearchHitsSizeBytes(), equalTo(0L));

            result.releaseCircuitBreakerBytes(breaker);
            assertThat(breakerUsed.get(), equalTo(0L));
            assertThat(result.getSearchHitsSizeBytes(), equalTo(0L));
        } finally {
            result.decRef();
        }
    }

    public void testExtractCircuitBreakerRelease() {
        AtomicLong breakerUsed = new AtomicLong(3000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setSearchHitsSizeBytes(3000L);

            Releasable release = result.extractCircuitBreakerRelease(breaker);
            assertThat(result.getSearchHitsSizeBytes(), equalTo(0L));
            assertThat(breakerUsed.get(), equalTo(3000L));

            Releasables.close(release);
            assertThat(breakerUsed.get(), equalTo(0L));
        } finally {
            result.decRef();
        }
    }

    public void testExtractCircuitBreakerReleaseIsIdempotent() {
        AtomicLong breakerUsed = new AtomicLong(2000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setSearchHitsSizeBytes(2000L);

            Releasable first = result.extractCircuitBreakerRelease(breaker);
            Releasable second = result.extractCircuitBreakerRelease(breaker);

            assertThat(result.getSearchHitsSizeBytes(), equalTo(0L));
            assertThat(breakerUsed.get(), equalTo(2000L));

            Releasables.close(first);
            assertThat(breakerUsed.get(), equalTo(0L));

            Releasables.close(second);
            assertThat(breakerUsed.get(), equalTo(0L));
        } finally {
            result.decRef();
        }
    }

    public void testExtractAndDirectReleaseAreIdempotent() {
        AtomicLong breakerUsed = new AtomicLong(4000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setSearchHitsSizeBytes(4000L);

            Releasable release = result.extractCircuitBreakerRelease(breaker);
            result.releaseCircuitBreakerBytes(breaker);
            assertThat(breakerUsed.get(), equalTo(4000L));

            Releasables.close(release);
            assertThat(breakerUsed.get(), equalTo(0L));
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
            result.setSearchHitsSizeBytes(largeBytes);

            fetchSearchResultListener(successCalled, failureCalled, breaker).onResponse(result);

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
            result1.setSearchHitsSizeBytes(1000L);
            result2.setSearchHitsSizeBytes(2000L);
            result3.setSearchHitsSizeBytes(3000L);

            result1.releaseCircuitBreakerBytes(breaker);
            assertThat(breakerUsed.get(), equalTo(5000L));

            result2.releaseCircuitBreakerBytes(breaker);
            assertThat(breakerUsed.get(), equalTo(3000L));

            result3.releaseCircuitBreakerBytes(breaker);
            assertThat(breakerUsed.get(), equalTo(0L));
        } finally {
            result1.decRef();
            result2.decRef();
            result3.decRef();
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

    /**
     * Wrap a listener with circuit breaker release after {@code onResponse}, mirroring
     * {@code SearchService.releaseCircuitBreakerOnResponse} (used for the query phase local path).
     */
    private <T> ActionListener<T> withCircuitBreakerReleaseAfterResponse(
        ActionListener<T> listener,
        CircuitBreaker breaker,
        Function<T, FetchSearchResult> fetchResultExtractor
    ) {
        return ActionListener.wrap(response -> {
            try {
                listener.onResponse(response);
            } finally {
                FetchSearchResult fetchResult = fetchResultExtractor.apply(response);
                if (fetchResult != null) {
                    fetchResult.releaseCircuitBreakerBytes(breaker);
                }
            }
        }, listener::onFailure);
    }

    /**
     * Wrap a listener with deferred circuit breaker release, mirroring the {@code asBytesResponse}
     * callback in {@code SearchTransportService} (used for transport fetch paths). The reservation
     * is atomically extracted before {@code onResponse} and released afterwards, simulating the
     * Netty write-completion callback that would close the deferred releasable in production.
     */
    private <T> ActionListener<T> withDeferredCircuitBreakerRelease(
        ActionListener<T> listener,
        CircuitBreaker breaker,
        Function<T, FetchSearchResult> fetchResultExtractor
    ) {
        return ActionListener.wrap(response -> {
            FetchSearchResult fetchResult = fetchResultExtractor.apply(response);
            Releasable cbRelease = (fetchResult != null) ? fetchResult.extractCircuitBreakerRelease(breaker) : () -> {};
            try {
                listener.onResponse(response);
            } finally {
                Releasables.close(cbRelease);
            }
        }, listener::onFailure);
    }

    private ActionListener<QuerySearchResult> querySearchResultListener(
        AtomicBoolean successCalled,
        AtomicBoolean failureCalled,
        CircuitBreaker breaker
    ) {
        return withCircuitBreakerReleaseAfterResponse(trackingListener(successCalled, failureCalled), breaker, qr -> null);
    }

    private ActionListener<FetchSearchResult> fetchSearchResultListener(
        AtomicBoolean successCalled,
        AtomicBoolean failureCalled,
        CircuitBreaker breaker
    ) {
        return withDeferredCircuitBreakerRelease(trackingListener(successCalled, failureCalled), breaker, Function.identity());
    }

    private ActionListener<QueryFetchSearchResult> queryFetchSearchResultListener(
        AtomicBoolean successCalled,
        AtomicBoolean failureCalled,
        CircuitBreaker breaker
    ) {
        return withCircuitBreakerReleaseAfterResponse(
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
        return withDeferredCircuitBreakerRelease(trackingListener(successCalled, failureCalled), breaker, sr -> sr.result().fetchResult());
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
