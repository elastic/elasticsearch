/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Integration coverage for the reader-heap budget: no reservation leaks across the engine lifecycle, refresh
 * proceeds when the breaker is unlimited, and refresh defers when the breaker has a positive limit.
 */
public class SearchEngineHeapBudgetTests extends AbstractEngineTestCase {

    @Override
    public String[] tmpPaths() {
        return new String[] { createTempDir().toAbsolutePath().toString() };
    }

    // Default to -1 (observation mode). Individual tests can set a positive limit to engage the breakable path.
    private final TrackingCircuitBreaker trackingBreaker = new TrackingCircuitBreaker(StatelessReaderHeapBreaker.NAME, -1L);

    @Override
    protected CircuitBreaker newReaderHeapBreaker() {
        return trackingBreaker;
    }

    public void testNoLeakAcrossEngineLifecycle() throws IOException {
        final var indexConfig = indexConfig();
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue)
        ) {
            // The initial commit of a stateless search engine is empty (no segments), so no bytes are reserved
            // until a refresh brings in actual segments via a commit notification.
            assertThat("empty initial commit reserves nothing", trackingBreaker.getUsed(), equalTo(0L));

            // Drive several refresh cycles. Each one allocates a new reader generation; new segments are
            // reserved, old segments shared across generations are refcounted.
            for (int i = 0; i < 5; i++) {
                indexEngine.index(randomDoc(String.valueOf(i)));
                indexEngine.flush();
                notifyCommits(indexEngine, searchEngine);
                searchTaskQueue.runAllRunnableTasks();
            }

            assertThat(
                "after several refreshes with data we must have reserved bytes against the breaker",
                trackingBreaker.getUsed(),
                greaterThan(0L)
            );
        }

        // Engine closed: the reader manager has decRef'd every alive reader, every close listener fired, every
        // reservation should now be released. This is the critical leak invariant.
        assertThat("reservation must drain to zero after engine close", trackingBreaker.getUsed(), equalTo(0L));
        assertWarnings(
            "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testUnlimitedBreakerNeverDefers() throws IOException {
        // The default limit of -1 is the observation-mode sentinel: bytes are tracked but refreshes never skip.
        // Sanity-check that the engine routes through addWithoutBreaking and the deferred counter stays at zero.
        assertEquals("default limit must be the observation sentinel", -1L, trackingBreaker.getLimit());
        final var indexConfig = indexConfig();
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue)
        ) {
            long startGen = searchEngine.getCurrentPrimaryTermAndGeneration().generation();

            indexEngine.index(randomDoc("d1"));
            indexEngine.flush();
            int notifications = notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();

            assertThat("refresh must advance with the unlimited breaker", notifications, greaterThan(0));
            assertThat(searchEngine.getCurrentPrimaryTermAndGeneration().generation(), greaterThan(startGen));
            assertThat("no deferrals when the breaker is unlimited", searchEngine.getRefreshDeferredCount(), equalTo(0L));
        }

        assertThat(trackingBreaker.getUsed(), equalTo(0L));
        assertWarnings(
            "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testRefreshDeferredWhenBreakerLimitExceeded() throws IOException {
        // Setting a positive limit engages the breakable path. With 1 byte every refresh past engine open trips
        // the breaker — refresh should defer, the engine should not advance, and the reservation should roll back.
        trackingBreaker.setLimit(1L);

        final var indexConfig = indexConfig();
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue)
        ) {
            long startGen = searchEngine.getCurrentPrimaryTermAndGeneration().generation();
            long initialReserved = trackingBreaker.getUsed();
            // The initial reader was added via the no-break path; with deferral on, the next refresh's reserve
            // call will throw because the limit is 1 byte and we already hold more than that.
            indexEngine.index(randomDoc("d1"));
            indexEngine.flush();
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();

            assertThat("refresh should have been deferred", searchEngine.getRefreshDeferredCount(), greaterThanOrEqualTo(1L));
            assertThat(
                "search engine generation must not advance when refresh is deferred",
                searchEngine.getCurrentPrimaryTermAndGeneration().generation(),
                equalTo(startGen)
            );
            assertThat(
                "deferred reservation must be rolled back — reservation stays at the initial amount",
                trackingBreaker.getUsed(),
                equalTo(initialReserved)
            );
        }

        assertThat("reservation drains to zero after engine close even on the deferral path", trackingBreaker.getUsed(), equalTo(0L));
        assertWarnings(
            "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version."
        );
    }

}
