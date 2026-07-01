/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.engine.Engine;

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

    /** Drain all currently runnable tasks, then advance the clock until any deferred task scheduled before
     *  {@code untilMillis} fires, draining runnable tasks at each step. */
    private static void advancePast(DeterministicTaskQueue searchTaskQueue, long untilMillis) {
        searchTaskQueue.runAllRunnableTasks();
        while (searchTaskQueue.hasDeferredTasks() && searchTaskQueue.getCurrentTimeMillis() <= untilMillis) {
            searchTaskQueue.advanceTime();
            searchTaskQueue.runAllRunnableTasks();
        }
    }

    public void testDeferredRefreshSchedulesRetry() throws IOException {
        // With the limit pinned at 1 byte, the initial defer schedules a retry. When the retry fires after
        // 2x refresh_interval the budget is still exhausted, so we expect another deferral and another scheduled
        // retry — confirming the retry path actually runs and reschedules itself rather than disappearing.
        trackingBreaker.setLimit(1L);

        final var indexConfig = indexConfig();
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue)
        ) {
            long startGen = searchEngine.getCurrentPrimaryTermAndGeneration().generation();

            indexEngine.index(randomDoc("d1"));
            indexEngine.flush();
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();

            assertThat("initial refresh must defer", searchEngine.getRefreshDeferredCount(), equalTo(1L));
            assertThat("a retry must be scheduled after defer", searchTaskQueue.hasDeferredTasks(), equalTo(true));

            long retryDelayMillis = searchEngine.config().getIndexSettings().getRefreshInterval().millis() * 2L;
            advancePast(searchTaskQueue, searchTaskQueue.getCurrentTimeMillis() + retryDelayMillis + 1L);

            assertThat(
                "retry must fire and defer again under sustained budget pressure",
                searchEngine.getRefreshDeferredCount(),
                greaterThanOrEqualTo(2L)
            );
            assertThat(
                "generation must not advance while the retry keeps deferring",
                searchEngine.getCurrentPrimaryTermAndGeneration().generation(),
                equalTo(startGen)
            );
            assertThat("retry must reschedule itself on a fresh defer", searchTaskQueue.hasDeferredTasks(), equalTo(true));
        }

        assertThat("reservation drains to zero after engine close even with a pending retry", trackingBreaker.getUsed(), equalTo(0L));
        assertWarnings(
            "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testDeferredRefreshSucceedsWhenBudgetReleased() throws IOException {
        // First refresh defers because the limit is 1 byte. We then lift the limit and let the retry fire —
        // the retry should advance the reader to the deferred generation without a new commit notification.
        trackingBreaker.setLimit(1L);

        final var indexConfig = indexConfig();
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue)
        ) {
            long startGen = searchEngine.getCurrentPrimaryTermAndGeneration().generation();

            indexEngine.index(randomDoc("d1"));
            indexEngine.flush();
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();

            assertThat("initial refresh must defer", searchEngine.getRefreshDeferredCount(), equalTo(1L));
            assertThat(
                "generation pinned at start before retry fires",
                searchEngine.getCurrentPrimaryTermAndGeneration().generation(),
                equalTo(startGen)
            );

            // Release pressure before the retry fires.
            trackingBreaker.setLimit(Long.MAX_VALUE);

            long retryDelayMillis = searchEngine.config().getIndexSettings().getRefreshInterval().millis() * 2L;
            advancePast(searchTaskQueue, searchTaskQueue.getCurrentTimeMillis() + retryDelayMillis + 1L);

            assertThat(
                "generation must advance once the retry runs against a relaxed limit",
                searchEngine.getCurrentPrimaryTermAndGeneration().generation(),
                greaterThan(startGen)
            );
            assertThat("no further defers after retry succeeds", searchEngine.getRefreshDeferredCount(), equalTo(1L));
            assertThat(
                "pending bytes counter clears once the retry actually opens the reader",
                searchEngine.getRefreshDeferredPendingBytes(),
                equalTo(0L)
            );
        }

        assertThat("reservation drains to zero after engine close", trackingBreaker.getUsed(), equalTo(0L));
        assertWarnings(
            "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testRetryIsIdempotentWhenNaturalNotificationWinsRace() throws IOException {
        // Defer commit N1 under a tight limit, then lift the limit and deliver a fresh commit N2 the natural way.
        // The natural refresh advances the reader to N2. When the still-scheduled retry fires later, it re-injects
        // N1 — findLatestNotification must skip it (gen <= current), leaving the reader pinned at N2.
        trackingBreaker.setLimit(1L);

        final var indexConfig = indexConfig();
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue)
        ) {
            long startGen = searchEngine.getCurrentPrimaryTermAndGeneration().generation();

            indexEngine.index(randomDoc("d1"));
            indexEngine.flush();
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();
            assertThat("N1 must defer", searchEngine.getRefreshDeferredCount(), equalTo(1L));

            // Relax the limit and deliver N2 through the natural notification path; it must win the race.
            trackingBreaker.setLimit(Long.MAX_VALUE);
            indexEngine.index(randomDoc("d2"));
            indexEngine.flush();
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();

            long postNaturalGen = searchEngine.getCurrentPrimaryTermAndGeneration().generation();
            assertThat("natural refresh must advance past start", postNaturalGen, greaterThan(startGen));
            long deferCountAfterNatural = searchEngine.getRefreshDeferredCount();

            // Fire the original retry. It should be a no-op: pendingDeferredNotification (N1) is <= current (N2).
            long retryDelayMillis = searchEngine.config().getIndexSettings().getRefreshInterval().millis() * 2L;
            advancePast(searchTaskQueue, searchTaskQueue.getCurrentTimeMillis() + retryDelayMillis + 1L);

            assertThat(
                "retry must not regress the reader nor re-open at N1",
                searchEngine.getCurrentPrimaryTermAndGeneration().generation(),
                equalTo(postNaturalGen)
            );
            assertThat(
                "retry against a stale notification must not count as a new defer",
                searchEngine.getRefreshDeferredCount(),
                equalTo(deferCountAfterNatural)
            );
        }

        assertThat("reservation drains to zero after engine close", trackingBreaker.getUsed(), equalTo(0L));
        assertWarnings(
            "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testCoalescedRetryPicksUpLatestDeferredNotification() throws IOException {
        // Andrei's scenario: N1 defers, then N2 also defers (limit stays tight, retry timer hasn't fired). The
        // coalesced retry slot must target the latest notification — when the limit is lifted, advancing past
        // the timer must land the reader at N2's generation, not N1's.
        trackingBreaker.setLimit(1L);

        final var indexConfig = indexConfig();
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue)
        ) {
            long startGen = searchEngine.getCurrentPrimaryTermAndGeneration().generation();

            // First defer (N1).
            indexEngine.index(randomDoc("d1"));
            indexEngine.flush();
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();
            assertThat("N1 must defer", searchEngine.getRefreshDeferredCount(), equalTo(1L));
            assertThat("retry timer must be scheduled after the first defer", searchTaskQueue.hasDeferredTasks(), equalTo(true));

            // Second defer (N2) — limit still tight, retry hasn't fired yet.
            indexEngine.index(randomDoc("d2"));
            indexEngine.flush();
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();
            assertThat("N2 must also defer", searchEngine.getRefreshDeferredCount(), equalTo(2L));
            assertThat("retry timer still pending after N2 (coalesced)", searchTaskQueue.hasDeferredTasks(), equalTo(true));

            // Lift the limit and let the coalesced retry fire. It must target N2 (the latest deferred
            // notification), not N1.
            trackingBreaker.setLimit(Long.MAX_VALUE);
            long expectedGen = indexEngine.getLastCommittedSegmentInfos().getGeneration();
            assertThat("indexer is at the N2 generation", expectedGen, greaterThan(startGen));

            long retryDelayMillis = searchEngine.config().getIndexSettings().getRefreshInterval().millis() * 2L;
            advancePast(searchTaskQueue, searchTaskQueue.getCurrentTimeMillis() + retryDelayMillis + 1L);

            assertThat(
                "coalesced retry must land the reader at N2, not N1",
                searchEngine.getCurrentPrimaryTermAndGeneration().generation(),
                equalTo(expectedGen)
            );
            assertThat("successful retry must not bump the deferred counter further", searchEngine.getRefreshDeferredCount(), equalTo(2L));
        }

        assertThat("reservation drains to zero after engine close", trackingBreaker.getUsed(), equalTo(0L));
        assertWarnings(
            "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testRetryLosesToNewerQueuedNotification() throws IOException {
        // Variant of testRetryIsIdempotentWhenNaturalNotificationWinsRace: instead of letting the natural N2 fully
        // process before the retry fires, drop the limit, enqueue N2, then advance the clock past the retry delay
        // BEFORE running tasks so the retry's re-injected N1 and the natural N2 race in the same drain. Since
        // findLatestNotification picks the max-generation entry in the polled batch, N2 must win regardless of
        // queue position; N1 is dropped implicitly.
        trackingBreaker.setLimit(1L);

        final var indexConfig = indexConfig();
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue)
        ) {
            long startGen = searchEngine.getCurrentPrimaryTermAndGeneration().generation();

            indexEngine.index(randomDoc("d1"));
            indexEngine.flush();
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();
            assertThat("N1 must defer", searchEngine.getRefreshDeferredCount(), equalTo(1L));
            assertThat("retry must be scheduled", searchTaskQueue.hasDeferredTasks(), equalTo(true));

            // Relax the limit, enqueue N2 the natural way, then advance time past the retry delay BEFORE running
            // tasks — both the retry timer and N2's processing become runnable in the same drain pass.
            trackingBreaker.setLimit(Long.MAX_VALUE);
            indexEngine.index(randomDoc("d2"));
            indexEngine.flush();
            notifyCommits(indexEngine, searchEngine);

            long retryDelayMillis = searchEngine.config().getIndexSettings().getRefreshInterval().millis() * 2L;
            long deadline = searchTaskQueue.getCurrentTimeMillis() + retryDelayMillis + 1L;
            while (searchTaskQueue.hasDeferredTasks() && searchTaskQueue.getCurrentTimeMillis() < deadline) {
                searchTaskQueue.advanceTime();
            }
            searchTaskQueue.runAllRunnableTasks();

            long expectedGen = indexEngine.getLastCommittedSegmentInfos().getGeneration();
            assertThat("reader must land on the natural N2 generation, not the older retry's N1", expectedGen, greaterThan(startGen));
            assertThat(
                "reader must end at the latest queued generation",
                searchEngine.getCurrentPrimaryTermAndGeneration().generation(),
                equalTo(expectedGen)
            );
        }

        assertThat("reservation drains to zero after engine close", trackingBreaker.getUsed(), equalTo(0L));
        assertWarnings(
            "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testImmediateRetryFiresOnReaderCloseAfterDefer() throws IOException {
        // Construct a scenario where a non-current reader pins reservation bytes that are NOT shared with the
        // current reader (forceMerge in between makes R1's pre-merge segments disjoint from R_post-merge), then
        // a subsequent refresh defers under tight budget. Releasing the pinning searcher closes R1; the close
        // listener releases real bytes and must kick the event-driven retry — observable through the
        // immediate-retry counter and the reader advancing without waiting for the timer.
        final var indexConfig = indexConfig();
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue)
        ) {
            // R1 — two segments. We pin this reader through a searcher so its reservation entries persist.
            indexEngine.index(randomDoc("d1"));
            indexEngine.flush();
            indexEngine.index(randomDoc("d2"));
            indexEngine.flush();
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();
            long r1Bytes = trackingBreaker.getUsed();
            assertThat("R1 must reserve bytes for its two segments", r1Bytes, greaterThan(0L));

            Engine.Searcher pinnedR1 = searchEngine.acquireSearcher("pin-r1");
            try {
                // Force-merge to a single segment so R_post-merge has DIFFERENT segments than R1. R1's
                // reservation entries are now unique to R1 — closing R1 frees real bytes.
                indexEngine.forceMerge(true, 1, false, UUIDs.randomBase64UUID());
                searchTaskQueue.runAllRunnableTasks();
                notifyCommits(indexEngine, searchEngine);
                searchTaskQueue.runAllRunnableTasks();
                long postMergeBytes = trackingBreaker.getUsed();
                assertThat("post-merge reader must hold its own reservation in addition to R1's", postMergeBytes, greaterThan(r1Bytes));

                // Tighten the limit so the next refresh's delta trips the breaker.
                trackingBreaker.setLimit(1L);
                long preDeferGen = searchEngine.getCurrentPrimaryTermAndGeneration().generation();
                indexEngine.index(randomDoc("d3"));
                indexEngine.flush();
                notifyCommits(indexEngine, searchEngine);
                searchTaskQueue.runAllRunnableTasks();

                assertThat("N3 must defer under the tight limit", searchEngine.getRefreshDeferredCount(), greaterThanOrEqualTo(1L));
                assertThat("no immediate retry has fired yet", searchEngine.getRefreshImmediateRetryCount(), equalTo(0L));
                assertThat(
                    "reader must still be pinned at the post-merge generation while N3 is deferred",
                    searchEngine.getCurrentPrimaryTermAndGeneration().generation(),
                    equalTo(preDeferGen)
                );

                // Lift the limit. Closing the searcher releases R1's now-unique reservation, which must fire the
                // event-driven retry. The deferred N3 should advance the reader without advancing the test clock.
                trackingBreaker.setLimit(Long.MAX_VALUE);
                pinnedR1.close();
                pinnedR1 = null;
                searchTaskQueue.runAllRunnableTasks();

                assertThat(
                    "close-listener-driven retry must have re-injected the deferred notification",
                    searchEngine.getRefreshImmediateRetryCount(),
                    greaterThanOrEqualTo(1L)
                );
                assertThat(
                    "reader must now have advanced past the deferred generation without waiting for the timer",
                    searchEngine.getCurrentPrimaryTermAndGeneration().generation(),
                    greaterThan(preDeferGen)
                );
            } finally {
                if (pinnedR1 != null) {
                    pinnedR1.close();
                }
            }
        }

        assertThat("reservation drains to zero after engine close", trackingBreaker.getUsed(), equalTo(0L));
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
