/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcherDynamicSettings;
import org.elasticsearch.xpack.stateless.commits.ClosedShardService;
import org.elasticsearch.xpack.stateless.reshard.ReshardSearchFilters;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Guards the reader-heap leak invariant on the "exception thrown after charge" paths. Each of the three engine
 * sites that takes a {@link SegmentReservations.Reservation} and charges the breaker before installing the
 * releasing close listener — engine open, refresh, and PIT relocation — must release the reservation and refund
 * the breaker if any call between charge and listener-install throws.
 *
 * <p>{@code trackLocalOpenReader} and {@code registerReaderHeapRelease} are the two real-world throw sources at
 * those sites (both call {@code ElasticsearchDirectoryReader.addReaderCloseListener}, which throws
 * {@code IllegalArgumentException} for an unsupported reader and {@code AlreadyClosedException} if the cache helper
 * is gone). Overriding either to throw exercises the leak-plug try/finally at every call site.
 */
public class SearchEngineHeapBudgetLeakInvariantTests extends AbstractEngineTestCase {

    @Override
    public String[] tmpPaths() {
        return new String[] { createTempDir().toAbsolutePath().toString() };
    }

    private final TrackingCircuitBreaker trackingBreaker = new TrackingCircuitBreaker(StatelessReaderHeapBreaker.NAME, -1L);

    // Latch flipped from individual tests to make the next attempted register / track call throw exactly once.
    private final AtomicBoolean failRegisterNextReaderHeapRelease = new AtomicBoolean(false);
    private final AtomicBoolean failTrackNextLocalOpenReader = new AtomicBoolean(false);

    @Override
    protected CircuitBreaker newReaderHeapBreaker() {
        return trackingBreaker;
    }

    @Override
    protected SearchEngine newSearchEngineSubclass(
        EngineConfig searchConfig,
        ClusterSettings clusterSettings,
        NodeEnvironment nodeEnvironment
    ) {
        ReshardSearchFilters reshardSearchFilters = new ReshardSearchFilters(Settings.EMPTY);
        return new SearchEngine(
            searchConfig,
            new ClosedShardService(),
            sharedBlobCacheService,
            clusterSettings,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new SearchCommitPrefetcherDynamicSettings(clusterSettings),
            newReaderHeapBreaker(),
            StatelessReaderHeapMetrics.NOOP,
            reshardSearchFilters
        ) {
            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    IOUtils.close(searchConfig.getStore()::decRef, nodeEnvironment, reshardSearchFilters);
                }
            }

            @Override
            void registerReaderHeapRelease(ElasticsearchDirectoryReader reader, SegmentReservations.Reservation reservation)
                throws IOException {
                if (failRegisterNextReaderHeapRelease.compareAndSet(true, false)) {
                    throw new IOException("simulated registerReaderHeapRelease failure");
                }
                super.registerReaderHeapRelease(reader, reservation);
            }

            @Override
            void trackLocalOpenReader(
                ElasticsearchDirectoryReader directoryReader,
                IndexCommit commit,
                SegmentReservations.Reservation reservation,
                Set<PrimaryTermAndGeneration> bccDependencies
            ) throws IOException {
                if (failTrackNextLocalOpenReader.compareAndSet(true, false)) {
                    throw new IOException("simulated trackLocalOpenReader failure");
                }
                super.trackLocalOpenReader(directoryReader, commit, reservation, bccDependencies);
            }
        };
    }

    public void testRefreshReleasesReservationWhenRegisterReaderHeapReleaseThrows() throws IOException {
        runRefreshLeakInvariant(failRegisterNextReaderHeapRelease);
    }

    public void testRefreshReleasesReservationWhenTrackLocalOpenReaderThrows() throws IOException {
        runRefreshLeakInvariant(failTrackNextLocalOpenReader);
    }

    /**
     * Drive a successful refresh that reserves bytes, then arm the failure latch and drive a second refresh that
     * trips the simulated IOException from track/register. The catch in {@code updateInternalState} routes the
     * throw to {@code failEngine}, which closes the engine and lets the first refresh's close listener release its
     * reservation. The second refresh — the one we are guarding — never installed a close listener, so without the
     * leak-plug try/finally in {@code refreshIfNeeded} / {@code addNextReader} its reservation is leaked and the
     * breaker would remain non-zero after engine close. The lifecycle invariant {@code getUsed() == 0} is the
     * post-fix signal; flipping the fix off makes this assertion fail.
     */
    private void runRefreshLeakInvariant(AtomicBoolean failLatch) throws IOException {
        final var indexConfig = indexConfig();
        final var searchTaskQueue = new DeterministicTaskQueue();

        try (
            var indexEngine = newIndexEngine(indexConfig);
            var searchEngine = newSearchEngineFromIndexEngine(indexEngine, searchTaskQueue)
        ) {
            // First refresh: succeeds. Establishes a non-trivial reservation to be released by engine close.
            indexEngine.index(randomDoc("d1"));
            indexEngine.flush();
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();
            assertThat("first refresh must have reserved bytes", trackingBreaker.getUsed(), greaterThan(0L));

            // Arm the failure latch and drive a second refresh against a new commit.
            failLatch.set(true);
            indexEngine.index(randomDoc("d2"));
            indexEngine.flush();
            notifyCommits(indexEngine, searchEngine);
            searchTaskQueue.runAllRunnableTasks();

            assertThat("failure latch must have fired exactly once", failLatch.get(), equalTo(false));
        }

        assertThat("reservation drains to zero after engine close even after a failed refresh", trackingBreaker.getUsed(), equalTo(0L));
        assertWarnings(
            "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version."
        );
    }
}
