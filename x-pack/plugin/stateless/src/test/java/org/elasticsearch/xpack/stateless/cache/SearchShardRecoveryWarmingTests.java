/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.instrumentation.HttpServerInstrumentation;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.SearchRecoveryWaitOutcome;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.WarmTarget;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.logging.log4j.Level.INFO;
import static org.apache.logging.log4j.Level.WARN;
import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingTestUtils.clusterStateInitializingSearchReplicaWithActivePeer;
import static org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingTestUtils.clusterStateOneSearchReplica;
import static org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingTestUtils.initializingSearchReplica;
import static org.elasticsearch.xpack.stateless.cache.SearchRecoveryWarmingTestUtils.mockIndexShard;
import static org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.SEARCH_RECOVERY_LOG_FIELD_PREFIX;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SharedBlobCacheWarmingService#warmCacheForSearchShardRecovery} and
 * {@link SharedBlobCacheWarmingService#searchRecoveryWarmingListener}. The timeout calculation itself is covered by
 * {@link SearchRecoveryTimeoutCalculationServiceTests}.
 */
public class SearchShardRecoveryWarmingTests extends ESTestCase {

    private static Set<Setting<?>> warmingServiceSettings() {
        return Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(
                SharedBlobCacheWarmingService.PREWARMING_RANGE_MINIMIZATION_STEP,
                SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_PREFETCH_COMMITS_ENABLED_SETTING,
                SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RESHARD_TARGET_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING,
                SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_WARMING_TOTAL_TIMEOUT_CAP_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_CACHE_RATIO_SETTING,
                SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING,
                SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_WARMING_TIMEOUT_REEVALUATION_ABORT_THRESHOLD_SETTING,
                SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_WARMING_TIMEOUT_REEVALUATION_MIN_BUDGET_PER_PENDING_SHARD_SETTING,
                DefaultWarmingRatioProviderFactory.SEARCH_RECOVERY_WARMING_RATIO_SETTING,
                SharedBlobCacheWarmingService.UPLOAD_PREWARM_MAX_SIZE_SETTING,
                SharedBlobCacheWarmingService.WARM_BYTE_RANGE_THROTTLE_RATIO_SETTING,
                SharedBlobCacheWarmingService.WARM_BYTE_RANGE_PER_FILE_CONCURRENCY_SETTING,
                SharedBlobCacheWarmingService.PREWARM_INDEX_SHARD_FOR_ID_LOOKUPS_SETTING,
                SharedBlobCacheWarmingService.ID_LOOKUP_PREWARM_RATIO_SETTING
            )
        ).collect(Collectors.toSet());
    }

    private static ClusterSettings newClusterSettings(Settings settings) {
        return new ClusterSettings(settings, warmingServiceSettings());
    }

    private static SharedBlobCacheWarmingService newWarmingService(ThreadPool threadPool) {
        return newWarmingService(threadPool, TelemetryProvider.NOOP);
    }

    private static SharedBlobCacheWarmingService newWarmingService(ThreadPool threadPool, TelemetryProvider telemetryProvider) {
        return newWarmingService(threadPool, telemetryProvider, null, null);
    }

    private static TelemetryProvider telemetryProvider(MeterRegistry meterRegistry) {
        return new TelemetryProvider() {
            @Override
            public Tracer getTracer() {
                return Tracer.NOOP;
            }

            @Override
            public MeterRegistry getMeterRegistry() {
                return meterRegistry;
            }

            @Override
            public HttpServerInstrumentation getHttpServerInstrumentation() {
                return HttpServerInstrumentation.NOOP;
            }

            @Override
            public void attemptFlush() {}
        };
    }

    /**
     * {@link SharedBlobCacheWarmingService} with {@link SharedBlobCacheWarmingService#warmCache} stubbed to sleep for
     * {@code delayMillis} before completing the listener, so that recorded warming-duration metrics are observably non-zero.
     */
    private static SharedBlobCacheWarmingService newWarmingService(
        ThreadPool threadPool,
        TelemetryProvider telemetryProvider,
        @Nullable CountDownLatch startWarmLatch,
        @Nullable CountDownLatch blockWarmLatch
    ) {
        return newWarmingService(threadPool, telemetryProvider, Settings.EMPTY, startWarmLatch, blockWarmLatch);
    }

    private static SharedBlobCacheWarmingService newWarmingService(
        ThreadPool threadPool,
        TelemetryProvider telemetryProvider,
        Settings settings,
        @Nullable CountDownLatch startWarmLatch,
        @Nullable CountDownLatch blockWarmLatch
    ) {
        ClusterSettings clusterSettings = newClusterSettings(settings);
        return new SharedBlobCacheWarmingService(
            Mockito.mock(StatelessSharedBlobCacheService.class),
            threadPool,
            telemetryProvider,
            clusterSettings,
            new DefaultWarmingRatioProviderFactory().create(clusterSettings)
        ) {
            @Override
            protected void warmCache(
                SharedBlobCacheWarmingService.Type type,
                IndexShard indexShard,
                StatelessCompoundCommit commit,
                BlobStoreCacheDirectory directory,
                @Nullable Map<BlobFile, WarmTarget> endTargetsToWarm,
                boolean preWarmForIdLookup,
                ActionListener<Void> listener
            ) {
                threadPool.generic().submit(() -> {
                    try {
                        if (startWarmLatch != null) {
                            startWarmLatch.countDown();
                        }
                        if (blockWarmLatch != null) {
                            blockWarmLatch.await();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    listener.onResponse(null);
                });
            }
        };
    }

    /**
     * A stand-in for the directory being warmed. {@link SharedBlobCacheWarmingService#searchRecoveryWarmingListener} only reads two
     * counters off it, but the real {@link BlobStoreCacheDirectory} implementations need a live shared blob cache, an object store and a
     * commit installed via {@code updateCommit} before those counters mean anything, none of which these tests set up. The warmed-bytes
     * counter is stubbed with two consecutive values: the baseline read when the listener is built, then the value read when the timeout
     * fires.
     */
    private static BlobStoreCacheDirectory mockDirectory(long dataSetSizeInBytes, long bytesWarmedAtStart, long bytesWarmedAtTimeout) {
        BlobStoreCacheDirectory directory = mock(BlobStoreCacheDirectory.class);
        when(directory.estimateDataSetSizeInBytes()).thenReturn(dataSetSizeInBytes);
        when(directory.totalBytesWarmedFromObjectStore()).thenReturn(bytesWarmedAtStart, bytesWarmedAtTimeout);
        return directory;
    }

    /** A {@link #mockDirectory} with zeroed counters, for tests that do not assert on the timeout log message. */
    private static BlobStoreCacheDirectory mockDirectory() {
        return mockDirectory(0L, 0L, 0L);
    }

    public void testWarmCacheForSearchShardRecoveryNullEndOffsetsUsesResumesRecoveryBeforeWarmingCompletes() throws Exception {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        long warmDurationMillis = randomLongBetween(50, 100);
        Settings threadPoolSettings = Settings.builder().put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0).build();
        try (
            var threadPool = new TestThreadPool(
                getTestName(),
                threadPoolSettings,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            PlainActionFuture<Void> resume = new PlainActionFuture<>();
            CountDownLatch startWarmLatch = new CountDownLatch(1);
            CountDownLatch blockWarmLatch = new CountDownLatch(1);
            var service = newWarmingService(threadPool, telemetryProvider(meterRegistry), startWarmLatch, blockWarmLatch);
            ClusterState state = clusterStateOneSearchReplica("idx", STARTED);
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shardId).replicaShards().get(0);
            service.warmCacheForSearchShardRecovery(() -> state, mockIndexShard(self), null, null, null, resume);
            // recovery is resumed
            assertTrue(resume.isDone());
            // make sure warming started running
            safeAwait(startWarmLatch);
            // warming still runs
            assertBusy(() -> assertThat(((EsThreadPoolExecutor) threadPool.generic()).getActiveCount(), equalTo(1)));
            Thread.sleep(warmDurationMillis);
            // warming is unblocked
            blockWarmLatch.countDown();
            safeGet(resume);
        }
        {
            List<Measurement> measurements = meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SharedBlobCacheWarmingService.SEARCH_RECOVERY_WAIT_DURATION_METRIC);
            assertThat(measurements, hasSize(1));
            Measurement measurement = measurements.get(0);
            assertThat(measurement.getDouble(), equalTo(0.0D));
            assertWaitOutcome(measurement, SearchRecoveryWaitOutcome.NO_WAIT);
        }
        assertSingleDurationMeasurementAtLeast(
            meterRegistry,
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARM_DURATION_METRIC,
            warmDurationMillis
        );
    }

    /**
     * Same routing layout as
     * {@link SearchRecoveryTimeoutCalculationServiceTests#testSearchRecoveryNonRelocationWaitsWhenAnotherActiveCopy}:
     * {@link ShardRoutingState#INITIALIZING} self search replica with a started search peer; warming uses the race listener
     * when {@code endOffsetsToWarm} is set.
     */
    public void testWarmCacheForSearchShardRecoveryWithReplica() throws Exception {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        long warmDurationMillis = randomLongBetween(50, 100);
        Settings threadPoolSettings = Settings.builder().put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0).build();
        try (
            var threadPool = new TestThreadPool(
                getTestName(),
                threadPoolSettings,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            CountDownLatch startWarmLatch = new CountDownLatch(1);
            CountDownLatch blockWarmLatch = new CountDownLatch(1);
            PlainActionFuture<Void> resume = new PlainActionFuture<>() {
                @Override
                public void onResponse(Void result) {
                    ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
                    super.onResponse(result);
                }
            };
            var service = newWarmingService(threadPool, telemetryProvider(meterRegistry), startWarmLatch, blockWarmLatch);
            ClusterState state = clusterStateInitializingSearchReplicaWithActivePeer("idx");
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = initializingSearchReplica(state, shardId);
            service.warmCacheForSearchShardRecovery(
                () -> state,
                mockIndexShard(self),
                null,
                mockDirectory(),
                Map.of(new BlobFile("test-blob", new PrimaryTermAndGeneration(0, -1)), WarmTarget.withUnknownTimestamp(1L, 1L)),
                resume
            );
            // recovery is NOT resumed
            assertFalse(resume.isDone());
            // make sure warming started running
            safeAwait(startWarmLatch);
            // warming still runs
            assertBusy(() -> assertThat(((EsThreadPoolExecutor) threadPool.generic()).getActiveCount(), equalTo(1)));
            Thread.sleep(warmDurationMillis);
            // warming is unblocked
            blockWarmLatch.countDown();
            safeGet(resume);
        }
        Measurement wait = assertSingleDurationMeasurementAtLeast(
            meterRegistry,
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WAIT_DURATION_METRIC,
            warmDurationMillis
        );
        assertWaitOutcome(wait, SearchRecoveryWaitOutcome.WARMING_COMPLETE);
        assertSingleDurationMeasurementAtLeast(
            meterRegistry,
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARM_DURATION_METRIC,
            warmDurationMillis
        );
    }

    /**
     * Same shard layout as {@link SearchRecoveryTimeoutCalculationServiceTests#testSearchRecoverySkipsWhenOnlyPrimaryActive}:
     * {@link SearchRecoveryTimeoutCalculationService#searchRecoveryTimeout} skips, so {@code warmCacheForSearchShardRecovery} resumes
     * recovery synchronously (fire-and-forget warming) even when {@code endOffsetsToWarm} is set.
     */
    public void testWarmCacheForSearchShardRecoveryNoOtherActive() throws Exception {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        long warmDurationMillis = randomLongBetween(50, 100);
        Settings threadPoolSettings = Settings.builder().put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0).build();
        try (
            var threadPool = new TestThreadPool(
                getTestName(),
                threadPoolSettings,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            CountDownLatch startWarmLatch = new CountDownLatch(1);
            CountDownLatch blockWarmLatch = new CountDownLatch(1);
            PlainActionFuture<Void> resume = new PlainActionFuture<>();
            var service = newWarmingService(threadPool, telemetryProvider(meterRegistry), startWarmLatch, blockWarmLatch);
            ClusterState state = clusterStateOneSearchReplica("idx", INITIALIZING);
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = state.routingTable(DEFAULT_PROJECT_ID).shardRoutingTable(shardId).replicaShards().get(0);
            service.warmCacheForSearchShardRecovery(
                () -> state,
                mockIndexShard(self),
                null,
                null,
                Map.of(new BlobFile("test-blob", new PrimaryTermAndGeneration(0, -1)), WarmTarget.withUnknownTimestamp(1L, 1L)),
                resume
            );
            // recovery resumed (synchronously)
            assertTrue(resume.isDone());
            // make sure warming started running
            safeAwait(startWarmLatch);
            // warming still runs
            assertBusy(() -> assertThat(((EsThreadPoolExecutor) threadPool.generic()).getActiveCount(), equalTo(1)));
            Thread.sleep(warmDurationMillis);
            // warming is unblocked
            blockWarmLatch.countDown();
            safeGet(resume);
        }
        {
            List<Measurement> measurements = meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SharedBlobCacheWarmingService.SEARCH_RECOVERY_WAIT_DURATION_METRIC);
            assertThat(measurements, hasSize(1));
            Measurement measurement = measurements.get(0);
            assertThat(measurement.getDouble(), equalTo(0.0D));
            assertWaitOutcome(measurement, SearchRecoveryWaitOutcome.NO_WAIT);
        }
        assertSingleDurationMeasurementAtLeast(
            meterRegistry,
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARM_DURATION_METRIC,
            warmDurationMillis
        );
    }

    /**
     * Configures {@link SharedBlobCacheWarmingService#SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING} (the timeout that applies
     * to this test's non-relocation, another-active-copy routing) so short that it always fires before the listener passed in
     * (simulating warming) is ever completed: the wait outcome must be {@code TIMEOUT}.
     */
    public void testSearchRecoveryWarmingListenerRecordsTimedOutOutcome() throws Exception {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        long waitMillis = randomLongBetween(1, 10);
        long delayMillis = randomLongBetween(20, 100);
        Settings threadPoolSettings = Settings.builder().put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0).build();
        try (
            var threadPool = new TestThreadPool(
                getTestName(),
                threadPoolSettings,
                StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true)
            )
        ) {
            CountDownLatch startWarmLatch = new CountDownLatch(1);
            CountDownLatch blockWarmLatch = new CountDownLatch(1);
            Settings settings = Settings.builder()
                .put(
                    SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING.getKey(),
                    TimeValue.timeValueMillis(waitMillis)
                )
                .build();
            var service = newWarmingService(threadPool, telemetryProvider(meterRegistry), settings, startWarmLatch, blockWarmLatch);
            ClusterState state = clusterStateInitializingSearchReplicaWithActivePeer("idx");
            ShardId shardId = new ShardId("idx", IndexMetadata.INDEX_UUID_NA_VALUE, 0);
            ShardRouting self = initializingSearchReplica(state, shardId);
            PlainActionFuture<Void> resume = new PlainActionFuture<>();
            service.warmCacheForSearchShardRecovery(
                () -> state,
                mockIndexShard(self),
                null,
                mockDirectory(),
                Map.of(new BlobFile("test-blob", new PrimaryTermAndGeneration(0, -1)), new WarmTarget(1L, 1L, 1L)),
                resume
            );
            // Note: must not assert that `resume` is still incomplete here; the timeout is 1-10ms and races with an assertion.

            // make sure warming started running
            safeAwait(startWarmLatch);
            // warming still runs
            assertBusy(() -> assertThat(((EsThreadPoolExecutor) threadPool.generic()).getActiveCount(), equalTo(1)));
            Thread.sleep(delayMillis);
            // warming is unblocked
            blockWarmLatch.countDown();
            safeGet(resume);
        }
        assertSingleDurationMeasurementAtLeast(
            meterRegistry,
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARM_DURATION_METRIC,
            delayMillis
        );
        Measurement wait = assertSingleDurationMeasurementAtLeast(
            meterRegistry,
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WAIT_DURATION_METRIC,
            waitMillis
        );
        assertWaitOutcome(wait, SearchRecoveryWaitOutcome.TIMEOUT);
    }

    /// [TestThreadPool] that captures the single command handed to [ThreadPool#schedule] instead of scheduling it, so tests decide
    /// deterministically whether and when the "timeout" fires. Its cancellable always reports a successful cancellation, mimicking
    /// the real-life window in which a scheduled task's command has already been dispatched to its target executor but the JDK
    /// future is not yet marked done, so a concurrent cancel() still "wins" (see https://github.com/elastic/elasticsearch/issues/154033).
    ///
    /// Time is simulated: [#relativeTimeInMillis()] only advances by the delay of a captured command when the test runs it, so the
    /// elapsed time seen by the code under test is deterministic.
    private static class CapturingScheduleThreadPool extends TestThreadPool {
        final AtomicReference<Runnable> scheduledCommand = new AtomicReference<>();
        private final AtomicLong currentTimeMillis = new AtomicLong();

        CapturingScheduleThreadPool(String name) {
            super(name, StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true));
        }

        @Override
        public long relativeTimeInMillis() {
            return currentTimeMillis.get();
        }

        @Override
        public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
            assertTrue("expected a single scheduled task", scheduledCommand.compareAndSet(null, () -> {
                currentTimeMillis.addAndGet(delay.millis());
                command.run();
            }));
            return new ScheduledCancellable() {
                @Override
                public long getDelay(TimeUnit unit) {
                    throw new AssertionError("not used");
                }

                @Override
                public int compareTo(Delayed o) {
                    throw new AssertionError("not used");
                }

                @Override
                public boolean cancel() {
                    return true;
                }

                @Override
                public boolean isCancelled() {
                    return true;
                }
            };
        }
    }

    /// [TestThreadPool] for re-evaluation loop tests. Unlike [CapturingScheduleThreadPool], each call to
    /// [#schedule] replaces the previous captured task, so tests can step through every cycle of the loop by
    /// popping and running one task at a time.
    ///
    /// A `null` result from [#drainTask()] means no task was scheduled, i.e. the timeout fired rather than rescheduling.
    private static class ReEvaluationThreadPool extends TestThreadPool {
        private final AtomicReference<Runnable> pendingTask = new AtomicReference<>();
        private final AtomicLong currentTimeMillis = new AtomicLong();

        ReEvaluationThreadPool(String name) {
            super(name, StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true));
        }

        @Override
        public long relativeTimeInMillis() {
            return currentTimeMillis.get();
        }

        @Override
        public ScheduledCancellable schedule(Runnable task, TimeValue delay, Executor executor) {
            pendingTask.set(() -> {
                currentTimeMillis.addAndGet(delay.millis());
                task.run();
            });
            return new ScheduledCancellable() {
                @Override
                public long getDelay(TimeUnit unit) {
                    throw new AssertionError("not used");
                }

                @Override
                public int compareTo(Delayed o) {
                    throw new AssertionError("not used");
                }

                @Override
                public boolean cancel() {
                    return true;
                }

                @Override
                public boolean isCancelled() {
                    return true;
                }
            };
        }

        Runnable drainTask() {
            return pendingTask.getAndSet(null);
        }
    }

    private static IndexShard randomMockIndexShard() {
        return mockIndexShard(
            TestShardRouting.newShardRouting(
                new ShardId(randomIdentifier(), IndexMetadata.INDEX_UUID_NA_VALUE, 0),
                randomIdentifier(),
                false,
                STARTED,
                ShardRouting.Role.SEARCH_ONLY
            )
        );
    }

    /// Deterministic regression test for https://github.com/elastic/elasticsearch/issues/154033: the timeout command fires first
    /// and decides the race, yet the subsequent best-effort cancel() of the scheduled task reports success (which can genuinely
    /// happen, see [CapturingScheduleThreadPool]). The recorded outcome must be `TIMEOUT` regardless of what cancel() reports,
    /// and the warming listener completing afterward must not record a second measurement.
    public void testSearchRecoveryWarmingListenerRecordsTimeoutOutcomeEvenWhenCancelReportsSuccess() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        try (var threadPool = new CapturingScheduleThreadPool(getTestName())) {
            var service = newWarmingService(threadPool, telemetryProvider(meterRegistry));
            PlainActionFuture<Void> resume = new PlainActionFuture<>();
            var warmingListener = service.searchRecoveryWarmingListener(
                SearchRecoveryTimeout.fixed(TimeValue.timeValueMillis(randomLongBetween(1, 100_000)), randomAlphaOfLength(10)),
                () -> null, // unused in this test case: re-evaluation is disabled, so the cluster state is never read
                randomMockIndexShard(),
                mockDirectory(),
                randomNonNegativeLong(),
                resume
            );
            // deterministic here: the timeout cannot fire on its own, the test holds the captured command
            assertFalse(resume.isDone());
            Runnable timeoutCommand = threadPool.scheduledCommand.get();
            assertNotNull(timeoutCommand);
            timeoutCommand.run();
            safeGet(resume);
            // warming completes after losing the race: a discarded no-op
            warmingListener.onResponse(null);
        }
        List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SharedBlobCacheWarmingService.SEARCH_RECOVERY_WAIT_DURATION_METRIC);
        assertThat(measurements, hasSize(1));
        assertWaitOutcome(measurements.get(0), SearchRecoveryWaitOutcome.TIMEOUT);
    }

    /// Deterministic counterpart of [#testSearchRecoveryWarmingListenerRecordsTimeoutOutcomeEvenWhenCancelReportsSuccess]: warming
    /// completes before the timeout fires, so the outcome must be `WARMING_COMPLETE`, and the timeout command firing late must not
    /// record a second measurement.
    public void testSearchRecoveryWarmingListenerRecordsWarmingCompleteOutcomeWhenWarmingWins() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        try (var threadPool = new CapturingScheduleThreadPool(getTestName())) {
            var service = newWarmingService(threadPool, telemetryProvider(meterRegistry));
            PlainActionFuture<Void> resume = new PlainActionFuture<>();
            var warmingListener = service.searchRecoveryWarmingListener(
                SearchRecoveryTimeout.fixed(TimeValue.timeValueMillis(randomLongBetween(1, 100_000)), randomAlphaOfLength(10)),
                () -> null, // unused in this test case: re-evaluation is disabled, so the cluster state is never read
                randomMockIndexShard(),
                mockDirectory(),
                randomNonNegativeLong(),
                resume
            );
            warmingListener.onResponse(null);
            safeGet(resume);
            // the timeout fires after losing the race: a discarded no-op
            threadPool.scheduledCommand.get().run();
        }
        List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SharedBlobCacheWarmingService.SEARCH_RECOVERY_WAIT_DURATION_METRIC);
        assertThat(measurements, hasSize(1));
        assertWaitOutcome(measurements.get(0), SearchRecoveryWaitOutcome.WARMING_COMPLETE);
    }

    /// A warming failure that beats the timeout must propagate to the resume listener and still record the wait metric (attributed
    /// to `WARMING_COMPLETE`, since the warming side won the race), preserving the behavior that predates the race fix.
    public void testSearchRecoveryWarmingListenerWarmingFailurePropagatesAndRecordsMetric() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        try (var threadPool = new CapturingScheduleThreadPool(getTestName())) {
            var service = newWarmingService(threadPool, telemetryProvider(meterRegistry));
            var failure = new ElasticsearchException(randomAlphaOfLength(10));
            Exception thrown = safeAwaitFailure(
                Void.class,
                resumeListener -> service.searchRecoveryWarmingListener(
                    SearchRecoveryTimeout.fixed(TimeValue.timeValueMillis(randomLongBetween(1, 100_000)), randomAlphaOfLength(10)),
                    () -> null, // unused in this test case: re-evaluation is disabled, so the cluster state is never read
                    randomMockIndexShard(),
                    mockDirectory(),
                    randomNonNegativeLong(),
                    resumeListener
                ).onFailure(failure)
            );
            assertSame(failure, thrown);
        }
        List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, SharedBlobCacheWarmingService.SEARCH_RECOVERY_WAIT_DURATION_METRIC);
        assertThat(measurements, hasSize(1));
        assertWaitOutcome(measurements.get(0), SearchRecoveryWaitOutcome.WARMING_COMPLETE);
    }

    /// [SharedBlobCacheWarmingService] that delegates every
    /// [SharedBlobCacheWarmingService#searchRecoveryTimeout] call to a supplied function, so re-evaluation loop
    /// tests can inject any sequence of plans without building cluster state. `warmCache` is a no-op; tests drive
    /// the listener directly via [SharedBlobCacheWarmingService#searchRecoveryWarmingListener].
    private static SharedBlobCacheWarmingService newReevaluatingService(
        ThreadPool threadPool,
        Settings settings,
        Supplier<SearchRecoveryTimeout> planSupplier
    ) {
        return newReevaluatingService(threadPool, settings, bytesToWarm -> planSupplier.get());
    }

    /// Like the overload taking a plan supplier, but the plan is computed from the `totalBytesToWarm` that the re-evaluation passes in.
    private static SharedBlobCacheWarmingService newReevaluatingService(
        ThreadPool threadPool,
        Settings settings,
        LongFunction<SearchRecoveryTimeout> planForBytesToWarm
    ) {
        final var clusterSettings = newClusterSettings(settings);
        return new SharedBlobCacheWarmingService(
            Mockito.mock(StatelessSharedBlobCacheService.class),
            threadPool,
            TelemetryProvider.NOOP,
            clusterSettings,
            new DefaultWarmingRatioProviderFactory().create(clusterSettings)
        ) {
            @Override
            protected SearchRecoveryTimeout searchRecoveryTimeout(
                ClusterState state,
                IndexShard indexShard,
                long totalBytesToWarm,
                boolean reevaluation
            ) {
                return planForBytesToWarm.apply(totalBytesToWarm);
            }

            @Override
            protected void warmCache(
                Type type,
                IndexShard indexShard,
                StatelessCompoundCommit commit,
                BlobStoreCacheDirectory directory,
                @Nullable Map<BlobFile, WarmTarget> endTargetsToWarm,
                boolean preWarmForIdLookup,
                ActionListener<Void> listener
            ) {}
        };
    }

    /// With re-evaluation enabled, the loop reschedules as long as the remaining grace budget exceeds the abort threshold.
    /// Once the budget is exhausted, it timeouts instead of rescheduling.
    public void testReevaluationLoopReschedulesWithinBudgetAndTerminatesWhenExhausted() {
        final var budget = TimeValue.timeValueMillis(500);
        final var sliceSize = TimeValue.timeValueMillis(200);
        final var abortThreshold = TimeValue.timeValueMillis(50);
        final var settings = Settings.builder()
            .put(SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING.getKey(), true)
            .put(
                SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_WARMING_TIMEOUT_REEVALUATION_ABORT_THRESHOLD_SETTING.getKey(),
                abortThreshold
            )
            .build();

        try (var threadPool = new ReEvaluationThreadPool(getTestName())) {
            final SharedBlobCacheWarmingService service = newReevaluatingService(
                threadPool,
                settings,
                () -> SearchRecoveryTimeout.fixed(sliceSize, "reeval-ctx")
            );
            final var resume = new PlainActionFuture<Void>();
            service.searchRecoveryWarmingListener(
                SearchRecoveryTimeout.extendable(sliceSize, "initial-ctx", budget),
                () -> null, // unused in this test case
                randomMockIndexShard(),
                mockDirectory(),
                0L,
                resume
            );

            final var task1 = threadPool.drainTask();
            assertThat("initial schedule must exist", task1, notNullValue());
            assertThat(resume.isDone(), is(false));

            task1.run(); // eval 1: remaining=300 → reschedule
            assertThat("race must not fire after eval 1", resume.isDone(), is(false));
            final var task2 = threadPool.drainTask();
            assertThat("eval 1 must reschedule", task2, notNullValue());

            task2.run(); // eval 2: remaining=100 → reschedule
            assertThat("race must not fire after eval 2", resume.isDone(), is(false));
            final var task3 = threadPool.drainTask();
            assertThat("eval 2 must reschedule", task3, notNullValue());

            task3.run(); // eval 3: remaining=0 → time out
            safeGet(resume);
            assertThat("no reschedule after budget exhausted", threadPool.drainTask(), nullValue());
        }
    }

    /// The cluster-state supplier passed to [SharedBlobCacheWarmingService#searchRecoveryWarmingListener] is called
    /// lazily on each re-evaluation, not captured once at the start. This is the mechanism that allows a recovery waiting
    /// for warming to react to a node shutdown that was registered after the wait began.
    ///
    /// The test verifies by switching the plan between re-evaluations: the INFO log on the first re-evaluation must
    /// reflect the new plan's context, not the one that was current when the listener was built.
    public void testReevaluationLoopPicksUpUpdatedPlanOnEachExpiry() {
        final var budget = TimeValue.timeValueMillis(1_000);
        final var sliceSize = TimeValue.timeValueMillis(300);
        final var abortThreshold = TimeValue.timeValueMillis(50);
        final var settings = Settings.builder()
            .put(SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING.getKey(), true)
            .put(
                SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_WARMING_TIMEOUT_REEVALUATION_ABORT_THRESHOLD_SETTING.getKey(),
                abortThreshold
            )
            .build();

        final var planRef = new AtomicReference<>(SearchRecoveryTimeout.fixed(sliceSize, "context-before-switch"));

        try (var threadPool = new ReEvaluationThreadPool(getTestName())) {
            final SharedBlobCacheWarmingService service = newReevaluatingService(threadPool, settings, planRef::get);
            final var resume = new PlainActionFuture<Void>();
            service.searchRecoveryWarmingListener(
                SearchRecoveryTimeout.extendable(sliceSize, "initial", budget),
                () -> null, // unused in this test case
                randomMockIndexShard(),
                mockDirectory(),
                0L,
                resume
            );

            final var task = threadPool.drainTask();
            assertThat(task, notNullValue());

            // switch the plan before the first re-evaluation fires — the supplier must be read lazily
            planRef.set(SearchRecoveryTimeout.fixed(sliceSize, "context-after-switch"));

            assertThatLogger(
                task,
                SharedBlobCacheWarmingService.class,
                new MockLog.SeenEventExpectation(
                    "INFO log must reflect the updated context, not the one current at listener-build time",
                    SharedBlobCacheWarmingService.class.getCanonicalName(),
                    INFO,
                    "*timeout extended*context-after-switch*"
                )
            );
        }
    }

    /// Each re-evaluation is given the bytes still to warm, i.e. the bytes to warm at the start minus what has been warmed from the
    /// object store since then, never below zero. The stand-in plan mimics the data-volume heuristic: while more than half of the bytes
    /// are still to be warmed it returns a zero timeout, which ends the wait; once warming has progressed it returns an extension.
    public void testReevaluationLoopUsesBytesStillToWarm() {
        final long bytesToWarm = 1_000L;
        final var budget = TimeValue.timeValueMillis(1_000);
        final var sliceSize = TimeValue.timeValueMillis(200);
        final var settings = Settings.builder()
            .put(SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING.getKey(), true)
            .put(
                SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_WARMING_TIMEOUT_REEVALUATION_ABORT_THRESHOLD_SETTING.getKey(),
                TimeValue.timeValueMillis(50)
            )
            .build();

        // (a) nothing warmed yet: still all bytes to warm, the plan ends the wait
        // (b) some bytes warmed: the plan extends, and a later re-evaluation sees fewer bytes remaining
        // (c) more bytes warmed than targeted (the directory counter also covers other reads): remaining is clamped to zero
        final var warmedFromObjectStore = new AtomicLong();
        final var bytesRemainingSeen = new ArrayList<Long>();
        final LongFunction<SearchRecoveryTimeout> plan = bytesRemaining -> {
            bytesRemainingSeen.add(bytesRemaining);
            return bytesRemaining > bytesToWarm / 2
                ? SearchRecoveryTimeout.fixed(TimeValue.ZERO, "data-volume-like")
                : SearchRecoveryTimeout.fixed(sliceSize, "extension");
        };

        try (var threadPool = new ReEvaluationThreadPool(getTestName())) {
            final var service = newReevaluatingService(threadPool, settings, plan);
            final var directory = mock(BlobStoreCacheDirectory.class);
            when(directory.totalBytesWarmedFromObjectStore()).thenAnswer(invocation -> warmedFromObjectStore.get());

            // a
            final var timedOut = new PlainActionFuture<Void>();
            service.searchRecoveryWarmingListener(
                SearchRecoveryTimeout.extendable(sliceSize, "initial", budget),
                () -> null, // unused in this test case
                randomMockIndexShard(),
                directory,
                bytesToWarm,
                timedOut
            );
            threadPool.drainTask().run();
            safeGet(timedOut);
            assertThat(bytesRemainingSeen, contains(bytesToWarm));
            assertThat("a zero timeout must end the wait", threadPool.drainTask(), nullValue());

            // b, c: bytes warmed before the listener is built do not count towards it
            bytesRemainingSeen.clear();
            warmedFromObjectStore.set(10_000L);
            final var resume = new PlainActionFuture<Void>();
            service.searchRecoveryWarmingListener(
                SearchRecoveryTimeout.extendable(sliceSize, "initial", budget),
                () -> null, // unused in this test case
                randomMockIndexShard(),
                directory,
                bytesToWarm,
                resume
            );
            final var task1 = threadPool.drainTask();
            warmedFromObjectStore.addAndGet(600L);
            task1.run(); // 400 bytes remaining → extended
            assertThat(resume.isDone(), is(false));
            final var task2 = threadPool.drainTask();
            assertThat("the first re-evaluation must have rescheduled", task2, notNullValue());
            warmedFromObjectStore.addAndGet(5_000L);
            task2.run(); // overcounted → clamped to 0 remaining → extended due to equal share
            assertThat(bytesRemainingSeen, contains(400L, 0L));
            assertThat(resume.isDone(), is(false));
            assertThat("the second re-evaluation must have rescheduled", threadPool.drainTask(), notNullValue());
        }
    }

    /// When warming finishes while a re-evaluation is still pending, recovery completes immediately with
    /// [SearchRecoveryWaitOutcome#WARMING_COMPLETE].
    public void testReevaluationWarmingFinishesWhileReEvaluationLoopIsPending() {
        final var budget = TimeValue.timeValueMillis(1_000);
        final var sliceSize = TimeValue.timeValueMillis(200);
        final var abortThreshold = TimeValue.timeValueMillis(50);
        final var settings = Settings.builder()
            .put(SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING.getKey(), true)
            .put(
                SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_WARMING_TIMEOUT_REEVALUATION_ABORT_THRESHOLD_SETTING.getKey(),
                abortThreshold
            )
            .build();

        try (var threadPool = new ReEvaluationThreadPool(getTestName())) {
            final var service = newReevaluatingService(threadPool, settings, () -> SearchRecoveryTimeout.fixed(sliceSize, "reeval-ctx"));
            final var resume = new PlainActionFuture<Void>();
            final var warmingListener = service.searchRecoveryWarmingListener(
                SearchRecoveryTimeout.extendable(sliceSize, "initial", budget),
                () -> null, // unused in this test case
                randomMockIndexShard(),
                mockDirectory(),
                0L,
                resume
            );

            final var task = threadPool.drainTask();
            assertThat(task, notNullValue());
            task.run(); // re-eval 1: reschedule
            assertThat("re-evaluation must have rescheduled", threadPool.drainTask(), notNullValue());
            assertThat(resume.isDone(), is(false));

            // warming finishes first while a re-evaluation command is still pending
            warmingListener.onResponse(null);
            safeGet(resume);
        }
    }

    /// The timeout branch reports how much of the shard was warmed before the deadline: the shard's data set size, the bytes offline
    /// warming was targeting, and the bytes actually warmed since the listener was built.
    public void testSearchRecoveryWarmingListenerLogsWarmingProgressOnTimeout() {
        final var dataSetSize = ByteSizeValue.ofGb(4);
        final var bytesToWarm = ByteSizeValue.ofMb(512);
        final var bytesWarmedBefore = ByteSizeValue.ofMb(128);
        final var bytesWarmed = ByteSizeValue.ofMb(64);
        final var shardId = new ShardId("logs", IndexMetadata.INDEX_UUID_NA_VALUE, 2);
        final var timeout = TimeValue.timeValueSeconds(30);
        final var timeoutContext = "relocation source shutting down";

        try (var threadPool = new CapturingScheduleThreadPool(getTestName())) {
            final var service = newWarmingService(threadPool);
            final var resume = new PlainActionFuture<Void>();
            final var warmingListener = service.searchRecoveryWarmingListener(
                SearchRecoveryTimeout.fixed(timeout, timeoutContext),
                () -> null, // unused in this test case
                mockIndexShard(
                    TestShardRouting.newShardRouting(shardId, randomIdentifier(), false, STARTED, ShardRouting.Role.SEARCH_ONLY)
                ),
                // the baseline is read when the listener is built, so only the 64mb warmed afterwards must be reported
                mockDirectory(dataSetSize.getBytes(), bytesWarmedBefore.getBytes(), bytesWarmedBefore.getBytes() + bytesWarmed.getBytes()),
                bytesToWarm.getBytes(),
                resume
            );
            assertThatLogger(() -> {
                threadPool.scheduledCommand.get().run();
                safeGet(resume);
            },
                SharedBlobCacheWarmingService.class,
                new MockLog.SeenEventExpectation(
                    "warming timeout reporting sizes",
                    SharedBlobCacheWarmingService.class.getCanonicalName(),
                    WARN,
                    "Search shard recovery cache warming timed out after [30s] (relocation source shutting down) for [logs][2], "
                        + "shard data set size [4gb], bytes to warm [512mb], bytes warmed [64mb]"
                ),
                new MockLog.SeenEventExpectation(
                    "data set size field",
                    SharedBlobCacheWarmingService.class.getCanonicalName(),
                    WARN,
                    SEARCH_RECOVERY_LOG_FIELD_PREFIX + "data_set_size_bytes=\"" + dataSetSize.getBytes() + '"'
                ),
                new MockLog.SeenEventExpectation(
                    "bytes to warm field",
                    SharedBlobCacheWarmingService.class.getCanonicalName(),
                    WARN,
                    SEARCH_RECOVERY_LOG_FIELD_PREFIX + "bytes_to_warm=\"" + bytesToWarm.getBytes() + '"'
                ),
                new MockLog.SeenEventExpectation(
                    "bytes warmed field",
                    SharedBlobCacheWarmingService.class.getCanonicalName(),
                    WARN,
                    SEARCH_RECOVERY_LOG_FIELD_PREFIX + "bytes_warmed=\"" + bytesWarmed.getBytes() + '"'
                )
            );
            // warming completing after losing the race is discarded, so it must not log a second timeout
            assertThatLogger(
                () -> warmingListener.onResponse(null),
                SharedBlobCacheWarmingService.class,
                new MockLog.UnseenEventExpectation(
                    "no timeout warning for the discarded event",
                    SharedBlobCacheWarmingService.class.getCanonicalName(),
                    WARN,
                    "Search shard recovery cache warming timed out"
                )
            );
        }
    }

    /**
     * Asserts that exactly one measurement was recorded for {@code metricName} and that its value (in seconds) is at least
     * {@code minMillis} (the artificial delay every caller injects via {@link #newWarmingService}) and, generously,
     * under a minute (catches gross unit/overflow errors without being sensitive to CI slowness). Returns the measurement so callers
     * can additionally assert on its attributes.
     */
    private static Measurement assertSingleDurationMeasurementAtLeast(
        RecordingMeterRegistry meterRegistry,
        String metricName,
        long minMillis
    ) {
        List<Measurement> measurements = meterRegistry.getRecorder().getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, metricName);
        assertThat(measurements, hasSize(1));
        Measurement measurement = measurements.get(0);
        assertThat(measurement.getDouble(), greaterThanOrEqualTo(minMillis / 1000.0));
        assertThat(measurement.getDouble(), lessThan(TimeValue.timeValueMinutes(1).millis() / 1000.0));
        return measurement;
    }

    private static void assertWaitOutcome(Measurement waitDurationMeasurement, SearchRecoveryWaitOutcome outcome) {
        assertThat(
            waitDurationMeasurement.attributes(),
            equalTo(Map.of(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WAIT_OUTCOME_ATTRIBUTE_KEY, outcome.name()))
        );
    }
}
