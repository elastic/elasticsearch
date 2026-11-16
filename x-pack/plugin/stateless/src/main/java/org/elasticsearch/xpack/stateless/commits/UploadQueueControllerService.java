/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class UploadQueueControllerService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(UploadQueueControllerService.class);

    public static final Setting<Boolean> STATELESS_UPLOAD_QUEUE_CONTROLLER_ENABLED = Setting.boolSetting(
        "stateless.upload.queue_controller.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Enable the controller by default to get metrics/logs from MonitoringThrottler but don't enable actual throttling by default
    // to be able to do per-project rollout first.
    // This is not dynamic to simplify the implementation.
    public static final Setting<Boolean> STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEXING_THROTTLING_ENABLED = Setting.boolSetting(
        "stateless.upload.queue_controller.indexing_throttling.enabled",
        true,
        Setting.Property.NodeScope
    );

    /**
     * How frequently the upload queue controller should check the commit upload backlog and apply throttling if needed.
     */
    public static final Setting<TimeValue> STATELESS_UPLOAD_QUEUE_CONTROLLER_INTERVAL = Setting.positiveTimeSetting(
        "stateless.upload.queue_controller.interval",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Age of the oldest commit in the upload queue when index throttling is applied.
     */
    public static final Setting<TimeValue> STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_THRESHOLD = Setting.positiveTimeSetting(
        "stateless.upload.queue_controller.index_throttle.threshold",
        TimeValue.timeValueSeconds(90),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Age of the oldest commit in the upload queue when index throttling is removed.
     */
    public static final Setting<TimeValue> STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_REMOVAL_THRESHOLD = Setting.positiveTimeSetting(
        "stateless.upload.queue_controller.index_throttle.removal_threshold",
        TimeValue.timeValueSeconds(45),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Minimum amount of time during which the decision to throttle/not throttle indexing can not be changed.
     * In other words minimum indexing throttle period and period between consecutive throttling applications.
     */
    public static final Setting<TimeValue> STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_COOLDOWN = Setting.positiveTimeSetting(
        "stateless.upload.queue_controller.index_throttle.cooldown",
        TimeValue.timeValueSeconds(20),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final UploadQueueControllerMonitor task;

    public UploadQueueControllerService(
        ThreadPool threadPool,
        Settings settings,
        ClusterService clusterService,
        StatelessCommitService statelessCommitService,
        IndicesService indicesService,
        TelemetryProvider telemetryProvider
    ) {
        var initialInterval = STATELESS_UPLOAD_QUEUE_CONTROLLER_INTERVAL.get(settings);
        var throttlingEnabled = STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEXING_THROTTLING_ENABLED.get(settings);
        this.task = new UploadQueueControllerMonitor(
            threadPool,
            threadPool.generic(),
            initialInterval,
            clusterService,
            statelessCommitService,
            indicesService,
            telemetryProvider,
            throttlingEnabled
        );
    }

    // visible for tests
    void runNow() {
        task.calculateNewState();
    }

    // visible for tests
    Map<ShardId, ThrottleState> currentState() {
        return task.currentState();
    }

    @Override
    protected void doStart() {
        task.rescheduleIfNecessary();
    }

    @Override
    protected void doStop() {
        task.close();
    }

    @Override
    protected void doClose() throws IOException {}

    private static class UploadQueueControllerMonitor extends AbstractAsyncTask {
        private volatile boolean enabled;

        private final StatelessCommitService statelessCommitService;
        private final IndicesService indicesService;
        private final boolean throttlingEnabled;

        private final Metrics metrics;
        private final ThrottleCalculator indexingThrottleCalculator;

        private volatile ThrottleSettings indexingThrottleSettings;

        // Written under a lock so not volatile.
        private Throttler throttler;
        private Map<ShardId, ThrottleState> indexingThrottleState;

        private final Object resetLock = new Object();

        UploadQueueControllerMonitor(
            ThreadPool threadPool,
            Executor executor,
            TimeValue interval,
            ClusterService clusterService,
            StatelessCommitService statelessCommitService,
            IndicesService indicesService,
            TelemetryProvider telemetryProvider,
            boolean throttlingEnabled
        ) {
            super(logger, threadPool, executor, interval, true);

            this.statelessCommitService = statelessCommitService;
            this.indicesService = indicesService;
            this.throttlingEnabled = throttlingEnabled;

            this.metrics = new Metrics(telemetryProvider, "indexing");
            this.indexingThrottleCalculator = new ThrottleCalculator(
                threadPool::relativeTimeInMillis,
                telemetryProvider.getMeterRegistry()
            );

            this.throttler = createThrottler();
            this.indexingThrottleState = Map.of();

            ClusterSettings clusterSettings = clusterService.getClusterSettings();
            clusterSettings.initializeAndWatch(STATELESS_UPLOAD_QUEUE_CONTROLLER_ENABLED, enabled -> {
                // The most interesting case here is when the service is enabled and then disabled as an override.
                // In this case first we remove scheduling of new tasks.
                this.enabled = enabled;
                rescheduleIfNecessary();
                // And then we remove all throttling applied by the latest task and clear the state.
                // See comments inside `reset()` as well.
                reset();
            });
            clusterSettings.addSettingsUpdateConsumer(STATELESS_UPLOAD_QUEUE_CONTROLLER_INTERVAL, this::setInterval);
            this.indexingThrottleSettings = new ThrottleSettings(
                clusterSettings.get(STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_THRESHOLD),
                clusterSettings.get(STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_REMOVAL_THRESHOLD),
                clusterSettings.get(STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_COOLDOWN).millis()
            );
            clusterSettings.addSettingsUpdateConsumer(
                settings -> this.indexingThrottleSettings = new ThrottleSettings(
                    STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_THRESHOLD.get(settings),
                    STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_REMOVAL_THRESHOLD.get(settings),
                    STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_COOLDOWN.get(settings).millis()
                ),
                List.of(
                    STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_THRESHOLD,
                    STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_REMOVAL_THRESHOLD,
                    STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_COOLDOWN
                )
            );
        }

        @Override
        protected boolean mustReschedule() {
            return enabled;
        }

        @Override
        protected void runInternal() {
            synchronized (resetLock) {
                if (enabled == false) {
                    return;
                }

                calculateNewState();
            }
        }

        private void calculateNewState() {
            indexingThrottleState = indexingThrottleCalculator.newState(
                indexingThrottleState,
                statelessCommitService.getShardCommitStats(),
                throttler,
                indexingThrottleSettings
            );
        }

        private void reset() {
            /// This synchronization is needed to prevent a currently running task from applying throttles
            /// during/after the reset.
            /// The task can run concurrently because we don't interrupt running scheduled tasks
            /// per [org.elasticsearch.threadpool.ScheduledCancellableAdapter].
            ///
            /// This method does get called from settings update callback and we don't want to block that thread.
            /// But the lock should be rarely contested, and it simplifies the implementation quite a bit.
            /// First of all, an operator changing the "enabled" setting leading to reset should be rare to begin with.
            /// Then, this lock is only contested if there is a running task and with an interval of 5 seconds that's not that likely.
            /// Finally, we don't do that much work under this lock.
            synchronized (resetLock) {
                throttler.close();
                throttler = createThrottler();
                indexingThrottleState = Map.of();
            }
        }

        private Throttler createThrottler() {
            return throttlingEnabled ? new IndexingThrottler(indicesService, metrics) : new NoopThrottler(metrics);
        }

        private Map<ShardId, ThrottleState> currentState() {
            synchronized (resetLock) {
                return Map.copyOf(indexingThrottleState);
            }
        }
    }

    record ThrottleSettings(TimeValue activationThreshold, TimeValue deactivationThreshold, long cooldownPeriodMs) {}

    static class ThrottleCalculator {
        public final int MAXIMUM_CONSECUTIVE_THROTTLING_PERIODS_LOGGING_THRESHOLD = 3;

        private static final Logger logger = LogManager.getLogger(ThrottleCalculator.class);

        private static final String OLDEST_COMMIT_AGE_HISTOGRAM_METRIC = "es.stateless.upload_queue.oldest_commit_age.histogram";

        private final Supplier<Long> relativeTimeMillis;
        private final LongHistogram oldestCommitAgeSecondsHistogram;

        ThrottleCalculator(Supplier<Long> relativeTimeMillis, MeterRegistry meterRegistry) {
            this.relativeTimeMillis = relativeTimeMillis;
            this.oldestCommitAgeSecondsHistogram = meterRegistry.registerLongHistogram(
                OLDEST_COMMIT_AGE_HISTOGRAM_METRIC,
                "Age of the oldest commit in the upload queue",
                "seconds"
            );
        }

        Map<ShardId, ThrottleState> newState(
            Map<ShardId, ThrottleState> currentState,
            Stream<? extends ShardCommitUploadStats> commitStats,
            Throttler throttler,
            ThrottleSettings settings
        ) {
            // Rebuilding a map allows us to drop entries for all shards that were closed.
            // The value stored in the map is small as well.
            var newState = new HashMap<ShardId, ThrottleState>(currentState.size());

            commitStats.forEach(stats -> {
                ShardId shardId = stats.shardId();

                ThrottleState shardState = currentState.get(shardId);
                if (shardState != null && shardState.expired(relativeTimeMillis.get(), settings.cooldownPeriodMs) == false) {
                    // We are still executing previous decision, keep doing it and not intervene.
                    newState.put(shardId, shardState);
                    return;
                }

                // Otherwise we can make a new decision.
                Long oldestCommitUploadStartTime = stats.oldestCommitUploadStartTimeRelativeMillis();

                TimeValue ageOfTheOldestCommitPendingUpload;
                if (oldestCommitUploadStartTime == null) {
                    // When `oldestCommitUploadStartTime` is null, it means that there are no pending upload commits,
                    // and as such we can remove throttling (TimeValue.ZERO should always be smaller than `settings.deactivationThreshold`).
                    ageOfTheOldestCommitPendingUpload = TimeValue.ZERO;
                } else {
                    ageOfTheOldestCommitPendingUpload = TimeValue.timeValueMillis(relativeTimeMillis.get() - oldestCommitUploadStartTime);
                    oldestCommitAgeSecondsHistogram.record(ageOfTheOldestCommitPendingUpload.seconds());
                }

                if (ageOfTheOldestCommitPendingUpload.compareTo(settings.activationThreshold) > 0) {
                    // This is a throttle condition.
                    if (shardState != null && shardState.latestDecision() == Type.THROTTLED) {
                        newState.put(shardId, renewThrottle(shardId, shardState));
                    } else {
                        // We know we can apply throttling - the grace period after the latest throttle expired
                        // or there was no prior decision.
                        // Note that the shard may be closed at this point.
                        // This is okay since we will drop it from the state on the next run anyway.
                        throttler.activate(shardId);
                        newState.put(shardId, ThrottleState.throttled(relativeTimeMillis.get(), 1));
                    }
                } else if (ageOfTheOldestCommitPendingUpload.compareTo(settings.deactivationThreshold) < 0) {
                    // This is a "stop throttle" condition.
                    if (shardState != null && shardState.latestDecision == Type.THROTTLED) {
                        // We are currently throttling, and we know that the throttling period has passed, stop it.
                        throttler.deactivate(shardId);
                        newState.put(shardId, ThrottleState.throttleRemoved(relativeTimeMillis.get()));
                    }
                } else if (shardState != null && shardState.latestDecision() == Type.THROTTLED) {
                    // The oldest pending upload's age is between the activation and removal thresholds.
                    // Keep tracking the active throttle and renew it for another cooldown period.
                    newState.put(shardId, renewThrottle(shardId, shardState));
                }
            });

            return newState;
        }

        private ThrottleState renewThrottle(ShardId shardId, ThrottleState shardState) {
            /// We are currently throttling, and we still see the queue.
            ///
            /// Indexing throttling reduces the amount of threads available for indexing to one.
            /// See [org.elasticsearch.indices.IndexingMemoryController#PAUSE_INDEXING_ON_THROTTLE].
            /// So if we see that we should throttle we'll keep it applied as long as needed
            /// since it is not a "full stop" scenario for the customer.
            /// We do want to understand how often this happens though.
            if (shardState.consecutiveApplications() >= MAXIMUM_CONSECUTIVE_THROTTLING_PERIODS_LOGGING_THRESHOLD) {
                logger.info(
                    "Indexing  throttling for shard {} has been applied {} consecutive times",
                    shardId,
                    shardState.consecutiveApplications()
                );
            }

            // We don't need to apply throttling since it's already applied.
            return ThrottleState.throttled(relativeTimeMillis.get(), shardState.consecutiveApplications() + 1);
        }
    }

    interface Throttler {
        /// Activate throttling for a shard with provided `ShardId`.
        void activate(ShardId shardId);

        void deactivate(ShardId shardId);

        /// Removes all currently applied throttling and marks this instance as closed.
        void close();
    }

    static final class NoopThrottler implements Throttler {
        private final Metrics metrics;

        NoopThrottler(Metrics metrics) {
            this.metrics = metrics;
        }

        @Override
        public void activate(ShardId shardId) {
            metrics.notifyActivated(shardId, true);
        }

        @Override
        public void deactivate(ShardId shardId) {
            metrics.notifyDeactivated(shardId, DeactivationMode.NOOP);
        }

        @Override
        public void close() {}
    }

    static final class IndexingThrottler implements Throttler {
        private final IndicesService indicesService;
        private final Metrics metrics;

        private boolean closed = false;

        // Throttling methods in `IndexEngine` are not idempotent so we need to make sure
        // we don't remove throttle if it was never applied.
        // It's possible to end up in this situation since queue stats are keyed only by ShardId
        // and there can be weird cases like shard relocating to a different node and then relocating back to the current node.
        private final Set<IndexShard> throttledShards = ConcurrentHashMap.newKeySet();

        IndexingThrottler(IndicesService indicesService, Metrics metrics) {
            this.indicesService = indicesService;
            this.metrics = metrics;
        }

        @Override
        public void activate(ShardId shardId) {
            assert closed == false : "already closed";

            IndexShard shard = indicesService.getShardOrNull(shardId);
            if (shard != null) {
                // This would imply that StatelessCommitService has re-created commit state for this shardId
                // without closeShard() ever being called which shouldn't happen.
                assert throttledShards.contains(shard) == false;

                shard.tryWithEngineOrNull(engine -> {
                    if (engine != null) {
                        engine.activateThrottling();
                        throttledShards.add(shard);
                        metrics.notifyActivated(shardId, false);
                    }

                    return null;
                });
            }
        }

        @Override
        public void deactivate(ShardId shardId) {
            assert closed == false : "already closed";

            IndexShard shard = indicesService.getShardOrNull(shardId);
            if (shard != null && throttledShards.remove(shard)) {
                shard.tryWithEngineOrNull(engine -> {
                    if (engine != null) {
                        engine.deactivateThrottling();
                        metrics.notifyDeactivated(shardId, DeactivationMode.NORMAL);
                    }
                    return null;
                });
            }
        }

        public void closeShard(IndexShard indexShard) {
            throttledShards.remove(indexShard);
        }

        // visible for tests
        Set<IndexShard> getThrottledShards() {
            return throttledShards;
        }

        @Override
        public void close() {
            closed = true;

            for (IndexShard shard : throttledShards) {
                // deactivateThrottling() is noop for closed shards so shouldn't throw.
                shard.deactivateThrottling();
                metrics.notifyDeactivated(shard.shardId(), DeactivationMode.SERVICE_DISABLED);
            }
            throttledShards.clear();
        }
    }

    static final class Metrics {
        private static final Logger logger = LogManager.getLogger(Metrics.class);

        private final String throttlerType;

        private final LongCounter activatedCount;
        private final LongCounter deactivatedCount;

        Metrics(TelemetryProvider telemetryProvider, String throttlerType) {
            this.throttlerType = throttlerType;

            String METRIC_NAME_FORMAT = "es.stateless.upload_queue.%s_throttling.%s.total";
            this.activatedCount = telemetryProvider.getMeterRegistry()
                .registerLongCounter(
                    String.format(Locale.ROOT, METRIC_NAME_FORMAT, throttlerType, "activated"),
                    String.format(Locale.ROOT, "how many times was %s throttling activated", throttlerType),
                    "unit"
                );
            this.deactivatedCount = telemetryProvider.getMeterRegistry()
                .registerLongCounter(
                    String.format(Locale.ROOT, METRIC_NAME_FORMAT, throttlerType, "deactivated"),
                    String.format(Locale.ROOT, "how many times was %s throttling deactivated", throttlerType),
                    "unit"
                );
        }

        public static final Metrics NOOP = new Metrics(TelemetryProvider.NOOP, "noop");

        public void notifyActivated(ShardId shardId, boolean noop) {
            if (noop) {
                logger.info("[METRICS ONLY] Activating {} throttling for shard {}", throttlerType, shardId);
            } else {
                logger.info("Activating {} throttling for shard {}", throttlerType, shardId);
            }
            activatedCount.increment();
        }

        public void notifyDeactivated(ShardId shardId, DeactivationMode note) {
            if (note == DeactivationMode.NOOP) {
                logger.info("[METRICS ONLY] Deactivating {} throttling for shard {}", throttlerType, shardId);
            } else if (note == DeactivationMode.SERVICE_DISABLED) {
                logger.info("Deactivating {} throttling for shard {} because the service is disabled", throttlerType, shardId);
            } else {
                logger.info("Deactivating {} throttling for shard {}", throttlerType, shardId);
            }
            deactivatedCount.increment();
        }
    }

    enum DeactivationMode {
        NOOP,
        SERVICE_DISABLED,
        NORMAL
    }

    enum Type {
        THROTTLED,
        THROTTLE_REMOVED
    }

    // Track when a particular decision was applied to be able to hold the decision for
    // configuration amount of time and avoid constant flapping.
    static class ThrottleState {
        private final Type latestDecision;
        private final long relativeApplicationTimeMs;
        private final int consecutiveApplications;

        private ThrottleState(Type latestDecision, long relativeApplicationTimeMs, int consecutiveApplications) {
            this.latestDecision = latestDecision;
            this.relativeApplicationTimeMs = relativeApplicationTimeMs;
            this.consecutiveApplications = consecutiveApplications;
        }

        static ThrottleState throttled(long relativeApplicationTimeMs, int consecutiveApplications) {
            return new ThrottleState(Type.THROTTLED, relativeApplicationTimeMs, consecutiveApplications);
        }

        static ThrottleState throttleRemoved(long relativeApplicationTimeMs) {
            // consecutiveApplications is currently unused in THROTTLE_REMOVED case
            return new ThrottleState(Type.THROTTLE_REMOVED, relativeApplicationTimeMs, -1);
        }

        boolean expired(long currentRelativeTimeMs, long expirationPeriodMs) {
            return currentRelativeTimeMs - relativeApplicationTimeMs > expirationPeriodMs;
        }

        public Type latestDecision() {
            return latestDecision;
        }

        public long relativeApplicationTimeMs() {
            return relativeApplicationTimeMs;
        }

        public int consecutiveApplications() {
            assert latestDecision == Type.THROTTLED;
            return consecutiveApplications;
        }
    }
}
