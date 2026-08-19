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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
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
     * Size of the commit upload backlog when index throttling is applied.
     */
    public static final Setting<TimeValue> STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_THRESHOLD = Setting.positiveTimeSetting(
        "stateless.upload.queue_controller.index_throttle.threshold",
        TimeValue.timeValueSeconds(90),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Size of the commit upload backlog when index throttling is removed.
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
        TelemetryProvider telemetryProvider
    ) {
        var initialInterval = STATELESS_UPLOAD_QUEUE_CONTROLLER_INTERVAL.get(settings);
        this.task = new UploadQueueControllerMonitor(
            threadPool,
            threadPool.generic(),
            initialInterval,
            clusterService,
            statelessCommitService,
            telemetryProvider
        );
    }

    // visible for tests
    void runNow() {
        task.runInternal();
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

        private final ThrottleCalculator indexingThrottleCalculator;

        private volatile ThrottleSettings indexingThrottleSettings;
        private volatile Map<ShardId, ThrottleState> indexingThrottleState = Map.of();

        UploadQueueControllerMonitor(
            ThreadPool threadPool,
            Executor executor,
            TimeValue interval,
            ClusterService clusterService,
            StatelessCommitService statelessCommitService,
            TelemetryProvider telemetryProvider
        ) {
            super(logger, threadPool, executor, interval, true);

            this.statelessCommitService = statelessCommitService;

            this.indexingThrottleCalculator = new ThrottleCalculator(
                threadPool::relativeTimeInMillis,
                // TODO
                // Using noop throttler here during initial roll out.
                new MonitoringThrottler(new NoopThrottler(), telemetryProvider, "indexing")
            );

            ClusterSettings clusterSettings = clusterService.getClusterSettings();
            clusterSettings.initializeAndWatch(STATELESS_UPLOAD_QUEUE_CONTROLLER_ENABLED, enabled -> {
                this.enabled = enabled;
                rescheduleIfNecessary();
            });
            clusterSettings.addSettingsUpdateConsumer(STATELESS_UPLOAD_QUEUE_CONTROLLER_INTERVAL, this::setInterval);
            this.indexingThrottleSettings = new ThrottleSettings(
                clusterSettings.get(STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_THRESHOLD).seconds(),
                clusterSettings.get(STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_REMOVAL_THRESHOLD).seconds(),
                clusterSettings.get(STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_COOLDOWN).millis()
            );
            clusterSettings.addSettingsUpdateConsumer(
                settings -> this.indexingThrottleSettings = new ThrottleSettings(
                    STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_THRESHOLD.get(settings).seconds(),
                    STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_REMOVAL_THRESHOLD.get(settings).seconds(),
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
            var currentState = indexingThrottleState;
            indexingThrottleState = indexingThrottleCalculator.newState(
                currentState,
                statelessCommitService.getShardCommitStats(),
                indexingThrottleSettings,
                statelessCommitService.getAverageCommitUploadThroughputMiBSec()
            );
        }
    }

    record ThrottleSettings(long activationThresholdSeconds, long deactivationThresholdSeconds, long cooldownPeriodMs) {}

    static class ThrottleCalculator {
        public final int MAXIMUM_CONSECUTIVE_THROTTLING_PERIODS_LOGGING_THRESHOLD = 3;

        private static final Logger logger = LogManager.getLogger(ThrottleCalculator.class);

        private final Supplier<Long> relativeTimeMillis;
        private final Throttler throttler;

        ThrottleCalculator(Supplier<Long> relativeTimeMillis, Throttler throttler) {
            this.relativeTimeMillis = relativeTimeMillis;
            this.throttler = throttler;
        }

        Map<ShardId, ThrottleState> newState(
            Map<ShardId, ThrottleState> currentState,
            Stream<? extends ShardCommitUploadStats> commitStats,
            ThrottleSettings settings,
            double uploadThroughputMiBSec
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

                long queueInMiB = ByteSizeUnit.BYTES.toMB(stats.pendingUploadBytes());
                long queueInSeconds = Math.round(queueInMiB / uploadThroughputMiBSec);

                if (queueInSeconds > settings.activationThresholdSeconds) {
                    // This is a throttle condition.
                    if (shardState != null && shardState.latestDecision() == Type.THROTTLED) {
                        /// We are currently throttling, and we still see the queue.
                        ///
                        /// Indexing throttling reduces the amount of threads available for indexing to one.
                        /// See [org.elasticsearch.indices.IndexingMemoryController#PAUSE_INDEXING_ON_THROTTLE].\
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
                        newState.put(shardId, ThrottleState.throttled(relativeTimeMillis.get(), shardState.consecutiveApplications() + 1));
                    } else {
                        // We know we can apply throttling - the grace period after the latest throttle expired
                        // or there was no prior decision.
                        // Note that the shard may be closed at this point.
                        // This is okay since we will drop it from the state on the next run anyway.
                        throttler.activate(shardId);
                        newState.put(shardId, ThrottleState.throttled(relativeTimeMillis.get(), 1));
                    }
                } else if (queueInSeconds < settings.deactivationThresholdSeconds) {
                    // This is a "stop throttle" condition.
                    if (shardState != null && shardState.latestDecision == Type.THROTTLED) {
                        // We are currently throttling, and we know that the throttling period has passed, stop it.
                        throttler.deactivate(shardId);
                        newState.put(shardId, ThrottleState.throttleRemoved(relativeTimeMillis.get()));
                    }
                }
            });

            return newState;
        }
    }

    interface Throttler {
        /// Activate throttling for a shard with provided `ShardId`.
        void activate(ShardId shardId);

        void deactivate(ShardId shardId);
    }

    record NoopThrottler() implements Throttler {
        @Override
        public void activate(ShardId shardId) {}

        @Override
        public void deactivate(ShardId shardId) {}
    }

    static class IndexingThrottler implements Throttler {
        private final IndicesService indicesService;

        // Throttling methods in `IndexEngine` are not idempotent so we need to make sure
        // we don't remove throttle if it was never applied.
        // It's possible to end up in this situation since queue stats are keyed only by ShardId.
        private final Set<IndexShard> throttledShards = ConcurrentHashMap.newKeySet();

        IndexingThrottler(IndicesService indicesService) {
            this.indicesService = indicesService;
        }

        @Override
        public void activate(ShardId shardId) {
            IndexShard shard = indicesService.getShardOrNull(shardId);
            if (shard != null) {
                // This would imply that StatelessCommitService has re-created commit state for this shardId
                // without closeShard() ever being called which shouldn't happen.
                assert throttledShards.contains(shard) == false;
                shard.activateThrottling();
                throttledShards.add(shard);
            }
        }

        @Override
        public void deactivate(ShardId shardId) {
            IndexShard shard = indicesService.getShardOrNull(shardId);
            if (shard != null && throttledShards.remove(shard)) {
                shard.deactivateThrottling();
            }
        }

        public void closeShard(IndexShard indexShard) {
            throttledShards.remove(indexShard);
        }

        // visible for tests
        Set<IndexShard> getThrottledShards() {
            return throttledShards;
        }
    }

    static class MonitoringThrottler implements Throttler {
        private static final Logger logger = LogManager.getLogger(MonitoringThrottler.class);

        private final Throttler delegate;
        private final String throttlerType;

        private final LongCounter activatedCount;
        private final LongCounter deactivatedCount;

        MonitoringThrottler(Throttler delegate, TelemetryProvider telemetryProvider, String throttlerType) {
            this.delegate = delegate;
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

        @Override
        public void activate(ShardId shardId) {
            logger.info("[Simulated] Activating {} throttling for shard {}", throttlerType, shardId);
            delegate.activate(shardId);
            activatedCount.increment();
        }

        @Override
        public void deactivate(ShardId shardId) {
            logger.info("[Simulated] Deactivating {} throttling for shard {}", throttlerType, shardId);
            delegate.deactivate(shardId);
            deactivatedCount.increment();
        }
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
