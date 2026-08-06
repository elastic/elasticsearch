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
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

class UploadQueueControllerService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(UploadQueueControllerService.class);

    public static final Setting<Boolean> STATELESS_UPLOAD_QUEUE_CONTROLLER_ENABLED = Setting.boolSetting(
        "stateless.upload.queue_controller.enabled",
        false, // TODO ??
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

    private final StatelessCommitService statelessCommitService;
    private final IndicesService indicesService;

    private Task task;

    public UploadQueueControllerService(
        StatelessCommitService statelessCommitService,
        ClusterService clusterService,
        ThreadPool threadPool,
        Settings settings,
        IndicesService indicesService
    ) {

        this.statelessCommitService = statelessCommitService;
        this.indicesService = indicesService;

        var initialInterval = STATELESS_UPLOAD_QUEUE_CONTROLLER_INTERVAL.get(settings);
        this.task = new Task(threadPool, threadPool.generic(), initialInterval, clusterService, indicesService);
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

    class Task extends AbstractAsyncTask {
        private volatile boolean enabled;

        private volatile ThrottleSettings indexingThrottleSettings;
        private final ThrottleCalculator indexingThrottleCalculator;
        private volatile Map<ShardId, ThrottleState> indexThrottleState = Map.of();

        Task(ThreadPool threadPool, Executor executor, TimeValue interval, ClusterService clusterService, IndicesService indicesService) {
            super(logger, threadPool, executor, interval, true);
            this.indexingThrottleCalculator = new ThrottleCalculator(
                threadPool::relativeTimeInMillis,
                new IndexingThrottler(indicesService)
            );

            ClusterSettings clusterSettings = clusterService.getClusterSettings();
            clusterSettings.initializeAndWatch(STATELESS_UPLOAD_QUEUE_CONTROLLER_ENABLED, enabled -> {
                this.enabled = enabled;
                rescheduleIfNecessary();
            });
            clusterSettings.addSettingsUpdateConsumer(STATELESS_UPLOAD_QUEUE_CONTROLLER_INTERVAL, this::setInterval);
            clusterSettings.addSettingsUpdateConsumer(settings -> {
                this.indexingThrottleSettings = new ThrottleSettings(
                    STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_THRESHOLD.get(settings).seconds(),
                    STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_REMOVAL_THRESHOLD.get(settings).seconds(),
                    STATELESS_UPLOAD_QUEUE_CONTROLLER_INDEX_THROTTLE_COOLDOWN.get(settings).seconds()
                );
            },
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
            // Rebuilding a map allows us to drop entries for all shards that were closed.
            // The map is also small.
            var newIndexThrottleState = new HashMap<ShardId, ThrottleState>(indexThrottleState.size());

            indexThrottleState = newIndexThrottleState;
        }
    }

    record ThrottleSettings(long activationThresholdSeconds, long deactivationThresholdSeconds, long cooldownPeriodSeconds) {}

    static class ThrottleCalculator {
        public final int MAXIMUM_CONSECUTIVE_THROTTLING_PERIODS = 3;

        private final Supplier<Long> relativeTimeMillis;
        private final Throttler throttler;

        ThrottleCalculator(Supplier<Long> relativeTimeMillis, Throttler throttler) {
            this.relativeTimeMillis = relativeTimeMillis;
            this.throttler = throttler;
        }

        Map<ShardId, ThrottleState> newState(
            Map<ShardId, ThrottleState> currentState,
            Iterable<ShardCommitStats> commitStats,
            ThrottleSettings settings,
            double uploadThroughputMiBSec
        ) {
            var newState = new HashMap<ShardId, ThrottleState>(currentState.size());

            commitStats.forEach(stats -> {
                ShardId shardId = stats.shardId();

                ThrottleState shardState = currentState.get(shardId);
                if (shardState != null && shardState.expired(relativeTimeMillis.get(), settings.cooldownPeriodSeconds) == false) {
                    // We are still executing previous decision, keep doing it and not intervene.
                    newState.put(shardId, shardState);
                    return;
                }

                // Otherwise we can make a new decision.

                long queueInBytes = stats.pendingUploadBytes();
                long queueInSeconds = Math.round(ByteSizeUnit.BYTES.toMB(queueInBytes) / uploadThroughputMiBSec);

                if (queueInSeconds > settings.activationThresholdSeconds) {
                    // This is a throttle condition.
                    if (shardState != null && shardState.latestDecision == Type.THROTTLED) {
                        // The throttle is currently applied, it just expired.
                        // We know that we still need throttling, apply unless we have reached
                        // maximum amount of consequtive periods.
                        if (shardState.consecutiveApplications < MAXIMUM_CONSECUTIVE_THROTTLING_PERIODS) {
                            newState.put(
                                shardId,
                                new ThrottleState(Type.THROTTLED, relativeTimeMillis.get(), shardState.consecutiveApplications + 1)
                            );
                        } else {
                            // If maximum periods are reached, deactivate to allow clients to make at least some progress.
                            throttler.deactivate(shardId);
                            newState.put(shardId, new ThrottleState(Type.THROTTLE_REMOVED, relativeTimeMillis.get(), 1));
                        }
                        return;
                    }

                    // Otherwise we can throttle - the grace period after the latest throttle expired
                    // or there was no prior decision.
                    if (throttler.activate(shardId)) {
                        newState.put(shardId, new ThrottleState(Type.THROTTLED, relativeTimeMillis.get(), 1));
                    }
                } else if (queueInSeconds < settings.deactivationThresholdSeconds) {
                    // This is a "stop throttle" condition.
                    if (shardState != null && shardState.latestDecision == Type.THROTTLED) {
                        // We are currently throttling, and we know that the throttling period has passed, stop it.
                        throttler.deactivate(shardId);
                        newState.put(shardId, new ThrottleState(Type.THROTTLE_REMOVED, relativeTimeMillis.get(), 1));
                    }
                }
            });

            return newState;
        }
    }

    interface Throttler {
        /// Activate a particular type of throttling for a shard.
        /// Returns `true` if throttling was successfully activated.
        boolean activate(ShardId shardId);

        void deactivate(ShardId shardId);
    }

    record IndexingThrottler(IndicesService indicesService) implements Throttler {
        @Override
        public boolean activate(ShardId shardId) {
            IndexShard shard = indicesService.getShardOrNull(shardId);
            if (shard != null) {
                shard.activateThrottling();
                return true;
            }
            return false;
        }

        @Override
        public void deactivate(ShardId shardId) {
            IndexShard shard = indicesService.getShardOrNull(shardId);
            if (shard != null) {
                shard.deactivateThrottling();
            }
        }
    }

    enum Type {
        THROTTLED,
        THROTTLE_REMOVED
    }

    // Track when a particular decision was applied to be able to hold the decision for
    // configuration amount of time and avoid constant flapping.
    record ThrottleState(Type latestDecision, long relativeApplicationTimeMs, int consecutiveApplications) {
        boolean expired(long currentRelativeTimeMs, long expirationPeriod) {
            return currentRelativeTimeMs - relativeApplicationTimeMs > expirationPeriod;
        }
    }
}
