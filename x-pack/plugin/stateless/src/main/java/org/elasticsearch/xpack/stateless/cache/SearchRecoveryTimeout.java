/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.core.TimeValue;

/// Search shard recovery warming for the internal replicated-files path: [#timeout()] drives the race in
/// [SharedBlobCacheWarmingService#searchRecoveryWarmingListener]; [TimeValue#ZERO] means do not await warming.
/// Use [#awaitWarming()] to branch.
///
/// [#timeoutContext]: the situation the plan was computed for. It decides whether the plan is [#extendable()] and, together with the
/// previous plan, whether a re-evaluation may extend the wait, see [#shouldExtendAfter].
///
/// [#perShardShareMs]: for equal-share plans, the per-shard share of the remaining grace period this plan was computed from (before scaling
/// by the number of parallel relocations), `0` otherwise. A re-evaluation compares its own share to this one to find the time saved by
/// shards that finished earlier than planned.
public record SearchRecoveryTimeout(TimeValue timeout, TimeoutContext timeoutContext, double perShardShareMs) {

    public SearchRecoveryTimeout(TimeValue timeout, TimeoutContext timeoutContext) {
        this(timeout, timeoutContext, 0);
    }

    /// The situation a plan was computed for. Its [#description()] is what gets logged.
    ///
    /// [#extendable()]: whether the slice of a plan with this context may be followed by another one. When `false`, the re-evaluation loop
    /// fires the race immediately on first expiry without re-reading the cluster state or rescheduling, regardless of the node-level
    /// re-evaluation setting. Whether a given re-evaluated plan may follow the previous one is decided separately, see
    /// [SearchRecoveryTimeout#shouldExtendAfter].
    public enum TimeoutContext {
        SKIP("", false),
        NON_RELOCATION_ANOTHER_ACTIVE_COPY("not a relocation, another active shard copy", true),
        RESHARD_SPLIT_TARGET("reshard split target", false),
        RELOCATION_SOURCE_NOT_SHUTTING_DOWN_NO_CLUSTER_SHUTDOWN("relocation source not shutting down, no cluster shutdown", true),
        RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT(
            "relocation source not shutting down, cluster shutdown metadata present",
            true
        ),
        RELOCATION_SOURCE_SHUTTING_DOWN_GRACE_ELAPSED("relocation source shutting down (grace period elapsed)", false),
        RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME(
            "relocation source shutting down (data volume proportional share of remaining time to capped grace deadline)",
            false
        ),
        RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE(
            "relocation source shutting down (equal share of remaining time to capped grace deadline)",
            true
        ),
        RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE_SAVED_TIME(
            "relocation source shutting down (share of the time saved by shards that finished earlier than planned)",
            true
        );

        private final String description;
        private final boolean extendable;

        TimeoutContext(String description, boolean extendable) {
            this.description = description;
            this.extendable = extendable;
        }

        public String description() {
            return description;
        }

        public boolean extendable() {
            return extendable;
        }

        /// Whether the plan was computed for a relocation whose source node is shutting down, i.e. against the grace deadline.
        public boolean sourceShuttingDown() {
            return switch (this) {
                case RELOCATION_SOURCE_SHUTTING_DOWN_GRACE_ELAPSED, RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME,
                    RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE, RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE_SAVED_TIME -> true;
                case SKIP, NON_RELOCATION_ANOTHER_ACTIVE_COPY, RESHARD_SPLIT_TARGET,
                    RELOCATION_SOURCE_NOT_SHUTTING_DOWN_NO_CLUSTER_SHUTDOWN,
                    RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT -> false;
            };
        }
    }

    public static SearchRecoveryTimeout skip() {
        return new SearchRecoveryTimeout(TimeValue.ZERO, TimeoutContext.SKIP);
    }

    public boolean extendable() {
        return timeoutContext.extendable();
    }

    /// Whether the wait may continue with this plan, which a re-evaluation computed on expiry of the `previous` plan. The caller has
    /// already checked that `previous` is [#extendable()], so only rules about this plan and the transition between the two live here.
    /// A data-volume plan is not itself extendable, so it is the last slice of a wait, and it is only accepted as the first plan after the
    /// source started shutting down, never after a plan that was already computed for a shutting-down source.
    public boolean shouldExtendAfter(SearchRecoveryTimeout previous) {
        assert previous.extendable();
        return switch (timeoutContext) {
            // A full data-volume share is only handed out as the first plan once the source started shutting down. Within the shutdown
            // phase only the time saved by shards that finished earlier than planned is handed out; the calculation service never
            // produces a data-volume plan then, so this is a safeguard.
            case RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME -> previous.timeoutContext.sourceShuttingDown() == false;
            case RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT ->
                previous.timeoutContext != TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT;
            // nothing to wait for any more
            case SKIP, RELOCATION_SOURCE_SHUTTING_DOWN_GRACE_ELAPSED -> false;
            case NON_RELOCATION_ANOTHER_ACTIVE_COPY, RESHARD_SPLIT_TARGET, RELOCATION_SOURCE_NOT_SHUTTING_DOWN_NO_CLUSTER_SHUTDOWN,
                RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE, RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE_SAVED_TIME -> true;
        };
    }

    /// When `true`, recovery should use [SharedBlobCacheWarmingService#searchRecoveryWarmingListener]
    /// with [#timeout()] (which is then > 0).
    public boolean awaitWarming() {
        return timeout.millis() > 0;
    }
}
