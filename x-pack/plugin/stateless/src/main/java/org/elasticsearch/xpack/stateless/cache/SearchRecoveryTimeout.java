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
/// [#timeoutContext]: the situation the plan was computed for. It decides whether the plan is [#extendable()] and, together with the previous
/// plan, whether a re-evaluation may extend the wait, see [#shouldExtendAfter].
public record SearchRecoveryTimeout(TimeValue timeout, TimeoutContext timeoutContext) {

    /// The situation a plan was computed for. Its [#description()] is what gets logged.
    ///
    /// [#extendable()]: when `false`, the re-evaluation loop fires the race immediately on first expiry without re-reading the cluster
    /// state or rescheduling, regardless of the node-level re-evaluation setting.
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
        RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE_CAPPED_FOR_PENDING_SHARDS(
            "relocation source shutting down (equal share of remaining time to capped grace deadline), "
                + "capped to reserve time for pending shards",
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
    }

    public static SearchRecoveryTimeout skip() {
        return new SearchRecoveryTimeout(TimeValue.ZERO, TimeoutContext.SKIP);
    }

    public boolean extendable() {
        return timeoutContext.extendable();
    }

    public boolean shouldExtendAfter(SearchRecoveryTimeout previous) {
        assert previous.extendable();
        if (timeoutContext.extendable() == false) {
            return false;
        }
        return timeoutContext != TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT
            || previous.timeoutContext != TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT;
    }

    /// When `true`, recovery should use [SharedBlobCacheWarmingService#searchRecoveryWarmingListener]
    /// with [#timeout()] (which is then > 0).
    public boolean awaitWarming() {
        return timeout.millis() > 0;
    }
}
