/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.core.TimeValue;

/// Search shard recovery warming for the internal replicated-files path: [#timeout()] drives the recovery wait in
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
    /// completes the recovery wait with a timeout immediately on first expiry without re-reading the cluster state or rescheduling,
    /// regardless of the node-level re-evaluation setting. Whether a given re-evaluated plan may follow the previous one is decided
    /// separately, see [SearchRecoveryTimeout#shouldExtendAfter] and [MayFollow].
    ///
    /// `phase` and `mayFollow`: the rule for a re-evaluated plan following the previous one lives in these attributes of the
    /// contexts, so a new context only has to state its own.
    public enum TimeoutContext {
        SKIP("", false, Phase.OTHER, MayFollow.NO_PLAN, false),
        NON_RELOCATION_ANOTHER_ACTIVE_COPY("not a relocation, another active shard copy", true, Phase.OTHER, MayFollow.ANY_PLAN, false),
        RESHARD_SPLIT_TARGET("reshard split target", false, Phase.OTHER, MayFollow.NO_PLAN, false),
        RELOCATION_SOURCE_NOT_SHUTTING_DOWN_NO_CLUSTER_SHUTDOWN(
            "relocation source not shutting down, no cluster shutdown",
            true,
            Phase.OTHER,
            MayFollow.ANY_PLAN,
            false
        ),
        RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT(
            "relocation source not shutting down, cluster shutdown metadata present",
            true,
            Phase.CLUSTER_SHUTDOWN_METADATA_PRESENT,
            MayFollow.PLAN_OF_ANOTHER_PHASE,
            false
        ),
        RELOCATION_SOURCE_SHUTTING_DOWN_GRACE_ELAPSED(
            "relocation source shutting down (grace period elapsed)",
            false,
            Phase.SOURCE_SHUTTING_DOWN,
            MayFollow.NO_PLAN,
            false
        ),
        RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME(
            "relocation source shutting down (data volume proportional share of remaining time to capped grace deadline)",
            false,
            Phase.SOURCE_SHUTTING_DOWN,
            MayFollow.PLAN_OF_ANOTHER_PHASE,
            false
        ),
        RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE(
            "relocation source shutting down (equal share of remaining time to capped grace deadline)",
            true,
            Phase.SOURCE_SHUTTING_DOWN,
            MayFollow.ANY_PLAN,
            true
        );

        private enum Phase {
            SOURCE_SHUTTING_DOWN,
            CLUSTER_SHUTDOWN_METADATA_PRESENT,
            OTHER
        }

        private enum MayFollow {
            NO_PLAN,
            ANY_PLAN,
            PLAN_OF_ANOTHER_PHASE
        }

        private final String description;
        private final boolean extendable;
        private final Phase phase;
        private final MayFollow mayFollow;
        private final boolean equalShare;

        TimeoutContext(String description, boolean extendable, Phase phase, MayFollow mayFollow, boolean equalShare) {
            this.description = description;
            this.extendable = extendable;
            this.phase = phase;
            this.mayFollow = mayFollow;
            this.equalShare = equalShare;
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

    /// Whether the wait may continue with this plan, which a re-evaluation computed on expiry of the `previous` plan. The caller has
    /// already checked that `previous` is [#extendable()], so only rules about this plan and the transition between the two live here.
    public boolean shouldExtendAfter(SearchRecoveryTimeout previous) {
        assert previous.extendable();
        return switch (timeoutContext.mayFollow) {
            case NO_PLAN -> false;
            case ANY_PLAN -> true;
            case PLAN_OF_ANOTHER_PHASE -> previous.timeoutContext.phase != timeoutContext.phase;
        };
    }

    public boolean isEqualSharePlan() {
        return timeoutContext.equalShare;
    }

    /// When `true`, recovery should use [SharedBlobCacheWarmingService#searchRecoveryWarmingListener]
    /// with [#timeout()] (which is then > 0).
    public boolean awaitWarming() {
        return timeout.millis() > 0;
    }
}
