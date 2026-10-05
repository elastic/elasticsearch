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
/// [#totalBudget]: when greater than zero, caps the total accumulated timeout across all re-evaluation slices.
///
/// [#extendable]: when `false`, the re-evaluation loop fires the race immediately on first expiry without
/// re-reading the cluster state or rescheduling, regardless of the node-level re-evaluation setting.
public record SearchRecoveryTimeout(TimeValue timeout, String timeoutContext, TimeValue totalBudget, boolean extendable) {

    public static SearchRecoveryTimeout skip() {
        return new SearchRecoveryTimeout(TimeValue.ZERO, "", TimeValue.ZERO, false);
    }

    public static SearchRecoveryTimeout fixed(TimeValue timeout, String timeoutContext) {
        return new SearchRecoveryTimeout(timeout, timeoutContext, TimeValue.ZERO, false);
    }

    public static SearchRecoveryTimeout extendable(TimeValue timeout, String timeoutContext, TimeValue totalBudget) {
        return new SearchRecoveryTimeout(timeout, timeoutContext, totalBudget, true);
    }

    /// When `true`, recovery should use [SharedBlobCacheWarmingService#searchRecoveryWarmingListener]
    /// with [#timeout()] (which is then > 0).
    public boolean awaitWarming() {
        return timeout.millis() > 0;
    }

    private boolean considerTotalBudget() {
        return totalBudget.millis() > 0;
    }

    public TimeValue timeoutCappedToTotalBudget(SearchRecoveryTimeout initialPlan, TimeValue timeElapsed) {
        if (initialPlan.considerTotalBudget() == false) {
            return timeout;
        }
        final long budgetLeftMs = Math.max(0L, initialPlan.totalBudget.millis() - timeElapsed.millis());
        if (budgetLeftMs >= timeout.millis()) {
            return timeout;
        }
        return TimeValue.timeValueMillis(budgetLeftMs);
    }
}
