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
public record SearchRecoveryTimeout(TimeValue timeout, String timeoutContext) {

    public static SearchRecoveryTimeout skip() {
        return new SearchRecoveryTimeout(TimeValue.ZERO, "");
    }

    /// When `true`, recovery should use [SharedBlobCacheWarmingService#searchRecoveryWarmingListener]
    /// with [#timeout()] (which is then > 0).
    public boolean awaitWarming() {
        return timeout.millis() > 0;
    }
}
