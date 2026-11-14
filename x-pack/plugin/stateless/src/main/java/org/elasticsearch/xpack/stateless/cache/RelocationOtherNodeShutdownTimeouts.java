/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout.TimeoutContext;

/// The warming timeouts of search shards that relocate from a source node that is not shutting down while other nodes are, so that
/// [SearchRecoveryTimeoutCalculationService] reads as a choice of the timeout context.
class RelocationOtherNodeShutdownTimeouts {

    private volatile TimeValue searchRecoveryWarmingRelocationWithShutdownTimeout;

    RelocationOtherNodeShutdownTimeouts(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING,
            value -> this.searchRecoveryWarmingRelocationWithShutdownTimeout = value
        );
    }

    /// Returns the warming timeout for a shard relocating from a source that is not shutting down, while other nodes are: the plain
    /// relocation-with-shutdown timeout.
    SearchRecoveryTimeout timeout() {
        return new SearchRecoveryTimeout(
            searchRecoveryWarmingRelocationWithShutdownTimeout,
            TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT
        );
    }
}
