/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.shards;

import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.health.node.tracker.HealthTracker;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;

import java.util.function.Supplier;

/// A [HealthTracker] which captures the [LocalRecoveryRetriesHealthInfo] for a node.
public class LocalRecoveryRetriesHealthTracker extends HealthTracker<LocalRecoveryRetriesHealthInfo> {

    // TODO: Resolve any potential discrepancies associated with the fact that the health indicator consumes localRetries the same as
    // failedAllocations – but, at time of writing, the former is reset by direct cancellation and the latter is not.
    // See elastic/elasticsearch-team#5557

    private final Supplier<IndicesClusterStateService> indicesClusterStateService;

    public LocalRecoveryRetriesHealthTracker(Supplier<IndicesClusterStateService> indicesClusterStateService) {
        this.indicesClusterStateService = indicesClusterStateService;
    }

    @Override
    protected LocalRecoveryRetriesHealthInfo determineCurrentHealth() {
        return new LocalRecoveryRetriesHealthInfo(indicesClusterStateService.get().getLocalRecoveryRetryCounts());
    }

    @Override
    protected void addToRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, LocalRecoveryRetriesHealthInfo healthInfo) {
        builder.localRecoveryRetriesHealthInfo(healthInfo);
    }
}
