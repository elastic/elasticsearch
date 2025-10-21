/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import java.util.Optional;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

/**
 * Utility to access {@link ClusterState} only when it is "ready", where "ready" means that we received a first clusterChanged event
 * with no global block of type {@code STATE_NOT_RECOVERED_BLOCK}
 * This guarantees that:
 * - the initial cluster state has been set (see
 * {@link org.elasticsearch.cluster.service.ClusterApplierService#setInitialState(ClusterState)});
 * - the initial recovery process has completed.
 */
public class SafeClusterStateSupplier implements ClusterStateSupplier, ClusterStateListener {
    private volatile ClusterState currentClusterState;

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // In this default implementation, "ready" is really "is cluster state available", which after the initial recovery it should be.
        // If you need a different condition, feel free to add a different implementation of ClusterStateSupplier
        if (isInitialized() || event.state().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
            currentClusterState = event.state();
        }
    }

    private boolean isInitialized() {
        return currentClusterState != null;
    }

    @Override
    public Optional<ClusterState> get() {
        return Optional.ofNullable(currentClusterState);
    }
}
