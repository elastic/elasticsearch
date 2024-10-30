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
 * Utility to access {@link ClusterState} only when it is "ready", where "ready" means initialized and recovered.
 */
public class SafeClusterStateSupplier implements ClusterStateSupplier, ClusterStateListener {
    private volatile ClusterState currentClusterState;

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (isInitialized() || event.state().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
            currentClusterState = event.state();
        }
    }

    private boolean isInitialized() {
        return currentClusterState != null;
    }

    @Override
    public Optional<ClusterState> getClusterStateIfReady() {
        return Optional.ofNullable(currentClusterState);
    }
}
