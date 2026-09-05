/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;

/**
 * Callbacks for restore initialization and completion, invoked by {@link RestoreService} inside
 * master-service cluster-state updates.
 */
public interface RestoreLifecycleListener {

    /**
     * Called within the cluster-state update that installs a new {@link RestoreInProgress} entry.
     * Defaults to returning {@code state} unchanged.
     *
     * @param entry the newly installed restore entry, already present in {@code state}
     * @param state cluster state after {@link RestoreInProgress} installation
     * @return the cluster state to publish; must not be {@code null}
     */
    default ClusterState onRestoreInitialized(RestoreInProgress.Entry entry, ClusterState state) {
        return state;
    }

    /**
     * Called within the cluster-state update that removes a completed {@link RestoreInProgress} entry.
     * Defaults to returning {@code state} unchanged.
     *
     * @param entry the completed restore entry, still present in {@code state}; its
     *              {@link RestoreInProgress.Entry#state()} is terminal
     * @param state cluster state with the completed entry still present
     * @return the cluster state to publish; must not be {@code null}
     */
    default ClusterState onRestoreCompleted(RestoreInProgress.Entry entry, ClusterState state) {
        return state;
    }

    /** No-op implementation used as the default before any listener is registered. */
    RestoreLifecycleListener NOOP = new RestoreLifecycleListener() {};
}
