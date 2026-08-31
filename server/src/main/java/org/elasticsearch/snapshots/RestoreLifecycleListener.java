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
 * Callback interface for the two atomic lifecycle events produced by {@link RestoreService}:
 * restore initialization and restore completion. Both methods run inside a master-service
 * cluster-state update and must return the (possibly modified) cluster state to publish.
 * Returning the input state unchanged is always safe.
 *
 * <p>Both events are keyed by a {@link RestoreInProgress.Entry}: a restore that installs no
 * entry (e.g. a metadata-only restore with no shards) produces no lifecycle events. Every
 * initialized restore therefore receives exactly one completion event.
 *
 * <p>The implementation is expected to be the persistent-task executor that coordinates
 * durable recovery checkpointing. It receives the exact entry so it can determine whether
 * the event belongs to the recovery it owns (via {@link RestoreInProgress.Entry#uuid()})
 * without scanning {@link RestoreInProgress}.
 */
public interface RestoreLifecycleListener {

    /**
     * Called atomically within the cluster-state update that installs a new
     * {@link RestoreInProgress} entry. The entry is already present in {@code state}.
     *
     * @param entry the newly installed restore entry
     * @param state cluster state after {@link RestoreInProgress} installation
     * @return the cluster state to publish; must not be {@code null}
     */
    ClusterState onRestoreInitialized(RestoreInProgress.Entry entry, ClusterState state);

    /**
     * Called atomically within the cluster-state update that removes a completed
     * {@link RestoreInProgress} entry. The entry is still present in {@code state} when
     * this method is called; removal happens after this method returns.
     *
     * @param entry the completed restore entry; its {@link RestoreInProgress.Entry#state()}
     *              is terminal ({@link RestoreInProgress.State#SUCCESS} or
     *              {@link RestoreInProgress.State#FAILURE})
     * @param state cluster state with the completed entry still present
     * @return the cluster state to publish; must not be {@code null}
     */
    ClusterState onRestoreCompleted(RestoreInProgress.Entry entry, ClusterState state);

    /** No-op implementation used as the default before any listener is registered. */
    RestoreLifecycleListener NOOP = new RestoreLifecycleListener() {
        @Override
        public ClusterState onRestoreInitialized(RestoreInProgress.Entry entry, ClusterState state) {
            return state;
        }

        @Override
        public ClusterState onRestoreCompleted(RestoreInProgress.Entry entry, ClusterState state) {
            return state;
        }
    };
}
