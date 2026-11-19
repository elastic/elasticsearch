/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster;

/**
 * Enables listening to master changes events of the local node (when the local node becomes the master, and when the local
 * node cease being a master).
 */
public interface LocalNodeMasterListener extends ClusterStateListener {

    /**
     * Called when the local node is elected to be the master.
     */
    void onMaster();

    /// Called when the local node has ceased to be the elected master of its term:
    /// * there is currently no master, in the (typically brief) period before the next election completes.
    /// * a new node was elected master and this node never applied a not-master state before it applied one from the new master.
    /// * immediately before invoking [#onMaster()], if the local node is re-elected in a new term without having first applied a state in
    ///   which it was not the master.
    void offMaster();

    @Override
    default void clusterChanged(ClusterChangedEvent event) {
        final boolean wasMaster = event.previousState().nodes().isLocalNodeElectedMaster();
        final boolean isMaster = event.localNodeMaster();
        if (event.isNewMaster()) {
            if (wasMaster) {
                offMaster();
            }
            onMaster();
        } else if (wasMaster && isMaster == false) {
            offMaster();
        }
    }
}
