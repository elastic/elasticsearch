/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

/**
 * Enables listening to master changes events of the local node (when the local node becomes the master, and when the local
 * node cease being a master).
 */
public interface LocalNodeMasterListener extends ClusterStateListener {

    /**
     * Called when local node is elected to be the master
     */
    void onMaster();

    /**
     * Called when the local node used to be the master, a new master was elected and it's no longer the local node.
     */
    void offMaster();

    @Override
    default void clusterChanged(ClusterChangedEvent event) {
        final boolean wasMaster = event.previousState().nodes().isLocalNodeElectedMaster();
        final boolean isMaster = event.localNodeMaster();
        if (wasMaster == false && isMaster) {
            onMaster();
        } else if (wasMaster && isMaster == false) {
            offMaster();
        }
    }
}

