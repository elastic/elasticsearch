/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.cluster.service.MasterService;

import java.util.List;

public interface ClusterStateTaskListener {

    /**
     * A callback called when execute fails.
     */
    void onFailure(String source, Exception e);

    /**
     * called when the task was rejected because the local node is no longer master.
     * Used only for tasks submitted to {@link MasterService}.
     */
    default void onNoLongerMaster(String source) {
        onFailure(source, new NotMasterException("no longer master. source: [" + source + "]"));
    }

    /**
     * Called when the result of the {@link ClusterStateTaskExecutor#execute(ClusterState, List)} have been processed
     * properly by all listeners.
     */
    default void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
    }
}
