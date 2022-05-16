/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;

/**
 * The {@link NodeAndClusterIdStateListener} listens to cluster state changes and ONLY when receives the first update
 * it sets the clusterUUID and nodeID in log4j pattern converter {@link NodeIdConverter}.
 * Once the first update is received, it will automatically be de-registered from subsequent updates.
 */
public class NodeAndClusterIdStateListener implements ClusterStateObserver.Listener {
    private static final Logger logger = LogManager.getLogger(NodeAndClusterIdStateListener.class);
    static final SetOnce<Tuple<String, String>> nodeAndClusterId = new SetOnce<>();

    private NodeAndClusterIdStateListener() {}

    /**
     * Subscribes for the first cluster state update where nodeId and clusterId is present
     * and sets these values in {@link NodeIdConverter}.
     */
    public static void getAndSetNodeIdAndClusterId(ClusterService clusterService, ThreadContext threadContext) {
        ClusterState clusterState = clusterService.state();
        ClusterStateObserver observer = new ClusterStateObserver(clusterState, clusterService, null, logger, threadContext);

        observer.waitForNextChange(new NodeAndClusterIdStateListener(), NodeAndClusterIdStateListener::isNodeAndClusterIdPresent);
    }

    private static boolean isNodeAndClusterIdPresent(ClusterState clusterState) {
        return getNodeId(clusterState) != null && getClusterUUID(clusterState) != null;
    }

    private static String getClusterUUID(ClusterState state) {
        return state.getMetadata().clusterUUID();
    }

    private static String getNodeId(ClusterState state) {
        return state.getNodes().getLocalNodeId();
    }

    @Override
    public void onNewClusterState(ClusterState state) {
        String nodeId = getNodeId(state);
        String clusterUUID = getClusterUUID(state);

        logger.debug("Received cluster state update. Setting nodeId=[{}] and clusterUuid=[{}]", nodeId, clusterUUID);
        setNodeIdAndClusterId(nodeId, clusterUUID);
    }

    static void setNodeIdAndClusterId(String nodeId, String clusterUUID) {
        nodeAndClusterId.set(Tuple.tuple(nodeId, clusterUUID));
    }

    @Override
    public void onClusterServiceClose() {}

    @Override
    public void onTimeout(TimeValue timeout) {}
}
