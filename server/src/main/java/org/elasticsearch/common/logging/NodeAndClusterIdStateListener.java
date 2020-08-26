/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;

/**
 * The {@link NodeAndClusterIdStateListener} listens to cluster state changes and ONLY when receives the first update
 * it sets the clusterUUID and nodeID in log4j pattern converter {@link NodeIdConverter}.
 * Once the first update is received, it will automatically be de-registered from subsequent updates.
 */
public class NodeAndClusterIdStateListener implements ClusterStateObserver.Listener {
    private static final Logger logger = LogManager.getLogger(NodeAndClusterIdStateListener.class);
    static final SetOnce<Tuple<String,String>> nodeAndClusterId = new SetOnce<>();

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

    void setNodeIdAndClusterId(String nodeId, String clusterUUID){
        nodeAndClusterId.set(Tuple.tuple(nodeId,clusterUUID));
    }

    @Override
    public void onClusterServiceClose() {}

    @Override
    public void onTimeout(TimeValue timeout) {}
}
