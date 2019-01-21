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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;

/**
 *  The {@link NodeAndClusterIdStateListener} listens to cluster state changes and ONLY when receives the first update
 *  it sets the clusterUUID and nodeID in log4j pattern converter {@link NodeAndClusterIdConverter}
 */
public class NodeAndClusterIdStateListener implements ClusterStateListener {
    private final Logger logger = LogManager.getLogger(NodeAndClusterIdStateListener.class);

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode localNode = event.state().getNodes().getLocalNode();
        String clusterUUID = event.state().getMetaData().clusterUUID();
        String nodeId = localNode.getId();

        boolean wasSet = NodeAndClusterIdConverter.setOnce(nodeId, clusterUUID);

        if (wasSet) {
            logger.debug("Received first cluster state update. Setting nodeId=[{}] and clusterUuid=[{}]", nodeId, clusterUUID);
        }
    }
}
