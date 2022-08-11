/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.node.selection.HealthNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps track of several health statuses per node that can be used in health.
 */
public class HealthInfoCache implements ClusterStateListener {

    private volatile ConcurrentHashMap<String, DiskHealthInfo> diskInfoByNode = new ConcurrentHashMap<>();

    public HealthInfoCache(ClusterService clusterService) {
        clusterService.addListener(this);
    }

    public void updateNodeHealth(String nodeId, DiskHealthInfo diskHealthInfo) {
        diskInfoByNode.put(nodeId, diskHealthInfo);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentHealthNode = HealthNode.findHealthNode(event.state());
        DiscoveryNode localNode = event.state().nodes().getLocalNode();
        if (localNode.equals(currentHealthNode)) {
            if (event.nodesRemoved()) {
                for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                    diskInfoByNode.remove(removedNode.getId());
                }
            }
            // Resetting the cache is not synchronized for efficiency and simplicity.
            // Processing a delayed update after the cache has been emptied because
            // the node is not the health node anymore has small impact since it will
            // be reset in the next round again.
        } else if (diskInfoByNode.isEmpty() == false) {
            diskInfoByNode = new ConcurrentHashMap<>();
        }
    }

    // A shallow copy is enough because the inner data is immutable.
    public Map<String, DiskHealthInfo> getDiskHealthInfo() {
        return Map.copyOf(diskInfoByNode);
    }
}
