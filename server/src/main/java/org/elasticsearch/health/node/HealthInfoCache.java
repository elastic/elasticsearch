/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.reservedstate.service.FileSettingsService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthInfo.INDETERMINATE;

/**
 * Keeps track of several health statuses per node that can be used in health.
 */
public class HealthInfoCache implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(HealthInfoCache.class);
    private volatile ConcurrentHashMap<String, DiskHealthInfo> diskInfoByNode = new ConcurrentHashMap<>();
    @Nullable
    private volatile DataStreamLifecycleHealthInfo dslHealthInfo = null;
    private volatile ConcurrentHashMap<String, RepositoriesHealthInfo> repositoriesInfoByNode = new ConcurrentHashMap<>();
    private volatile FileSettingsService.FileSettingsHealthInfo fileSettingsHealthInfo = INDETERMINATE;

    private HealthInfoCache() {}

    public static HealthInfoCache create(ClusterService clusterService) {
        HealthInfoCache healthInfoCache = new HealthInfoCache();
        clusterService.addListener(healthInfoCache);
        return healthInfoCache;
    }

    public void updateNodeHealth(
        String nodeId,
        @Nullable DiskHealthInfo diskHealthInfo,
        @Nullable DataStreamLifecycleHealthInfo latestDslHealthInfo,
        @Nullable RepositoriesHealthInfo repositoriesHealthInfo,
        @Nullable FileSettingsService.FileSettingsHealthInfo fileSettingsHealthInfo
    ) {
        if (diskHealthInfo != null) {
            diskInfoByNode.put(nodeId, diskHealthInfo);
        }
        if (latestDslHealthInfo != null) {
            dslHealthInfo = latestDslHealthInfo;
        }
        if (repositoriesHealthInfo != null) {
            repositoriesInfoByNode.put(nodeId, repositoriesHealthInfo);
        }
        if (fileSettingsHealthInfo != null) {
            this.fileSettingsHealthInfo = fileSettingsHealthInfo;
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentHealthNode = HealthNode.findHealthNode(event.state());
        DiscoveryNode localNode = event.state().nodes().getLocalNode();
        if (currentHealthNode != null && localNode.getId().equals(currentHealthNode.getId())) {
            if (event.nodesRemoved()) {
                for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                    diskInfoByNode.remove(removedNode.getId());
                    repositoriesInfoByNode.remove(removedNode.getId());
                }
            }
            // Resetting the cache is not synchronized for efficiency and simplicity.
            // Processing a delayed update after the cache has been emptied because
            // the node is not the health node anymore has small impact since it will
            // be reset in the next round again.
        } else if (diskInfoByNode.isEmpty() == false || dslHealthInfo != null || repositoriesInfoByNode.isEmpty() == false) {
            logger.debug("Node [{}][{}] is no longer the health node, emptying the cache.", localNode.getName(), localNode.getId());
            diskInfoByNode = new ConcurrentHashMap<>();
            dslHealthInfo = null;
            repositoriesInfoByNode = new ConcurrentHashMap<>();
            fileSettingsHealthInfo = INDETERMINATE;
        }
    }

    /**
     * This returns all the health info stored in this cache
     * @return A HealthInfo object wrapping all health data in the cache
     */
    public HealthInfo getHealthInfo() {
        // A shallow copy is enough because the inner data is immutable.
        return new HealthInfo(Map.copyOf(diskInfoByNode), dslHealthInfo, Map.copyOf(repositoriesInfoByNode), fileSettingsHealthInfo);
    }
}
