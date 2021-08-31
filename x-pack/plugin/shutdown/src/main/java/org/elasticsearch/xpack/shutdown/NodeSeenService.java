/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A class that handles ongoing reactive logic related to Node Shutdown.
 *
 * Currently, this consists of keeping track of whether we've seen nodes which are marked for shutdown.
 */
public class NodeSeenService implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(NodeSeenService.class);

    final ClusterService clusterService;

    public NodeSeenService(ClusterService clusterService) {
        this.clusterService = clusterService;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().nodes().isLocalNodeElectedMaster() == false) {
            // Only do this if we're the current master node.
            return;
        }

        if (event.nodesAdded() == false) {
            // If there's no new nodes this cluster state update, nothing to do.
            return;
        }

        NodesShutdownMetadata eventShutdownMetadata = event.state().metadata().custom(NodesShutdownMetadata.TYPE);

        if (eventShutdownMetadata == null) {
            // Since there's no shutdown metadata at all, we know no shutdowns have ever been registered and we can bail.
            return;
        }

        final Set<String> nodesNotPreviouslySeen = eventShutdownMetadata.getAllNodeMetadataMap()
            .values()
            .stream()
            .filter(singleNodeShutdownMetadata -> singleNodeShutdownMetadata.getNodeSeen() == false)
            .map(SingleNodeShutdownMetadata::getNodeId)
            .filter(nodeId -> event.state().nodes().nodeExists(nodeId))
            .collect(Collectors.toSet());

        if (nodesNotPreviouslySeen.isEmpty() == false) {
            clusterService.submitStateUpdateTask("shutdown-seen-nodes-updater", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    NodesShutdownMetadata currentShutdownMetadata = currentState.metadata().custom(NodesShutdownMetadata.TYPE);

                    final Map<String, SingleNodeShutdownMetadata> newShutdownMetadataMap = currentShutdownMetadata.getAllNodeMetadataMap()
                        .values()
                        .stream()
                        .map(singleNodeShutdownMetadata -> {
                            if (nodesNotPreviouslySeen.contains(singleNodeShutdownMetadata.getNodeId())
                                || currentState.nodes().nodeExists(singleNodeShutdownMetadata.getNodeId())) {
                                return SingleNodeShutdownMetadata.builder(singleNodeShutdownMetadata).setNodeSeen(true).build();
                            }
                            return singleNodeShutdownMetadata;
                        })
                        .collect(Collectors.toMap(SingleNodeShutdownMetadata::getNodeId, Function.identity()));

                    final NodesShutdownMetadata newNodesMetadata = new NodesShutdownMetadata(newShutdownMetadataMap);
                    if (newNodesMetadata.equals(currentShutdownMetadata)) {
                        // Turns out the update was a no-op
                        return currentState;
                    }

                    return ClusterState.builder(currentState)
                        .metadata(Metadata.builder(currentState.metadata()).putCustom(NodesShutdownMetadata.TYPE, newNodesMetadata).build())
                        .build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(new ParameterizedMessage("failed to mark shutting down nodes as seen: {}", nodesNotPreviouslySeen), e);
                }
            });
        }
    }
}
