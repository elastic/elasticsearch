/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

/**
 * A class that handles ongoing reactive logic related to Node Shutdown.
 *
 * Currently, this consists of keeping track of whether we've seen nodes which are marked for shutdown.
 */
public class NodeSeenService implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(NodeSeenService.class);

    final ClusterService clusterService;

    private final MasterServiceTaskQueue<SetSeenNodesShutdownTask> setSeenTaskQueue;

    public NodeSeenService(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.setSeenTaskQueue = clusterService.createTaskQueue(
            "shutdown-seen-nodes-updater",
            Priority.NORMAL,
            new SetSeenNodesShutdownExecutor()
        );
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().nodes().isLocalNodeElectedMaster() == false) {
            // Only do this if we're the current master node.
            return;
        }

        final boolean thisNodeJustBecameMaster = event.previousState().nodes().isLocalNodeElectedMaster() == false
            && event.state().nodes().isLocalNodeElectedMaster();
        if ((event.nodesAdded() || thisNodeJustBecameMaster) == false) {
            // If there's both 1) no new nodes this cluster state update and 2) this node has not just become the master node, nothing to do
            return;
        }

        NodesShutdownMetadata eventShutdownMetadata = event.state().metadata().custom(NodesShutdownMetadata.TYPE);

        if (eventShutdownMetadata == null) {
            // Since there's no shutdown metadata at all, we know no shutdowns have ever been registered and we can bail.
            return;
        }

        final Set<String> nodesNotPreviouslySeen = eventShutdownMetadata.getAll()
            .values()
            .stream()
            .filter(singleNodeShutdownMetadata -> singleNodeShutdownMetadata.getNodeSeen() == false)
            .map(SingleNodeShutdownMetadata::getNodeId)
            .filter(nodeId -> event.state().nodes().nodeExists(nodeId))
            .collect(Collectors.toUnmodifiableSet());

        if (nodesNotPreviouslySeen.isEmpty() == false) {
            setSeenTaskQueue.submitTask("saw new nodes", new SetSeenNodesShutdownTask(nodesNotPreviouslySeen), null);
        }
    }

    record SetSeenNodesShutdownTask(Set<String> nodesNotPreviouslySeen) implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            logger.warn(() -> format("failed to mark shutting down nodes as seen: %s", nodesNotPreviouslySeen), e);
        }
    }

    private static class SetSeenNodesShutdownExecutor implements ClusterStateTaskExecutor<SetSeenNodesShutdownTask> {

        @Override
        public ClusterState execute(BatchExecutionContext<SetSeenNodesShutdownTask> batchExecutionContext) throws Exception {
            final var initialState = batchExecutionContext.initialState();
            var shutdownMetadata = new HashMap<>(initialState.metadata().nodeShutdowns().getAll());

            var nodesNotPreviouslySeen = new HashSet<>();
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                nodesNotPreviouslySeen.addAll(taskContext.getTask().nodesNotPreviouslySeen());
            }

            var nodes = initialState.nodes();
            shutdownMetadata.replaceAll((k, v) -> {
                if (v.getNodeSeen() == false && (nodesNotPreviouslySeen.contains(v.getNodeId()) || nodes.nodeExists(v.getNodeId()))) {
                    return SingleNodeShutdownMetadata.builder(v).setNodeSeen(true).build();
                }
                return v;
            });

            if (shutdownMetadata.equals(initialState.metadata().nodeShutdowns().getAll())) {
                return initialState;
            }

            return ClusterState.builder(initialState)
                .metadata(
                    Metadata.builder(initialState.metadata())
                        .putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(shutdownMetadata))
                        .build()
                )
                .build();
        }
    }
}
