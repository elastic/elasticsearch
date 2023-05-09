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
import org.elasticsearch.core.TimeValue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.NodesShutdownMetadata.getShutdownsOrEmpty;
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
    private final MasterServiceTaskQueue<RemoveSigtermShutdownTask> cleanupSigtermTaskQueue;

    public NodeSeenService(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.setSeenTaskQueue = clusterService.createTaskQueue(
            "shutdown-seen-nodes-updater",
            Priority.NORMAL,
            new SetSeenNodesShutdownExecutor()
        );
        this.cleanupSigtermTaskQueue = clusterService.createTaskQueue(
            "shutdown-sigterm-cleaner",
            Priority.NORMAL,
            new RemoveSigtermShutdownTaskExecutor(clusterService.threadPool()::absoluteTimeInMillis)
        );
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        var nodes = event.state().nodes();
        if (nodes.isLocalNodeElectedMaster() == false) {
            // Only do this if we're the current master node.
            return;
        }

        NodesShutdownMetadata eventShutdownMetadata = event.state().metadata().custom(NodesShutdownMetadata.TYPE);

        if (eventShutdownMetadata == null) {
            // Since there's no shutdown metadata at all, we know no shutdowns have ever been registered and we can bail.
            return;
        }

        final boolean sigtermNodesRemoved = eventShutdownMetadata.getAllNodeMetadataMap()
            .values()
            .stream()
            .filter(singleNodeShutdownMetadata -> singleNodeShutdownMetadata.getType() == SingleNodeShutdownMetadata.Type.SIGTERM)
            .map(SingleNodeShutdownMetadata::getNodeId)
            .anyMatch(nodeId -> nodes.nodeExists(nodeId) == false);

        if (sigtermNodesRemoved) {
            cleanupSigtermTaskQueue.submitTask("sigterm nodes left cluster", new RemoveSigtermShutdownTask(), null);
        }

        final boolean thisNodeJustBecameMaster = event.previousState().nodes().isLocalNodeElectedMaster() == false
            && nodes.isLocalNodeElectedMaster();
        if ((event.nodesAdded() || thisNodeJustBecameMaster) == false) {
            // If there's both 1) no new nodes this cluster state update and 2) this node has not just become the master node, nothing to do
            return;
        }

        final Set<String> nodesNotPreviouslySeen = eventShutdownMetadata.getAllNodeMetadataMap()
            .values()
            .stream()
            .filter(singleNodeShutdownMetadata -> singleNodeShutdownMetadata.getNodeSeen() == false)
            .map(SingleNodeShutdownMetadata::getNodeId)
            .filter(nodes::nodeExists)
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
            var shutdownMetadata = new HashMap<>(getShutdownsOrEmpty(initialState).getAllNodeMetadataMap());

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

            if (shutdownMetadata.equals(getShutdownsOrEmpty(initialState).getAllNodeMetadataMap())) {
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

    record RemoveSigtermShutdownTask() implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            logger.warn(() -> format("failed to remove shutdown metadata for sigtermed nodes"), e);
        }
    }

    record RemoveSigtermShutdownTaskExecutor(LongSupplier timeSupplier) implements ClusterStateTaskExecutor<RemoveSigtermShutdownTask> {
        RemoveSigtermShutdownTaskExecutor(LongSupplier timeSupplier) {
            this.timeSupplier = Objects.requireNonNull(timeSupplier);
        }

        @Override
        public ClusterState execute(BatchExecutionContext<RemoveSigtermShutdownTask> batchExecutionContext) throws Exception {
            // This makes it easier to test since there's nothing we need from the batchExecutionContext other than the initial state
            return removeStaleSigtermShutdowns(batchExecutionContext.initialState());
        }

        ClusterState removeStaleSigtermShutdowns(ClusterState initialState) {
            var shutdownMetadata = new HashMap<>(getShutdownsOrEmpty(initialState).getAllNodeMetadataMap());

            long now = timeSupplier.getAsLong();
            Predicate<String> nodeExists = initialState.nodes()::nodeExists;
            boolean modified = false;
            Iterator<SingleNodeShutdownMetadata> it = shutdownMetadata.values().iterator();
            while (it.hasNext()) {
                if (isSigtermShutdownStale(it.next(), now, nodeExists)) {
                    it.remove();
                    modified = true;
                }
            }
            if (modified == false) {
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

        /**
         * The {@link SingleNodeShutdownMetadata} is stale if the target node still exists, or the shutdown has been running for less time
         * than the grace period (plus safety factor).
         */
        static boolean isSigtermShutdownStale(SingleNodeShutdownMetadata shutdown, long nowMillis, Predicate<String> nodeExists) {
            if (shutdown.getType() != SingleNodeShutdownMetadata.Type.SIGTERM) {
                return false;
            }

            TimeValue grace = shutdown.getGracePeriod();
            if (grace == null) {
                return false;
            }
            if (nodeExists.test(shutdown.getNodeId())) {
                return false;
            }

            long timeRunning = nowMillis - shutdown.getStartedAtMillis();
            if (timeRunning < -1 * 60_000) {
                return true; // clock skew too big
            }
            long cleanupGrace = grace.millis() + (grace.millis() / 10);
            return cleanupGrace <= timeRunning;
        }
    }
}
