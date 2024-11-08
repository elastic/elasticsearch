/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotsService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener.rerouteCompletionIsNotRequired;

/**
 * Deletes indices.
 */
public class MetadataDeleteIndexService {

    private static final Logger logger = LogManager.getLogger(MetadataDeleteIndexService.class);

    // package private for tests
    final ClusterStateTaskExecutor<DeleteIndicesClusterStateUpdateTask> executor;
    private final MasterServiceTaskQueue<DeleteIndicesClusterStateUpdateTask> taskQueue;

    @Inject
    public MetadataDeleteIndexService(Settings settings, ClusterService clusterService, AllocationService allocationService) {
        executor = new SimpleBatchedAckListenerTaskExecutor<>() {
            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(
                DeleteIndicesClusterStateUpdateTask task,
                ClusterState clusterState
            ) {
                return Tuple.tuple(MetadataDeleteIndexService.deleteIndices(clusterState, task.indices, settings), task);
            }

            @Override
            public ClusterState afterBatchExecution(ClusterState clusterState, boolean clusterStateChanged) {
                if (clusterStateChanged) {
                    return allocationService.reroute(
                        clusterState,
                        "deleted indices",
                        rerouteCompletionIsNotRequired() // it is not required to balance shard to report index deletion success
                    );
                }
                return clusterState;
            }
        };
        taskQueue = clusterService.createTaskQueue("delete-index", Priority.URGENT, executor);
    }

    public void deleteIndices(
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        Set<Index> indices,
        ActionListener<AcknowledgedResponse> listener
    ) {
        if (indices == null || indices.isEmpty()) {
            throw new IllegalArgumentException("Indices are required");
        }
        taskQueue.submitTask(
            "delete-index " + indices,
            new DeleteIndicesClusterStateUpdateTask(indices, ackTimeout, listener),
            masterNodeTimeout
        );
    }

    // package private for tests
    static class DeleteIndicesClusterStateUpdateTask implements ClusterStateTaskListener, ClusterStateAckListener {

        private final Set<Index> indices;
        private final TimeValue ackTimeout;
        private final ActionListener<AcknowledgedResponse> listener;

        DeleteIndicesClusterStateUpdateTask(Set<Index> indices, TimeValue ackTimeout, ActionListener<AcknowledgedResponse> listener) {
            this.indices = Objects.requireNonNull(indices);
            this.ackTimeout = Objects.requireNonNull(ackTimeout);
            this.listener = Objects.requireNonNull(listener);
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return true;
        }

        @Override
        public void onAllNodesAcked() {
            listener.onResponse(AcknowledgedResponse.TRUE);
        }

        @Override
        public void onAckFailure(Exception e) {
            listener.onResponse(AcknowledgedResponse.FALSE);
        }

        @Override
        public void onAckTimeout() {
            listener.onResponse(AcknowledgedResponse.FALSE);
        }

        @Override
        public TimeValue ackTimeout() {
            return ackTimeout;
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Delete some indices from the cluster state.
     */
    public static ClusterState deleteIndices(ClusterState currentState, Set<Index> indices, Settings settings) {
        final Metadata meta = currentState.metadata();
        final Set<Index> indicesToDelete = new HashSet<>();
        final Map<Index, DataStream> dataStreamIndices = new HashMap<>();
        for (Index index : indices) {
            IndexMetadata im = meta.getIndexSafe(index);
            DataStream parent = meta.getIndicesLookup().get(im.getIndex().getName()).getParentDataStream();
            if (parent != null) {
                boolean isFailureStoreWriteIndex = im.getIndex().equals(parent.getFailureStoreWriteIndex());
                if (isFailureStoreWriteIndex || im.getIndex().equals(parent.getWriteIndex())) {
                    throw new IllegalArgumentException(
                        "index ["
                            + index.getName()
                            + "] is the "
                            + (isFailureStoreWriteIndex ? "failure store " : "")
                            + "write index for data stream ["
                            + parent.getName()
                            + "] and cannot be deleted"
                    );
                } else {
                    dataStreamIndices.put(index, parent);
                }
            }
            indicesToDelete.add(im.getIndex());
        }

        // Check if index deletion conflicts with any running snapshots
        Set<Index> snapshottingIndices = SnapshotsService.snapshottingIndices(currentState, indicesToDelete);
        if (snapshottingIndices.isEmpty() == false) {
            throw new SnapshotInProgressException(
                "Cannot delete indices that are being snapshotted: "
                    + snapshottingIndices
                    + ". Try again after snapshot finishes or cancel the currently running snapshot."
            );
        }

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        Metadata.Builder metadataBuilder = Metadata.builder(meta);
        ClusterBlocks.Builder clusterBlocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());

        final IndexGraveyard.Builder graveyardBuilder = IndexGraveyard.builder(metadataBuilder.indexGraveyard());
        final int previousGraveyardSize = graveyardBuilder.tombstones().size();
        for (final Index index : indices) {
            String indexName = index.getName();
            logger.info("{} deleting index", index);
            routingTableBuilder.remove(indexName);
            clusterBlocksBuilder.removeIndexBlocks(indexName);
            metadataBuilder.remove(indexName);
            if (dataStreamIndices.containsKey(index)) {
                DataStream parent = metadataBuilder.dataStream(dataStreamIndices.get(index).getName());
                if (parent.isFailureStoreIndex(index.getName())) {
                    metadataBuilder.put(parent.removeFailureStoreIndex(index));
                } else {
                    metadataBuilder.put(parent.removeBackingIndex(index));
                }
            }
        }
        // add tombstones to the cluster state for each deleted index
        final IndexGraveyard currentGraveyard = graveyardBuilder.addTombstones(indices).build(settings);
        metadataBuilder.indexGraveyard(currentGraveyard); // the new graveyard set on the metadata
        logger.trace(
            "{} tombstones purged from the cluster state. Previous tombstone size: {}. Current tombstone size: {}.",
            graveyardBuilder.getNumPurged(),
            previousGraveyardSize,
            currentGraveyard.getTombstones().size()
        );

        Metadata newMetadata = metadataBuilder.build();
        ClusterBlocks blocks = clusterBlocksBuilder.build();

        // update snapshot restore entries
        Map<String, ClusterState.Custom> customs = currentState.getCustoms();
        final RestoreInProgress restoreInProgress = RestoreInProgress.get(currentState);
        RestoreInProgress updatedRestoreInProgress = RestoreService.updateRestoreStateWithDeletedIndices(restoreInProgress, indices);
        if (updatedRestoreInProgress != restoreInProgress) {
            ImmutableOpenMap.Builder<String, ClusterState.Custom> builder = ImmutableOpenMap.builder(customs);
            builder.put(RestoreInProgress.TYPE, updatedRestoreInProgress);
            customs = builder.build();
        }

        return ClusterState.builder(currentState)
            .routingTable(routingTableBuilder.build())
            .metadata(newMetadata)
            .blocks(blocks)
            .customs(customs)
            .build();
    }
}
