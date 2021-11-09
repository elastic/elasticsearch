/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexClusterStateUpdateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsPending;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotsService;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY;

/**
 * Deletes indices.
 */
public class MetadataDeleteIndexService {

    private static final Logger logger = LogManager.getLogger(MetadataDeleteIndexService.class);

    private final Settings settings;
    private final ClusterService clusterService;
    private final AllocationService allocationService;

    @Inject
    public MetadataDeleteIndexService(Settings settings, ClusterService clusterService, AllocationService allocationService) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
    }

    public void deleteIndices(final DeleteIndexClusterStateUpdateRequest request, final ActionListener<AcknowledgedResponse> listener) {
        if (request.indices() == null || request.indices().length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        clusterService.submitStateUpdateTask(
            "delete-index " + Arrays.toString(request.indices()),
            new AckedClusterStateUpdateTask(Priority.URGENT, request, listener) {
                @Override
                public ClusterState execute(final ClusterState currentState) {
                    return deleteIndices(currentState, Sets.newHashSet(request.indices()));
                }
            }
        );
    }

    /**
     * Delete some indices from the cluster state.
     */
    public ClusterState deleteIndices(ClusterState currentState, Set<Index> indices) {
        final Metadata meta = currentState.metadata();
        final Set<Index> indicesToDelete = new HashSet<>();
        final Map<Index, DataStream> backingIndices = new HashMap<>();
        for (Index index : indices) {
            IndexMetadata im = meta.getIndexSafe(index);
            IndexAbstraction.DataStream parent = meta.getIndicesLookup().get(im.getIndex().getName()).getParentDataStream();
            if (parent != null) {
                if (parent.getWriteIndex().equals(im.getIndex())) {
                    throw new IllegalArgumentException(
                        "index ["
                            + index.getName()
                            + "] is the write index for data stream ["
                            + parent.getName()
                            + "] and cannot be deleted"
                    );
                } else {
                    backingIndices.put(index, parent.getDataStream());
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
            if (backingIndices.containsKey(index)) {
                DataStream parent = metadataBuilder.dataStream(backingIndices.get(index).getName());
                metadataBuilder.put(parent.removeBackingIndex(index));
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

        final ClusterState.Builder builder = ClusterState.builder(currentState)
            .routingTable(routingTableBuilder.build())
            .blocks(clusterBlocksBuilder.build())
            .metadata(metadataBuilder.build());

        ImmutableOpenMap.Builder<String, ClusterState.Custom> customBuilder = null;

        // update snapshot restore entries
        final RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY);
        RestoreInProgress updatedRestoreInProgress = RestoreService.updateRestoreStateWithDeletedIndices(restoreInProgress, indices);
        if (updatedRestoreInProgress != restoreInProgress) {
            customBuilder = ImmutableOpenMap.builder(currentState.getCustoms());
            customBuilder.put(RestoreInProgress.TYPE, updatedRestoreInProgress);
        }

        // update snapshot(s) marked as to delete
        final SnapshotDeletionsPending deletionsInPending = currentState.custom(
            SnapshotDeletionsPending.TYPE,
            SnapshotDeletionsPending.EMPTY
        );
        final SnapshotDeletionsPending updatedPendingDeletes = updateSnapshotDeletionsPending(deletionsInPending, indicesToDelete, meta);
        if (updatedPendingDeletes != deletionsInPending) {
            if (customBuilder == null) {
                customBuilder = ImmutableOpenMap.builder(currentState.getCustoms());
            }
            customBuilder.put(SnapshotDeletionsPending.TYPE, updatedPendingDeletes);
        }
        if (customBuilder != null) {
            builder.customs(customBuilder.build());
        }
        return allocationService.reroute(builder.build(), "deleted indices [" + indices + "]");
    }

    private SnapshotDeletionsPending updateSnapshotDeletionsPending(
        final SnapshotDeletionsPending pendingDeletions,
        final Set<Index> indicesToDelete,
        final Metadata metadata
    ) {
        final long timestamp = Instant.now().toEpochMilli();
        SnapshotDeletionsPending.Builder builder = null;
        boolean changed = false;

        for (Index indexToDelete : indicesToDelete) {
            final Settings indexSettings = metadata.getIndexSafe(indexToDelete).getSettings();
            if (SearchableSnapshotsSettings.isSearchableSnapshotStore(indexSettings) == false) {
                continue; // not a searchable snapshot index
            }
            if (indexSettings.getAsBoolean(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION, false) == false) {
                continue; // do not delete the snapshot when this searchable snapshot index is deleted
            }
            final SnapshotId snapshotId = new SnapshotId(
                indexSettings.get(SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME_SETTING_KEY),
                indexSettings.get(SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY)
            );
            boolean canDeleteSnapshot = true;
            for (IndexMetadata other : metadata) {
                if (indexToDelete.equals(other.getIndex())) {
                    continue; // do not check against itself
                }
                final Settings otherSettings = other.getSettings();
                if (SearchableSnapshotsSettings.isSearchableSnapshotStore(otherSettings) == false) {
                    continue; // other index is not a searchable snapshot index, skip
                }
                final String otherSnapshotUuid = otherSettings.get(SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY);
                if (Objects.equals(snapshotId.getUUID(), otherSnapshotUuid) == false) {
                    continue; // other index is backed by a different snapshot, skip
                }
                assert otherSettings.getAsBoolean(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION, false) : other;
                if (indicesToDelete.contains(other.getIndex())) {
                    continue; // other index is going to be deleted as part of the same cluster state update
                }
                logger.debug(
                    "snapshot [{}] cannot be marked as to delete, another index [{}] is using the snapshot",
                    snapshotId,
                    other.getIndex()
                );
                canDeleteSnapshot = false; // another index is using the same snapshot, do not delete the snapshot
                break;
            }
            if (canDeleteSnapshot) {
                if (builder == null) {
                    final int maxPendingDeletions = SnapshotDeletionsPending.MAX_PENDING_DELETIONS_SETTING.get(settings);
                    builder = new SnapshotDeletionsPending.Builder(
                        pendingDeletions,
                        evicted -> logger.warn(
                            () -> new ParameterizedMessage(
                                "maximum number of snapshots [{}] awaiting deletion has been reached in "
                                    + "cluster state before snapshot [{}] deleted on [{}] in repository [{}/{}] could be deleted",
                                maxPendingDeletions,
                                evicted.getSnapshotId(),
                                Instant.ofEpochMilli(evicted.getIndexDeletionTime()).atZone(ZoneOffset.UTC),
                                evicted.getRepositoryName(),
                                evicted.getRepositoryUuid()
                            )
                        )
                    );
                }
                builder.add(
                    indexSettings.get(SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY),
                    indexSettings.get(SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY, RepositoryData.MISSING_UUID),
                    snapshotId,
                    timestamp
                );
                changed = true;
            }
        }
        if (changed) {
            return builder.build(settings);
        }
        return pendingDeletions;
    }
}
