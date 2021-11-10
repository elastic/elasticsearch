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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY;
import static org.elasticsearch.snapshots.SnapshotsService.findRepositoryForPendingDeletion;

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
        final SnapshotDeletionsPending updatedPendingDeletes = updateSnapshotDeletionsPending(
            deletionsInPending,
            indicesToDelete,
            currentState
        );
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

    /**
     * This method updates the list of snapshots marked as to be deleted if one or more searchable snapshots are deleted.
     *
     * The snapshots cannot be deleted at the same time of the searchable snapshots indices because deleting one or more snapshot requires a
     * consistent view of their repositories data, and getting the consistent views cannot be done in the same cluster state update. It is
     * also possible than one (or more) snapshot cannot be deleted immediately because the snapshot is involved in another restore or
     * cloning or the repository might not be writeable etc. To address those conflicting situations this method only captures the snapshot
     * information that are required to later delete the snapshot and stores them in a {@link SnapshotDeletionsPending.Entry} in cluster
     * state. Once a snapshot is pending deletion it cannot be restored, mounted or cloned. If the snapshot pending deletion is involved in
     * a snapshot operation at the time it is deleted then the deletion will happen once the conflicting operation is terminated.
     */
    private SnapshotDeletionsPending updateSnapshotDeletionsPending(
        final SnapshotDeletionsPending pendingDeletions,
        final Set<Index> indicesToDelete,
        final ClusterState state
    ) {
        final List<Settings> deletedIndicesSettings = indicesToDelete.stream()
            .map(index -> state.metadata().getIndexSafe(index).getSettings())
            .filter(SearchableSnapshotsSettings::isSearchableSnapshotStore)
            .filter(indexSettings -> indexSettings.getAsBoolean(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION, false))
            .collect(Collectors.toList());
        if (deletedIndicesSettings.isEmpty()) {
            return pendingDeletions;
        }

        final Set<SnapshotId> activeSearchableSnapshots = state.metadata()
            .indices()
            .stream()
            .map(Map.Entry::getValue)
            .filter(index -> indicesToDelete.contains(index.getIndex()) == false)
            .map(IndexMetadata::getSettings)
            .filter(SearchableSnapshotsSettings::isSearchableSnapshotStore)
            .map(MetadataDeleteIndexService::toSnapshotId)
            .collect(Collectors.toUnmodifiableSet());

        final RepositoriesMetadata repositories = state.metadata().custom(RepositoriesMetadata.TYPE);
        // used to deduplicate snapshots that were used by multiple deleted indices
        final Map<SnapshotId, RepositoryMetadata> snapshotsWithRepository = new HashMap<>();
        // also used to log a warning for snapshots with unknown repository
        final Map<SnapshotId, Tuple<String, String>> snapshotsWithoutRepository = new HashMap<>();

        for (Settings deletedIndexSettings : deletedIndicesSettings) {
            SnapshotId snapshotId = toSnapshotId(deletedIndexSettings);
            if (activeSearchableSnapshots.contains(snapshotId) == false) {
                String repositoryUuid = deletedIndexSettings.get(SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY);
                String repositoryName = deletedIndexSettings.get(SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY);
                Optional<RepositoryMetadata> repository = findRepositoryForPendingDeletion(repositories, repositoryName, repositoryUuid);
                if (repository.isPresent()) {
                    snapshotsWithRepository.putIfAbsent(snapshotId, repository.get());
                } else {
                    snapshotsWithoutRepository.putIfAbsent(snapshotId, Tuple.tuple(repositoryName, repositoryUuid));
                }
            }
        }

        final int maxPendingDeletions = SnapshotDeletionsPending.MAX_PENDING_DELETIONS_SETTING.get(settings);
        final SnapshotDeletionsPending.Builder builder = new SnapshotDeletionsPending.Builder(
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

        final long timestamp = Instant.now().toEpochMilli();
        for (Map.Entry<SnapshotId, RepositoryMetadata> entry : snapshotsWithRepository.entrySet()) {
            logger.info("snapshot [{}:{}] added to the list of snapshots pending deletion", entry.getValue().name(), entry.getKey());
            builder.add(entry.getValue().name(), entry.getValue().uuid(), entry.getKey(), timestamp);
        }
        for (Map.Entry<SnapshotId, Tuple<String, String>> entry : snapshotsWithoutRepository.entrySet()) {
            // TODO also log that it will stay as pending for a given time/attempts and then be removed?
            logger.warn(
                "snapshot [{}] added to the list of snapshots pending deletion but refers to an unregistered repository [{}/{}]",
                entry.getKey(),
                entry.getValue().v1(),
                entry.getValue().v2()
            );
            builder.add(entry.getValue().v1(), entry.getValue().v2(), entry.getKey(), timestamp);
        }
        return builder.build(settings);
    }

    private static SnapshotId toSnapshotId(final Settings indexSettings) {
        assert SearchableSnapshotsSettings.isSearchableSnapshotStore(indexSettings);
        return new SnapshotId(
            indexSettings.get(SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME_SETTING_KEY),
            indexSettings.get(SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY)
        );
    }
}
