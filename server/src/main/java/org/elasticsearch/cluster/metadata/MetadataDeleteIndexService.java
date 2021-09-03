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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexClusterStateUpdateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotsService;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptyList;
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

        clusterService.submitStateUpdateTask("delete-index " + Arrays.toString(request.indices()),
            new AckedClusterStateUpdateTask(Priority.URGENT, request, listener) {
                @Override
                public ClusterState execute(final ClusterState currentState) {
                    return deleteIndices(currentState, Sets.newHashSet(request.indices()));
                }
            });
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
                if (parent.getWriteIndex().equals(im)) {
                    throw new IllegalArgumentException("index [" + index.getName() + "] is the write index for data stream [" +
                        parent.getName() + "] and cannot be deleted");
                } else {
                    backingIndices.put(index, parent.getDataStream());
                }
            }
            indicesToDelete.add(im.getIndex());
        }

        // Check if index deletion conflicts with any running snapshots
        Set<Index> snapshottingIndices = SnapshotsService.snapshottingIndices(currentState, indicesToDelete);
        if (snapshottingIndices.isEmpty() == false) {
            throw new SnapshotInProgressException("Cannot delete indices that are being snapshotted: " + snapshottingIndices +
                ". Try again after snapshot finishes or cancel the currently running snapshot.");
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
        logger.trace("{} tombstones purged from the cluster state. Previous tombstone size: {}. Current tombstone size: {}.",
            graveyardBuilder.getNumPurged(), previousGraveyardSize, currentGraveyard.getTombstones().size());

        // add snapshot(s) marked as to delete to the cluster state
        final Map<String, Set<SnapshotId>> snapshotsToDelete = listOfSnapshotsToDelete(currentState, indicesToDelete);
        if (snapshotsToDelete.isEmpty() == false) {
            RepositoriesMetadata repositories = currentState.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
            boolean changed = false;
            for (Map.Entry<String, Set<SnapshotId>> snapshotToDelete : snapshotsToDelete.entrySet()) {
                RepositoryMetadata repository = repositories.repository(snapshotToDelete.getKey());
                if (repository != null) {
                    repositories = repositories.addSnapshotsToDelete(repository.name(), snapshotToDelete.getValue());
                    changed = true;
                }
            }
            if (changed) {
                metadataBuilder.putCustom(RepositoriesMetadata.TYPE, repositories);
            }
        }

        Metadata newMetadata = metadataBuilder.build();
        ClusterBlocks blocks = clusterBlocksBuilder.build();

        // update snapshot restore entries
        ImmutableOpenMap<String, ClusterState.Custom> customs = currentState.getCustoms();
        final RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY);
        RestoreInProgress updatedRestoreInProgress = RestoreService.updateRestoreStateWithDeletedIndices(restoreInProgress, indices);
        if (updatedRestoreInProgress != restoreInProgress) {
            ImmutableOpenMap.Builder<String, ClusterState.Custom> builder = ImmutableOpenMap.builder(customs);
            builder.put(RestoreInProgress.TYPE, updatedRestoreInProgress);
            customs = builder.build();
        }

        return allocationService.reroute(
                ClusterState.builder(currentState)
                    .routingTable(routingTableBuilder.build())
                    .metadata(newMetadata)
                    .blocks(blocks)
                    .customs(customs)
                    .build(),
                "deleted indices [" + indices + "]");
    }

    private static Map<String, Set<SnapshotId>> listOfSnapshotsToDelete(final ClusterState currentState, final Set<Index> indicesToDelete) {
        final Map<String, Set<SnapshotId>> snapshotsToDelete = new HashMap<>();

        for (Index indexToDelete : indicesToDelete) {
            final Settings indexSettings = currentState.metadata().getIndexSafe(indexToDelete).getSettings();
            if (SearchableSnapshotsSettings.isSearchableSnapshotIndexWithSnapshotDeletion(indexSettings) == false) {
                continue;
            }

            final String repositoryName = repositoryNameFromIndexSettings(currentState, indexSettings);
            final String snapshotName = indexSettings.get(SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME_SETTING_KEY);
            final String snapshotUuid = indexSettings.get(SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY);

            boolean canDeleteSnapshot = true;

            // TODO change this to an assertion once it becomes impossible to delete a snapshot that is mounted as an index
            if (currentState.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY)
                .getEntries().stream().anyMatch(entry -> entry.getSnapshots().contains(new SnapshotId(snapshotName, snapshotUuid)))) {
                continue; // this snapshot is part of an existing snapshot deletion in progress, nothing to do
            }

            for (IndexMetadata other : currentState.metadata()) {
                if (indicesToDelete.contains(other.getIndex())) {
                    continue; // do not check indices that are going to be deleted
                }
                final Settings otherSettings = other.getSettings();
                if (SearchableSnapshotsSettings.isSearchableSnapshotStore(otherSettings) == false) {
                    continue; // other index is not a searchable snapshot index, skip
                }
                final String otherSnapshotUuid = otherSettings.get(SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY);
                if (Objects.equals(snapshotUuid, otherSnapshotUuid) == false) {
                    continue; // other index is backed by a different snapshot, skip
                }
                assert otherSettings.getAsBoolean(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION, false) : other;
                canDeleteSnapshot = false; // another index is using the same snapshot, do not delete
                break;
            }
            if (canDeleteSnapshot) {
                snapshotsToDelete.computeIfAbsent(repositoryName, r -> new HashSet<>())
                    .add(new SnapshotId(indexSettings.get(SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME_SETTING_KEY), snapshotUuid));
            }
        }
        return snapshotsToDelete;
    }

    private static String repositoryNameFromIndexSettings(ClusterState currentState, Settings indexSettings) {
        final String repositoryUuid = indexSettings.get(SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY);
        if (Strings.hasLength(repositoryUuid) == false) {
            return indexSettings.get(SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY);
        }
        final RepositoriesMetadata repoMetadata = currentState.metadata().custom(RepositoriesMetadata.TYPE);
        final List<RepositoryMetadata> repositories = repoMetadata == null ? emptyList() : repoMetadata.repositories();
        return repositories.stream()
            .filter(r -> repositoryUuid.equals(r.uuid()))
            .map(RepositoryMetadata::name)
            .findFirst()
            .orElseThrow(() -> new RepositoryMissingException(repositoryUuid));
    }
}
