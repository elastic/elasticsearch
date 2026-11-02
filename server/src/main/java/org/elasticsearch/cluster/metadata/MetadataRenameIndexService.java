/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotsServiceUtils;

import java.util.Map;
import java.util.Set;

/**
 * Renames an index in place in a cluster state: the index keeps its UUID, its shards, its settings, mappings, aliases and
 * blocks, and only the name under which it is addressed changes. Like {@link MetadataDeleteIndexService#deleteIndices}, this is
 * a building block of the aliases executor ({@link MetadataIndexAliasesService}), so that a rename is one cluster state update
 * together with the other actions of an {@code _aliases} request.
 * <p>
 * Nothing marks the rename in the cluster state: appliers recognise it because an index with the same UUID is still present
 * under another name (see {@link org.elasticsearch.cluster.ClusterChangedEvent#indicesRenamed()}). That holds however many
 * cluster states a node missed in between. See {@code docs/internal/atomic-index-rename.md} for the tradeoffs of this design.
 */
public class MetadataRenameIndexService {

    private static final Logger logger = LogManager.getLogger(MetadataRenameIndexService.class);

    private MetadataRenameIndexService() {}

    /**
     * Validates the rename against the given state: the index must exist under {@code originalName}, {@code destinationName}
     * must be a valid name that nothing else uses, and the index must not be part of a snapshot or restore in progress. The
     * checks that do not depend on the current cluster state (feature support, system indices) are made by the transport action
     * before the request is queued.
     */
    public static void validateRename(ProjectState projectState, String originalName, String destinationName) {
        final ProjectMetadata project = projectState.metadata();
        final IndexMetadata indexMetadata = project.index(originalName);
        if (indexMetadata == null) {
            throw new IndexNotFoundException(originalName);
        }
        MetadataCreateIndexService.validateIndexName(destinationName, project, projectState.routingTable());
        final Index index = indexMetadata.getIndex();
        final Map<Snapshot, Set<Index>> snapshottingIndices = SnapshotsServiceUtils.snapshottingIndicesBySnapshot(
            projectState,
            Set.of(index)
        );
        if (snapshottingIndices.isEmpty() == false) {
            throw new SnapshotInProgressException(
                "cannot rename index ["
                    + originalName
                    + "] because it is being snapshotted: "
                    + SnapshotsServiceUtils.describeSnapshottingIndices(snapshottingIndices)
                    + ". Try again after these snapshots finish, or cancel them."
            );
        }
        if (RestoreService.restoringIndices(projectState, Set.of(index)).isEmpty() == false) {
            throw new IllegalStateException("cannot rename index [" + originalName + "] because it is being restored");
        }
    }

    /**
     * Builds the cluster state with {@code originalName} renamed to {@code destinationName}: the index metadata is re-keyed under
     * the new name, the routing table and the index blocks are rewritten for it, and a data stream it belongs to is updated.
     */
    public static ClusterState renameIndex(ProjectState projectState, String originalName, String destinationName) {
        validateRename(projectState, originalName, destinationName);
        final ClusterState currentState = projectState.cluster();
        final ProjectId projectId = projectState.projectId();
        final ProjectMetadata currentProject = projectState.metadata();
        final IndexMetadata currentMetadata = currentProject.index(originalName);
        final Index index = currentMetadata.getIndex();
        final Index newIndex = new Index(destinationName, index.getUUID());

        final RoutingTable existingRoutingTable = projectState.routingTable();
        final RoutingTable.Builder rtBuilder = RoutingTable.builder(existingRoutingTable);
        final IndexRoutingTable indexTable = existingRoutingTable.index(originalName);
        final IndexRoutingTable.Builder tableBuilder = IndexRoutingTable.builder(newIndex);
        indexTable.allShards().forEach(shard -> tableBuilder.addIndexShard(shard.rename(newIndex)));
        rtBuilder.add(tableBuilder);
        rtBuilder.remove(originalName);

        var clusterBlocks = currentState.blocks();
        final Set<ClusterBlock> indexBlocks = clusterBlocks.projectBlocks(projectId).indices().get(originalName);
        if (indexBlocks != null) {
            final var blocksBuilder = ClusterBlocks.builder(currentState.blocks());
            for (ClusterBlock indexBlock : indexBlocks) {
                blocksBuilder.removeIndexBlock(projectId, originalName, indexBlock);
                blocksBuilder.addIndexBlock(projectId, destinationName, indexBlock);
            }
            clusterBlocks = blocksBuilder.build();
        }

        // index.provided_name records the name the index was created with and is read back by tooling as "the index name", so it
        // follows the rename; that is a settings change and bumps the settings version like any other
        final Settings renamedSettings = Settings.builder()
            .put(currentMetadata.getSettings())
            .put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, destinationName)
            .build();
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(currentProject)
            // Add the metadata under the new name
            .put(
                IndexMetadata.builder(currentMetadata)
                    .index(destinationName)
                    .settings(renamedSettings)
                    .settingsVersion(currentMetadata.getSettingsVersion() + 1)
            )
            // Remove the existing one
            .remove(originalName);

        final IndexAbstraction indexAbstraction = currentProject.getIndicesLookup().get(originalName);
        final DataStream dataStream = indexAbstraction == null ? null : indexAbstraction.getParentDataStream();
        if (dataStream != null) {
            // any backing or failure index may be renamed, including the write indices; the generation is unchanged
            projectBuilder.put(dataStream.renameIndex(index, newIndex));
        }

        logger.debug("renaming index [{}] to [{}] in project [{}]", originalName, destinationName, projectId);
        return ClusterState.builder(currentState)
            .putRoutingTable(projectId, rtBuilder.build())
            .blocks(clusterBlocks)
            .putProjectMetadata(projectBuilder)
            .build();
    }
}
