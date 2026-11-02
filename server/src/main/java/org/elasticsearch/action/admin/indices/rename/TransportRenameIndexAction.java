/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rename;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexAliasesService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexFeatures;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotsServiceUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Set;

/**
 * Renames an index in place: the index keeps its UUID, its shards, its settings, mappings, aliases and blocks, and only the name
 * under which it is addressed changes. The rename is a single cluster state update, so from the point of view of any other cluster
 * state consumer the old name disappears and the new name appears atomically.
 * <p>
 * The rename is applied in two cluster state updates. The first one performs the rename and tags the renamed {@link IndexMetadata}
 * with {@link MetadataIndexAliasesService#CUSTOM_RENAME_METADATA_KEY} so that cluster state appliers (most importantly
 * {@link org.elasticsearch.indices.cluster.IndicesClusterStateService}) can tell a rename apart from a delete-and-create. The
 * second one removes that tag again. See {@code docs/internal/atomic-index-rename.md} for the tradeoffs of this design.
 */
public class TransportRenameIndexAction extends TransportMasterNodeAction<RenameIndexAction.Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportRenameIndexAction.class);

    private final ProjectResolver projectResolver;
    private final FeatureService featureService;

    @Inject
    public TransportRenameIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        FeatureService featureService
    ) {
        super(
            RenameIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RenameIndexAction.Request::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.projectResolver = projectResolver;
        this.featureService = featureService;
    }

    @Override
    protected ClusterBlockException checkBlock(RenameIndexAction.Request request, ClusterState state) {
        final ProjectId projectId = projectResolver.getProjectId();
        final ClusterBlockException globalBlock = state.blocks().globalBlockedException(projectId, ClusterBlockLevel.METADATA_WRITE);
        if (globalBlock != null) {
            return globalBlock;
        }
        return state.blocks()
            .indicesBlockedException(projectId, ClusterBlockLevel.METADATA_WRITE, new String[] { request.getSourceIndex() });
    }

    @SuppressForbidden(reason = "usage of unbatched task") // TODO batch renames, ideally through the aliases executor
    @Override
    protected void masterOperation(
        Task task,
        RenameIndexAction.Request request,
        ClusterState clusterState,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final var originalName = request.getSourceIndex();
        final var destinationName = request.getDestinationIndex();
        final var projectState = projectResolver.getProjectState(clusterState);
        try {
            validate(clusterState, projectState, originalName, destinationName);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        clusterService.submitUnbatchedStateUpdateTask(
            "rename from [" + originalName + "] to [" + destinationName + "]",
            new RenameIndexClusterStateUpdateTask(projectState.projectId(), originalName, destinationName, listener)
        );
    }

    /**
     * Validates the rename against the cluster state the request was received with. The validation is repeated against the
     * current state inside the cluster state update task, since the state may have changed in between.
     */
    private void validate(ClusterState clusterState, ProjectState projectState, String originalName, String destinationName) {
        if (featureService.clusterHasFeature(clusterState, IndexFeatures.INDEX_RENAME) == false) {
            throw new IllegalStateException(
                "cannot rename index ["
                    + originalName
                    + "] because not all nodes in the cluster support renaming indices; "
                    + "upgrade all nodes to a version that supports ["
                    + IndexFeatures.INDEX_RENAME.id()
                    + "]"
            );
        }
        final ProjectMetadata project = projectState.metadata();
        final IndexMetadata indexMetadata = project.index(originalName);
        if (indexMetadata == null) {
            throw new IndexNotFoundException(originalName);
        }
        if (indexMetadata.isSystem()) {
            throw new IllegalArgumentException("cannot rename system index [" + originalName + "]");
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
        final DataStream dataStream = parentDataStream(project, originalName);
        if (dataStream != null && dataStream.getWriteIndex().equals(index)) {
            throw new IllegalArgumentException(
                "cannot rename index [" + originalName + "] because it is the write index of data stream [" + dataStream.getName() + "]"
            );
        }
        if (dataStream != null && dataStream.getWriteFailureIndex() != null && dataStream.getWriteFailureIndex().equals(index)) {
            throw new IllegalArgumentException(
                "cannot rename index ["
                    + originalName
                    + "] because it is the failure store write index of data stream ["
                    + dataStream.getName()
                    + "]"
            );
        }
    }

    private static DataStream parentDataStream(ProjectMetadata project, String indexName) {
        final IndexAbstraction indexAbstraction = project.getIndicesLookup().get(indexName);
        return indexAbstraction == null ? null : indexAbstraction.getParentDataStream();
    }

    private class RenameIndexClusterStateUpdateTask extends ClusterStateUpdateTask {

        private final ProjectId projectId;
        private final String originalName;
        private final String destinationName;
        private final ActionListener<AcknowledgedResponse> listener;

        RenameIndexClusterStateUpdateTask(
            ProjectId projectId,
            String originalName,
            String destinationName,
            ActionListener<AcknowledgedResponse> listener
        ) {
            this.projectId = projectId;
            this.originalName = originalName;
            this.destinationName = destinationName;
            this.listener = listener;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            final ProjectState projectState = currentState.projectState(projectId);
            validate(currentState, projectState, originalName, destinationName);
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

            final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(currentProject)
                // Add the metadata under the new name
                .put(
                    IndexMetadata.builder(currentMetadata)
                        .index(destinationName)
                        .putCustom(
                            MetadataIndexAliasesService.CUSTOM_RENAME_METADATA_KEY,
                            Map.of("original_name", originalName, "new_name", destinationName)
                        )
                )
                // Remove the existing one
                .remove(originalName);

            final DataStream dataStream = parentDataStream(currentProject, originalName);
            if (dataStream != null) {
                // TODO: replaceBackingIndex/replaceFailureStoreIndex bump the data stream generation, which a rename should not do
                if (dataStream.isFailureStoreIndex(originalName)) {
                    projectBuilder.put(dataStream.replaceFailureStoreIndex(index, newIndex));
                } else {
                    projectBuilder.put(dataStream.replaceBackingIndex(index, newIndex));
                }
            }

            logger.debug("renaming index [{}] to [{}] in project [{}]", originalName, destinationName, projectId);
            return ClusterState.builder(currentState)
                .putRoutingTable(projectId, rtBuilder.build())
                .blocks(clusterBlocks)
                .putProjectMetadata(projectBuilder)
                .build();
        }

        @SuppressForbidden(reason = "usage of unbatched task") // TODO add support for batching here
        @Override
        public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
            clusterService.submitUnbatchedStateUpdateTask(
                "remove rename metadata for index [" + destinationName + "] in project [" + projectId + "]",
                new RemoveRenameMetadataClusterStateUpdateTask(projectId, destinationName, listener)
            );
        }

        @Override
        public void onFailure(Exception e) {
            logger.debug(() -> "failed to rename index [" + originalName + "] to [" + destinationName + "] in project [" + projectId + "]", e);
            listener.onFailure(e);
        }
    }

    private static class RemoveRenameMetadataClusterStateUpdateTask extends ClusterStateUpdateTask {

        private final ProjectId projectId;
        private final String indexName;
        private final ActionListener<AcknowledgedResponse> listener;

        RemoveRenameMetadataClusterStateUpdateTask(ProjectId projectId, String indexName, ActionListener<AcknowledgedResponse> listener) {
            this.projectId = projectId;
            this.indexName = indexName;
            this.listener = listener;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            ProjectMetadata project = currentState.metadata().getProject(projectId);
            IndexMetadata indexMetadata = project.index(indexName);
            if (indexMetadata == null) {
                logger.debug("index [{}] not found in project [{}] for rename metadata removal", indexName, projectId);
                return currentState;
            }
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
            indexMetadataBuilder.removeCustom(MetadataIndexAliasesService.CUSTOM_RENAME_METADATA_KEY);

            logger.debug("removing rename metadata from index [{}] in project [{}]", indexName, projectId);
            return ClusterState.builder(currentState)
                .putProjectMetadata(ProjectMetadata.builder(project).put(indexMetadataBuilder))
                .build();
        }

        @Override
        public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
            listener.onResponse(AcknowledgedResponse.TRUE);
        }

        @Override
        public void onFailure(Exception e) {
            logger.debug(() -> "failed to remove rename metadata from index [" + indexName + "] in project [" + projectId + "]", e);
            listener.onFailure(e);
        }
    }
}
