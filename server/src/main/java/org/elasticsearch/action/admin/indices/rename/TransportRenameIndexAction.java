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
import org.elasticsearch.cluster.AckedBatchedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Tuple;
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
 * Nothing marks the rename in the cluster state: appliers recognise it because an index with the same UUID is still present under
 * another name (see {@link org.elasticsearch.cluster.ClusterChangedEvent#indicesRenamed()}). That holds however many cluster
 * states a node missed in between. See {@code docs/internal/atomic-index-rename.md} for the tradeoffs of this design.
 */
public class TransportRenameIndexAction extends TransportMasterNodeAction<RenameIndexAction.Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportRenameIndexAction.class);

    private final ProjectResolver projectResolver;
    private final FeatureService featureService;
    private final MasterServiceTaskQueue<RenameIndexTask> taskQueue;

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
        // Renames batch like other index metadata changes. Each task is validated and applied against the state produced by the
        // tasks before it in the batch, so two renames of the same index in one batch behave as if they had run one after another.
        this.taskQueue = clusterService.createTaskQueue("rename-index", Priority.URGENT, new SimpleBatchedAckListenerTaskExecutor<>() {
            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(RenameIndexTask task, ClusterState clusterState) {
                return Tuple.tuple(rename(clusterState, task.projectId, task.originalName, task.destinationName), task);
            }
        });
    }

    /**
     * One queued rename. Acknowledged once every node has applied the cluster state that carries the rename, so that a caller
     * who sees the acknowledgement can address the index by its new name on any node.
     */
    private static class RenameIndexTask extends AckedBatchedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final String originalName;
        private final String destinationName;

        RenameIndexTask(RenameIndexAction.Request request, ProjectId projectId, ActionListener<AcknowledgedResponse> listener) {
            super(request.ackTimeout(), listener);
            this.projectId = projectId;
            this.originalName = request.getSourceIndex();
            this.destinationName = request.getDestinationIndex();
        }
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
        taskQueue.submitTask(
            "rename from [" + originalName + "] to [" + destinationName + "]",
            new RenameIndexTask(request, projectState.projectId(), listener),
            request.masterNodeTimeout()
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
    }

    private static DataStream parentDataStream(ProjectMetadata project, String indexName) {
        final IndexAbstraction indexAbstraction = project.getIndicesLookup().get(indexName);
        return indexAbstraction == null ? null : indexAbstraction.getParentDataStream();
    }

    /**
     * Builds the cluster state with {@code originalName} renamed to {@code destinationName}: the index metadata is re-keyed under
     * the new name, the routing table and the index blocks are rewritten for it, and a data stream it belongs to is updated.
     */
    private ClusterState rename(ClusterState currentState, ProjectId projectId, String originalName, String destinationName) {
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

        final DataStream dataStream = parentDataStream(currentProject, originalName);
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
