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
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
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
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Set;

public class TransportRenameIndexAction extends TransportMasterNodeAction<RenameIndexAction.Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportRenameIndexAction.class);

    private final ProjectResolver projectResolver;

    @Inject
    public TransportRenameIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
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
    }

    @Override
    protected ClusterBlockException checkBlock(RenameIndexAction.Request request, ClusterState state) {
        return null;
    }

    @SuppressForbidden(reason = "usage of unbatched task") // TODO add support for batching here
    @Override
    protected void masterOperation(
        Task task,
        RenameIndexAction.Request request,
        ClusterState clusterState,
        ActionListener<AcknowledgedResponse> listener
    ) {
        // TODO: Validate that source index exists and destination index does not exist
        // TODO: Validate destination index name (valid characters, system index, etc.)
        // TODO: Perhaps some kind of validation that the source index is not being used by any aliases and/or data streams?
        // TODO: Should we validate that the source index is in a green state?
        final var originalName = request.getSourceIndex();
        final var destinationName = request.getDestinationIndex();
        final var projectState = projectResolver.getProjectState(clusterState);
        MetadataCreateIndexService.validateIndexName(destinationName, projectState.metadata(), projectState.routingTable());
        clusterService.submitUnbatchedStateUpdateTask(
            "rename from [" + originalName + "] to [" + destinationName + "]",
            new RenameIndexClusterStateUpdateTask(projectState.projectId(), originalName, destinationName, listener)
        );
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
            final ProjectMetadata currentProject = currentState.metadata().getProject(projectId);
            final Index index = currentProject.index(originalName).getIndex();
            final Index newIndex = new Index(destinationName, index.getUUID());

            RoutingTable existingRoutingTable = currentState.routingTable(projectId);
            RoutingTable.Builder rtBuilder = RoutingTable.builder(existingRoutingTable);
            IndexRoutingTable indexTable = existingRoutingTable.index(originalName);
            IndexRoutingTable.Builder tableBuilder = IndexRoutingTable.builder(new Index(destinationName, index.getUUID()));
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

            IndexMetadata currentMetadata = currentProject.index(index);
            logger.info("===> Renaming index [{}] to [{}] in project [{}]", originalName, destinationName, currentProject.id());
            return ClusterState.builder(currentState)
                .putRoutingTable(currentProject.id(), rtBuilder.build())
                .blocks(clusterBlocks)
                .putProjectMetadata(
                    ProjectMetadata.builder(currentProject)
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
                        .remove(index.getName())
                )
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
            logger.error("failed to rename index [{}] to [{}] in project [{}]", originalName, destinationName, projectId, e);
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
                logger.warn("Index [{}] not found in project [{}] for rename metadata removal", indexName, projectId);
                return currentState;
            }
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
            indexMetadataBuilder.removeCustom(MetadataIndexAliasesService.CUSTOM_RENAME_METADATA_KEY);

            logger.info("===> Removing rename metadata from index [{}] in project [{}]", indexName, projectId);
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
            logger.error("failed to remove rename metadata from index [{}] in project [{}]", indexName, projectId, e);
            listener.onFailure(e);
        }
    }
}
