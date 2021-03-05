/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Objects;

/**
 * Main class to initiate PrepareReIndexTargetAction for cloning index into a new index
 */
public class TransportPrepareReindexTargetAction extends TransportMasterNodeAction<PrepareReindexRequest, CreateIndexResponse> {

    private final MetadataCreateIndexService createIndexService;

    @Inject
    public TransportPrepareReindexTargetAction(TransportService transportService, ClusterService clusterService,
                                 ThreadPool threadPool, MetadataCreateIndexService createIndexService,
                                 ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        this(PrepareReindexTargetAction.NAME, transportService, clusterService, threadPool, createIndexService, actionFilters,
            indexNameExpressionResolver);
    }

    public TransportPrepareReindexTargetAction(String actionName, TransportService transportService,
                                               ClusterService clusterService, ThreadPool threadPool,
                                               MetadataCreateIndexService createIndexService, ActionFilters actionFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        super(actionName, transportService, clusterService, threadPool, actionFilters,
            PrepareReindexRequest::new, indexNameExpressionResolver, CreateIndexResponse::new, ThreadPool.Names.SAME);
        this.createIndexService = createIndexService;
    }

    public MetadataCreateIndexService getCreateIndexService() {
        return createIndexService;
    }

    @Override
    protected void masterOperation(Task task, PrepareReindexRequest request, ClusterState state,
                                   ActionListener<CreateIndexResponse> listener) throws Exception {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        CreateIndexClusterStateUpdateRequest reindexRequest = prepareReindexRequest(request, state);
        createIndexService.createIndex(reindexRequest, listener.map(
            response -> new ResizeResponse(response.isAcknowledged(),
                response.isShardsAcknowledged(), reindexRequest.index())));
    }

    static CreateIndexClusterStateUpdateRequest prepareReindexRequest(final PrepareReindexRequest createIndexRequest,
                                                                      final ClusterState state) {
        final String sourceIndexName = createIndexRequest.getSourceIndex();
        final String targetIndexName = createIndexRequest.getTargetIndexRequest().index();
        final CreateIndexRequest targetIndex = createIndexRequest.getTargetIndexRequest();
        final IndexMetadata sourceIndexMetadata = state.metadata().index(sourceIndexName);
        if (sourceIndexMetadata == null) {
            throw new IndexNotFoundException(sourceIndexName);
        }

        if (state.metadata().hasIndex(targetIndexName)) {
            throw new ResourceAlreadyExistsException("index already present");
        }

        final Settings targetIndexSettings = Settings.builder().put(targetIndex.settings())
            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
        final int numShards = sourceIndexMetadata.getNumberOfShards();

        for (int i = 0; i < numShards; i++) {
            Objects.requireNonNull(IndexMetadata.selectCloneShard(i, sourceIndexMetadata, numShards));
        }

        Settings.Builder settingsBuilder = Settings.builder().put(targetIndexSettings);
        settingsBuilder.put("index.number_of_shards", numShards);
        targetIndex.settings(settingsBuilder);

        return new CreateIndexClusterStateUpdateRequest("prepare_reindex_target", targetIndex.index(), targetIndexName)
            .ackTimeout(targetIndex.timeout())
            .masterNodeTimeout(targetIndex.masterNodeTimeout())
            .settings(targetIndex.settings())
            .aliases(targetIndex.aliases())
            .waitForActiveShards(targetIndex.waitForActiveShards())
            .recoverFrom(sourceIndexMetadata.getIndex());
    }

    @Override
    protected ClusterBlockException checkBlock(PrepareReindexRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getSourceIndex());
    }
}
