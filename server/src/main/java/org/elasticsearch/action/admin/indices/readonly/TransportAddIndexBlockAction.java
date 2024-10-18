/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.readonly;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;

/**
 * Adds a single index level block to a given set of indices. Not only does it set the correct setting,
 * but it ensures that, in case of a write block, once successfully returning to the user, all shards
 * of the index are properly accounting for the block, for instance, when adding a write block all
 * in-flight writes to an index have been completed prior to the response being returned. These actions
 * are done in multiple cluster state updates (at least two). See also {@link TransportVerifyShardIndexBlockAction}
 * for the eventual delegation for shard-level verification.
 */
public class TransportAddIndexBlockAction extends TransportMasterNodeAction<AddIndexBlockRequest, AddIndexBlockResponse> {

    public static final ActionType<AddIndexBlockResponse> TYPE = new ActionType<>("indices:admin/block/add");
    private static final Logger logger = LogManager.getLogger(TransportAddIndexBlockAction.class);

    private final MetadataIndexStateService indexStateService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportAddIndexBlockAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataIndexStateService indexStateService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            AddIndexBlockRequest::new,
            indexNameExpressionResolver,
            AddIndexBlockResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexStateService = indexStateService;
        this.destructiveOperations = destructiveOperations;
    }

    @Override
    protected void doExecute(Task task, AddIndexBlockRequest request, ActionListener<AddIndexBlockResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(AddIndexBlockRequest request, ClusterState state) {
        if (request.getBlock().getBlock().levels().contains(ClusterBlockLevel.METADATA_WRITE)
            && state.blocks().global(ClusterBlockLevel.METADATA_WRITE).isEmpty()) {
            return null;
        }
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void masterOperation(
        final Task task,
        final AddIndexBlockRequest request,
        final ClusterState state,
        final ActionListener<AddIndexBlockResponse> listener
    ) throws Exception {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices == null || concreteIndices.length == 0) {
            listener.onResponse(new AddIndexBlockResponse(true, false, Collections.emptyList()));
            return;
        }

        indexStateService.addIndexBlock(
            new AddIndexBlockClusterStateUpdateRequest(
                request.masterNodeTimeout(),
                request.ackTimeout(),
                request.getBlock(),
                task.getId(),
                concreteIndices
            ),
            listener.delegateResponse((delegatedListener, t) -> {
                logger.debug(() -> "failed to mark indices as readonly [" + Arrays.toString(concreteIndices) + "]", t);
                delegatedListener.onFailure(t);
            })
        );
    }
}
