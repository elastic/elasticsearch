/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetDesiredBalanceAction extends TransportMasterNodeReadAction<DesiredBalanceRequest, DesiredBalanceResponse> {

    public static final ActionType<DesiredBalanceStatsResponse> TYPE = new ActionType<>("cluster:admin/desired_balance");
    @Nullable
    private final DesiredBalanceShardsAllocator desiredBalanceShardsAllocator;

    @Inject
    public TransportGetDesiredBalanceAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ShardsAllocator shardsAllocator
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DesiredBalanceRequest::new,
            indexNameExpressionResolver,
            DesiredBalanceResponse::readFrom,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.desiredBalanceShardsAllocator = shardsAllocator instanceof DesiredBalanceShardsAllocator allocator ? allocator : null;
    }

    @Override
    protected void masterOperation(
        Task task,
        DesiredBalanceRequest request,
        ClusterState state,
        ActionListener<DesiredBalanceResponse> listener
    ) throws Exception {
        listener.onResponse(
            new DesiredBalanceResponse(desiredBalanceShardsAllocator != null ? desiredBalanceShardsAllocator.getDesiredBalance() : null)
        );
    }

    @Override
    protected ClusterBlockException checkBlock(DesiredBalanceRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
