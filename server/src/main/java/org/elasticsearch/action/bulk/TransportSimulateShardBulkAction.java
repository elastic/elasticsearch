/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.PostWriteRefresh;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;
import java.util.function.BiFunction;

public class TransportSimulateShardBulkAction extends TransportAction<BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = SimulateBulkAction.NAME + "[s]";
    public static final ActionType<BulkShardResponse> TYPE = new ActionType<>(ACTION_NAME);

    private final UpdateHelper updateHelper;
    private final DocumentParsingProvider documentParsingProvider;
    private final ExecutorSelector executorSelector;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final PostWriteRefresh postWriteRefresh;
    private final BiFunction<ExecutorSelector, IndexShard, Executor> executorFunction;

    @Inject
    public TransportSimulateShardBulkAction(
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        UpdateHelper updateHelper,
        ActionFilters actionFilters,
        SystemIndices systemIndices,
        DocumentParsingProvider documentParsingProvider
    ) {
        super(ACTION_NAME, actionFilters, transportService.getTaskManager());
        this.executorFunction = ExecutorSelector.getWriteExecutorForShard(threadPool);
        this.executorSelector = systemIndices.getExecutorSelector();
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.postWriteRefresh = new PostWriteRefresh(transportService) {
            public void refreshShard(
                WriteRequest.RefreshPolicy policy,
                IndexShard indexShard,
                @Nullable Translog.Location location,
                ActionListener<Boolean> listener,
                @Nullable TimeValue postWriteRefreshTimeout
            ) {
                // no op
            }
        };
        this.updateHelper = updateHelper;
        this.documentParsingProvider = documentParsingProvider;
    }

    @Override
    protected void doExecute(Task task, BulkShardRequest request, ActionListener<BulkShardResponse> listener) {
        IndexShard primary = getPrimaryIndexShard(request);
        ActionListener<TransportReplicationAction.PrimaryResult<BulkShardRequest, BulkShardResponse>> primaryResultListener =
            new ActionListener<>() {
                @Override
                public void onResponse(
                    TransportReplicationAction.PrimaryResult<
                        BulkShardRequest,
                        BulkShardResponse> bulkShardRequestBulkShardResponsePrimaryResult
                ) {
                    listener.onResponse(bulkShardRequestBulkShardResponsePrimaryResult.replicationResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            };

        executorFunction.apply(executorSelector, primary).execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() {
                TransportShardBulkAction.dispatchedShardOperationOnPrimary(
                    request,
                    primary,
                    primaryResultListener,
                    clusterService,
                    threadPool,
                    updateHelper,
                    null,
                    postWriteRefresh,
                    Runnable::run,
                    documentParsingProvider,
                    ExecutorSelector.getWriteExecutorForShard(threadPool),
                    executorSelector
                );
            }
        });
    }

    private IndexShard getPrimaryIndexShard(BulkShardRequest request) {
        ShardId shardId = clusterService.state().getRoutingTable().shardRoutingTable(request.shardId()).primaryShard().shardId();
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        return indexService.getShard(shardId.id());
    }
}
