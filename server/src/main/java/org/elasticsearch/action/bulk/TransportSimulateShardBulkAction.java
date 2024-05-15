/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.PostWriteRefresh;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/*
 * This is the simulate ingest equivalent to TransportShardBulkAction. It only simulates indexing -- no writes are made to shards, and no
 * refresh is performed.
 */
public class TransportSimulateShardBulkAction extends TransportReplicationAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = SimulateBulkAction.NAME + "[s]";
    public static final ActionType<BulkShardResponse> TYPE = new ActionType<>(ACTION_NAME);

    private final UpdateHelper updateHelper;
    private final DocumentParsingProvider documentParsingProvider;
    private final ExecutorSelector executorSelector;
    private final PostWriteRefresh postWriteRefresh;

    @Inject
    public TransportSimulateShardBulkAction(
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        UpdateHelper updateHelper,
        ActionFilters actionFilters,
        SystemIndices systemIndices,
        DocumentParsingProvider documentParsingProvider
    ) {
        super(
            clusterService.getSettings(),
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            BulkShardRequest::new,
            BulkShardRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            false,
            true
        );
        this.executorSelector = systemIndices.getExecutorSelector();
        this.postWriteRefresh = new PostWriteRefresh(transportService) {
            public void refreshShard(
                WriteRequest.RefreshPolicy policy,
                IndexShard indexShard,
                @Nullable Translog.Location location,
                ActionListener<Boolean> listener,
                @Nullable TimeValue postWriteRefreshTimeout
            ) {
                // no op because there is no need to refresh since we are not actually writing during a simulation
            }
        };
        this.updateHelper = updateHelper;
        this.documentParsingProvider = documentParsingProvider;
    }

    @Override
    protected BulkShardResponse newResponseInstance(StreamInput in) throws IOException {
        return new BulkShardResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(
        BulkShardRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener
    ) {
        TransportShardBulkAction.dispatchedShardOperationOnPrimary(
            request,
            primary,
            listener,
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

    @Override
    protected void shardOperationOnReplica(BulkShardRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        // We don't need to do anything on replicas since this is just a simulation
        listener.onResponse(new ReplicaResult());
    }
}
