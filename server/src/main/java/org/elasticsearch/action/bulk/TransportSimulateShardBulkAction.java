/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/** Performs shard-level bulk (index, delete or update) operations */
public class TransportSimulateShardBulkAction extends TransportReplicationAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = SimulateBulkAction.NAME + "[s]";
    public static final ActionType<BulkShardResponse> TYPE = new ActionType<>(ACTION_NAME);

    private static final Logger logger = LogManager.getLogger(TransportShardBulkAction.class);

    private final UpdateHelper updateHelper;

    private final DocumentParsingProvider documentParsingProvider;
    protected final IndexingPressure indexingPressure;
    protected final SystemIndices systemIndices;
    protected final ExecutorSelector executorSelector;

    protected final PostWriteRefresh postWriteRefresh;
    private final BiFunction<ExecutorSelector, IndexShard, Executor> executorFunction;

    @Inject
    public TransportSimulateShardBulkAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        UpdateHelper updateHelper,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices,
        DocumentParsingProvider documentParsingProvider
    ) {

        super(
            settings,
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
            true,
            false
        );
        this.executorFunction = ExecutorSelector.getWriteExecutorForShard(threadPool);
        this.indexingPressure = indexingPressure;
        this.systemIndices = systemIndices;
        this.executorSelector = systemIndices.getExecutorSelector();
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

    private static final TransportRequestOptions TRANSPORT_REQUEST_OPTIONS = TransportRequestOptions.of(
        null,
        TransportRequestOptions.Type.BULK
    );

    @Override
    protected TransportRequestOptions transportOptions() {
        return TRANSPORT_REQUEST_OPTIONS;
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
        executorFunction.apply(executorSelector, primary).execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() {
                dispatchedShardOperationOnPrimary(request, primary, listener);
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }
        });
    }

    @Override
    protected void shardOperationOnReplica(BulkShardRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        executorFunction.apply(executorSelector, replica).execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() {
                // no op
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }
        });
    }

    protected void dispatchedShardOperationOnPrimary(
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
}
