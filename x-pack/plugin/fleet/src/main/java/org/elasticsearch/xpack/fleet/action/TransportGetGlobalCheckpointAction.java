/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.GlobalCheckpointListeners;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class TransportGetGlobalCheckpointAction extends TransportSingleShardAction<GetGlobalCheckpointAction.Request, GetGlobalCheckpointAction.Response> {

    private final IndicesService indicesService;

    @Inject
    public TransportGetGlobalCheckpointAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                              ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                              IndicesService indicesService) {
        super(GetGlobalCheckpointAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
            GetGlobalCheckpointAction.Request::new, ThreadPool.Names.GENERIC);
        this.indicesService = indicesService;
    }

    @Override
    protected GetGlobalCheckpointAction.Response shardOperation(GetGlobalCheckpointAction.Request request, ShardId shardId) throws IOException {
        return null;
    }

    @Override
    protected void asyncShardOperation(GetGlobalCheckpointAction.Request request, ShardId shardId, ActionListener<GetGlobalCheckpointAction.Response> listener) throws IOException {
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(shardId.id());
        final SeqNoStats seqNoStats = indexShard.seqNoStats();

        if (request.getFromSeqNo() > seqNoStats.getGlobalCheckpoint()) {
            logger.trace(
                "{} waiting for global checkpoint advancement from [{}] to [{}]",
                shardId,
                seqNoStats.getGlobalCheckpoint(),
                request.getFromSeqNo());
            indexShard.addGlobalCheckpointListener(
                request.getFromSeqNo(),
                new GlobalCheckpointListeners.GlobalCheckpointListener() {

                    @Override
                    public Executor executor() {
                        return threadPool.executor(ThreadPool.Names.GENERIC);
                    }

                    @Override
                    public void accept(final long g, final Exception e) {
                        if (g != UNASSIGNED_SEQ_NO) {
                            assert request.getFromSeqNo() <= g
                                : shardId + " only advanced to [" + g + "] while waiting for [" + request.getFromSeqNo() + "]";
                            globalCheckpointAdvanced(shardId, g, request, listener);
                        } else {
                            assert e != null;
                            globalCheckpointAdvancementFailure(shardId, e, request, listener, indexShard);
                        }
                    }

                },
                request.getPollTimeout());
        } else {
            super.asyncShardOperation(request, shardId, listener);
        }
    }

    private void globalCheckpointAdvanced(
        final ShardId shardId,
        final long globalCheckpoint,
        final GetGlobalCheckpointAction.Request request,
        final ActionListener<GetGlobalCheckpointAction.Response> listener) {
        try {
            super.asyncShardOperation(request, shardId, listener);
        } catch (final IOException caught) {
            listener.onFailure(caught);
        }
    }

    private void globalCheckpointAdvancementFailure(
        final ShardId shardId,
        final Exception e,
        final GetGlobalCheckpointAction.Request request,
        final ActionListener<GetGlobalCheckpointAction.Response> listener,
        final IndexShard indexShard) {
        if (e instanceof TimeoutException) {
            try {
                final IndexMetadata indexMetadata = clusterService.state().metadata().index(shardId.getIndex());
                if (indexMetadata == null) {
                    listener.onFailure(new IndexNotFoundException(shardId.getIndex()));
                    return;
                }
                final SeqNoStats latestSeqNoStats = indexShard.seqNoStats();
                final long maxSeqNoOfUpdatesOrDeletes = indexShard.getMaxSeqNoOfUpdatesOrDeletes();
//                listener.onResponse(
//                    getResponse(
//                        mappingVersion,
//                        settingsVersion,
//                        aliasesVersion,
//                        latestSeqNoStats,
//                        maxSeqNoOfUpdatesOrDeletes,
//                        EMPTY_OPERATIONS_ARRAY,
//                        request.relativeStartNanos));
            } catch (final Exception caught) {
                caught.addSuppressed(e);
                listener.onFailure(caught);
            }
        } else {
            listener.onFailure(e);
        }
    }

    @Override
    protected Writeable.Reader<GetGlobalCheckpointAction.Response> getResponseReader() {
        return GetGlobalCheckpointAction.Response::new;
    }

    @Override
    protected boolean resolveIndex(GetGlobalCheckpointAction.Request request) {
        return true;
    }

    @Override
    protected ShardsIterator shards(ClusterState state, InternalRequest request) {
        // TODO: What if primary shard cannot be resolved?
        return state
            .routingTable()
            .shardRoutingTable(request.concreteIndex(), request.request().getShard().id())
            .primaryShardIt();
    }
}
