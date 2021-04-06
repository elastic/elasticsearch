/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
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

public class GetGlobalCheckpointShardAction extends ActionType<GetGlobalCheckpointShardAction.Response> {

    public static final GetGlobalCheckpointShardAction INSTANCE = new GetGlobalCheckpointShardAction();
    public static final String NAME = "indices:monitor/fleet/global_checkpoint[s]";

    private GetGlobalCheckpointShardAction() {
        super(NAME, GetGlobalCheckpointShardAction.Response::new);
    }

    public static class Response extends ActionResponse {

        private final long globalCheckpoint;

        public Response(long globalCheckpoint) {
            this.globalCheckpoint = globalCheckpoint;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            globalCheckpoint = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(globalCheckpoint);
        }

        public long getGlobalCheckpoint() {
            return globalCheckpoint;
        }
    }

    public static class Request extends SingleShardRequest<Request> {

        private final ShardId shardId;
        private final boolean waitForAdvance;
        private final long currentCheckpoint;
        private final TimeValue pollTimeout;

        Request(ShardId shardId, boolean waitForAdvance, long currentCheckpoint, TimeValue pollTimeout) {
            super(shardId.getIndexName());
            this.shardId = shardId;
            this.waitForAdvance = waitForAdvance;
            this.currentCheckpoint = currentCheckpoint;
            this.pollTimeout = pollTimeout;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
            this.waitForAdvance = in.readBoolean();
            this.currentCheckpoint = in.readLong();
            this.pollTimeout = in.readTimeValue();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public ShardId getShardId() {
            return shardId;
        }

        public TimeValue pollTimeout() {
            return pollTimeout;
        }

        public boolean waitForAdvance() {
            return waitForAdvance;
        }

        public long currentCheckpoint() {
            return currentCheckpoint;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeBoolean(waitForAdvance);
            out.writeLong(currentCheckpoint);
            out.writeTimeValue(pollTimeout);
        }
    }

    public static class TransportGetGlobalCheckpointAction extends TransportSingleShardAction<Request, Response> {

        private final IndicesService indicesService;

        @Inject
        public TransportGetGlobalCheckpointAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            IndicesService indicesService
        ) {
            super(
                NAME,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                indexNameExpressionResolver,
                Request::new,
                ThreadPool.Names.GENERIC
            );
            this.indicesService = indicesService;
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) {
            final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            final IndexShard indexShard = indexService.getShard(shardId.id());
            final SeqNoStats seqNoStats = indexShard.seqNoStats();
            return new Response(seqNoStats.getGlobalCheckpoint());
        }

        @Override
        protected void asyncShardOperation(Request request, ShardId shardId, ActionListener<Response> listener) throws IOException {
            final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            final IndexShard indexShard = indexService.getShard(shardId.id());
            final SeqNoStats seqNoStats = indexShard.seqNoStats();

            if (request.waitForAdvance() && request.currentCheckpoint() >= seqNoStats.getGlobalCheckpoint()) {
                indexShard.addGlobalCheckpointListener(
                    request.currentCheckpoint() + 1,
                    new GlobalCheckpointListeners.GlobalCheckpointListener() {

                        @Override
                        public Executor executor() {
                            return threadPool.executor(ThreadPool.Names.GENERIC);
                        }

                        @Override
                        public void accept(final long g, final Exception e) {
                            if (g != UNASSIGNED_SEQ_NO) {
                                assert request.currentCheckpoint() <= g : shardId
                                    + " only advanced to ["
                                    + g
                                    + "] while waiting for ["
                                    + request.currentCheckpoint()
                                    + "]";
                                globalCheckpointAdvanced(shardId, request, listener);
                            } else {
                                assert e != null;
                                globalCheckpointAdvancementFailure(shardId, request.pollTimeout(), e, listener);
                            }
                        }

                    },
                    request.pollTimeout()
                );
            } else {
                super.asyncShardOperation(request, shardId, listener);
            }
        }

        private void globalCheckpointAdvanced(final ShardId shardId, final Request request, final ActionListener<Response> listener) {
            try {
                super.asyncShardOperation(request, shardId, listener);
            } catch (final IOException caught) {
                listener.onFailure(caught);
            }
        }

        private void globalCheckpointAdvancementFailure(
            final ShardId shardId,
            final TimeValue timeout,
            final Exception e,
            final ActionListener<Response> listener
        ) {
            if (e instanceof TimeoutException) {
                listener.onFailure(
                    new ElasticsearchTimeoutException(
                        "Wait for global checkpoint advance timed out [timeout: " + timeout + ", shard_id: " + shardId + "]"
                    )
                );
            } else {
                listener.onFailure(e);
            }
        }

        @Override
        protected Writeable.Reader<Response> getResponseReader() {
            return Response::new;
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return true;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            return state.routingTable().shardRoutingTable(request.request().getShardId()).primaryShardIt();
        }
    }
}
