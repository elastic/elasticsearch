/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.GlobalCheckpointListeners;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class GetGlobalCheckpointsShardAction extends ActionType<GetGlobalCheckpointsShardAction.Response> {

    public static final GetGlobalCheckpointsShardAction INSTANCE = new GetGlobalCheckpointsShardAction();
    public static final String NAME = "indices:monitor/fleet/global_checkpoints[s]";

    private GetGlobalCheckpointsShardAction() {
        super(NAME);
    }

    public static class Response extends ActionResponse {

        private final long globalCheckpoint;
        private final boolean timedOut;

        public Response(long globalCheckpoint, boolean timedOut) {
            this.globalCheckpoint = globalCheckpoint;
            this.timedOut = timedOut;
        }

        public Response(StreamInput in) throws IOException {
            globalCheckpoint = in.readLong();
            timedOut = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(globalCheckpoint);
            out.writeBoolean(timedOut);
        }

        public long getGlobalCheckpoint() {
            return globalCheckpoint;
        }

        public boolean timedOut() {
            return timedOut;
        }
    }

    public static class Request extends SingleShardRequest<Request> {

        private final ShardId shardId;
        private final boolean waitForAdvance;
        private final long checkpoint;
        private final TimeValue timeout;

        Request(ShardId shardId, boolean waitForAdvance, long checkpoint, TimeValue timeout) {
            super(shardId.getIndexName());
            this.shardId = shardId;
            this.waitForAdvance = waitForAdvance;
            this.checkpoint = checkpoint;
            this.timeout = timeout;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
            this.waitForAdvance = in.readBoolean();
            this.checkpoint = in.readLong();
            this.timeout = in.readTimeValue();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public ShardId getShardId() {
            return shardId;
        }

        public TimeValue timeout() {
            return timeout;
        }

        public boolean waitForAdvance() {
            return waitForAdvance;
        }

        public long checkpoint() {
            return checkpoint;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeBoolean(waitForAdvance);
            out.writeLong(checkpoint);
            out.writeTimeValue(timeout);
        }
    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final IndicesService indicesService;

        @Inject
        public TransportAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            ProjectResolver projectResolver,
            IndexNameExpressionResolver indexNameExpressionResolver,
            IndicesService indicesService
        ) {
            super(
                NAME,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                projectResolver,
                indexNameExpressionResolver,
                Request::new,
                threadPool.executor(ThreadPool.Names.GENERIC)
            );
            this.indicesService = indicesService;
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) {
            final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            final IndexShard indexShard = indexService.getShard(shardId.id());
            final SeqNoStats seqNoStats = indexShard.seqNoStats();
            return new Response(seqNoStats.getGlobalCheckpoint(), false);
        }

        @Override
        protected void asyncShardOperation(Request request, ShardId shardId, ActionListener<Response> listener) throws IOException {
            final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            final IndexShard indexShard = indexService.getShard(shardId.id());
            final SeqNoStats seqNoStats = indexShard.seqNoStats();

            if (request.waitForAdvance() && request.checkpoint() >= seqNoStats.getGlobalCheckpoint()) {
                indexShard.addGlobalCheckpointListener(request.checkpoint() + 1, new GlobalCheckpointListeners.GlobalCheckpointListener() {

                    @Override
                    public Executor executor() {
                        return threadPool.executor(ThreadPool.Names.GENERIC);
                    }

                    @Override
                    public void accept(final long g, final Exception e) {
                        if (g != UNASSIGNED_SEQ_NO) {
                            assert request.checkpoint() < g
                                : shardId + " only advanced to [" + g + "] while waiting for [" + request.checkpoint() + "]";
                            globalCheckpointAdvanced(shardId, request, listener);
                        } else {
                            assert e != null;
                            globalCheckpointAdvancementFailure(indexShard, request, e, listener);
                        }
                    }

                }, request.timeout());
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

        private static void globalCheckpointAdvancementFailure(
            final IndexShard indexShard,
            final Request request,
            final Exception e,
            final ActionListener<Response> listener
        ) {
            try {
                if (e instanceof TimeoutException) {
                    final long globalCheckpoint = indexShard.seqNoStats().getGlobalCheckpoint();
                    if (request.checkpoint() >= globalCheckpoint) {
                        listener.onResponse(new Response(globalCheckpoint, true));
                    } else {
                        listener.onResponse(new Response(globalCheckpoint, false));
                    }
                } else {
                    listener.onFailure(e);
                }
            } catch (RuntimeException e2) {
                listener.onFailure(e2);
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
        protected ShardsIterator shards(ProjectState state, InternalRequest request) {
            return state.routingTable().shardRoutingTable(request.request().getShardId()).primaryShardIt();
        }
    }
}
