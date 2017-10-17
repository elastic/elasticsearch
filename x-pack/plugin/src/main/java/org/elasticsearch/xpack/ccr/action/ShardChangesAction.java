/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardOperationRequestBuilder;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ShardChangesAction extends Action<ShardChangesAction.Request, ShardChangesAction.Response, ShardChangesAction.RequestBuilder> {

    public static final ShardChangesAction INSTANCE = new ShardChangesAction();
    public static final String NAME = "cluster:admin/xpack/ccr/shard_changes";

    private ShardChangesAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends SingleShardRequest<Request> {

        private long minSeqNo;
        private long maxSeqNo;
        private ShardId shardId;

        public Request(ShardId shardId) {
            super(shardId.getIndexName());
            this.shardId = shardId;
        }

        Request() {
        }

        public ShardId getShard() {
            return shardId;
        }

        public long getMinSeqNo() {
            return minSeqNo;
        }

        public void setMinSeqNo(long minSeqNo) {
            this.minSeqNo = minSeqNo;
        }

        public long getMaxSeqNo() {
            return maxSeqNo;
        }

        public void setMaxSeqNo(long maxSeqNo) {
            this.maxSeqNo = maxSeqNo;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (minSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                validationException = addValidationError("minSeqNo cannot be unassigned", validationException);
            }
            if (maxSeqNo < minSeqNo) {
                validationException = addValidationError("minSeqNo [" + minSeqNo + "] cannot be larger than maxSeqNo ["
                        + maxSeqNo +  "]", validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            minSeqNo = in.readZLong();
            maxSeqNo = in.readZLong();
            shardId = ShardId.readShardId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeZLong(minSeqNo);
            out.writeZLong(maxSeqNo);
            shardId.writeTo(out);
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return minSeqNo == request.minSeqNo &&
                    maxSeqNo == request.maxSeqNo &&
                    Objects.equals(shardId, request.shardId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(minSeqNo, maxSeqNo, shardId);
        }
    }

    public static class Response extends ActionResponse {

        private List<Translog.Operation> operations;

        Response() {
        }

        Response(List<Translog.Operation> operations) {
            this.operations = operations;
        }

        public List<Translog.Operation> getOperations() {
            return operations;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            operations = Translog.readOperations(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            Translog.writeOperations(out, operations);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(operations, response.operations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(operations);
        }
    }

    static class RequestBuilder extends SingleShardOperationRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, Action<Request, Response, RequestBuilder> action) {
            super(client, action, new Request());
        }
    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final IndicesService indicesService;

        @Inject
        public TransportAction(Settings settings,
                               ThreadPool threadPool,
                               ClusterService clusterService,
                               TransportService transportService,
                               ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               IndicesService indicesService) {
            super(settings, NAME, threadPool, clusterService, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new, ThreadPool.Names.GET);
            this.indicesService = indicesService;
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            IndexService indexService = indicesService.indexServiceSafe(request.getShard().getIndex());
            IndexShard indexShard = indexService.getShard(request.getShard().id());

            List<Translog.Operation> operations = getOperationsBetween(indexShard, request.minSeqNo, request.maxSeqNo);
            return new Response(operations);
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return true;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            return state.routingTable()
                    .index(request.concreteIndex())
                    .shard(request.request().getShard().id())
                    .activeInitializingShardsRandomIt();
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

    }

    static List<Translog.Operation> getOperationsBetween(IndexShard indexShard, long minSeqNo, long maxSeqNo) throws IOException {
        if (indexShard.state() != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(indexShard.shardId(), indexShard.state());
        }

        final List<Translog.Operation> operations = new ArrayList<>();
        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(indexShard.indexSettings(), maxSeqNo, minSeqNo);
        try (Translog.Snapshot snapshot = indexShard.getTranslog().getSnapshotBetween(minSeqNo, maxSeqNo)) {
            for (Translog.Operation op = snapshot.next(); op != null; op = snapshot.next()) {
                if (op.seqNo() >= minSeqNo && op.seqNo() <= maxSeqNo) {
                    operations.add(op);
                    tracker.markSeqNoAsCompleted(op.seqNo());
                }
            }
        }

        if (tracker.getCheckpoint() == maxSeqNo) {
            return operations;
        } else {
            String message = "Not all operations between min_seq_no [" + minSeqNo + "] and max_seq_no [" + maxSeqNo +
                    "] found, tracker checkpoint [" + tracker.getCheckpoint() + "]";
            throw new IllegalStateException(message);
        }
    }

}
