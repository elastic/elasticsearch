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
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ShardChangesAction extends Action<ShardChangesAction.Response> {

    public static final ShardChangesAction INSTANCE = new ShardChangesAction();
    public static final String NAME = "indices:data/read/xpack/ccr/shard_changes";

    private ShardChangesAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends SingleShardRequest<Request> {

        private long minSeqNo;
        private long maxSeqNo;
        private ShardId shardId;
        private long maxOperationSizeInBytes = ShardFollowNodeTask.DEFAULT_MAX_OPERATIONS_SIZE_IN_BYTES;

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

        public long getMaxOperationSizeInBytes() {
            return maxOperationSizeInBytes;
        }

        public void setMaxOperationSizeInBytes(long maxOperationSizeInBytes) {
            this.maxOperationSizeInBytes = maxOperationSizeInBytes;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (minSeqNo < 0) {
                validationException = addValidationError("minSeqNo [" + minSeqNo + "] cannot be lower than 0", validationException);
            }
            if (maxSeqNo < minSeqNo) {
                validationException = addValidationError("minSeqNo [" + minSeqNo + "] cannot be larger than maxSeqNo ["
                        + maxSeqNo +  "]", validationException);
            }
            if (maxOperationSizeInBytes <= 0) {
                validationException = addValidationError("maxOperationSizeInBytes [" + maxOperationSizeInBytes + "] must be larger than 0",
                        validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            minSeqNo = in.readVLong();
            maxSeqNo = in.readVLong();
            shardId = ShardId.readShardId(in);
            maxOperationSizeInBytes = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(minSeqNo);
            out.writeVLong(maxSeqNo);
            shardId.writeTo(out);
            out.writeVLong(maxOperationSizeInBytes);
        }


        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Request request = (Request) o;
            return minSeqNo == request.minSeqNo &&
                    maxSeqNo == request.maxSeqNo &&
                    Objects.equals(shardId, request.shardId) &&
                    maxOperationSizeInBytes == request.maxOperationSizeInBytes;
        }

        @Override
        public int hashCode() {
            return Objects.hash(minSeqNo, maxSeqNo, shardId, maxOperationSizeInBytes);
        }
    }

    public static final class Response extends ActionResponse {

        private long indexMetadataVersion;
        private long leaderGlobalCheckpoint;
        private Translog.Operation[] operations;

        Response() {
        }

        Response(long indexMetadataVersion, long leaderGlobalCheckpoint, final Translog.Operation[] operations) {
            this.indexMetadataVersion = indexMetadataVersion;
            this.leaderGlobalCheckpoint = leaderGlobalCheckpoint;
            this.operations = operations;
        }

        public long getIndexMetadataVersion() {
            return indexMetadataVersion;
        }

        public long getLeaderGlobalCheckpoint() {
            return leaderGlobalCheckpoint;
        }

        public Translog.Operation[] getOperations() {
            return operations;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            indexMetadataVersion = in.readVLong();
            leaderGlobalCheckpoint = in.readZLong();
            operations = in.readArray(Translog.Operation::readOperation, Translog.Operation[]::new);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(indexMetadataVersion);
            out.writeZLong(leaderGlobalCheckpoint);
            out.writeArray(Translog.Operation::writeOperation, operations);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Response response = (Response) o;
            return indexMetadataVersion == response.indexMetadataVersion &&
                leaderGlobalCheckpoint == response.leaderGlobalCheckpoint &&
                Arrays.equals(operations, response.operations);
        }

        @Override
        public int hashCode() {
            int result = 1;
            result += Objects.hashCode(indexMetadataVersion);
            result += Objects.hashCode(leaderGlobalCheckpoint);
            result += Arrays.hashCode(operations);
            return result;
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
            final long indexMetaDataVersion = clusterService.state().metaData().index(shardId.getIndex()).getVersion();

            final Translog.Operation[] operations =
                getOperationsBetween(indexShard, request.minSeqNo, request.maxSeqNo, request.maxOperationSizeInBytes);
            return new Response(indexMetaDataVersion, indexShard.getGlobalCheckpoint(), operations);
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return false;
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

    private static final Translog.Operation[] EMPTY_OPERATIONS_ARRAY = new Translog.Operation[0];

    static Translog.Operation[] getOperationsBetween(IndexShard indexShard, long minSeqNo, long maxSeqNo,
                                                     long byteLimit) throws IOException {
        if (indexShard.state() != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(indexShard.shardId(), indexShard.state());
        }
        int seenBytes = 0;
        final List<Translog.Operation> operations = new ArrayList<>();
        try (Translog.Snapshot snapshot = indexShard.newLuceneChangesSnapshot("ccr", minSeqNo, maxSeqNo, true)) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                if (op.getSource() == null) {
                    throw new IllegalStateException("source not found for operation: " + op + " minSeqNo: " + minSeqNo + " maxSeqNo: " +
                        maxSeqNo);
                }
                operations.add(op);
                seenBytes += op.estimateSize();
                if (seenBytes > byteLimit) {
                    break;
                }
            }
        }
        return operations.toArray(EMPTY_OPERATIONS_ARRAY);
    }
}
