/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

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

        private long fromSeqNo;
        private int maxOperationCount;
        private ShardId shardId;
        private String expectedHistoryUUID;
        private TimeValue pollTimeout = TransportResumeFollowAction.DEFAULT_READ_POLL_TIMEOUT;
        private ByteSizeValue maxBatchSize = TransportResumeFollowAction.DEFAULT_MAX_READ_REQUEST_SIZE;

        private long relativeStartNanos;

        public Request(ShardId shardId, String expectedHistoryUUID) {
            super(shardId.getIndexName());
            this.shardId = shardId;
            this.expectedHistoryUUID = expectedHistoryUUID;
        }

        Request() {
        }

        public ShardId getShard() {
            return shardId;
        }

        public long getFromSeqNo() {
            return fromSeqNo;
        }

        public void setFromSeqNo(long fromSeqNo) {
            this.fromSeqNo = fromSeqNo;
        }

        public int getMaxOperationCount() {
            return maxOperationCount;
        }

        public void setMaxOperationCount(int maxOperationCount) {
            this.maxOperationCount = maxOperationCount;
        }

        public ByteSizeValue getMaxBatchSize() {
            return maxBatchSize;
        }

        public void setMaxBatchSize(ByteSizeValue maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
        }

        public String getExpectedHistoryUUID() {
            return expectedHistoryUUID;
        }

        public TimeValue getPollTimeout() {
            return pollTimeout;
        }

        public void setPollTimeout(final TimeValue pollTimeout) {
            this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (fromSeqNo < 0) {
                validationException = addValidationError("fromSeqNo [" + fromSeqNo + "] cannot be lower than 0", validationException);
            }
            if (maxOperationCount < 0) {
                validationException = addValidationError("maxOperationCount [" + maxOperationCount +
                    "] cannot be lower than 0", validationException);
            }
            if (maxBatchSize.compareTo(ByteSizeValue.ZERO) <= 0) {
                validationException =
                        addValidationError("maxBatchSize [" + maxBatchSize.getStringRep() + "] must be larger than 0", validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            fromSeqNo = in.readVLong();
            maxOperationCount = in.readVInt();
            shardId = ShardId.readShardId(in);
            expectedHistoryUUID = in.readString();
            pollTimeout = in.readTimeValue();
            maxBatchSize = new ByteSizeValue(in);

            // Starting the clock in order to know how much time is spent on fetching operations:
            relativeStartNanos = System.nanoTime();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(fromSeqNo);
            out.writeVInt(maxOperationCount);
            shardId.writeTo(out);
            out.writeString(expectedHistoryUUID);
            out.writeTimeValue(pollTimeout);
            maxBatchSize.writeTo(out);
        }


        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Request request = (Request) o;
            return fromSeqNo == request.fromSeqNo &&
                    maxOperationCount == request.maxOperationCount &&
                    Objects.equals(shardId, request.shardId) &&
                    Objects.equals(expectedHistoryUUID, request.expectedHistoryUUID) &&
                    Objects.equals(pollTimeout, request.pollTimeout) &&
                    maxBatchSize.equals(request.maxBatchSize);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fromSeqNo, maxOperationCount, shardId, expectedHistoryUUID, pollTimeout, maxBatchSize);
        }

        @Override
        public String toString() {
            return "Request{" +
                    "fromSeqNo=" + fromSeqNo +
                    ", maxOperationCount=" + maxOperationCount +
                    ", shardId=" + shardId +
                    ", expectedHistoryUUID=" + expectedHistoryUUID +
                    ", pollTimeout=" + pollTimeout +
                    ", maxBatchSize=" + maxBatchSize.getStringRep() +
                    '}';
        }

    }

    public static final class Response extends ActionResponse {

        private long mappingVersion;

        public long getMappingVersion() {
            return mappingVersion;
        }

        private long settingsVersion;

        public long getSettingsVersion() {
            return settingsVersion;
        }

        private long globalCheckpoint;

        public long getGlobalCheckpoint() {
            return globalCheckpoint;
        }

        private long maxSeqNo;

        public long getMaxSeqNo() {
            return maxSeqNo;
        }

        private long maxSeqNoOfUpdatesOrDeletes;

        public long getMaxSeqNoOfUpdatesOrDeletes() {
            return maxSeqNoOfUpdatesOrDeletes;
        }

        private Translog.Operation[] operations;

        public Translog.Operation[] getOperations() {
            return operations;
        }

        private long tookInMillis;

        public long getTookInMillis() {
            return tookInMillis;
        }

        Response() {
        }

        Response(
            final long mappingVersion,
            final long settingsVersion,
            final long globalCheckpoint,
            final long maxSeqNo,
            final long maxSeqNoOfUpdatesOrDeletes,
            final Translog.Operation[] operations,
            final long tookInMillis) {

            this.mappingVersion = mappingVersion;
            this.settingsVersion = settingsVersion;
            this.globalCheckpoint = globalCheckpoint;
            this.maxSeqNo = maxSeqNo;
            this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes;
            this.operations = operations;
            this.tookInMillis = tookInMillis;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            mappingVersion = in.readVLong();
            settingsVersion = in.readVLong();
            globalCheckpoint = in.readZLong();
            maxSeqNo = in.readZLong();
            maxSeqNoOfUpdatesOrDeletes = in.readZLong();
            operations = in.readArray(Translog.Operation::readOperation, Translog.Operation[]::new);
            tookInMillis = in.readVLong();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(mappingVersion);
            out.writeVLong(settingsVersion);
            out.writeZLong(globalCheckpoint);
            out.writeZLong(maxSeqNo);
            out.writeZLong(maxSeqNoOfUpdatesOrDeletes);
            out.writeArray(Translog.Operation::writeOperation, operations);
            out.writeVLong(tookInMillis);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Response that = (Response) o;
            return mappingVersion == that.mappingVersion &&
                    settingsVersion == that.settingsVersion &&
                    globalCheckpoint == that.globalCheckpoint &&
                    maxSeqNo == that.maxSeqNo &&
                    maxSeqNoOfUpdatesOrDeletes == that.maxSeqNoOfUpdatesOrDeletes &&
                    Arrays.equals(operations, that.operations) &&
                    tookInMillis == that.tookInMillis;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    mappingVersion,
                    settingsVersion,
                    globalCheckpoint,
                    maxSeqNo,
                    maxSeqNoOfUpdatesOrDeletes,
                    Arrays.hashCode(operations),
                    tookInMillis);
        }
    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final IndicesService indicesService;

        @Inject
        public TransportAction(ThreadPool threadPool,
                               ClusterService clusterService,
                               TransportService transportService,
                               ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               IndicesService indicesService) {
            super(NAME, threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, Request::new, ThreadPool.Names.SEARCH);
            this.indicesService = indicesService;
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            final IndexService indexService = indicesService.indexServiceSafe(request.getShard().getIndex());
            final IndexShard indexShard = indexService.getShard(request.getShard().id());
            final SeqNoStats seqNoStats = indexShard.seqNoStats();
            final IndexMetaData indexMetaData = clusterService.state().metaData().index(shardId.getIndex());
            final long mappingVersion = indexMetaData.getMappingVersion();
            final long settingsVersion = indexMetaData.getSettingsVersion();

            final Translog.Operation[] operations = getOperations(
                    indexShard,
                    seqNoStats.getGlobalCheckpoint(),
                    request.getFromSeqNo(),
                    request.getMaxOperationCount(),
                    request.getExpectedHistoryUUID(),
                    request.getMaxBatchSize());
            // must capture after after snapshotting operations to ensure this MUS is at least the highest MUS of any of these operations.
            final long maxSeqNoOfUpdatesOrDeletes = indexShard.getMaxSeqNoOfUpdatesOrDeletes();
            return getResponse(
                    mappingVersion,
                    settingsVersion,
                    seqNoStats,
                    maxSeqNoOfUpdatesOrDeletes,
                    operations,
                    request.relativeStartNanos);
        }

        @Override
        protected void asyncShardOperation(
                final Request request,
                final ShardId shardId,
                final ActionListener<Response> listener) throws IOException {
            final IndexService indexService = indicesService.indexServiceSafe(request.getShard().getIndex());
            final IndexShard indexShard = indexService.getShard(request.getShard().id());
            final SeqNoStats seqNoStats = indexShard.seqNoStats();

            if (request.getFromSeqNo() > seqNoStats.getGlobalCheckpoint()) {
                logger.trace(
                        "{} waiting for global checkpoint advancement from [{}] to [{}]",
                        shardId,
                        seqNoStats.getGlobalCheckpoint(),
                        request.getFromSeqNo());
                indexShard.addGlobalCheckpointListener(
                        request.getFromSeqNo(),
                        (g, e) -> {
                            if (g != UNASSIGNED_SEQ_NO) {
                                assert request.getFromSeqNo() <= g
                                        : shardId + " only advanced to [" + g + "] while waiting for [" + request.getFromSeqNo() + "]";
                                globalCheckpointAdvanced(shardId, g, request, listener);
                            } else {
                                assert e != null;
                                globalCheckpointAdvancementFailure(shardId, e, request, listener, indexShard);
                            }
                        },
                        request.getPollTimeout());
            } else {
                super.asyncShardOperation(request, shardId, listener);
            }
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            ActionListener<Response> wrappedListener = ActionListener.wrap(listener::onResponse, e -> {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof IllegalStateException && cause.getMessage().contains("Not all operations between from_seqno [")) {
                    String message = "Operations are no longer available for replicating. Maybe increase the retention setting [" +
                        IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey() + "]?";
                    listener.onFailure(new ElasticsearchException(message, e));
                } else {
                    listener.onFailure(e);
                }
            });
            super.doExecute(task, request, wrappedListener);
        }

        private void globalCheckpointAdvanced(
                final ShardId shardId,
                final long globalCheckpoint,
                final Request request,
                final ActionListener<Response> listener) {
            logger.trace("{} global checkpoint advanced to [{}] after waiting for [{}]", shardId, globalCheckpoint, request.getFromSeqNo());
            try {
                super.asyncShardOperation(request, shardId, listener);
            } catch (final IOException caught) {
                listener.onFailure(caught);
            }
        }

        private void globalCheckpointAdvancementFailure(
                final ShardId shardId,
                final Exception e,
                final Request request,
                final ActionListener<Response> listener,
                final IndexShard indexShard) {
            logger.trace(
                    () -> new ParameterizedMessage(
                            "{} exception waiting for global checkpoint advancement to [{}]", shardId, request.getFromSeqNo()),
                    e);
            if (e instanceof TimeoutException) {
                try {
                    final IndexMetaData indexMetaData = clusterService.state().metaData().index(shardId.getIndex());
                    final long mappingVersion = indexMetaData.getMappingVersion();
                    final long settingsVersion = indexMetaData.getSettingsVersion();
                    final SeqNoStats latestSeqNoStats = indexShard.seqNoStats();
                    final long maxSeqNoOfUpdatesOrDeletes = indexShard.getMaxSeqNoOfUpdatesOrDeletes();
                    listener.onResponse(
                            getResponse(
                                    mappingVersion,
                                    settingsVersion,
                                    latestSeqNoStats,
                                    maxSeqNoOfUpdatesOrDeletes,
                                    EMPTY_OPERATIONS_ARRAY,
                                    request.relativeStartNanos));
                } catch (final Exception caught) {
                    caught.addSuppressed(e);
                    listener.onFailure(caught);
                }
            } else {
                listener.onFailure(e);
            }
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return false;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            return state
                    .routingTable()
                    .shardRoutingTable(request.concreteIndex(), request.request().getShard().id())
                    .activeInitializingShardsRandomIt();
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

    }

    static final Translog.Operation[] EMPTY_OPERATIONS_ARRAY = new Translog.Operation[0];

    /**
     * Returns at most the specified maximum number of operations from the specified from sequence number. This method will never return
     * operations above the specified global checkpoint.
     *
     * Also if the sum of collected operations size is above the specified maximum batch size then this method stops collecting more
     * operations and returns what has been collected so far.
     *
     * @param indexShard the shard
     * @param globalCheckpoint the global checkpoint
     * @param fromSeqNo the starting sequence number
     * @param maxOperationCount the maximum number of operations
     * @param expectedHistoryUUID the expected history UUID for the shard
     * @param maxBatchSize the maximum batch size
     * @return the operations
     * @throws IOException if an I/O exception occurs reading the operations
     */
    static Translog.Operation[] getOperations(
            final IndexShard indexShard,
            final long globalCheckpoint,
            final long fromSeqNo,
            final int maxOperationCount,
            final String expectedHistoryUUID,
            final ByteSizeValue maxBatchSize) throws IOException {
        if (indexShard.state() != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(indexShard.shardId(), indexShard.state());
        }
        final String historyUUID = indexShard.getHistoryUUID();
        if (historyUUID.equals(expectedHistoryUUID) == false) {
            throw new IllegalStateException("unexpected history uuid, expected [" + expectedHistoryUUID + "], actual [" +
                historyUUID + "]");
        }
        if (fromSeqNo > globalCheckpoint) {
            throw new IllegalStateException(
                    "not exposing operations from [" + fromSeqNo + "] greater than the global checkpoint [" + globalCheckpoint + "]");
        }
        int seenBytes = 0;
        // - 1 is needed, because toSeqNo is inclusive
        long toSeqNo = Math.min(globalCheckpoint, (fromSeqNo + maxOperationCount) - 1);
        assert fromSeqNo <= toSeqNo : "invalid range from_seqno[" + fromSeqNo + "] > to_seqno[" + toSeqNo + "]";
        final List<Translog.Operation> operations = new ArrayList<>();
        try (Translog.Snapshot snapshot = indexShard.newChangesSnapshot("ccr", fromSeqNo, toSeqNo, true)) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                operations.add(op);
                seenBytes += op.estimateSize();
                if (seenBytes > maxBatchSize.getBytes()) {
                    break;
                }
            }
        }
        return operations.toArray(EMPTY_OPERATIONS_ARRAY);
    }

    static Response getResponse(
            final long mappingVersion,
            final long settingsVersion,
            final SeqNoStats seqNoStats,
            final long maxSeqNoOfUpdates,
            final Translog.Operation[] operations,
            long relativeStartNanos) {
        long tookInNanos = System.nanoTime() - relativeStartNanos;
        long tookInMillis = TimeUnit.NANOSECONDS.toMillis(tookInNanos);
        return new Response(
                mappingVersion,
                settingsVersion,
                seqNoStats.getGlobalCheckpoint(),
                seqNoStats.getMaxSeqNo(),
                maxSeqNoOfUpdates,
                operations,
                tookInMillis);
    }

}
