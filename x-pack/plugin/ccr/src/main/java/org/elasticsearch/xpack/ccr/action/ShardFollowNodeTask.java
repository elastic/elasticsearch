/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * The node task that fetch the write operations from a leader shard and
 * persists these ops in the follower shard.
 */
public abstract class ShardFollowNodeTask extends AllocatedPersistentTask {

    public static final int DEFAULT_MAX_BATCH_OPERATION_COUNT = 1024;
    public static final int DEFAULT_MAX_CONCURRENT_READ_BATCHES = 1;
    public static final int DEFAULT_MAX_CONCURRENT_WRITE_BATCHES = 1;
    public static final int DEFAULT_MAX_WRITE_BUFFER_SIZE = 10240;
    public static final long DEFAULT_MAX_BATCH_SIZE_IN_BYTES = Long.MAX_VALUE;
    private static final int RETRY_LIMIT = 10;
    public static final TimeValue DEFAULT_RETRY_TIMEOUT = new TimeValue(500);
    public static final TimeValue DEFAULT_IDLE_SHARD_RETRY_DELAY = TimeValue.timeValueSeconds(10);

    private static final Logger LOGGER = Loggers.getLogger(ShardFollowNodeTask.class);

    private final ShardFollowTask params;
    private final TimeValue retryTimeout;
    private final TimeValue idleShardChangesRequestDelay;
    private final BiConsumer<TimeValue, Runnable> scheduler;

    private volatile long lastRequestedSeqno;
    private volatile long leaderGlobalCheckpoint;

    private volatile int numConcurrentReads = 0;
    private volatile int numConcurrentWrites = 0;
    private volatile long followerGlobalCheckpoint = 0;
    private volatile long currentIndexMetadataVersion = 0;
    private final Queue<Translog.Operation> buffer = new PriorityQueue<>(Comparator.comparing(Translog.Operation::seqNo));

    ShardFollowNodeTask(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers,
                        ShardFollowTask params, BiConsumer<TimeValue, Runnable> scheduler) {
        super(id, type, action, description, parentTask, headers);
        this.params = params;
        this.scheduler = scheduler;
        this.retryTimeout = params.getRetryTimeout();
        this.idleShardChangesRequestDelay = params.getIdleShardRetryDelay();
    }

    void start(long leaderGlobalCheckpoint, long followerGlobalCheckpoint) {
        this.lastRequestedSeqno = followerGlobalCheckpoint;
        this.followerGlobalCheckpoint = followerGlobalCheckpoint;
        this.leaderGlobalCheckpoint = leaderGlobalCheckpoint;

        // Forcefully updates follower mapping, this gets us the leader imd version and
        // makes sure that leader and follower mapping are identical.
        updateMapping(imdVersion -> {
            currentIndexMetadataVersion = imdVersion;
            LOGGER.info("{} Started to follow leader shard {}, followGlobalCheckPoint={}, indexMetaDataVersion={}",
                params.getFollowShardId(), params.getLeaderShardId(), followerGlobalCheckpoint, imdVersion);
            coordinateReads();
        });
    }

    synchronized void coordinateReads() {
        if (isStopped()) {
            LOGGER.info("{} shard follow task has been stopped", params.getFollowShardId());
            return;
        }

        LOGGER.trace("{} coordinate reads, lastRequestedSeqno={}, leaderGlobalCheckpoint={}",
            params.getFollowShardId(), lastRequestedSeqno, leaderGlobalCheckpoint);
        final int maxBatchOperationCount = params.getMaxBatchOperationCount();
        while (hasReadBudget() && lastRequestedSeqno < leaderGlobalCheckpoint) {
            numConcurrentReads++;
            long from = lastRequestedSeqno + 1;
            // -1 is needed, because maxRequiredSeqno is inclusive
            long maxRequiredSeqno = Math.min(leaderGlobalCheckpoint, (from + maxBatchOperationCount) - 1);
            LOGGER.trace("{}[{}] read [{}/{}]", params.getFollowShardId(), numConcurrentReads, maxRequiredSeqno, maxBatchOperationCount);
            sendShardChangesRequest(from, maxBatchOperationCount, maxRequiredSeqno);
            lastRequestedSeqno = maxRequiredSeqno;
        }

        if (numConcurrentReads == 0 && hasReadBudget()) {
            assert lastRequestedSeqno == leaderGlobalCheckpoint;
            // We sneak peek if there is any thing new in the leader.
            // If there is we will happily accept
            numConcurrentReads++;
            long from = lastRequestedSeqno + 1;
            LOGGER.trace("{}[{}] peek read [{}]", params.getFollowShardId(), numConcurrentReads, from);
            sendShardChangesRequest(from, maxBatchOperationCount, lastRequestedSeqno);
        }
    }

    private boolean hasReadBudget() {
        assert Thread.holdsLock(this);
        if (numConcurrentReads >= params.getMaxConcurrentReadBatches()) {
            LOGGER.trace("{} no new reads, maximum number of concurrent reads have been reached [{}]",
                params.getFollowShardId(), numConcurrentReads);
            return false;
        }
        if (buffer.size() > params.getMaxWriteBufferSize()) {
            LOGGER.trace("{} no new reads, buffer limit has been reached [{}]", params.getFollowShardId(), buffer.size());
            return false;
        }
        return true;
    }

    private synchronized void coordinateWrites() {
        if (isStopped()) {
            LOGGER.info("{} shard follow task has been stopped", params.getFollowShardId());
            return;
        }

        while (hasWriteBudget() && buffer.isEmpty() == false) {
            long sumEstimatedSize = 0L;
            int length = Math.min(params.getMaxBatchOperationCount(), buffer.size());
            List<Translog.Operation> ops = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                Translog.Operation op = buffer.remove();
                ops.add(op);
                sumEstimatedSize += op.estimateSize();
                if (sumEstimatedSize > params.getMaxBatchSizeInBytes()) {
                    break;
                }
            }
            numConcurrentWrites++;
            LOGGER.trace("{}[{}] write [{}/{}] [{}]", params.getFollowShardId(), numConcurrentWrites, ops.get(0).seqNo(),
                ops.get(ops.size() - 1).seqNo(), ops.size());
            sendBulkShardOperationsRequest(ops);
        }
    }

    private boolean hasWriteBudget() {
        assert Thread.holdsLock(this);
        if (numConcurrentWrites >= params.getMaxConcurrentWriteBatches()) {
            LOGGER.trace("{} maximum number of concurrent writes have been reached [{}]",
                params.getFollowShardId(), numConcurrentWrites);
            return false;
        }
        return true;
    }

    private void sendShardChangesRequest(long from, int maxOperationCount, long maxRequiredSeqNo) {
        sendShardChangesRequest(from, maxOperationCount, maxRequiredSeqNo, new AtomicInteger(0));
    }

    private void sendShardChangesRequest(long from, int maxOperationCount, long maxRequiredSeqNo, AtomicInteger retryCounter) {
        innerSendShardChangesRequest(from, maxOperationCount,
            response -> handleReadResponse(from, maxRequiredSeqNo, response),
            e -> handleFailure(e, retryCounter, () -> sendShardChangesRequest(from, maxOperationCount, maxRequiredSeqNo, retryCounter)));
    }

    void handleReadResponse(long from, long maxRequiredSeqNo, ShardChangesAction.Response response) {
        maybeUpdateMapping(response.getIndexMetadataVersion(), () -> innerHandleReadResponse(from, maxRequiredSeqNo, response));
    }

    synchronized void innerHandleReadResponse(long from, long maxRequiredSeqNo, ShardChangesAction.Response response) {
        leaderGlobalCheckpoint = Math.max(leaderGlobalCheckpoint, response.getGlobalCheckpoint());
        final long newFromSeqNo;
        if (response.getOperations().length == 0) {
            newFromSeqNo = from;
        } else {
            assert response.getOperations()[0].seqNo() == from :
                "first operation is not what we asked for. From is [" + from + "], got " + response.getOperations()[0];
            buffer.addAll(Arrays.asList(response.getOperations()));
            final long maxSeqNo = response.getOperations()[response.getOperations().length - 1].seqNo();
            assert maxSeqNo ==
                Arrays.stream(response.getOperations()).mapToLong(Translog.Operation::seqNo).max().getAsLong();
            newFromSeqNo = maxSeqNo + 1;
            // update last requested seq no as we may have gotten more than we asked for and we don't want to ask it again.
            lastRequestedSeqno = Math.max(lastRequestedSeqno, maxSeqNo);
            assert lastRequestedSeqno <= leaderGlobalCheckpoint :  "lastRequestedSeqno [" + lastRequestedSeqno +
                "] is larger than the global checkpoint [" + leaderGlobalCheckpoint + "]";
            coordinateWrites();
        }
        if (newFromSeqNo <= maxRequiredSeqNo && isStopped() == false) {
            int newSize = Math.toIntExact(maxRequiredSeqNo - newFromSeqNo + 1);
            LOGGER.trace("{} received [{}] ops, still missing [{}/{}], continuing to read...",
                params.getFollowShardId(), response.getOperations().length, newFromSeqNo, maxRequiredSeqNo);
            sendShardChangesRequest(newFromSeqNo, newSize, maxRequiredSeqNo);
        } else {
            // read is completed, decrement
            numConcurrentReads--;
            if (response.getOperations().length == 0 && leaderGlobalCheckpoint == lastRequestedSeqno)  {
                // we got nothing and we have no reason to believe asking again well get us more, treat shard as idle and delay
                // future requests
                LOGGER.trace("{} received no ops and no known ops to fetch, scheduling to coordinate reads",
                    params.getFollowShardId());
                scheduler.accept(idleShardChangesRequestDelay, this::coordinateReads);
            } else {
                coordinateReads();
            }
        }
    }

    private void sendBulkShardOperationsRequest(List<Translog.Operation> operations) {
        sendBulkShardOperationsRequest(operations, new AtomicInteger(0));
    }

    private void sendBulkShardOperationsRequest(List<Translog.Operation> operations, AtomicInteger retryCounter) {
        innerSendBulkShardOperationsRequest(operations,
            this::handleWriteResponse,
            e -> handleFailure(e, retryCounter, () -> sendBulkShardOperationsRequest(operations, retryCounter))
        );
    }

    private synchronized void handleWriteResponse(long followerLocalCheckpoint) {
        this.followerGlobalCheckpoint = Math.max(this.followerGlobalCheckpoint, followerLocalCheckpoint);
        numConcurrentWrites--;
        assert numConcurrentWrites >= 0;
        coordinateWrites();

        // In case that buffer has more ops than is allowed then reads may all have been stopped,
        // this invocation makes sure that we start a read when there is budget in case no reads are being performed.
        coordinateReads();
    }

    private synchronized void maybeUpdateMapping(Long minimumRequiredIndexMetadataVersion, Runnable task) {
        if (currentIndexMetadataVersion >= minimumRequiredIndexMetadataVersion) {
            LOGGER.trace("{} index metadata version [{}] is higher or equal than minimum required index metadata version [{}]",
                params.getFollowShardId(), currentIndexMetadataVersion, minimumRequiredIndexMetadataVersion);
            task.run();
        } else {
            LOGGER.trace("{} updating mapping, index metadata version [{}] is lower than minimum required index metadata version [{}]",
                params.getFollowShardId(), currentIndexMetadataVersion, minimumRequiredIndexMetadataVersion);
            updateMapping(imdVersion -> {
                currentIndexMetadataVersion = imdVersion;
                task.run();
            });
        }
    }

    private void updateMapping(LongConsumer handler) {
        updateMapping(handler, new AtomicInteger(0));
    }

    private void updateMapping(LongConsumer handler, AtomicInteger retryCounter) {
        innerUpdateMapping(handler, e -> handleFailure(e, retryCounter, () -> updateMapping(handler, retryCounter)));
    }

    private void handleFailure(Exception e, AtomicInteger retryCounter, Runnable task) {
        assert e != null;
        if (shouldRetry(e)) {
            if (isStopped() == false && retryCounter.incrementAndGet() <= RETRY_LIMIT) {
                LOGGER.debug(new ParameterizedMessage("{} error during follow shard task, retrying...", params.getFollowShardId()), e);
                scheduler.accept(retryTimeout, task);
            } else {
                markAsFailed(new ElasticsearchException("retrying failed [" + retryCounter.get() +
                    "] times, aborting...", e));
            }
        } else {
            markAsFailed(e);
        }
    }

    private boolean shouldRetry(Exception e) {
        return NetworkExceptionHelper.isConnectException(e) ||
            NetworkExceptionHelper.isCloseConnectionException(e) ||
            TransportActions.isShardNotAvailableException(e);
    }

    // These methods are protected for testing purposes:
    protected abstract void innerUpdateMapping(LongConsumer handler, Consumer<Exception> errorHandler);

    protected abstract void innerSendBulkShardOperationsRequest(List<Translog.Operation> operations, LongConsumer handler,
                                                                Consumer<Exception> errorHandler);

    protected abstract void innerSendShardChangesRequest(long from, int maxOperationCount, Consumer<ShardChangesAction.Response> handler,
                                                         Consumer<Exception> errorHandler);

    @Override
    protected void onCancelled() {
        markAsCompleted();
    }

    protected boolean isStopped() {
        return isCancelled() || isCompleted();
    }

    @Override
    public Status getStatus() {
        return new Status(leaderGlobalCheckpoint, lastRequestedSeqno, followerGlobalCheckpoint, numConcurrentReads, numConcurrentWrites,
            currentIndexMetadataVersion);
    }

    public static class Status implements Task.Status {

        public static final String NAME = "shard-follow-node-task-status";

        static final ParseField LEADER_GLOBAL_CHECKPOINT_FIELD = new ParseField("leader_global_checkpoint");
        static final ParseField FOLLOWER_GLOBAL_CHECKPOINT_FIELD = new ParseField("follower_global_checkpoint");
        static final ParseField LAST_REQUESTED_SEQNO_FIELD = new ParseField("last_requested_seqno");
        static final ParseField NUMBER_OF_CONCURRENT_READS_FIELD = new ParseField("number_of_concurrent_reads");
        static final ParseField NUMBER_OF_CONCURRENT_WRITES_FIELD = new ParseField("number_of_concurrent_writes");
        static final ParseField INDEX_METADATA_VERSION_FIELD = new ParseField("index_metadata_version");

        static final ConstructingObjectParser<Status, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> new Status((long) args[0], (long) args[1], (long) args[2], (int) args[3], (int) args[4], (long) args[5]));

        static {
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), LEADER_GLOBAL_CHECKPOINT_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_REQUESTED_SEQNO_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), FOLLOWER_GLOBAL_CHECKPOINT_FIELD);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_CONCURRENT_READS_FIELD);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_CONCURRENT_WRITES_FIELD);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), INDEX_METADATA_VERSION_FIELD);
        }

        private final long leaderGlobalCheckpoint;
        private final long lastRequestedSeqno;
        private final long followerGlobalCheckpoint;
        private final int numberOfConcurrentReads;
        private final int numberOfConcurrentWrites;
        private final long indexMetadataVersion;

        Status(long leaderGlobalCheckpoint, long lastRequestedSeqno, long followerGlobalCheckpoint,
               int numberOfConcurrentReads, int numberOfConcurrentWrites, long indexMetadataVersion) {
            this.leaderGlobalCheckpoint = leaderGlobalCheckpoint;
            this.lastRequestedSeqno = lastRequestedSeqno;
            this.followerGlobalCheckpoint = followerGlobalCheckpoint;
            this.numberOfConcurrentReads = numberOfConcurrentReads;
            this.numberOfConcurrentWrites = numberOfConcurrentWrites;
            this.indexMetadataVersion = indexMetadataVersion;
        }

        public Status(StreamInput in) throws IOException {
            this.leaderGlobalCheckpoint = in.readZLong();
            this.lastRequestedSeqno = in.readZLong();
            this.followerGlobalCheckpoint = in.readZLong();
            this.numberOfConcurrentReads = in.readVInt();
            this.numberOfConcurrentWrites = in.readVInt();
            this.indexMetadataVersion = in.readVLong();
        }

        public long getLeaderGlobalCheckpoint() {
            return leaderGlobalCheckpoint;
        }

        public long getLastRequestedSeqno() {
            return lastRequestedSeqno;
        }

        public long getFollowerGlobalCheckpoint() {
            return followerGlobalCheckpoint;
        }

        public int getNumberOfConcurrentReads() {
            return numberOfConcurrentReads;
        }

        public int getNumberOfConcurrentWrites() {
            return numberOfConcurrentWrites;
        }

        public long getIndexMetadataVersion() {
            return indexMetadataVersion;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(leaderGlobalCheckpoint);
            out.writeZLong(lastRequestedSeqno);
            out.writeZLong(followerGlobalCheckpoint);
            out.writeVInt(numberOfConcurrentReads);
            out.writeVInt(numberOfConcurrentWrites);
            out.writeVLong(indexMetadataVersion);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(LEADER_GLOBAL_CHECKPOINT_FIELD.getPreferredName(), leaderGlobalCheckpoint);
                builder.field(FOLLOWER_GLOBAL_CHECKPOINT_FIELD.getPreferredName(), followerGlobalCheckpoint);
                builder.field(LAST_REQUESTED_SEQNO_FIELD.getPreferredName(), lastRequestedSeqno);
                builder.field(NUMBER_OF_CONCURRENT_READS_FIELD.getPreferredName(), numberOfConcurrentReads);
                builder.field(NUMBER_OF_CONCURRENT_WRITES_FIELD.getPreferredName(), numberOfConcurrentWrites);
                builder.field(INDEX_METADATA_VERSION_FIELD.getPreferredName(), indexMetadataVersion);
            }
            builder.endObject();
            return builder;
        }

        public static Status fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return leaderGlobalCheckpoint == status.leaderGlobalCheckpoint &&
                lastRequestedSeqno == status.lastRequestedSeqno &&
                followerGlobalCheckpoint == status.followerGlobalCheckpoint &&
                numberOfConcurrentReads == status.numberOfConcurrentReads &&
                numberOfConcurrentWrites == status.numberOfConcurrentWrites &&
                indexMetadataVersion == status.indexMetadataVersion;
        }

        @Override
        public int hashCode() {
            return Objects.hash(leaderGlobalCheckpoint, lastRequestedSeqno, followerGlobalCheckpoint, numberOfConcurrentReads,
                numberOfConcurrentWrites, indexMetadataVersion);
        }

        public String toString() {
            return Strings.toString(this);
        }
    }

}
