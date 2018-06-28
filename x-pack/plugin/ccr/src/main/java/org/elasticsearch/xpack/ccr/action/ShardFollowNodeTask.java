/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.ActionTransportException;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsRequest;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * The node task that fetch the write operations from a leader shard and
 * persists these ops in the follower shard.
 */
public class ShardFollowNodeTask extends AllocatedPersistentTask {

    static final int DEFAULT_MAX_READ_SIZE = 1024;
    static final int DEFAULT_MAX_WRITE_SIZE = 1024;
    static final int RETRY_LIMIT = 10;
    static final int DEFAULT_MAX_CONCURRENT_READS = 1;
    static final int DEFAULT_MAX_CONCURRENT_WRITES = 1;
    static final int DEFAULT_MAX_BUFFER_SIZE = 10240;
    static final long DEFAULT_MAX_OPERATIONS_SIZE_IN_BYTES = Long.MAX_VALUE;
    private static final TimeValue RETRY_TIMEOUT = TimeValue.timeValueMillis(500);

    private static final Logger LOGGER = Loggers.getLogger(ShardFollowNodeTask.class);

    final Client leaderClient;
    final Client followerClient;
    private final ShardFollowTask params;
    private final BiConsumer<TimeValue, Runnable> scheduler;

    private volatile long lastRequestedSeqno;
    private volatile long globalCheckpoint;

    private volatile int numConcurrentReads = 0;
    private volatile int numConcurrentWrites = 0;
    private volatile long processedGlobalCheckpoint = 0;
    private volatile long currentIndexMetadataVersion = 0;
    private final AtomicInteger retryCounter = new AtomicInteger(0);
    private final Queue<Translog.Operation> buffer = new LinkedList<>();

    ShardFollowNodeTask(long id,
                        String type,
                        String action,
                        String description,
                        TaskId parentTask,
                        Map<String, String> headers,
                        Client leaderClient,
                        Client followerClient,
                        ShardFollowTask params,
                        BiConsumer<TimeValue, Runnable> scheduler) {
        super(id, type, action, description, parentTask, headers);
        this.leaderClient = leaderClient;
        this.followerClient = followerClient;
        this.params = params;
        this.scheduler = scheduler;
    }

    void start(long followGlobalCheckpoint) {
        this.lastRequestedSeqno = followGlobalCheckpoint;
        this.processedGlobalCheckpoint = followGlobalCheckpoint;
        this.globalCheckpoint = followGlobalCheckpoint;

        // Forcefully updates follower mapping, this gets us the leader imd version and
        // makes sure that leader and follower mapping are identical.
        updateMapping(imdVersion -> {
            currentIndexMetadataVersion = imdVersion;
            LOGGER.info("{} Started to follow leader shard {}, followGlobalCheckPoint={}",
                params.getFollowShardId(), params.getLeaderShardId(), followGlobalCheckpoint);
            coordinateReads();
        });
    }

    private synchronized void coordinateReads() {
        if (isStopped()) {
            LOGGER.info("{} shard follow task has been stopped", params.getFollowShardId());
            return;
        }

        LOGGER.trace("{} coordinate reads, lastRequestedSeqno={}, globalCheckpoint={}",
            params.getFollowShardId(), lastRequestedSeqno, globalCheckpoint);
        final long maxReadSize = params.getMaxReadSize();
        if (lastRequestedSeqno < globalCheckpoint) {
            while (hasReadBudget() && lastRequestedSeqno < globalCheckpoint) {
                numConcurrentReads++;
                long from = lastRequestedSeqno + 1;
                long size;
                if (from + maxReadSize <= globalCheckpoint) {
                    size = maxReadSize;
                } else {
                    size = globalCheckpoint - from;
                }
                LOGGER.trace("{}[{}] read [{}/{}]", params.getFollowShardId(), numConcurrentReads, from, size);
                sendShardChangesRequest(from, size, from + size);
                lastRequestedSeqno = from + size;
            }
            if (numConcurrentReads == 0) {
                LOGGER.trace("{} re-scheduling coordinate reads phase", params.getFollowShardId());
                scheduler.accept(TimeValue.timeValueMillis(500), this::coordinateReads);
            }
        } else {
            if (numConcurrentReads == 0) {
                // We sneak peek if there is any thing new in the leader primary.
                // If there is we will happily accept
                numConcurrentReads++;
                long from = lastRequestedSeqno + 1;
                LOGGER.trace("{}[{}] peek read [{}]", params.getFollowShardId(), numConcurrentReads, from);
                sendShardChangesRequest(from, maxReadSize, null);
            }
        }
    }

    private boolean hasReadBudget() {
        assert Thread.holdsLock(this);
        if (numConcurrentReads >= params.getMaxConcurrentReads()) {
            LOGGER.trace("{} no new reads, maximum number of concurrent reads have been reached [{}]",
                params.getFollowShardId(), numConcurrentReads);
            return false;
        }
        if (buffer.size() > params.getMaxBufferSize()) {
            LOGGER.trace("{} no new reads, buffer limit has been reached [{}]", params.getFollowShardId(), buffer.size());
            return false;
        }
        return true;
    }

    private synchronized void coordinateWrites() {
        while (hasWriteBudget() && buffer.isEmpty() == false) {
            if (numConcurrentWrites >= params.getMaxConcurrentWrites()) {
                LOGGER.trace("{} maximum number of concurrent writes have been reached [{}]",
                    params.getFollowShardId(), numConcurrentWrites);
                break;
            }
            Translog.Operation[] ops = new Translog.Operation[Math.min(params.getMaxWriteSize(), buffer.size())];
            for (int i = 0; i < ops.length; i++) {
                ops[i] = buffer.remove();
            }
            numConcurrentWrites++;
            LOGGER.trace("{}[{}] write [{}/{}] [{}]", params.getFollowShardId(), numConcurrentWrites, ops[0].seqNo(),
                ops[ops.length - 1].seqNo(), ops.length);
            sendBulkShardOperationsRequest(ops);
        }
    }

    private boolean hasWriteBudget() {
        assert Thread.holdsLock(this);
        if (numConcurrentWrites >= params.getMaxConcurrentWrites()) {
            LOGGER.trace("{} maximum number of concurrent writes have been reached [{}]",
                params.getFollowShardId(), numConcurrentWrites);
            return false;
        }
        return true;
    }


    private void sendShardChangesRequest(long from, long size, Long targetSeqNo) {
        innerSendShardChangesRequest(from, size,
            response -> {
                retryCounter.set(0);
                handleResponse(from, size, targetSeqNo, response);
            },
            e -> handleFailure(e, () -> sendShardChangesRequest(from, size, targetSeqNo)));
    }

    private void handleResponse(long from, long size, Long targetSeqNo, ShardChangesAction.Response response) {
        maybeUpdateMapping(response.getIndexMetadataVersion(), () -> {
            synchronized (ShardFollowNodeTask.this) {
                globalCheckpoint = Math.max(globalCheckpoint, response.getLeaderGlobalCheckpoint());
                if (response.getOperations().length == 0) {
                    if (targetSeqNo != null) {
                        LOGGER.trace("{} received no ops while [{}/{}] was expected, re-executing read...",
                            params.getFollowShardId(), from, size);
                        sendShardChangesRequest(from, size, targetSeqNo);
                    } else {
                        numConcurrentReads--;
                        if (numConcurrentWrites == 0) {
                            coordinateWrites();
                        }
                        LOGGER.trace("{} received no ops, scheduling to coordinate reads", params.getFollowShardId());
                        scheduler.accept(TimeValue.timeValueMillis(500), this::coordinateReads);
                    }
                } else {
                    Translog.Operation firstOp = response.getOperations()[0];
                    assert firstOp.seqNo() == from;
                    Translog.Operation lastOp = response.getOperations()[response.getOperations().length - 1];

                    LOGGER.trace("{} received [{}/{}]", params.getFollowShardId(), firstOp.seqNo(), lastOp.seqNo());
                    buffer.addAll(Arrays.asList(response.getOperations()));
                    if (targetSeqNo != null) {
                        if (lastOp.seqNo() < targetSeqNo) {
                            long newFrom = lastOp.seqNo() + 1;
                            long newSize = targetSeqNo - lastOp.seqNo();
                            LOGGER.trace("{} received [{}] as last op while [{}] was expected, continue to read [{}/{}]...",
                                params.getFollowShardId(), lastOp.seqNo(), targetSeqNo, newFrom, size);
                            sendShardChangesRequest(newFrom, newSize, targetSeqNo);
                        } else {
                            numConcurrentReads--;
                        }
                    } else {
                        lastRequestedSeqno = Math.max(lastRequestedSeqno, lastOp.seqNo());
                        LOGGER.trace("{} post updating lastRequestedSeqno to [{}]", params.getFollowShardId(), lastRequestedSeqno);
                        numConcurrentReads--;
                    }
                    if (numConcurrentWrites == 0) {
                        coordinateWrites();
                    }
                    coordinateReads();
                }
                assert numConcurrentReads >= 0;
            }
        });
    }

    private void sendBulkShardOperationsRequest(Translog.Operation[] operations) {
        innerSendBulkShardOperationsRequest(operations,
            followerLocalCheckpoint -> {
                retryCounter.set(0);
                handleResponse(followerLocalCheckpoint);
            },
            e -> handleFailure(e, () -> sendBulkShardOperationsRequest(operations))
        );
    }

    private synchronized void handleResponse(long followerLocalCheckpoint) {
        processedGlobalCheckpoint = Math.max(processedGlobalCheckpoint, followerLocalCheckpoint);
        numConcurrentWrites--;
        assert numConcurrentWrites >= 0;
        coordinateWrites();
    }

    private synchronized void maybeUpdateMapping(Long minimumRequiredIndexMetadataVersion, Runnable task) {
        if (currentIndexMetadataVersion >= minimumRequiredIndexMetadataVersion) {
            LOGGER.trace("{} index metadata version [{}] is higher or equal than minimum required index metadata version [{}]",
                params.getFollowShardId(), currentIndexMetadataVersion, minimumRequiredIndexMetadataVersion);
            task.run();
        } else {
            LOGGER.debug("{} updating mapping, index metadata version [{}] is lower than minimum required index metadata version [{}]",
                params.getFollowShardId(), currentIndexMetadataVersion, minimumRequiredIndexMetadataVersion);
            updateMapping(imdVersion -> {
                retryCounter.set(0);
                currentIndexMetadataVersion = imdVersion;
                task.run();
            });
        }
    }

    private void handleFailure(Exception e, Runnable task) {
        assert e != null;
        if (shouldRetry(e)) {
            if (isStopped() == false && retryCounter.incrementAndGet() <= RETRY_LIMIT) {
                LOGGER.debug(new ParameterizedMessage("{} error during follow shard task, retrying...",
                    new Object[]{params.getFollowShardId()}, e));
                scheduler.accept(RETRY_TIMEOUT, task);
            } else {
                markAsFailed(new ElasticsearchException("retrying failed [" + retryCounter.get() +
                    "] times, aborting...", e));
            }
        } else {
            markAsFailed(e);
        }
    }

    private boolean shouldRetry(Exception e) {
        // TODO: What other exceptions should be retried?
        return NetworkExceptionHelper.isConnectException(e) ||
            NetworkExceptionHelper.isCloseConnectionException(e) ||
            e instanceof ActionTransportException ||
            e instanceof NodeClosedException ||
            e instanceof UnavailableShardsException ||
            e instanceof NoShardAvailableActionException;
    }

    // These methods are protected for testing purposes:
    protected void updateMapping(LongConsumer handler) {
        Index leaderIndex = params.getLeaderShardId().getIndex();
        Index followIndex = params.getFollowShardId().getIndex();

        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear();
        clusterStateRequest.metaData(true);
        clusterStateRequest.indices(leaderIndex.getName());

        leaderClient.admin().cluster().state(clusterStateRequest, ActionListener.wrap(clusterStateResponse -> {
            IndexMetaData indexMetaData = clusterStateResponse.getState().metaData().getIndexSafe(leaderIndex);
            assert indexMetaData.getMappings().size() == 1;
            MappingMetaData mappingMetaData = indexMetaData.getMappings().iterator().next().value;

            PutMappingRequest putMappingRequest = new PutMappingRequest(followIndex.getName());
            putMappingRequest.type(mappingMetaData.type());
            putMappingRequest.source(mappingMetaData.source().string(), XContentType.JSON);
            followerClient.admin().indices().putMapping(putMappingRequest, ActionListener.wrap(
                putMappingResponse -> handler.accept(indexMetaData.getVersion()),
                e -> handleFailure(e, () -> updateMapping(handler))));
        }, e -> handleFailure(e, () -> updateMapping(handler))));
    }

    protected void innerSendBulkShardOperationsRequest(Translog.Operation[] operations,
                                                     LongConsumer handler,
                                                     Consumer<Exception> errorHandler) {
        final BulkShardOperationsRequest request = new BulkShardOperationsRequest(params.getFollowShardId(), operations);
        followerClient.execute(BulkShardOperationsAction.INSTANCE, request,
            new ActionListener<BulkShardOperationsResponse>() {
                @Override
                public void onResponse(BulkShardOperationsResponse response) {
                    handler.accept(response.getLocalCheckpoint());
                }

                @Override
                public void onFailure(Exception e) {
                    errorHandler.accept(e);
                }
            }
        );
    }

    protected void innerSendShardChangesRequest(long from,
                                                long size,
                                                Consumer<ShardChangesAction.Response> handler,
                                                Consumer<Exception> errorHandler) {
        ShardChangesAction.Request request = new ShardChangesAction.Request(params.getLeaderShardId());
        request.setFromSeqNo(from);
        request.setMaxOperationCount(size);
        request.setMaxOperationSizeInBytes(params.getMaxOperationSizeInBytes());
        leaderClient.execute(ShardChangesAction.INSTANCE, request, new ActionListener<ShardChangesAction.Response>() {
            @Override
            public void onResponse(ShardChangesAction.Response response) {
                handler.accept(response);
            }

            @Override
            public void onFailure(Exception e) {
                errorHandler.accept(e);
            }
        });
    }

    @Override
    protected void onCancelled() {
        markAsCompleted();
    }

    protected boolean isStopped() {
        return isCancelled() || isCompleted();
    }

    @Override
    public Status getStatus() {
        return new Status(processedGlobalCheckpoint, numConcurrentReads, numConcurrentWrites);
    }

    public static class Status implements Task.Status {

        public static final String NAME = "shard-follow-node-task-status";

        static final ParseField PROCESSED_GLOBAL_CHECKPOINT_FIELD = new ParseField("processed_global_checkpoint");
        static final ParseField NUMBER_OF_CONCURRENT_READS_FIELD = new ParseField("number_of_concurrent_reads");
        static final ParseField NUMBER_OF_CONCURRENT_WRITES_FIELD = new ParseField("number_of_concurrent_writes");

        static final ConstructingObjectParser<Status, Void> PARSER =
                new ConstructingObjectParser<>(NAME, args -> new Status((long) args[0], (int) args[1], (int) args[2]));

        static {
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), PROCESSED_GLOBAL_CHECKPOINT_FIELD);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_CONCURRENT_READS_FIELD);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_CONCURRENT_WRITES_FIELD);
        }

        private final long processedGlobalCheckpoint;
        private final int numberOfConcurrentReads;
        private final int numberOfConcurrentWrites;

        Status(long processedGlobalCheckpoint, int numberOfConcurrentReads, int numberOfConcurrentWrites) {
            this.processedGlobalCheckpoint = processedGlobalCheckpoint;
            this.numberOfConcurrentReads = numberOfConcurrentReads;
            this.numberOfConcurrentWrites = numberOfConcurrentWrites;
        }

        public Status(StreamInput in) throws IOException {
            this.processedGlobalCheckpoint = in.readZLong();
            this.numberOfConcurrentReads = in.readVInt();
            this.numberOfConcurrentWrites = in.readVInt();
        }

        public long getProcessedGlobalCheckpoint() {
            return processedGlobalCheckpoint;
        }

        public int getNumberOfConcurrentReads() {
            return numberOfConcurrentReads;
        }

        public int getNumberOfConcurrentWrites() {
            return numberOfConcurrentWrites;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(processedGlobalCheckpoint);
            out.writeVInt(numberOfConcurrentReads);
            out.writeVInt(numberOfConcurrentWrites);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(PROCESSED_GLOBAL_CHECKPOINT_FIELD.getPreferredName(), processedGlobalCheckpoint);
            }
            {
                builder.field(NUMBER_OF_CONCURRENT_READS_FIELD.getPreferredName(), numberOfConcurrentReads);
            }
            {
                builder.field(NUMBER_OF_CONCURRENT_WRITES_FIELD.getPreferredName(), numberOfConcurrentWrites);
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
            return processedGlobalCheckpoint == status.processedGlobalCheckpoint &&
                numberOfConcurrentReads == status.numberOfConcurrentReads &&
                numberOfConcurrentWrites == status.numberOfConcurrentWrites;
        }

        @Override
        public int hashCode() {
            return Objects.hash(processedGlobalCheckpoint, numberOfConcurrentReads, numberOfConcurrentWrites);
        }

        public String toString() {
            return Strings.toString(this);
        }
    }

}
