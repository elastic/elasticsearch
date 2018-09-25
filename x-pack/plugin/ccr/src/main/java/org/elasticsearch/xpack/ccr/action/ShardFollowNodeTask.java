/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * The node task that fetch the write operations from a leader shard and
 * persists these ops in the follower shard.
 */
public abstract class ShardFollowNodeTask extends AllocatedPersistentTask {

    private static final int DELAY_MILLIS = 50;
    private static final Logger LOGGER = Loggers.getLogger(ShardFollowNodeTask.class);

    private final String leaderIndex;
    private final ShardFollowTask params;
    private final BiConsumer<TimeValue, Runnable> scheduler;
    private final LongSupplier relativeTimeProvider;

    private long leaderGlobalCheckpoint;
    private long leaderMaxSeqNo;
    private long leaderMaxSeqNoOfUpdatesOrDeletes = SequenceNumbers.UNASSIGNED_SEQ_NO;
    private long lastRequestedSeqNo;
    private long followerGlobalCheckpoint = 0;
    private long followerMaxSeqNo = 0;
    private int numConcurrentReads = 0;
    private int numConcurrentWrites = 0;
    private long currentMappingVersion = 0;
    private long totalFetchTimeMillis = 0;
    private long numberOfSuccessfulFetches = 0;
    private long numberOfFailedFetches = 0;
    private long operationsReceived = 0;
    private long totalTransferredBytes = 0;
    private long totalIndexTimeMillis = 0;
    private long numberOfSuccessfulBulkOperations = 0;
    private long numberOfFailedBulkOperations = 0;
    private long numberOfOperationsIndexed = 0;
    private long lastFetchTime = -1;
    private final Queue<Translog.Operation> buffer = new PriorityQueue<>(Comparator.comparing(Translog.Operation::seqNo));
    private final LinkedHashMap<Long, Tuple<AtomicInteger, ElasticsearchException>> fetchExceptions;

    ShardFollowNodeTask(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers,
                        ShardFollowTask params, BiConsumer<TimeValue, Runnable> scheduler, final LongSupplier relativeTimeProvider) {
        super(id, type, action, description, parentTask, headers);
        this.params = params;
        this.scheduler = scheduler;
        this.relativeTimeProvider = relativeTimeProvider;
        /*
         * We keep track of the most recent fetch exceptions, with the number of exceptions that we track equal to the maximum number of
         * concurrent fetches. For each failed fetch, we track the from sequence number associated with the request, and we clear the entry
         * when the fetch task associated with that from sequence number succeeds.
         */
        this.fetchExceptions = new LinkedHashMap<Long, Tuple<AtomicInteger, ElasticsearchException>>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<Long, Tuple<AtomicInteger, ElasticsearchException>> eldest) {
                return size() > params.getMaxConcurrentReadBatches();
            }
        };

        if (params.getLeaderClusterAlias() != null) {
            leaderIndex = params.getLeaderClusterAlias() + ":" + params.getLeaderShardId().getIndexName();
        } else {
            leaderIndex = params.getLeaderShardId().getIndexName();
        }
    }

    void start(
            final long leaderGlobalCheckpoint,
            final long leaderMaxSeqNo,
            final long followerGlobalCheckpoint,
            final long followerMaxSeqNo) {
        /*
         * While this should only ever be called once and before any other threads can touch these fields, we use synchronization here to
         * avoid the need to declare these fields as volatile. That is, we are ensuring thesefields are always accessed under the same lock.
         */
        synchronized (this) {
            this.leaderGlobalCheckpoint = leaderGlobalCheckpoint;
            this.leaderMaxSeqNo = leaderMaxSeqNo;
            this.followerGlobalCheckpoint = followerGlobalCheckpoint;
            this.followerMaxSeqNo = followerMaxSeqNo;
            this.lastRequestedSeqNo = followerGlobalCheckpoint;
        }

        // updates follower mapping, this gets us the leader mapping version and makes sure that leader and follower mapping are identical
        updateMapping(mappingVersion -> {
            synchronized (ShardFollowNodeTask.this) {
                currentMappingVersion = mappingVersion;
            }
            LOGGER.info("{} Started to follow leader shard {}, followGlobalCheckPoint={}, mappingVersion={}",
                params.getFollowShardId(), params.getLeaderShardId(), followerGlobalCheckpoint, mappingVersion);
            coordinateReads();
        });
    }

    synchronized void coordinateReads() {
        if (isStopped()) {
            LOGGER.info("{} shard follow task has been stopped", params.getFollowShardId());
            return;
        }

        LOGGER.trace("{} coordinate reads, lastRequestedSeqNo={}, leaderGlobalCheckpoint={}",
            params.getFollowShardId(), lastRequestedSeqNo, leaderGlobalCheckpoint);
        final int maxBatchOperationCount = params.getMaxBatchOperationCount();
        while (hasReadBudget() && lastRequestedSeqNo < leaderGlobalCheckpoint) {
            final long from = lastRequestedSeqNo + 1;
            final long maxRequiredSeqNo = Math.min(leaderGlobalCheckpoint, from + maxBatchOperationCount - 1);
            final int requestBatchCount;
            if (numConcurrentReads == 0) {
                // This is the only request, we can optimistically fetch more documents if possible but not enforce max_required_seqno.
                requestBatchCount = maxBatchOperationCount;
            } else {
                requestBatchCount = Math.toIntExact(maxRequiredSeqNo - from + 1);
            }
            assert 0 < requestBatchCount && requestBatchCount <= maxBatchOperationCount : "request_batch_count=" + requestBatchCount;
            LOGGER.trace("{}[{} ongoing reads] read from_seqno={} max_required_seqno={} batch_count={}",
                params.getFollowShardId(), numConcurrentReads, from, maxRequiredSeqNo, requestBatchCount);
            numConcurrentReads++;
            sendShardChangesRequest(from, requestBatchCount, maxRequiredSeqNo);
            lastRequestedSeqNo = maxRequiredSeqNo;
        }

        if (numConcurrentReads == 0 && hasReadBudget()) {
            assert lastRequestedSeqNo == leaderGlobalCheckpoint;
            // We sneak peek if there is any thing new in the leader.
            // If there is we will happily accept
            numConcurrentReads++;
            long from = lastRequestedSeqNo + 1;
            LOGGER.trace("{}[{}] peek read [{}]", params.getFollowShardId(), numConcurrentReads, from);
            sendShardChangesRequest(from, maxBatchOperationCount, lastRequestedSeqNo);
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
            sendBulkShardOperationsRequest(ops, leaderMaxSeqNoOfUpdatesOrDeletes, new AtomicInteger(0));
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
        final long startTime = relativeTimeProvider.getAsLong();
        synchronized (this) {
            lastFetchTime = startTime;
        }
        innerSendShardChangesRequest(from, maxOperationCount,
                response -> {
                    if (response.getOperations().length > 0) {
                        // do not count polls against fetch stats
                        synchronized (ShardFollowNodeTask.this) {
                            totalFetchTimeMillis += TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTime);
                            numberOfSuccessfulFetches++;
                            fetchExceptions.remove(from);
                            operationsReceived += response.getOperations().length;
                            totalTransferredBytes +=
                                    Arrays.stream(response.getOperations()).mapToLong(Translog.Operation::estimateSize).sum();
                        }
                    }
                    handleReadResponse(from, maxRequiredSeqNo, response);
                },
                e -> {
                    synchronized (ShardFollowNodeTask.this) {
                        totalFetchTimeMillis += TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTime);
                        numberOfFailedFetches++;
                        fetchExceptions.put(from, Tuple.tuple(retryCounter, ExceptionsHelper.convertToElastic(e)));
                    }
                    handleFailure(e, retryCounter, () -> sendShardChangesRequest(from, maxOperationCount, maxRequiredSeqNo, retryCounter));
                });
    }

    void handleReadResponse(long from, long maxRequiredSeqNo, ShardChangesAction.Response response) {
        maybeUpdateMapping(response.getMappingVersion(), () -> innerHandleReadResponse(from, maxRequiredSeqNo, response));
    }

    /** Called when some operations are fetched from the leading */
    protected void onOperationsFetched(Translog.Operation[] operations) {

    }

    synchronized void innerHandleReadResponse(long from, long maxRequiredSeqNo, ShardChangesAction.Response response) {
        onOperationsFetched(response.getOperations());
        leaderGlobalCheckpoint = Math.max(leaderGlobalCheckpoint, response.getGlobalCheckpoint());
        leaderMaxSeqNo = Math.max(leaderMaxSeqNo, response.getMaxSeqNo());
        leaderMaxSeqNoOfUpdatesOrDeletes = SequenceNumbers.max(leaderMaxSeqNoOfUpdatesOrDeletes, response.getMaxSeqNoOfUpdatesOrDeletes());
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
            lastRequestedSeqNo = Math.max(lastRequestedSeqNo, maxSeqNo);
            assert lastRequestedSeqNo <= leaderGlobalCheckpoint :  "lastRequestedSeqNo [" + lastRequestedSeqNo +
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
            coordinateReads();
        }
    }

    private void sendBulkShardOperationsRequest(List<Translog.Operation> operations, long leaderMaxSeqNoOfUpdatesOrDeletes,
                                                AtomicInteger retryCounter) {
        assert leaderMaxSeqNoOfUpdatesOrDeletes != SequenceNumbers.UNASSIGNED_SEQ_NO : "mus is not replicated";
        final long startTime = relativeTimeProvider.getAsLong();
        innerSendBulkShardOperationsRequest(operations, leaderMaxSeqNoOfUpdatesOrDeletes,
                response -> {
                    synchronized (ShardFollowNodeTask.this) {
                        totalIndexTimeMillis += TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTime);
                        numberOfSuccessfulBulkOperations++;
                        numberOfOperationsIndexed += operations.size();
                    }
                    handleWriteResponse(response);
                },
                e -> {
                    synchronized (ShardFollowNodeTask.this) {
                        totalIndexTimeMillis += TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTime);
                        numberOfFailedBulkOperations++;
                    }
                    handleFailure(e, retryCounter,
                        () -> sendBulkShardOperationsRequest(operations, leaderMaxSeqNoOfUpdatesOrDeletes, retryCounter));
                }
        );
    }

    private synchronized void handleWriteResponse(final BulkShardOperationsResponse response) {
        this.followerGlobalCheckpoint = Math.max(this.followerGlobalCheckpoint, response.getGlobalCheckpoint());
        this.followerMaxSeqNo = Math.max(this.followerMaxSeqNo, response.getMaxSeqNo());
        numConcurrentWrites--;
        assert numConcurrentWrites >= 0;
        coordinateWrites();

        // In case that buffer has more ops than is allowed then reads may all have been stopped,
        // this invocation makes sure that we start a read when there is budget in case no reads are being performed.
        coordinateReads();
    }

    private synchronized void maybeUpdateMapping(Long minimumRequiredMappingVersion, Runnable task) {
        if (currentMappingVersion >= minimumRequiredMappingVersion) {
            LOGGER.trace("{} mapping version [{}] is higher or equal than minimum required mapping version [{}]",
                params.getFollowShardId(), currentMappingVersion, minimumRequiredMappingVersion);
            task.run();
        } else {
            LOGGER.trace("{} updating mapping, mapping version [{}] is lower than minimum required mapping version [{}]",
                params.getFollowShardId(), currentMappingVersion, minimumRequiredMappingVersion);
            updateMapping(mappingVersion -> {
                currentMappingVersion = mappingVersion;
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
        if (shouldRetry(e) && isStopped() == false) {
            int currentRetry = retryCounter.incrementAndGet();
            LOGGER.debug(new ParameterizedMessage("{} error during follow shard task, retrying [{}]",
                params.getFollowShardId(), currentRetry), e);
            long delay = computeDelay(currentRetry, params.getPollTimeout().getMillis());
            scheduler.accept(TimeValue.timeValueMillis(delay), task);
        } else {
            markAsFailed(e);
        }
    }

    static long computeDelay(int currentRetry, long maxRetryDelayInMillis) {
        // Cap currentRetry to avoid overflow when computing n variable
        int maxCurrentRetry = Math.min(currentRetry, 24);
        long n = Math.round(Math.pow(2, maxCurrentRetry - 1));
        // + 1 here, because nextInt(...) bound is exclusive and otherwise the first delay would always be zero.
        int k = Randomness.get().nextInt(Math.toIntExact(n + 1));
        int backOffDelay = k * DELAY_MILLIS;
        return Math.min(backOffDelay, maxRetryDelayInMillis);
    }

    private static boolean shouldRetry(Exception e) {
        return NetworkExceptionHelper.isConnectException(e) ||
            NetworkExceptionHelper.isCloseConnectionException(e) ||
            TransportActions.isShardNotAvailableException(e);
    }

    // These methods are protected for testing purposes:
    protected abstract void innerUpdateMapping(LongConsumer handler, Consumer<Exception> errorHandler);

    protected abstract void innerSendBulkShardOperationsRequest(List<Translog.Operation> operations, long leaderMaxSeqNoOfUpdatesOrDeletes,
                                    Consumer<BulkShardOperationsResponse> handler, Consumer<Exception> errorHandler);

    protected abstract void innerSendShardChangesRequest(long from, int maxOperationCount, Consumer<ShardChangesAction.Response> handler,
                                                         Consumer<Exception> errorHandler);

    @Override
    protected void onCancelled() {
        markAsCompleted();
    }

    protected boolean isStopped() {
        return isCancelled() || isCompleted();
    }

    public ShardId getFollowShardId() {
        return params.getFollowShardId();
    }

    @Override
    public synchronized ShardFollowNodeTaskStatus getStatus() {
        final long timeSinceLastFetchMillis;
        if (lastFetchTime != -1) {
             timeSinceLastFetchMillis = TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - lastFetchTime);
        } else {
            // To avoid confusion when ccr didn't yet execute a fetch:
            timeSinceLastFetchMillis = -1;
        }
        return new ShardFollowNodeTaskStatus(
                leaderIndex,
                params.getFollowShardId().getIndexName(),
                getFollowShardId().getId(),
                leaderGlobalCheckpoint,
                leaderMaxSeqNo,
                followerGlobalCheckpoint,
                followerMaxSeqNo,
                lastRequestedSeqNo,
                numConcurrentReads,
                numConcurrentWrites,
                buffer.size(),
                currentMappingVersion,
                totalFetchTimeMillis,
                numberOfSuccessfulFetches,
                numberOfFailedFetches,
                operationsReceived,
                totalTransferredBytes,
                totalIndexTimeMillis,
                numberOfSuccessfulBulkOperations,
                numberOfFailedBulkOperations,
                numberOfOperationsIndexed,
                new TreeMap<>(
                        fetchExceptions
                                .entrySet()
                                .stream()
                                .collect(
                                        Collectors.toMap(Map.Entry::getKey, e -> Tuple.tuple(e.getValue().v1().get(), e.getValue().v2())))),
                timeSinceLastFetchMillis);
    }

}
