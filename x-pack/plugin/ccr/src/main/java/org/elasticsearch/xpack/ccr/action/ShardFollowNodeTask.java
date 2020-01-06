/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.xpack.ccr.Ccr;
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
    private static final Logger LOGGER = LogManager.getLogger(ShardFollowNodeTask.class);

    private final ShardFollowTask params;
    private final BiConsumer<TimeValue, Runnable> scheduler;
    private final LongSupplier relativeTimeProvider;

    private String followerHistoryUUID;
    private long leaderGlobalCheckpoint;
    private long leaderMaxSeqNo;
    private long leaderMaxSeqNoOfUpdatesOrDeletes = SequenceNumbers.UNASSIGNED_SEQ_NO;
    private long lastRequestedSeqNo;
    private long followerGlobalCheckpoint = 0;
    private long followerMaxSeqNo = 0;
    private int numOutstandingReads = 0;
    private int numOutstandingWrites = 0;
    private long currentMappingVersion = 0;
    private long currentSettingsVersion = 0;
    private long currentAliasesVersion = 0;
    private long totalReadRemoteExecTimeMillis = 0;
    private long totalReadTimeMillis = 0;
    private long successfulReadRequests = 0;
    private long failedReadRequests = 0;
    private long operationsRead = 0;
    private long bytesRead = 0;
    private long totalWriteTimeMillis = 0;
    private long successfulWriteRequests = 0;
    private long failedWriteRequests = 0;
    private long operationWritten = 0;
    private long lastFetchTime = -1;
    private final Queue<Translog.Operation> buffer = new PriorityQueue<>(Comparator.comparing(Translog.Operation::seqNo));
    private long bufferSizeInBytes = 0;
    private final LinkedHashMap<Long, Tuple<AtomicInteger, ElasticsearchException>> fetchExceptions;

    private volatile ElasticsearchException fatalException;

    private Scheduler.Cancellable renewable;

    synchronized Scheduler.Cancellable getRenewable() {
        return renewable;
    }

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
                return size() > params.getMaxOutstandingReadRequests();
            }
        };
    }

    void start(
        final String followerHistoryUUID,
        final long leaderGlobalCheckpoint,
        final long leaderMaxSeqNo,
        final long followerGlobalCheckpoint,
        final long followerMaxSeqNo) {
        /*
         * While this should only ever be called once and before any other threads can touch these fields, we use synchronization here to
         * avoid the need to declare these fields as volatile. That is, we are ensuring these fields are always accessed under the same
         * lock.
         */
        synchronized (this) {
            this.followerHistoryUUID = followerHistoryUUID;
            this.leaderGlobalCheckpoint = leaderGlobalCheckpoint;
            this.leaderMaxSeqNo = leaderMaxSeqNo;
            this.followerGlobalCheckpoint = followerGlobalCheckpoint;
            this.followerMaxSeqNo = followerMaxSeqNo;
            this.lastRequestedSeqNo = followerGlobalCheckpoint;
            renewable = scheduleBackgroundRetentionLeaseRenewal(() -> {
                synchronized (ShardFollowNodeTask.this) {
                    return this.followerGlobalCheckpoint;
                }
            });
        }

        // updates follower mapping, this gets us the leader mapping version and makes sure that leader and follower mapping are identical
        updateMapping(0L, leaderMappingVersion -> {
            synchronized (ShardFollowNodeTask.this) {
                currentMappingVersion = leaderMappingVersion;
            }
            updateSettings(leaderSettingsVersion -> {
                synchronized (ShardFollowNodeTask.this) {
                    currentSettingsVersion = leaderSettingsVersion;
                }
            });
            updateAliases(leaderAliasesVersion -> {
                synchronized (ShardFollowNodeTask.this) {
                    currentAliasesVersion = leaderAliasesVersion;
                }
            });
            synchronized (ShardFollowNodeTask.this) {
                LOGGER.info(
                        "{} following leader shard {}, " +
                                "follower global checkpoint=[{}], " +
                                "mapping version=[{}], " +
                                "settings version=[{}], " +
                                "aliases version=[{}]",
                        params.getFollowShardId(),
                        params.getLeaderShardId(),
                        followerGlobalCheckpoint,
                        currentMappingVersion,
                        currentSettingsVersion,
                        currentAliasesVersion);
            }
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
        final int maxReadRequestOperationCount = params.getMaxReadRequestOperationCount();
        while (hasReadBudget() && lastRequestedSeqNo < leaderGlobalCheckpoint) {
            final long from = lastRequestedSeqNo + 1;
            final long maxRequiredSeqNo = Math.min(leaderGlobalCheckpoint, from + maxReadRequestOperationCount - 1);
            final int requestOpCount;
            if (numOutstandingReads == 0) {
                // This is the only request, we can optimistically fetch more documents if possible but not enforce max_required_seqno.
                requestOpCount = maxReadRequestOperationCount;
            } else {
                requestOpCount = Math.toIntExact(maxRequiredSeqNo - from + 1);
            }
            assert 0 < requestOpCount && requestOpCount <= maxReadRequestOperationCount : "read_request_operation_count=" + requestOpCount;
            LOGGER.trace("{}[{} ongoing reads] read from_seqno={} max_required_seqno={} batch_count={}",
                params.getFollowShardId(), numOutstandingReads, from, maxRequiredSeqNo, requestOpCount);
            numOutstandingReads++;
            sendShardChangesRequest(from, requestOpCount, maxRequiredSeqNo);
            lastRequestedSeqNo = maxRequiredSeqNo;
        }

        if (numOutstandingReads == 0 && hasReadBudget()) {
            assert lastRequestedSeqNo == leaderGlobalCheckpoint;
            // We sneak peek if there is any thing new in the leader.
            // If there is we will happily accept
            numOutstandingReads++;
            long from = lastRequestedSeqNo + 1;
            LOGGER.trace("{}[{}] peek read [{}]", params.getFollowShardId(), numOutstandingReads, from);
            sendShardChangesRequest(from, maxReadRequestOperationCount, lastRequestedSeqNo);
        }
    }

    private boolean hasReadBudget() {
        assert Thread.holdsLock(this);
        if (numOutstandingReads >= params.getMaxOutstandingReadRequests()) {
            LOGGER.trace("{} no new reads, maximum number of concurrent reads have been reached [{}]",
                params.getFollowShardId(), numOutstandingReads);
            return false;
        }
        if (bufferSizeInBytes >= params.getMaxWriteBufferSize().getBytes()) {
            LOGGER.trace("{} no new reads, buffer size limit has been reached [{}]", params.getFollowShardId(), bufferSizeInBytes);
            return false;
        }
        if (buffer.size() > params.getMaxWriteBufferCount()) {
            LOGGER.trace("{} no new reads, buffer count limit has been reached [{}]", params.getFollowShardId(), buffer.size());
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
            int length = Math.min(params.getMaxWriteRequestOperationCount(), buffer.size());
            List<Translog.Operation> ops = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                Translog.Operation op = buffer.remove();
                ops.add(op);
                sumEstimatedSize += op.estimateSize();
                if (sumEstimatedSize > params.getMaxWriteRequestSize().getBytes()) {
                    break;
                }
            }
            bufferSizeInBytes -= sumEstimatedSize;
            numOutstandingWrites++;
            LOGGER.trace("{}[{}] write [{}/{}] [{}]", params.getFollowShardId(), numOutstandingWrites, ops.get(0).seqNo(),
                ops.get(ops.size() - 1).seqNo(), ops.size());
            sendBulkShardOperationsRequest(ops, leaderMaxSeqNoOfUpdatesOrDeletes, new AtomicInteger(0));
        }
    }

    private boolean hasWriteBudget() {
        assert Thread.holdsLock(this);
        if (numOutstandingWrites >= params.getMaxOutstandingWriteRequests()) {
            LOGGER.trace("{} maximum number of concurrent writes have been reached [{}]",
                params.getFollowShardId(), numOutstandingWrites);
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
                    synchronized (ShardFollowNodeTask.this) {
                        // Always clear fetch exceptions:
                        fetchExceptions.remove(from);
                        if (response.getOperations().length > 0) {
                            // do not count polls against fetch stats
                            totalReadRemoteExecTimeMillis += response.getTookInMillis();
                            totalReadTimeMillis += TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTime);
                            successfulReadRequests++;
                            operationsRead += response.getOperations().length;
                            bytesRead +=
                                Arrays.stream(response.getOperations()).mapToLong(Translog.Operation::estimateSize).sum();
                        }
                    }
                    handleReadResponse(from, maxRequiredSeqNo, response);
                },
                e -> {
                    synchronized (ShardFollowNodeTask.this) {
                        totalReadTimeMillis += TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTime);
                        failedReadRequests++;
                        fetchExceptions.put(from, Tuple.tuple(retryCounter, ExceptionsHelper.convertToElastic(e)));
                    }
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof ResourceNotFoundException) {
                        ResourceNotFoundException resourceNotFoundException = (ResourceNotFoundException) cause;
                        if (resourceNotFoundException.getMetadataKeys().contains(Ccr.REQUESTED_OPS_MISSING_METADATA_KEY)) {
                            handleFallenBehindLeaderShard(e, from, maxOperationCount, maxRequiredSeqNo, retryCounter);
                            return;
                        }
                    }
                    handleFailure(e, retryCounter, () -> sendShardChangesRequest(from, maxOperationCount, maxRequiredSeqNo, retryCounter));
                });
    }

    void handleReadResponse(long from, long maxRequiredSeqNo, ShardChangesAction.Response response) {
        // In order to process this read response (3), we need to check and potentially update the follow index's setting (1) and
        // check and potentially update the follow index's mappings (2).

        // 4) handle read response:
        Runnable handleResponseTask = () -> innerHandleReadResponse(from, maxRequiredSeqNo, response);
        // 3) update follow index mapping:
        Runnable updateMappingsTask = () -> maybeUpdateMapping(response.getMappingVersion(), handleResponseTask);
        // 2) update follow index settings:
        Runnable updateSettingsTask = () -> maybeUpdateSettings(response.getSettingsVersion(), updateMappingsTask);
        // 1) update follow index aliases:
        maybeUpdateAliases(response.getAliasesVersion(), updateSettingsTask);
    }

    void handleFallenBehindLeaderShard(Exception e, long from, int maxOperationCount, long maxRequiredSeqNo, AtomicInteger retryCounter) {
        // Do restore from repository here and after that
        // start() should be invoked and stats should be reset

        // For now handle like any other failure:
        // need a more robust approach to avoid the scenario where an outstanding request
        // can trigger another restore while the shard was restored already.
        // https://github.com/elastic/elasticsearch/pull/37562#discussion_r250009367

        handleFailure(e, retryCounter, () -> sendShardChangesRequest(from, maxOperationCount, maxRequiredSeqNo, retryCounter));
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
            List<Translog.Operation> operations = Arrays.asList(response.getOperations());
            long operationsSize = operations.stream()
                .mapToLong(Translog.Operation::estimateSize)
                .sum();
            buffer.addAll(operations);
            bufferSizeInBytes += operationsSize;
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
            numOutstandingReads--;
            coordinateReads();
        }
    }

    private void sendBulkShardOperationsRequest(List<Translog.Operation> operations, long leaderMaxSeqNoOfUpdatesOrDeletes,
                                                AtomicInteger retryCounter) {
        assert leaderMaxSeqNoOfUpdatesOrDeletes != SequenceNumbers.UNASSIGNED_SEQ_NO : "mus is not replicated";
        final long startTime = relativeTimeProvider.getAsLong();
        innerSendBulkShardOperationsRequest(followerHistoryUUID, operations, leaderMaxSeqNoOfUpdatesOrDeletes,
                response -> {
                    synchronized (ShardFollowNodeTask.this) {
                        totalWriteTimeMillis += TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTime);
                        successfulWriteRequests++;
                        operationWritten += operations.size();
                    }
                    handleWriteResponse(response);
                },
                e -> {
                    synchronized (ShardFollowNodeTask.this) {
                        totalWriteTimeMillis += TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTime);
                        failedWriteRequests++;
                    }
                    handleFailure(e, retryCounter,
                        () -> sendBulkShardOperationsRequest(operations, leaderMaxSeqNoOfUpdatesOrDeletes, retryCounter));
                }
        );
    }

    private synchronized void handleWriteResponse(final BulkShardOperationsResponse response) {
        this.followerGlobalCheckpoint = Math.max(this.followerGlobalCheckpoint, response.getGlobalCheckpoint());
        this.followerMaxSeqNo = Math.max(this.followerMaxSeqNo, response.getMaxSeqNo());
        numOutstandingWrites--;
        assert numOutstandingWrites >= 0;
        coordinateWrites();

        // In case that buffer has more ops than is allowed then reads may all have been stopped,
        // this invocation makes sure that we start a read when there is budget in case no reads are being performed.
        coordinateReads();
    }

    private synchronized void maybeUpdateMapping(long minimumRequiredMappingVersion, Runnable task) {
        if (currentMappingVersion >= minimumRequiredMappingVersion) {
            LOGGER.trace("{} mapping version [{}] is higher or equal than minimum required mapping version [{}]",
                params.getFollowShardId(), currentMappingVersion, minimumRequiredMappingVersion);
            task.run();
        } else {
            LOGGER.trace("{} updating mapping, mapping version [{}] is lower than minimum required mapping version [{}]",
                params.getFollowShardId(), currentMappingVersion, minimumRequiredMappingVersion);
            updateMapping(minimumRequiredMappingVersion, mappingVersion -> {
                currentMappingVersion = mappingVersion;
                task.run();
            });
        }
    }

    private synchronized void maybeUpdateSettings(final Long minimumRequiredSettingsVersion, Runnable task) {
        if (currentSettingsVersion >= minimumRequiredSettingsVersion) {
            LOGGER.trace("{} settings version [{}] is higher or equal than minimum required settings version [{}]",
                    params.getFollowShardId(), currentSettingsVersion, minimumRequiredSettingsVersion);
            task.run();
        } else {
            LOGGER.trace("{} updating settings, settings version [{}] is lower than minimum required settings version [{}]",
                    params.getFollowShardId(), currentSettingsVersion, minimumRequiredSettingsVersion);
            updateSettings(settingsVersion -> {
                currentSettingsVersion = settingsVersion;
                task.run();
            });
        }
    }

    private synchronized void maybeUpdateAliases(final Long minimumRequiredAliasesVersion, final Runnable task) {
        if (currentAliasesVersion >= minimumRequiredAliasesVersion) {
            LOGGER.trace(
                    "{} aliases version [{}] is higher or equal than minimum required aliases version [{}]",
                    params.getFollowShardId(),
                    currentAliasesVersion,
                    minimumRequiredAliasesVersion);
            task.run();
        } else {
            LOGGER.trace(
                    "{} updating aliases, aliases version [{}] is lower than minimum required aliases version [{}]",
                    params.getFollowShardId(),
                    currentAliasesVersion,
                    minimumRequiredAliasesVersion);
            updateAliases(aliasesVersion -> {
                currentAliasesVersion = aliasesVersion;
                task.run();
            });
        }
    }

    private void updateMapping(long minRequiredMappingVersion, LongConsumer handler) {
        updateMapping(minRequiredMappingVersion, handler, new AtomicInteger(0));
    }

    private void updateMapping(long minRequiredMappingVersion, LongConsumer handler, AtomicInteger retryCounter) {
        innerUpdateMapping(minRequiredMappingVersion, handler,
            e -> handleFailure(e, retryCounter, () -> updateMapping(minRequiredMappingVersion, handler, retryCounter)));
    }

    private void updateSettings(final LongConsumer handler) {
        updateSettings(handler, new AtomicInteger(0));
    }

    private void updateSettings(final LongConsumer handler, final AtomicInteger retryCounter) {
        innerUpdateSettings(handler, e -> handleFailure(e, retryCounter, () -> updateSettings(handler, retryCounter)));
    }

    private void updateAliases(final LongConsumer handler) {
        updateAliases(handler, new AtomicInteger());
    }

    private void updateAliases(final LongConsumer handler, final AtomicInteger retryCounter) {
        innerUpdateAliases(handler, e -> handleFailure(e, retryCounter, () -> updateAliases(handler, retryCounter)));
    }

    private void handleFailure(Exception e, AtomicInteger retryCounter, Runnable task) {
        assert e != null;
        if (shouldRetry(e)) {
            if (isStopped() == false) {
                // Only retry is the shard follow task is not stopped.
                int currentRetry = retryCounter.incrementAndGet();
                LOGGER.debug(new ParameterizedMessage("{} error during follow shard task, retrying [{}]",
                    params.getFollowShardId(), currentRetry), e);
                long delay = computeDelay(currentRetry, params.getReadPollTimeout().getMillis());
                scheduler.accept(TimeValue.timeValueMillis(delay), task);
            }
        } else {
            setFatalException(e);
        }
    }

    void setFatalException(Exception e) {
        fatalException = ExceptionsHelper.convertToElastic(e);
        LOGGER.warn("shard follow task encounter non-retryable error", e);
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

    static boolean shouldRetry(final Exception e) {
        if (NetworkExceptionHelper.isConnectException(e)) {
            return true;
        } else if (NetworkExceptionHelper.isCloseConnectionException(e)) {
            return true;
        }

        final Throwable actual = ExceptionsHelper.unwrapCause(e);
        return actual instanceof ShardNotFoundException ||
            actual instanceof IllegalIndexShardStateException ||
            actual instanceof NoShardAvailableActionException ||
            actual instanceof UnavailableShardsException ||
            actual instanceof AlreadyClosedException ||
            actual instanceof ElasticsearchSecurityException || // If user does not have sufficient privileges
            actual instanceof ClusterBlockException || // If leader index is closed or no elected master
            actual instanceof IndexClosedException || // If follow index is closed
            actual instanceof ConnectTransportException ||
            actual instanceof NodeClosedException ||
            actual instanceof NoSuchRemoteClusterException ||
            actual instanceof EsRejectedExecutionException;
    }

    // These methods are protected for testing purposes:
    protected abstract void innerUpdateMapping(long minRequiredMappingVersion, LongConsumer handler, Consumer<Exception> errorHandler);

    protected abstract void innerUpdateSettings(LongConsumer handler, Consumer<Exception> errorHandler);

    protected abstract void innerUpdateAliases(LongConsumer handler, Consumer<Exception> errorHandler);

    protected abstract void innerSendBulkShardOperationsRequest(String followerHistoryUUID,
                                                                List<Translog.Operation> operations,
                                                                long leaderMaxSeqNoOfUpdatesOrDeletes,
                                                                Consumer<BulkShardOperationsResponse> handler,
                                                                Consumer<Exception> errorHandler);

    protected abstract void innerSendShardChangesRequest(long from, int maxOperationCount, Consumer<ShardChangesAction.Response> handler,
                                                         Consumer<Exception> errorHandler);

    protected abstract Scheduler.Cancellable scheduleBackgroundRetentionLeaseRenewal(LongSupplier followerGlobalCheckpoint);

    @Override
    protected void onCancelled() {
        synchronized (this) {
            if (renewable != null) {
                renewable.cancel();
                renewable = null;
            }
        }
        markAsCompleted();
    }

    protected boolean isStopped() {
        return fatalException != null || isCancelled() || isCompleted();
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
                params.getRemoteCluster(),
                params.getLeaderShardId().getIndexName(),
                params.getFollowShardId().getIndexName(),
                getFollowShardId().getId(),
                leaderGlobalCheckpoint,
                leaderMaxSeqNo,
                followerGlobalCheckpoint,
                followerMaxSeqNo,
                lastRequestedSeqNo,
                numOutstandingReads,
                numOutstandingWrites,
                buffer.size(),
                bufferSizeInBytes,
                currentMappingVersion,
                currentSettingsVersion,
                currentAliasesVersion,
                totalReadTimeMillis,
                totalReadRemoteExecTimeMillis,
                successfulReadRequests,
                failedReadRequests,
                operationsRead,
                bytesRead,
                totalWriteTimeMillis,
                successfulWriteRequests,
                failedWriteRequests,
                operationWritten,
                new TreeMap<>(
                        fetchExceptions
                                .entrySet()
                                .stream()
                                .collect(
                                        Collectors.toMap(Map.Entry::getKey, e -> Tuple.tuple(e.getValue().v1().get(), e.getValue().v2())))),
                timeSinceLastFetchMillis,
                fatalException);
    }

}
