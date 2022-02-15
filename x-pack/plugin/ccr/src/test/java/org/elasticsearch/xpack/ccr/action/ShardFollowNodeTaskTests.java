/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ShardFollowNodeTaskTests extends ESTestCase {

    private List<long[]> shardChangesRequests;
    private List<List<Translog.Operation>> bulkShardOperationRequests;
    private BiConsumer<TimeValue, Runnable> scheduler = (delay, task) -> task.run();

    private Consumer<ShardFollowNodeTaskStatus> beforeSendShardChangesRequest = status -> {};

    private AtomicBoolean scheduleRetentionLeaseRenewal = new AtomicBoolean();
    private LongConsumer retentionLeaseRenewal = followerGlobalCheckpoint -> {};

    private AtomicBoolean simulateResponse = new AtomicBoolean();

    private Queue<Exception> readFailures;
    private Queue<Exception> writeFailures;
    private Queue<Exception> mappingUpdateFailures;
    private Queue<Long> mappingVersions;
    private Queue<Exception> settingsUpdateFailures;
    private Queue<Long> settingsVersions;
    private Queue<Exception> aliasesUpdateFailures;
    private Queue<Long> aliasesVersions;
    private Queue<Long> leaderGlobalCheckpoints;
    private Queue<Long> followerGlobalCheckpoints;
    private Queue<Long> maxSeqNos;
    private Queue<Integer> responseSizes;
    private Queue<ActionListener<BulkShardOperationsResponse>> pendingBulkShardRequests;

    public void testCoordinateReads() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 8;
        params.maxOutstandingReadRequests = between(8, 20);
        params.maxOutstandingWriteRequests = between(1, 20);

        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 3, -1);
        task.coordinateReads();
        assertThat(shardChangesRequests, contains(new long[] { 0L, 8L })); // treat this a peak request
        shardChangesRequests.clear();
        task.innerHandleReadResponse(0, 5L, generateShardChangesResponse(0, 5L, 0L, 0L, 1L, 60L));
        assertThat(
            shardChangesRequests,
            contains(new long[][] { { 6L, 8L }, { 14L, 8L }, { 22L, 8L }, { 30L, 8L }, { 38L, 8L }, { 46L, 8L }, { 54L, 7L } })
        );
        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingReadRequests(), equalTo(7));
        assertThat(status.lastRequestedSeqNo(), equalTo(60L));
    }

    public void testMaxWriteBufferCount() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 0; // need to set outstandingWrites to 0, other the write buffer gets flushed immediately
        params.maxWriteBufferCount = 32;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        // Also invokes the coordinatesReads() method:
        task.innerHandleReadResponse(0L, 63L, generateShardChangesResponse(0, 63, 0L, 0L, 1L, 128L));
        assertThat(shardChangesRequests.size(), equalTo(0)); // no more reads, because write buffer count limit has been reached

        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingReadRequests(), equalTo(0));
        assertThat(status.outstandingWriteRequests(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(128L));
    }

    public void testMaxWriteBufferSize() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 0; // need to set outstandingWrites to 0, other the write buffer gets flushed immediately
        params.maxWriteBufferSize = new ByteSizeValue(1, ByteSizeUnit.KB);
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        // Also invokes the coordinatesReads() method:
        task.innerHandleReadResponse(0L, 63L, generateShardChangesResponse(0, 63, 0L, 0L, 1L, 128L));
        assertThat(shardChangesRequests.size(), equalTo(0)); // no more reads, because write buffer size limit has been reached

        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingReadRequests(), equalTo(0));
        assertThat(status.outstandingWriteRequests(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(128L));
    }

    public void testMaxOutstandingReads() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 8;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(8L));

        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(7L));
    }

    public void testTaskCancelled() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        // The call the updateMapping is a noop, so noting happens.
        task.start("uuid", 128L, 128L, task.getStatus().followerGlobalCheckpoint(), task.getStatus().followerMaxSeqNo());
        task.markAsCompleted();
        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(0));
    }

    public void testTaskCancelledAfterReadLimitHasBeenReached() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 16;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 31, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(16L));

        task.markAsCompleted();
        shardChangesRequests.clear();
        // Also invokes the coordinatesReads() method:
        task.innerHandleReadResponse(0L, 15L, generateShardChangesResponse(0, 15, 0L, 0L, 1L, 31L));
        assertThat(shardChangesRequests.size(), equalTo(0)); // no more reads, because task has been cancelled
        assertThat(bulkShardOperationRequests.size(), equalTo(0)); // no more writes, because task has been cancelled

        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingReadRequests(), equalTo(0));
        assertThat(status.outstandingWriteRequests(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(15L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(31L));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testTaskCancelledAfterWriteBufferLimitHasBeenReached() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        params.maxWriteBufferCount = 32;

        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        task.markAsCompleted();
        shardChangesRequests.clear();
        // Also invokes the coordinatesReads() method:
        task.innerHandleReadResponse(0L, 63L, generateShardChangesResponse(0, 63, 0L, 0L, 1L, 128L));
        assertThat(shardChangesRequests.size(), equalTo(0)); // no more reads, because task has been cancelled
        assertThat(bulkShardOperationRequests.size(), equalTo(0)); // no more writes, because task has been cancelled

        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingReadRequests(), equalTo(0));
        assertThat(status.outstandingWriteRequests(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(128L));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testReceiveRetryableError() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        int max = randomIntBetween(1, 30);
        final Exception[] exceptions = new Exception[max];
        for (int i = 0; i < max; i++) {
            final Exception exception;
            if (randomBoolean()) {
                exception = new ShardNotFoundException(new ShardId("leader_index", "", 0));
            } else {
                exception = new EsRejectedExecutionException("leader_index rejected");
            }
            exceptions[i] = exception;
            readFailures.add(exception);
        }
        mappingVersions.add(1L);
        leaderGlobalCheckpoints.add(63L);
        maxSeqNos.add(63L);
        responseSizes.add(64);
        simulateResponse.set(true);
        final AtomicLong retryCounter = new AtomicLong();
        // before each retry, we assert the fetch failures; after the last retry, the fetch failure should clear
        beforeSendShardChangesRequest = status -> {
            assertThat(status.failedReadRequests(), equalTo(retryCounter.get()));
            if (retryCounter.get() > 0) {
                assertThat(status.readExceptions().entrySet(), hasSize(1));
                final Map.Entry<Long, Tuple<Integer, ElasticsearchException>> entry = status.readExceptions().entrySet().iterator().next();
                assertThat(entry.getValue().v1(), equalTo(Math.toIntExact(retryCounter.get())));
                assertThat(entry.getKey(), equalTo(0L));
                if (exceptions[Math.toIntExact(retryCounter.get()) - 1] instanceof ShardNotFoundException) {
                    assertThat(entry.getValue().v2(), instanceOf(ShardNotFoundException.class));
                    final ShardNotFoundException shardNotFoundException = (ShardNotFoundException) entry.getValue().v2();
                    assertThat(shardNotFoundException.getShardId().getIndexName(), equalTo("leader_index"));
                    assertThat(shardNotFoundException.getShardId().getId(), equalTo(0));
                } else {
                    assertThat(entry.getValue().v2().getCause(), instanceOf(EsRejectedExecutionException.class));
                    final EsRejectedExecutionException rejectedExecutionException = (EsRejectedExecutionException) entry.getValue()
                        .v2()
                        .getCause();
                    assertThat(rejectedExecutionException.getMessage(), equalTo("leader_index rejected"));
                }
            }
            retryCounter.incrementAndGet();
        };
        task.coordinateReads();

        // NUmber of requests is equal to initial request + retried attempts
        assertThat(shardChangesRequests.size(), equalTo(max + 1));
        for (long[] shardChangesRequest : shardChangesRequests) {
            assertThat(shardChangesRequest[0], equalTo(0L));
            assertThat(shardChangesRequest[1], equalTo(64L));
        }

        assertFalse("task is not stopped", task.isStopped());
        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(0));
        assertThat(status.failedReadRequests(), equalTo((long) max));
        assertThat(status.successfulReadRequests(), equalTo(1L));
        // the fetch failure has cleared
        assertThat(status.readExceptions().entrySet(), hasSize(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testFatalExceptionNotSetWhenStoppingWhileFetchingOps() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        readFailures.add(new ShardNotFoundException(new ShardId("leader_index", "", 0)));

        mappingVersions.add(1L);
        leaderGlobalCheckpoints.add(63L);
        maxSeqNos.add(63L);
        responseSizes.add(64);
        simulateResponse.set(true);
        beforeSendShardChangesRequest = status -> {
            // Cancel just before attempting to fetch operations:
            task.onCancelled();
        };
        task.coordinateReads();

        assertThat(task.isStopped(), is(true));
        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.getFatalException(), nullValue());
        assertThat(status.failedReadRequests(), equalTo(1L));
        assertThat(status.successfulReadRequests(), equalTo(0L));
        assertThat(status.readExceptions().size(), equalTo(1));
    }

    public void testEmptyShardChangesResponseShouldClearFetchException() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, -1, -1);

        readFailures.add(new ShardNotFoundException(new ShardId("leader_index", "", 0)));
        mappingVersions.add(1L);
        leaderGlobalCheckpoints.add(-1L);
        maxSeqNos.add(-1L);
        simulateResponse.set(true);
        task.coordinateReads();

        // number of requests is equal to initial request + retried attempts
        assertThat(shardChangesRequests.size(), equalTo(2));
        for (long[] shardChangesRequest : shardChangesRequests) {
            assertThat(shardChangesRequest[0], equalTo(0L));
            assertThat(shardChangesRequest[1], equalTo(64L));
        }

        assertFalse("task is not stopped", task.isStopped());
        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(0));
        assertThat(status.failedReadRequests(), equalTo(1L));
        // the fetch failure should have been cleared:
        assertThat(status.readExceptions().entrySet(), hasSize(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(-1L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(-1L));
    }

    public void testReceiveTimeout() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        final ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        final int numberOfTimeouts = randomIntBetween(1, 32);
        for (int i = 0; i < numberOfTimeouts; i++) {
            mappingVersions.add(1L);
            leaderGlobalCheckpoints.add(63L);
            maxSeqNos.add(63L);
            responseSizes.add(0);
        }

        final AtomicInteger counter = new AtomicInteger();
        beforeSendShardChangesRequest = status -> {
            if (counter.get() <= numberOfTimeouts) {
                assertThat(status.successfulReadRequests(), equalTo(0L));
                assertThat(status.totalReadTimeMillis(), equalTo(0L));
                assertThat(status.operationsReads(), equalTo(0L));
                assertThat(status.bytesRead(), equalTo(0L));

                assertThat(status.readExceptions().entrySet(), hasSize(0));
                assertThat(status.totalReadTimeMillis(), equalTo(0L));
                assertThat(status.failedReadRequests(), equalTo(0L));
            } else {
                // otherwise we will keep looping as if we were repeatedly polling and timing out
                simulateResponse.set(false);
            }
            counter.incrementAndGet();
        };

        mappingVersions.add(1L);
        mappingVersions.add(1L);
        leaderGlobalCheckpoints.add(63L);
        maxSeqNos.add(63L);
        responseSizes.add(64);
        simulateResponse.set(true);

        task.coordinateReads();

        // one request for each request that we simulate timedout, plus our request that receives a reply, and then a follow-up request
        assertThat(shardChangesRequests, hasSize(1 + 1 + numberOfTimeouts));
        for (final long[] shardChangesRequest : shardChangesRequests.subList(0, shardChangesRequests.size() - 2)) {
            assertNotNull(shardChangesRequest);
            assertThat(shardChangesRequest.length, equalTo(2));
            assertThat(shardChangesRequest[0], equalTo(0L));
            assertThat(shardChangesRequest[1], equalTo(64L));
        }
        final long[] lastShardChangesRequest = shardChangesRequests.get(shardChangesRequests.size() - 1);
        assertNotNull(lastShardChangesRequest);
        assertThat(lastShardChangesRequest.length, equalTo(2));
        assertThat(lastShardChangesRequest[0], equalTo(64L));
        assertThat(lastShardChangesRequest[1], equalTo(64L));

        final ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.successfulReadRequests(), equalTo(1L));
        assertThat(status.failedReadRequests(), equalTo(0L));
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(1));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.leaderMaxSeqNo(), equalTo(63L));

        assertThat(counter.get(), equalTo(1 + 1 + numberOfTimeouts));
    }

    public void testReceiveNonRetryableError() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        Exception failure = new RuntimeException("replication failed");
        readFailures.add(failure);
        final AtomicBoolean invoked = new AtomicBoolean();
        // since there will be only one failure, this should only be invoked once and there should not be a fetch failure
        beforeSendShardChangesRequest = status -> {
            if (invoked.compareAndSet(false, true)) {
                assertThat(status.failedReadRequests(), equalTo(0L));
                assertThat(status.readExceptions().entrySet(), hasSize(0));
            } else {
                fail("invoked twice");
            }
        };
        task.coordinateReads();

        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        assertTrue("task is stopped", task.isStopped());
        assertThat(task.getStatus().getFatalException().getRootCause(), sameInstance(failure));
        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(0));
        assertThat(status.failedReadRequests(), equalTo(1L));
        assertThat(status.readExceptions().entrySet(), hasSize(1));
        final Map.Entry<Long, Tuple<Integer, ElasticsearchException>> entry = status.readExceptions().entrySet().iterator().next();
        assertThat(entry.getKey(), equalTo(0L));
        assertThat(entry.getValue().v2(), instanceOf(ElasticsearchException.class));
        assertNotNull(entry.getValue().v2().getCause());
        assertThat(entry.getValue().v2().getCause(), instanceOf(RuntimeException.class));
        final RuntimeException cause = (RuntimeException) entry.getValue().v2().getCause();
        assertThat(cause.getMessage(), equalTo("replication failed"));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testHandleReadResponse() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 0L, 1L, 63L);
        task.innerHandleReadResponse(0L, 63L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.followerMappingVersion(), equalTo(0L));
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testReceiveLessThanRequested() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 20, 0L, 0L, 1L, 31L);
        task.innerHandleReadResponse(0L, 63L, response);

        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(21L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(43L));

        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testCancelAndReceiveLessThanRequested() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        task.markAsCompleted();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 31, 0L, 0L, 1L, 31L);
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(shardChangesRequests.size(), equalTo(0));
        assertThat(bulkShardOperationRequests.size(), equalTo(0));
        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingReadRequests(), equalTo(0));
        assertThat(status.outstandingWriteRequests(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testReceiveNothingExpectedSomething() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        task.innerHandleReadResponse(0L, 63L, new ShardChangesAction.Response(0, 0, 0, 0, 0, 100, new Translog.Operation[0], 1L));

        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testHandlePartialResponses() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 10;
        params.maxOutstandingReadRequests = 2;
        params.maxOutstandingWriteRequests = 1;
        params.maxWriteBufferCount = 3;

        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 99, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(2));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(10L));
        assertThat(shardChangesRequests.get(1)[0], equalTo(10L));
        assertThat(shardChangesRequests.get(1)[1], equalTo(10L));

        task.innerHandleReadResponse(0L, 9L, generateShardChangesResponse(0L, 5L, 0L, 0L, 1L, 99L));
        assertThat(pendingBulkShardRequests, hasSize(1));
        assertThat("continue the partial request", shardChangesRequests, hasSize(3));
        assertThat(shardChangesRequests.get(2)[0], equalTo(6L));
        assertThat(shardChangesRequests.get(2)[1], equalTo(4L));
        assertThat(pendingBulkShardRequests, hasSize(1));
        task.innerHandleReadResponse(10, 19L, generateShardChangesResponse(10L, 17L, 0L, 0L, 1L, 99L));
        assertThat("do not continue partial reads as the buffer is full", shardChangesRequests, hasSize(3));
        task.innerHandleReadResponse(6L, 9L, generateShardChangesResponse(6L, 8L, 0L, 0L, 1L, 99L));
        assertThat("do not continue partial reads as the buffer is full", shardChangesRequests, hasSize(3));
        pendingBulkShardRequests.remove().onResponse(new BulkShardOperationsResponse());
        assertThat(pendingBulkShardRequests, hasSize(1));

        assertThat("continue two partial requests as the buffer is empty after sending", shardChangesRequests, hasSize(5));
        assertThat(shardChangesRequests.get(3)[0], equalTo(9L));
        assertThat(shardChangesRequests.get(3)[1], equalTo(1L));
        assertThat(shardChangesRequests.get(4)[0], equalTo(18L));
        assertThat(shardChangesRequests.get(4)[1], equalTo(2L));

        task.innerHandleReadResponse(18L, 19L, generateShardChangesResponse(18L, 19L, 0L, 0L, 1L, 99L));
        assertThat("start new range as the buffer has empty slots", shardChangesRequests, hasSize(6));
        assertThat(shardChangesRequests.get(5)[0], equalTo(20L));
        assertThat(shardChangesRequests.get(5)[1], equalTo(10L));

        task.innerHandleReadResponse(9L, 9L, generateShardChangesResponse(9L, 9L, 0L, 0L, 1L, 99L));
        assertThat("do not start new range as the buffer is full", shardChangesRequests, hasSize(6));
        pendingBulkShardRequests.remove().onResponse(new BulkShardOperationsResponse());
        assertThat("start new range as the buffer is empty after sending", shardChangesRequests, hasSize(7));
        assertThat(shardChangesRequests.get(6)[0], equalTo(30L));
        assertThat(shardChangesRequests.get(6)[1], equalTo(10L));
    }

    public void testMappingUpdate() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        mappingVersions.add(1L);
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 1L, 0L, 0L, 63L);
        task.handleReadResponse(0L, 63L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.followerMappingVersion(), equalTo(1L));
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testMappingUpdateRetryableError() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        int max = randomIntBetween(1, 30);
        for (int i = 0; i < max; i++) {
            mappingUpdateFailures.add(new ConnectException());
        }
        mappingVersions.add(1L);
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 1L, 0L, 0L, 63L);
        task.handleReadResponse(0L, 63L, response);

        assertThat(mappingUpdateFailures.size(), equalTo(0));
        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(task.isStopped(), equalTo(false));
        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.followerMappingVersion(), equalTo(1L));
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));

    }

    public void testMappingUpdateNonRetryableError() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        mappingUpdateFailures.add(new RuntimeException());
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 1L, 0L, 0L, 64L);
        task.handleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(0));
        assertThat(task.isStopped(), equalTo(true));
        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.followerMappingVersion(), equalTo(0L));
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testSettingsUpdate() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        settingsVersions.add(1L);
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 1L, 0L, 63L);
        task.handleReadResponse(0L, 63L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.followerMappingVersion(), equalTo(0L));
        assertThat(status.followerSettingsVersion(), equalTo(1L));
        assertThat(status.followerAliasesVersion(), equalTo(0L));
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testSettingsUpdateRetryableError() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        int max = randomIntBetween(1, 30);
        for (int i = 0; i < max; i++) {
            settingsUpdateFailures.add(new ConnectException());
        }
        settingsVersions.add(1L);
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 1L, 0L, 63L);
        task.handleReadResponse(0L, 63L, response);

        assertThat(settingsUpdateFailures.size(), equalTo(0));
        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(task.isStopped(), equalTo(false));
        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.followerMappingVersion(), equalTo(0L));
        assertThat(status.followerSettingsVersion(), equalTo(1L));
        assertThat(status.followerAliasesVersion(), equalTo(0L));
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testSettingsUpdateNonRetryableError() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        settingsUpdateFailures.add(new RuntimeException());
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 0L, 1L, 0L, 64L);
        task.handleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(0));
        assertThat(task.isStopped(), equalTo(true));
        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.followerMappingVersion(), equalTo(0L));
        assertThat(status.followerSettingsVersion(), equalTo(0L));
        assertThat(status.followerAliasesVersion(), equalTo(0L));
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testAliasUpdate() {
        final ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        final ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        aliasesVersions.add(1L);
        task.coordinateReads();
        final ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 0L, 1L, 63L);
        task.handleReadResponse(0L, 63L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        final ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.followerMappingVersion(), equalTo(0L));
        assertThat(status.followerSettingsVersion(), equalTo(0L));
        assertThat(status.followerAliasesVersion(), equalTo(1L));
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testAliasUpdateRetryableError() {
        final ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        final ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        int max = randomIntBetween(1, 30);
        for (int i = 0; i < max; i++) {
            aliasesUpdateFailures.add(new ConnectException());
        }
        aliasesVersions.add(1L);
        task.coordinateReads();
        final ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 0L, 1L, 63L);
        task.handleReadResponse(0L, 63L, response);

        assertThat(aliasesUpdateFailures.size(), equalTo(0));
        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(task.isStopped(), equalTo(false));
        final ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.followerMappingVersion(), equalTo(0L));
        assertThat(status.followerSettingsVersion(), equalTo(0L));
        assertThat(status.followerAliasesVersion(), equalTo(1L));
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testAliasUpdateNonRetryableError() {
        final ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        final ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        aliasesUpdateFailures.add(new RuntimeException());
        task.coordinateReads();
        final ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 0L, 0L, 1L, 64L);
        task.handleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(0));
        assertThat(task.isStopped(), equalTo(true));
        final ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.followerMappingVersion(), equalTo(0L));
        assertThat(status.followerSettingsVersion(), equalTo(0L));
        assertThat(status.followerAliasesVersion(), equalTo(0L));
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testCoordinateWrites() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 128;
        params.maxOutstandingReadRequests = 1;
        params.maxWriteRequestOperationCount = 64;
        params.maxOutstandingWriteRequests = 1;

        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(128L));

        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 0L, 1L, 63L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 63L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.outstandingWriteRequests(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testMaxOutstandingWrites() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxWriteRequestOperationCount = 64;
        params.maxOutstandingWriteRequests = 2;
        ShardFollowNodeTask task = createShardFollowTask(params);
        ShardChangesAction.Response response = generateShardChangesResponse(0, 256, 0L, 0L, 1L, 256L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(2));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations()).subList(0, 64)));
        assertThat(bulkShardOperationRequests.get(1), equalTo(Arrays.asList(response.getOperations()).subList(64, 128)));

        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingWriteRequests(), equalTo(2));

        params.maxOutstandingWriteRequests = 4; // change to 4 outstanding writers
        task = createShardFollowTask(params);
        response = generateShardChangesResponse(0, 256, 0L, 0L, 1L, 256L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(4));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations()).subList(0, 64)));
        assertThat(bulkShardOperationRequests.get(1), equalTo(Arrays.asList(response.getOperations()).subList(64, 128)));
        assertThat(bulkShardOperationRequests.get(2), equalTo(Arrays.asList(response.getOperations()).subList(128, 192)));
        assertThat(bulkShardOperationRequests.get(3), equalTo(Arrays.asList(response.getOperations()).subList(192, 256)));

        status = task.getStatus();
        assertThat(status.outstandingWriteRequests(), equalTo(4));
    }

    public void testMaxWriteRequestCount() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxWriteRequestOperationCount = 8;
        params.maxOutstandingWriteRequests = 32;
        ShardFollowNodeTask task = createShardFollowTask(params);
        ShardChangesAction.Response response = generateShardChangesResponse(0, 256, 0L, 0L, 1L, 256L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(32));
        for (int i = 0; i < 32; i += 8) {
            int offset = i * 8;
            assertThat(bulkShardOperationRequests.get(i), equalTo(Arrays.asList(response.getOperations()).subList(offset, offset + 8)));
        }

        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingWriteRequests(), equalTo(32));
    }

    public void testRetryableError() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        int max = randomIntBetween(1, 30);
        for (int i = 0; i < max; i++) {
            writeFailures.add(new ShardNotFoundException(new ShardId("leader_index", "", 0)));
        }
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 0L, 1L, 63L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 63L, response);

        // Number of requests is equal to initial request + retried attempts:
        assertThat(bulkShardOperationRequests.size(), equalTo(max + 1));
        for (List<Translog.Operation> operations : bulkShardOperationRequests) {
            assertThat(operations, equalTo(Arrays.asList(response.getOperations())));
        }
        assertThat(task.isStopped(), equalTo(false));
        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingWriteRequests(), equalTo(1));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testNonRetryableError() throws Exception {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        writeFailures.add(new RuntimeException());
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 0L, 1L, 63L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 63L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));
        assertThat(task.isStopped(), equalTo(true));
        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingWriteRequests(), equalTo(1));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
        assertBusy(() -> assertNull(task.getRenewable()));
    }

    public void testMaxWriteRequestSize() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxWriteRequestSize = new ByteSizeValue(1, ByteSizeUnit.BYTES);
        params.maxOutstandingWriteRequests = 128;

        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 0L, 1L, 64L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 63L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(64));
    }

    public void testHandleWriteResponse() {
        ShardFollowTaskParams params = new ShardFollowTaskParams();
        params.maxReadRequestOperationCount = 64;
        params.maxOutstandingReadRequests = 1;
        params.maxWriteRequestOperationCount = 64;
        params.maxOutstandingWriteRequests = 1;
        ShardFollowNodeTask task = createShardFollowTask(params);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        followerGlobalCheckpoints.add(63L);
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 0L, 1L, 63L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 63L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        // handleWrite() also delegates to coordinateReads
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(64L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        ShardFollowNodeTaskStatus status = task.getStatus();
        assertThat(status.outstandingReadRequests(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.followerGlobalCheckpoint(), equalTo(63L));
    }

    public void testComputeDelay() {
        long maxDelayInMillis = 1000;
        assertThat(ShardFollowNodeTask.computeDelay(0, maxDelayInMillis), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(50L)));
        assertThat(ShardFollowNodeTask.computeDelay(1, maxDelayInMillis), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(50L)));
        assertThat(ShardFollowNodeTask.computeDelay(2, maxDelayInMillis), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(100L)));
        assertThat(ShardFollowNodeTask.computeDelay(3, maxDelayInMillis), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(200L)));
        assertThat(ShardFollowNodeTask.computeDelay(4, maxDelayInMillis), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(400L)));
        assertThat(ShardFollowNodeTask.computeDelay(5, maxDelayInMillis), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(800L)));
        assertThat(ShardFollowNodeTask.computeDelay(6, maxDelayInMillis), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(1000L)));
        assertThat(ShardFollowNodeTask.computeDelay(7, maxDelayInMillis), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(1000L)));
        assertThat(ShardFollowNodeTask.computeDelay(8, maxDelayInMillis), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(1000L)));
        assertThat(ShardFollowNodeTask.computeDelay(1024, maxDelayInMillis), allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(1000L)));
    }

    public void testRetentionLeaseRenewal() throws InterruptedException {
        scheduleRetentionLeaseRenewal.set(true);
        final CountDownLatch latch = new CountDownLatch(1);
        final long expectedFollowerGlobalChekcpoint = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
        retentionLeaseRenewal = followerGlobalCheckpoint -> {
            assertThat(followerGlobalCheckpoint, equalTo(expectedFollowerGlobalChekcpoint));
            latch.countDown();
        };

        final ShardFollowTaskParams params = new ShardFollowTaskParams();
        final ShardFollowNodeTask task = createShardFollowTask(params);

        try {
            startTask(task, randomLongBetween(expectedFollowerGlobalChekcpoint, Long.MAX_VALUE), expectedFollowerGlobalChekcpoint);
            latch.await();
        } finally {
            task.onCancelled();
            scheduleRetentionLeaseRenewal.set(false);
        }
    }

    public void testUpdateMappingSettingsAndAliasesConcurrently() throws Exception {
        final ShardFollowTask followTask = new ShardFollowTask(
            "test",
            new ShardId("leader_index", "", 0),
            new ShardId("follow_index", "", 0),
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            new ByteSizeValue(Long.MAX_VALUE),
            new ByteSizeValue(Long.MAX_VALUE),
            Integer.MAX_VALUE,
            new ByteSizeValue(Long.MAX_VALUE),
            TimeValue.ZERO,
            TimeValue.ZERO,
            Collections.emptyMap()
        );
        final ThreadPool threadPool = new TestThreadPool(getTestClass().getSimpleName());
        final AtomicLong leaderMappingVersion = new AtomicLong(0L);
        final AtomicLong followerMappingVersion = new AtomicLong(0L);
        final AtomicLong leaderSettingsVersion = new AtomicLong(0L);
        final AtomicLong followerSettingsVersion = new AtomicLong(0L);
        final AtomicLong leaderAliasesVersion = new AtomicLong(0L);
        final AtomicLong followerAliasesVersion = new AtomicLong(0L);

        final Phaser updates = new Phaser(1);
        final ShardFollowNodeTask shardFollowNodeTask = new ShardFollowNodeTask(
            1L,
            "type",
            ShardFollowTask.NAME,
            "description",
            null,
            Collections.emptyMap(),
            followTask,
            scheduler,
            System::nanoTime
        ) {
            @Override
            protected void innerUpdateMapping(long minRequiredMappingVersion, LongConsumer handler, Consumer<Exception> errorHandler) {
                updates.register();
                final long fetchedVersion = randomLongBetween(minRequiredMappingVersion, leaderMappingVersion.get());
                followerMappingVersion.updateAndGet(curr -> Math.max(curr, fetchedVersion));
                threadPool.generic().execute(() -> {
                    handler.accept(fetchedVersion);
                    updates.arriveAndDeregister();
                });
            }

            @Override
            protected void innerUpdateSettings(LongConsumer handler, Consumer<Exception> errorHandler) {
                updates.register();
                final long fetchedVersion = randomLongBetween(0L, leaderSettingsVersion.get());
                followerSettingsVersion.updateAndGet(curr -> Math.max(curr, fetchedVersion));
                threadPool.generic().execute(() -> {
                    handler.accept(fetchedVersion);
                    updates.arriveAndDeregister();
                });
            }

            @Override
            protected void innerUpdateAliases(LongConsumer handler, Consumer<Exception> errorHandler) {
                updates.register();
                final long fetchedVersion = randomLongBetween(0L, leaderAliasesVersion.get());
                followerAliasesVersion.updateAndGet(curr -> Math.max(curr, fetchedVersion));
                threadPool.generic().execute(() -> {
                    handler.accept(fetchedVersion);
                    updates.arriveAndDeregister();
                });
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(
                String followerHistoryUUID,
                List<Translog.Operation> operations,
                long leaderMaxSeqNoOfUpdatesOrDeletes,
                Consumer<BulkShardOperationsResponse> handler,
                Consumer<Exception> errorHandler
            ) {

            }

            @Override
            protected void innerSendShardChangesRequest(
                long from,
                int maxOperationCount,
                Consumer<ShardChangesAction.Response> handler,
                Consumer<Exception> errorHandler
            ) {

            }

            @Override
            protected Scheduler.Cancellable scheduleBackgroundRetentionLeaseRenewal(LongSupplier followerGlobalCheckpoint) {
                return null;
            }

            @Override
            synchronized void coordinateReads() {

            }
        };
        int responses = between(10, 5000);
        for (int i = 0; i < responses; i++) {
            ShardChangesAction.Response response = new ShardChangesAction.Response(
                leaderMappingVersion.addAndGet(between(0, Integer.MAX_VALUE)),
                leaderSettingsVersion.addAndGet(between(0, Integer.MAX_VALUE)),
                leaderAliasesVersion.addAndGet(between(0, Integer.MAX_VALUE)),
                SequenceNumbers.NO_OPS_PERFORMED,
                SequenceNumbers.NO_OPS_PERFORMED,
                -1,
                new Translog.Operation[0],
                randomLong()
            );
            shardFollowNodeTask.handleReadResponse(0, -1, response);
        }
        try {
            updates.arriveAndAwaitAdvance();
            final ShardFollowNodeTaskStatus status = shardFollowNodeTask.getStatus();
            assertThat(status.followerMappingVersion(), equalTo(followerMappingVersion.get()));
            assertThat(status.followerSettingsVersion(), equalTo(followerSettingsVersion.get()));
            assertThat(status.followerAliasesVersion(), equalTo(followerAliasesVersion.get()));
        } finally {
            terminate(threadPool);
        }
    }

    static final class ShardFollowTaskParams {
        private String remoteCluster = null;
        private ShardId followShardId = new ShardId("follow_index", "", 0);
        private ShardId leaderShardId = new ShardId("leader_index", "", 0);
        private int maxReadRequestOperationCount = Integer.MAX_VALUE;
        private ByteSizeValue maxReadRequestSize = new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES);
        private int maxOutstandingReadRequests = Integer.MAX_VALUE;
        private int maxWriteRequestOperationCount = Integer.MAX_VALUE;
        private ByteSizeValue maxWriteRequestSize = new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES);
        private int maxOutstandingWriteRequests = Integer.MAX_VALUE;
        private int maxWriteBufferCount = Integer.MAX_VALUE;
        private ByteSizeValue maxWriteBufferSize = new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES);
        private TimeValue maxRetryDelay = TimeValue.ZERO;
        private TimeValue readPollTimeout = TimeValue.ZERO;
        private Map<String, String> headers = Collections.emptyMap();
    }

    private ShardFollowNodeTask createShardFollowTask(ShardFollowTaskParams params) {
        AtomicBoolean stopped = new AtomicBoolean(false);
        ShardFollowTask followTask = new ShardFollowTask(
            params.remoteCluster,
            params.followShardId,
            params.leaderShardId,
            params.maxReadRequestOperationCount,
            params.maxWriteRequestOperationCount,
            params.maxOutstandingReadRequests,
            params.maxOutstandingWriteRequests,
            params.maxReadRequestSize,
            params.maxWriteRequestSize,
            params.maxWriteBufferCount,
            params.maxWriteBufferSize,
            params.maxRetryDelay,
            params.readPollTimeout,
            params.headers
        );

        shardChangesRequests = new ArrayList<>();
        bulkShardOperationRequests = new ArrayList<>();
        readFailures = new LinkedList<>();
        writeFailures = new LinkedList<>();
        mappingUpdateFailures = new LinkedList<>();
        mappingVersions = new LinkedList<>();
        settingsUpdateFailures = new LinkedList<>();
        settingsVersions = new LinkedList<>();
        aliasesUpdateFailures = new LinkedList<>();
        aliasesVersions = new LinkedList<>();
        leaderGlobalCheckpoints = new LinkedList<>();
        followerGlobalCheckpoints = new LinkedList<>();
        maxSeqNos = new LinkedList<>();
        responseSizes = new LinkedList<>();
        pendingBulkShardRequests = new LinkedList<>();
        return new ShardFollowNodeTask(
            1L,
            "type",
            ShardFollowTask.NAME,
            "description",
            null,
            Collections.emptyMap(),
            followTask,
            scheduler,
            System::nanoTime
        ) {

            @Override
            protected void innerUpdateMapping(long minRequiredMappingVersion, LongConsumer handler, Consumer<Exception> errorHandler) {
                Exception failure = mappingUpdateFailures.poll();
                if (failure != null) {
                    errorHandler.accept(failure);
                    return;
                }

                final Long mappingVersion = mappingVersions.poll();
                if (mappingVersion != null) {
                    handler.accept(mappingVersion);
                }
            }

            @Override
            protected void innerUpdateSettings(LongConsumer handler, Consumer<Exception> errorHandler) {
                Exception failure = settingsUpdateFailures.poll();
                if (failure != null) {
                    errorHandler.accept(failure);
                    return;
                }

                final Long settingsVersion = settingsVersions.poll();
                if (settingsVersion != null) {
                    handler.accept(settingsVersion);
                }
            }

            @Override
            protected void innerUpdateAliases(final LongConsumer handler, final Consumer<Exception> errorHandler) {
                final Exception failure = aliasesUpdateFailures.poll();
                if (failure != null) {
                    errorHandler.accept(failure);
                    return;
                }

                final Long aliasesVersion = aliasesVersions.poll();
                if (aliasesVersion != null) {
                    handler.accept(aliasesVersion);
                }
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(
                String followerHistoryUUID,
                final List<Translog.Operation> operations,
                final long maxSeqNoOfUpdates,
                final Consumer<BulkShardOperationsResponse> handler,
                final Consumer<Exception> errorHandler
            ) {
                bulkShardOperationRequests.add(operations);
                Exception writeFailure = ShardFollowNodeTaskTests.this.writeFailures.poll();
                if (writeFailure != null) {
                    errorHandler.accept(writeFailure);
                    return;
                }
                Long followerGlobalCheckpoint = followerGlobalCheckpoints.poll();
                if (followerGlobalCheckpoint != null) {
                    final BulkShardOperationsResponse response = new BulkShardOperationsResponse();
                    response.setGlobalCheckpoint(followerGlobalCheckpoint);
                    response.setMaxSeqNo(followerGlobalCheckpoint);
                    handler.accept(response);
                } else {
                    pendingBulkShardRequests.add(ActionListener.wrap(handler::accept, errorHandler));
                }
            }

            @Override
            protected void innerSendShardChangesRequest(
                long from,
                int requestBatchSize,
                Consumer<ShardChangesAction.Response> handler,
                Consumer<Exception> errorHandler
            ) {
                beforeSendShardChangesRequest.accept(getStatus());
                shardChangesRequests.add(new long[] { from, requestBatchSize });
                Exception readFailure = ShardFollowNodeTaskTests.this.readFailures.poll();
                if (readFailure != null) {
                    errorHandler.accept(readFailure);
                } else if (simulateResponse.get()) {
                    final int responseSize = responseSizes.size() == 0 ? 0 : responseSizes.poll();
                    final Translog.Operation[] operations = new Translog.Operation[responseSize];
                    for (int i = 0; i < responseSize; i++) {
                        operations[i] = new Translog.NoOp(from + i, 0, "test");
                    }
                    final ShardChangesAction.Response response = new ShardChangesAction.Response(
                        mappingVersions.poll(),
                        0L,
                        0L,
                        leaderGlobalCheckpoints.poll(),
                        maxSeqNos.poll(),
                        randomNonNegativeLong(),
                        operations,
                        1L
                    );
                    handler.accept(response);
                }
            }

            @Override
            protected Scheduler.Cancellable scheduleBackgroundRetentionLeaseRenewal(final LongSupplier followerGlobalCheckpoint) {
                if (scheduleRetentionLeaseRenewal.get()) {
                    final ScheduledThreadPoolExecutor testScheduler = Scheduler.initScheduler(Settings.EMPTY, "test-scheduler");
                    final ScheduledFuture<?> future = testScheduler.scheduleWithFixedDelay(
                        () -> retentionLeaseRenewal.accept(followerGlobalCheckpoint.getAsLong()),
                        0,
                        TimeValue.timeValueMillis(200).millis(),
                        TimeUnit.MILLISECONDS
                    );
                    return new Scheduler.Cancellable() {

                        @Override
                        public boolean cancel() {
                            final boolean cancel = future.cancel(true);
                            testScheduler.shutdown();
                            return cancel;
                        }

                        @Override
                        public boolean isCancelled() {
                            return future.isCancelled();
                        }

                    };
                } else {
                    return new Scheduler.Cancellable() {

                        @Override
                        public boolean cancel() {
                            return true;
                        }

                        @Override
                        public boolean isCancelled() {
                            return true;
                        }

                    };
                }
            }

            @Override
            protected boolean isStopped() {
                return super.isStopped() || stopped.get();
            }

            @Override
            public void markAsCompleted() {
                stopped.set(true);
            }
        };
    }

    private static ShardChangesAction.Response generateShardChangesResponse(
        long fromSeqNo,
        long toSeqNo,
        long mappingVersion,
        long settingsVersion,
        long aliasesVersion,
        long leaderGlobalCheckPoint
    ) {
        List<Translog.Operation> ops = new ArrayList<>();
        for (long seqNo = fromSeqNo; seqNo <= toSeqNo; seqNo++) {
            String id = UUIDs.randomBase64UUID();
            byte[] source = "{}".getBytes(StandardCharsets.UTF_8);
            ops.add(new Translog.Index(id, seqNo, 0, source));
        }
        return new ShardChangesAction.Response(
            mappingVersion,
            settingsVersion,
            aliasesVersion,
            leaderGlobalCheckPoint,
            leaderGlobalCheckPoint,
            randomNonNegativeLong(),
            ops.toArray(new Translog.Operation[0]),
            1L
        );
    }

    void startTask(ShardFollowNodeTask task, long leaderGlobalCheckpoint, long followerGlobalCheckpoint) {
        // The call the updateMapping is a noop, so noting happens.
        task.start("uuid", leaderGlobalCheckpoint, leaderGlobalCheckpoint, followerGlobalCheckpoint, followerGlobalCheckpoint);
    }

}
