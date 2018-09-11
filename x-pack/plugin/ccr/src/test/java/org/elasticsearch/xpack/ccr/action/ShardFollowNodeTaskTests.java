/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;

import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ShardFollowNodeTaskTests extends ESTestCase {

    private Exception fatalError;
    private List<long[]> shardChangesRequests;
    private List<List<Translog.Operation>> bulkShardOperationRequests;
    private BiConsumer<TimeValue, Runnable> scheduler = (delay, task) -> task.run();

    private Consumer<ShardFollowNodeTask.Status> beforeSendShardChangesRequest = status -> {};

    private AtomicBoolean simulateResponse = new AtomicBoolean();

    private Queue<Exception> readFailures;
    private Queue<Exception> writeFailures;
    private Queue<Exception> mappingUpdateFailures;
    private Queue<Long> mappingVersions;
    private Queue<Long> leaderGlobalCheckpoints;
    private Queue<Long> followerGlobalCheckpoints;
    private Queue<Long> maxSeqNos;

    public void testCoordinateReads() {
        ShardFollowNodeTask task = createShardFollowTask(8, between(8, 20), between(1, 20), Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 3, -1);
        task.coordinateReads();
        assertThat(shardChangesRequests, contains(new long[]{0L, 8L})); // treat this a peak request
        shardChangesRequests.clear();
        task.innerHandleReadResponse(0, 5L, generateShardChangesResponse(0, 5L, 0L, 60L));
        assertThat(shardChangesRequests, contains(new long[][]{
            {6L, 8L}, {14L, 8L}, {22L, 8L}, {30L, 8L}, {38L, 8L}, {46L, 8L}, {54L, 7L}}
        ));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentReads(), equalTo(7));
        assertThat(status.lastRequestedSeqNo(), equalTo(60L));
    }

    public void testWriteBuffer() {
        // Need to set concurrentWrites to 0, other the write buffer gets flushed immediately:
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 0, 32, Long.MAX_VALUE);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        // Also invokes the coordinatesReads() method:
        task.innerHandleReadResponse(0L, 63L, generateShardChangesResponse(0, 63, 0L, 128L));
        assertThat(shardChangesRequests.size(), equalTo(0)); // no more reads, because write buffer is full

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentReads(), equalTo(0));
        assertThat(status.numberOfConcurrentWrites(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(128L));
    }

    public void testMaxConcurrentReads() {
        ShardFollowNodeTask task = createShardFollowTask(8, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(8L));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentReads(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(7L));
    }

    public void testTaskCancelled() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        // The call the updateMapping is a noop, so noting happens.
        task.start(128L, 128L, task.getStatus().followerGlobalCheckpoint(), task.getStatus().followerMaxSeqNo());
        task.markAsCompleted();
        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(0));
    }

    public void testTaskCancelledAfterReadLimitHasBeenReached() {
        ShardFollowNodeTask task = createShardFollowTask(16, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 31, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(16L));

        task.markAsCompleted();
        shardChangesRequests.clear();
        // Also invokes the coordinatesReads() method:
        task.innerHandleReadResponse(0L, 15L, generateShardChangesResponse(0, 15, 0L, 31L));
        assertThat(shardChangesRequests.size(), equalTo(0)); // no more reads, because task has been cancelled
        assertThat(bulkShardOperationRequests.size(), equalTo(0)); // no more writes, because task has been cancelled

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentReads(), equalTo(0));
        assertThat(status.numberOfConcurrentWrites(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(15L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(31L));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testTaskCancelledAfterWriteBufferLimitHasBeenReached() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, 32, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        task.markAsCompleted();
        shardChangesRequests.clear();
        // Also invokes the coordinatesReads() method:
        task.innerHandleReadResponse(0L, 63L, generateShardChangesResponse(0, 63, 0L, 128L));
        assertThat(shardChangesRequests.size(), equalTo(0)); // no more reads, because task has been cancelled
        assertThat(bulkShardOperationRequests.size(), equalTo(0)); // no more writes, because task has been cancelled

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentReads(), equalTo(0));
        assertThat(status.numberOfConcurrentWrites(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(128L));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testReceiveRetryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        int max = randomIntBetween(1, 10);
        for (int i = 0; i < max; i++) {
            readFailures.add(new ShardNotFoundException(new ShardId("leader_index", "", 0)));
        }
        mappingVersions.add(1L);
        leaderGlobalCheckpoints.add(63L);
        maxSeqNos.add(63L);
        simulateResponse.set(true);
        final AtomicLong retryCounter = new AtomicLong();
        // before each retry, we assert the fetch failures; after the last retry, the fetch failure should clear
        beforeSendShardChangesRequest = status -> {
            assertThat(status.numberOfFailedFetches(), equalTo(retryCounter.get()));
            if (retryCounter.get() > 0) {
                assertThat(status.fetchExceptions().entrySet(), hasSize(1));
                final Map.Entry<Long, ElasticsearchException> entry = status.fetchExceptions().entrySet().iterator().next();
                assertThat(entry.getKey(), equalTo(0L));
                assertThat(entry.getValue(), instanceOf(ElasticsearchException.class));
                assertNotNull(entry.getValue().getCause());
                assertThat(entry.getValue().getCause(), instanceOf(ShardNotFoundException.class));
                final ShardNotFoundException cause = (ShardNotFoundException) entry.getValue().getCause();
                assertThat(cause.getShardId().getIndexName(), equalTo("leader_index"));
                assertThat(cause.getShardId().getId(), equalTo(0));
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
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentReads(), equalTo(1));
        assertThat(status.numberOfConcurrentWrites(), equalTo(0));
        assertThat(status.numberOfFailedFetches(), equalTo((long)max));
        assertThat(status.numberOfSuccessfulFetches(), equalTo(1L));
        // the fetch failure has cleared
        assertThat(status.fetchExceptions().entrySet(), hasSize(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testReceiveRetryableErrorRetriedTooManyTimes() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        int max = randomIntBetween(11, 32);
        for (int i = 0; i < max; i++) {
            readFailures.add(new ShardNotFoundException(new ShardId("leader_index", "", 0)));
        }
        final AtomicLong retryCounter = new AtomicLong();
        // before each retry, we assert the fetch failures; after the last retry, the fetch failure should persist
        beforeSendShardChangesRequest = status -> {
            assertThat(status.numberOfFailedFetches(), equalTo(retryCounter.get()));
            if (retryCounter.get() > 0) {
                assertThat(status.fetchExceptions().entrySet(), hasSize(1));
                final Map.Entry<Long, ElasticsearchException> entry = status.fetchExceptions().entrySet().iterator().next();
                assertThat(entry.getKey(), equalTo(0L));
                assertThat(entry.getValue(), instanceOf(ElasticsearchException.class));
                assertNotNull(entry.getValue().getCause());
                assertThat(entry.getValue().getCause(), instanceOf(ShardNotFoundException.class));
                final ShardNotFoundException cause = (ShardNotFoundException) entry.getValue().getCause();
                assertThat(cause.getShardId().getIndexName(), equalTo("leader_index"));
                assertThat(cause.getShardId().getId(), equalTo(0));
            }
            retryCounter.incrementAndGet();
        };
        task.coordinateReads();

        assertThat(shardChangesRequests.size(), equalTo(11));
        for (long[] shardChangesRequest : shardChangesRequests) {
            assertThat(shardChangesRequest[0], equalTo(0L));
            assertThat(shardChangesRequest[1], equalTo(64L));
        }

        assertTrue("task is stopped", task.isStopped());
        assertThat(fatalError, notNullValue());
        assertThat(fatalError.getMessage(), containsString("retrying failed ["));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentReads(), equalTo(1));
        assertThat(status.numberOfConcurrentWrites(), equalTo(0));
        assertThat(status.numberOfFailedFetches(), equalTo(11L));
        assertThat(status.fetchExceptions().entrySet(), hasSize(1));
        final Map.Entry<Long, ElasticsearchException> entry = status.fetchExceptions().entrySet().iterator().next();
        assertThat(entry.getKey(), equalTo(0L));
        assertThat(entry.getValue(), instanceOf(ElasticsearchException.class));
        assertNotNull(entry.getValue().getCause());
        assertThat(entry.getValue().getCause(), instanceOf(ShardNotFoundException.class));
        final ShardNotFoundException cause = (ShardNotFoundException) entry.getValue().getCause();
        assertThat(cause.getShardId().getIndexName(), equalTo("leader_index"));
        assertThat(cause.getShardId().getId(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testReceiveNonRetryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        Exception failure = new RuntimeException("replication failed");
        readFailures.add(failure);
        final AtomicBoolean invoked = new AtomicBoolean();
        // since there will be only one failure, this should only be invoked once and there should not be a fetch failure
        beforeSendShardChangesRequest = status -> {
            if (invoked.compareAndSet(false, true)) {
                assertThat(status.numberOfFailedFetches(), equalTo(0L));
                assertThat(status.fetchExceptions().entrySet(), hasSize(0));
            } else {
                fail("invoked twice");
            }
        };
        task.coordinateReads();

        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        assertTrue("task is stopped", task.isStopped());
        assertThat(fatalError, sameInstance(failure));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentReads(), equalTo(1));
        assertThat(status.numberOfConcurrentWrites(), equalTo(0));
        assertThat(status.numberOfFailedFetches(), equalTo(1L));
        assertThat(status.fetchExceptions().entrySet(), hasSize(1));
        final Map.Entry<Long, ElasticsearchException> entry = status.fetchExceptions().entrySet().iterator().next();
        assertThat(entry.getKey(), equalTo(0L));
        assertThat(entry.getValue(), instanceOf(ElasticsearchException.class));
        assertNotNull(entry.getValue().getCause());
        assertThat(entry.getValue().getCause(), instanceOf(RuntimeException.class));
        final RuntimeException cause = (RuntimeException) entry.getValue().getCause();
        assertThat(cause.getMessage(), equalTo("replication failed"));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testHandleReadResponse() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 63L);
        task.innerHandleReadResponse(0L, 63L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.mappingVersion(), equalTo(0L));
        assertThat(status.numberOfConcurrentReads(), equalTo(1));
        assertThat(status.numberOfConcurrentReads(), equalTo(1));
        assertThat(status.numberOfConcurrentWrites(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testReceiveLessThanRequested() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 20, 0L, 31L);
        task.innerHandleReadResponse(0L, 63L, response);

        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(21L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(43L));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentReads(), equalTo(1));
        assertThat(status.numberOfConcurrentWrites(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testCancelAndReceiveLessThanRequested() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        task.markAsCompleted();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 31, 0L, 31L);
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(shardChangesRequests.size(), equalTo(0));
        assertThat(bulkShardOperationRequests.size(), equalTo(0));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentReads(), equalTo(0));
        assertThat(status.numberOfConcurrentWrites(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testReceiveNothingExpectedSomething() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        task.innerHandleReadResponse(0L, 63L, new ShardChangesAction.Response(0, 0, 0, new Translog.Operation[0]));

        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentReads(), equalTo(1));
        assertThat(status.numberOfConcurrentWrites(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testDelayCoordinatesRead() {
        int[] counter = new int[]{0};
        scheduler = (delay, task) -> {
            counter[0]++;
            task.run();
        };
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 63L);
        // Also invokes coordinateReads()
        task.innerHandleReadResponse(0L, 63L, response);
        task.innerHandleReadResponse(64L, 63L,
            new ShardChangesAction.Response(0, 63L, 63L, new Translog.Operation[0]));
        assertThat(counter[0], equalTo(1));
    }

    public void testMappingUpdate() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        mappingVersions.add(1L);
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 1L, 63L);
        task.handleReadResponse(0L, 63L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.mappingVersion(), equalTo(1L));
        assertThat(status.numberOfConcurrentReads(), equalTo(1));
        assertThat(status.numberOfConcurrentWrites(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testMappingUpdateRetryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        int max = randomIntBetween(1, 10);
        for (int i = 0; i < max; i++) {
            mappingUpdateFailures.add(new ConnectException());
        }
        mappingVersions.add(1L);
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 1L, 63L);
        task.handleReadResponse(0L, 63L, response);

        assertThat(mappingUpdateFailures.size(), equalTo(0));
        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(task.isStopped(), equalTo(false));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.mappingVersion(), equalTo(1L));
        assertThat(status.numberOfConcurrentReads(), equalTo(1));
        assertThat(status.numberOfConcurrentWrites(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));

    }

    public void testMappingUpdateRetryableErrorRetriedTooManyTimes() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        int max = randomIntBetween(11, 20);
        for (int i = 0; i < max; i++) {
            mappingUpdateFailures.add(new ConnectException());
        }
        mappingVersions.add(1L);
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 1L, 64L);
        task.handleReadResponse(0L, 64L, response);

        assertThat(mappingUpdateFailures.size(), equalTo(max - 11));
        assertThat(mappingVersions.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.size(), equalTo(0));
        assertThat(task.isStopped(), equalTo(true));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.mappingVersion(), equalTo(0L));
        assertThat(status.numberOfConcurrentReads(), equalTo(1));
        assertThat(status.numberOfConcurrentWrites(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testMappingUpdateNonRetryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        mappingUpdateFailures.add(new RuntimeException());
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 1L, 64L);
        task.handleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(0));
        assertThat(task.isStopped(), equalTo(true));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.mappingVersion(), equalTo(0L));
        assertThat(status.numberOfConcurrentReads(), equalTo(1));
        assertThat(status.numberOfConcurrentWrites(), equalTo(0));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testCoordinateWrites() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 63L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 63L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentReads(), equalTo(1));
        assertThat(status.numberOfConcurrentWrites(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testMaxConcurrentWrites() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 2, Integer.MAX_VALUE, Long.MAX_VALUE);
        ShardChangesAction.Response response = generateShardChangesResponse(0, 256, 0L, 256L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(2));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations()).subList(0, 64)));
        assertThat(bulkShardOperationRequests.get(1), equalTo(Arrays.asList(response.getOperations()).subList(64, 128)));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentWrites(), equalTo(2));

        task = createShardFollowTask(64, 1, 4, Integer.MAX_VALUE, Long.MAX_VALUE);
        response = generateShardChangesResponse(0, 256, 0L, 256L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(4));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations()).subList(0, 64)));
        assertThat(bulkShardOperationRequests.get(1), equalTo(Arrays.asList(response.getOperations()).subList(64, 128)));
        assertThat(bulkShardOperationRequests.get(2), equalTo(Arrays.asList(response.getOperations()).subList(128, 192)));
        assertThat(bulkShardOperationRequests.get(3), equalTo(Arrays.asList(response.getOperations()).subList(192, 256)));

        status = task.getStatus();
        assertThat(status.numberOfConcurrentWrites(), equalTo(4));
    }

    public void testMaxBatchOperationCount() {
        ShardFollowNodeTask task = createShardFollowTask(8, 1, 32, Integer.MAX_VALUE, Long.MAX_VALUE);
        ShardChangesAction.Response response = generateShardChangesResponse(0, 256, 0L, 256L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(32));
        for (int i = 0; i < 32; i += 8) {
            int offset = i * 8;
            assertThat(bulkShardOperationRequests.get(i), equalTo(Arrays.asList(response.getOperations()).subList(offset, offset + 8)));
        }

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentWrites(), equalTo(32));
    }

    public void testRetryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        int max = randomIntBetween(1, 10);
        for (int i = 0; i < max; i++) {
            writeFailures.add(new ShardNotFoundException(new ShardId("leader_index", "", 0)));
        }
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 63L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 63L, response);

        // Number of requests is equal to initial request + retried attempts:
        assertThat(bulkShardOperationRequests.size(), equalTo(max + 1));
        for (List<Translog.Operation> operations : bulkShardOperationRequests) {
            assertThat(operations, equalTo(Arrays.asList(response.getOperations())));
        }
        assertThat(task.isStopped(), equalTo(false));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentWrites(), equalTo(1));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testRetryableErrorRetriedTooManyTimes() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        int max = randomIntBetween(11, 32);
        for (int i = 0; i < max; i++) {
            writeFailures.add(new ShardNotFoundException(new ShardId("leader_index", "", 0)));
        }
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 643);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 63L, response);

        // Number of requests is equal to initial request + retried attempts:
        assertThat(bulkShardOperationRequests.size(), equalTo(11));
        for (List<Translog.Operation> operations : bulkShardOperationRequests) {
            assertThat(operations, equalTo(Arrays.asList(response.getOperations())));
        }
        assertThat(task.isStopped(), equalTo(true));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentWrites(), equalTo(1));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testNonRetryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        writeFailures.add(new RuntimeException());
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 63L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 63L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));
        assertThat(task.isStopped(), equalTo(true));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentWrites(), equalTo(1));
        assertThat(status.followerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testMaxBatchBytesLimit() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 128, Integer.MAX_VALUE, 1L);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 64L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(64));
    }

    public void testHandleWriteResponse() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        followerGlobalCheckpoints.add(63L);
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 0L, 63L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 63L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        // handleWrite() also delegates to coordinateReads
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(64L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.numberOfConcurrentReads(), equalTo(1));
        assertThat(status.lastRequestedSeqNo(), equalTo(63L));
        assertThat(status.leaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.followerGlobalCheckpoint(), equalTo(63L));
    }

    ShardFollowNodeTask createShardFollowTask(int maxBatchOperationCount, int maxConcurrentReadBatches, int maxConcurrentWriteBatches,
                                              int bufferWriteLimit, long maxBatchSizeInBytes) {
        AtomicBoolean stopped = new AtomicBoolean(false);
        ShardFollowTask params = new ShardFollowTask(null, new ShardId("follow_index", "", 0),
            new ShardId("leader_index", "", 0), maxBatchOperationCount, maxConcurrentReadBatches, maxBatchSizeInBytes,
            maxConcurrentWriteBatches, bufferWriteLimit, TimeValue.ZERO, TimeValue.ZERO, Collections.emptyMap());

        shardChangesRequests = new ArrayList<>();
        bulkShardOperationRequests = new ArrayList<>();
        readFailures = new LinkedList<>();
        writeFailures = new LinkedList<>();
        mappingUpdateFailures = new LinkedList<>();
        mappingVersions = new LinkedList<>();
        leaderGlobalCheckpoints = new LinkedList<>();
        followerGlobalCheckpoints = new LinkedList<>();
        maxSeqNos = new LinkedList<>();
        return new ShardFollowNodeTask(
                1L, "type", ShardFollowTask.NAME, "description", null, Collections.emptyMap(), params, scheduler, System::nanoTime) {

            @Override
            protected void innerUpdateMapping(LongConsumer handler, Consumer<Exception> errorHandler) {
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
            protected void innerSendBulkShardOperationsRequest(
                    final List<Translog.Operation> operations,
                    final Consumer<BulkShardOperationsResponse> handler,
                    final Consumer<Exception> errorHandler) {
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
                }
            }

            @Override
            protected void innerSendShardChangesRequest(long from, int requestBatchSize, Consumer<ShardChangesAction.Response> handler,
                                                        Consumer<Exception> errorHandler) {
                beforeSendShardChangesRequest.accept(getStatus());
                shardChangesRequests.add(new long[]{from, requestBatchSize});
                Exception readFailure = ShardFollowNodeTaskTests.this.readFailures.poll();
                if (readFailure != null) {
                    errorHandler.accept(readFailure);
                } else if (simulateResponse.get()) {
                    final Translog.Operation[] operations = new Translog.Operation[requestBatchSize];
                    for (int i = 0; i < requestBatchSize; i++) {
                        operations[i] = new Translog.NoOp(from + i, 0, "test");
                    }
                    final ShardChangesAction.Response response =
                            new ShardChangesAction.Response(
                                    mappingVersions.poll(),
                                    leaderGlobalCheckpoints.poll(),
                                    maxSeqNos.poll(),
                                    operations);
                    handler.accept(response);
                }
            }

            @Override
            protected boolean isStopped() {
                return stopped.get();
            }

            @Override
            public void markAsCompleted() {
                stopped.set(true);
            }

            @Override
            public void markAsFailed(Exception e) {
                fatalError = e;
                stopped.set(true);
            }
        };
    }

    private static ShardChangesAction.Response generateShardChangesResponse(long fromSeqNo, long toSeqNo, long mappingVersion,
                                                                            long leaderGlobalCheckPoint) {
        List<Translog.Operation> ops = new ArrayList<>();
        for (long seqNo = fromSeqNo; seqNo <= toSeqNo; seqNo++) {
            String id = UUIDs.randomBase64UUID();
            byte[] source = "{}".getBytes(StandardCharsets.UTF_8);
            ops.add(new Translog.Index("doc", id, seqNo, 0, source));
        }
        return new ShardChangesAction.Response(
                mappingVersion, leaderGlobalCheckPoint, leaderGlobalCheckPoint, ops.toArray(new Translog.Operation[0]));
    }

    void startTask(ShardFollowNodeTask task, long leaderGlobalCheckpoint, long followerGlobalCheckpoint) {
        // The call the updateMapping is a noop, so noting happens.
        task.start(leaderGlobalCheckpoint, leaderGlobalCheckpoint, followerGlobalCheckpoint, followerGlobalCheckpoint);
    }


}
