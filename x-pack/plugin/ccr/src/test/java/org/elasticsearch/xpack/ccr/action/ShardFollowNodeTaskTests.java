/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;

import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ShardFollowNodeTaskTests extends ESTestCase {

    private Exception fatalError;
    private List<long[]> shardChangesRequests;
    private List<List<Translog.Operation>> bulkShardOperationRequests;
    private BiConsumer<TimeValue, Runnable> scheduler = (delay, task) -> task.run();

    private Queue<Exception> readFailures;
    private Queue<Exception> writeFailures;
    private Queue<Exception> mappingUpdateFailures;
    private Queue<Long> imdVersions;
    private Queue<Long> followerGlobalCheckpoints;

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
        assertThat(status.getNumberOfConcurrentReads(), equalTo(7));
        assertThat(status.getLastRequestedSeqno(), equalTo(60L));
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
        assertThat(status.getNumberOfConcurrentReads(), equalTo(0));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(0));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(128L));
    }

    public void testMaxConcurrentReads() {
        ShardFollowNodeTask task = createShardFollowTask(8, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(8L));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getLastRequestedSeqno(), equalTo(7L));
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
        task.start(128L, task.getStatus().getFollowerGlobalCheckpoint());
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
        assertThat(status.getNumberOfConcurrentReads(), equalTo(0));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(0));
        assertThat(status.getLastRequestedSeqno(), equalTo(15L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(31L));
        assertThat(status.getFollowerGlobalCheckpoint(), equalTo(-1L));
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
        assertThat(status.getNumberOfConcurrentReads(), equalTo(0));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(0));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(128L));
        assertThat(status.getFollowerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testReceiveRetryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        int max = randomIntBetween(1, 10);
        for (int i = 0; i < max; i++) {
            readFailures.add(new ShardNotFoundException(new ShardId("leader_index", "", 0)));
        }
        task.coordinateReads();

        // NUmber of requests is equal to initial request + retried attempts
        assertThat(shardChangesRequests.size(), equalTo(max + 1));
        for (long[] shardChangesRequest : shardChangesRequests) {
            assertThat(shardChangesRequest[0], equalTo(0L));
            assertThat(shardChangesRequest[1], equalTo(64L));
        }

        assertThat(task.isStopped(), equalTo(false));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(0));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testReceiveRetryableErrorRetriedTooManyTimes() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        int max = randomIntBetween(11, 32);
        for (int i = 0; i < max; i++) {
            readFailures.add(new ShardNotFoundException(new ShardId("leader_index", "", 0)));
        }
        task.coordinateReads();

        assertThat(shardChangesRequests.size(), equalTo(11));
        for (long[] shardChangesRequest : shardChangesRequests) {
            assertThat(shardChangesRequest[0], equalTo(0L));
            assertThat(shardChangesRequest[1], equalTo(64L));
        }

        assertThat(task.isStopped(), equalTo(true));
        assertThat(fatalError, notNullValue());
        assertThat(fatalError.getMessage(), containsString("retrying failed ["));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(0));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testReceiveNonRetryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        Exception failure = new RuntimeException();
        readFailures.add(failure);
        task.coordinateReads();

        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        assertThat(task.isStopped(), equalTo(true));
        assertThat(fatalError, sameInstance(failure));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(0));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(63L));
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
        assertThat(status.getIndexMetadataVersion(), equalTo(0L));
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(1));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.getFollowerGlobalCheckpoint(), equalTo(-1L));
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
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(1));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(63L));
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
        assertThat(status.getNumberOfConcurrentReads(), equalTo(0));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(0));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(63L));
    }

    public void testReceiveNothingExpectedSomething() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        task.innerHandleReadResponse(0L, 63L,
            new ShardChangesAction.Response(0, 0, new Translog.Operation[0]));

        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(0));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(63L));
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
            new ShardChangesAction.Response(0, 63L, new Translog.Operation[0]));
        assertThat(counter[0], equalTo(1));
    }

    public void testMappingUpdate() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        imdVersions.add(1L);
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 1L, 63L);
        task.handleReadResponse(0L, 63L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getIndexMetadataVersion(), equalTo(1L));
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(1));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.getFollowerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testMappingUpdateRetryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        int max = randomIntBetween(1, 10);
        for (int i = 0; i < max; i++) {
            mappingUpdateFailures.add(new ConnectException());
        }
        imdVersions.add(1L);
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 63, 1L, 63L);
        task.handleReadResponse(0L, 63L, response);

        assertThat(mappingUpdateFailures.size(), equalTo(0));
        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(task.isStopped(), equalTo(false));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getIndexMetadataVersion(), equalTo(1L));
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(1));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(63L));

    }

    public void testMappingUpdateRetryableErrorRetriedTooManyTimes() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 63, -1);

        int max = randomIntBetween(11, 20);
        for (int i = 0; i < max; i++) {
            mappingUpdateFailures.add(new ConnectException());
        }
        imdVersions.add(1L);
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 1L, 64L);
        task.handleReadResponse(0L, 64L, response);

        assertThat(mappingUpdateFailures.size(), equalTo(max - 11));
        assertThat(imdVersions.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.size(), equalTo(0));
        assertThat(task.isStopped(), equalTo(true));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getIndexMetadataVersion(), equalTo(0L));
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(0));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(63L));
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
        assertThat(status.getIndexMetadataVersion(), equalTo(0L));
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(0));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(63L));
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
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(1));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.getFollowerGlobalCheckpoint(), equalTo(-1L));
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
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(2));

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
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(4));
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
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(32));
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
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(1));
        assertThat(status.getFollowerGlobalCheckpoint(), equalTo(-1L));
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
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(1));
        assertThat(status.getFollowerGlobalCheckpoint(), equalTo(-1L));
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
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(1));
        assertThat(status.getFollowerGlobalCheckpoint(), equalTo(-1L));
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
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getLastRequestedSeqno(), equalTo(63L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(63L));
        assertThat(status.getFollowerGlobalCheckpoint(), equalTo(63L));
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
        imdVersions = new LinkedList<>();
        followerGlobalCheckpoints = new LinkedList<>();
        return new ShardFollowNodeTask(1L, "type", ShardFollowTask.NAME, "description", null, Collections.emptyMap(), params, scheduler) {

            @Override
            protected void innerUpdateMapping(LongConsumer handler, Consumer<Exception> errorHandler) {
                Exception failure = mappingUpdateFailures.poll();
                if (failure != null) {
                    errorHandler.accept(failure);
                    return;
                }

                Long imdVersion = imdVersions.poll();
                if (imdVersion != null) {
                    handler.accept(imdVersion);
                }
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(List<Translog.Operation> operations, LongConsumer handler,
                                                               Consumer<Exception> errorHandler) {
                bulkShardOperationRequests.add(operations);
                Exception writeFailure = ShardFollowNodeTaskTests.this.writeFailures.poll();
                if (writeFailure != null) {
                    errorHandler.accept(writeFailure);
                    return;
                }
                Long followerGlobalCheckpoint = followerGlobalCheckpoints.poll();
                if (followerGlobalCheckpoint != null) {
                    handler.accept(followerGlobalCheckpoint);
                }
            }

            @Override
            protected void innerSendShardChangesRequest(long from, int requestBatchSize, Consumer<ShardChangesAction.Response> handler,
                                                        Consumer<Exception> errorHandler) {
                shardChangesRequests.add(new long[]{from, requestBatchSize});
                Exception readFailure = ShardFollowNodeTaskTests.this.readFailures.poll();
                if (readFailure != null) {
                    errorHandler.accept(readFailure);
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

    private static ShardChangesAction.Response generateShardChangesResponse(long fromSeqNo, long toSeqNo, long imdVersion,
                                                                            long leaderGlobalCheckPoint) {
        List<Translog.Operation> ops = new ArrayList<>();
        for (long seqNo = fromSeqNo; seqNo <= toSeqNo; seqNo++) {
            String id = UUIDs.randomBase64UUID();
            byte[] source = "{}".getBytes(StandardCharsets.UTF_8);
            ops.add(new Translog.Index("doc", id, seqNo, 0, source));
        }
        return new ShardChangesAction.Response(imdVersion, leaderGlobalCheckPoint, ops.toArray(new Translog.Operation[0]));
    }

    void startTask(ShardFollowNodeTask task, long leaderGlobalCheckpoint, long followerGlobalCheckpoint) {
        // The call the updateMapping is a noop, so noting happens.
        task.start(leaderGlobalCheckpoint, followerGlobalCheckpoint);
    }


}
