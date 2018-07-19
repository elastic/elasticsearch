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
        ShardFollowNodeTask task = createShardFollowTask(8, 8, 8, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(8));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(8L));
        assertThat(shardChangesRequests.get(1)[0], equalTo(9L));
        assertThat(shardChangesRequests.get(1)[1], equalTo(8L));
        assertThat(shardChangesRequests.get(2)[0], equalTo(18L));
        assertThat(shardChangesRequests.get(2)[1], equalTo(8L));
        assertThat(shardChangesRequests.get(3)[0], equalTo(27L));
        assertThat(shardChangesRequests.get(3)[1], equalTo(8L));
        assertThat(shardChangesRequests.get(4)[0], equalTo(36L));
        assertThat(shardChangesRequests.get(4)[1], equalTo(8L));
        assertThat(shardChangesRequests.get(5)[0], equalTo(45L));
        assertThat(shardChangesRequests.get(5)[1], equalTo(8L));
        assertThat(shardChangesRequests.get(6)[0], equalTo(54L));
        assertThat(shardChangesRequests.get(6)[1], equalTo(8L));
        assertThat(shardChangesRequests.get(7)[0], equalTo(63L));
        assertThat(shardChangesRequests.get(7)[1], equalTo(8L));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getNumberOfConcurrentReads(), equalTo(8));
        assertThat(status.getLastRequestedSeqno(), equalTo(64L));
    }

    public void testCoordinateReads_writeBuffer() {
        // Need to set concurrentWrites to 0, other the write buffer gets flushed immediately:
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 0, 32, Long.MAX_VALUE);
        startTask(task, 64, -1);

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
        assertThat(status.getLastRequestedSeqno(), equalTo(64L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(128L));
    }

    public void testCoordinateReads_maxConcurrentReads() {
        ShardFollowNodeTask task = createShardFollowTask(8, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(8L));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getLastRequestedSeqno(), equalTo(8L));
    }

    public void testCoordinateReads_taskCancelled() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        synchronized (task) {
            task.updateLeaderGlobalCheckpoint(128L);
        }
        task.markAsCompleted();
        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(0));
    }

    public void testCoordinateReads_receiveRetryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

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
        assertThat(status.getLastRequestedSeqno(), equalTo(64L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(64L));
    }

    public void testCoordinateReads_receiveRetryableErrorRetriedTooManyTimes() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

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
        assertThat(status.getLastRequestedSeqno(), equalTo(64L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(64L));
    }

    public void testCoordinateReads_receiveNonRetryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

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
        assertThat(status.getLastRequestedSeqno(), equalTo(64L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(64L));
    }

    public void testHandleReadResponse() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 0L, 64L);
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getIndexMetadataVersion(), equalTo(0L));
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(1));
        assertThat(status.getLastRequestedSeqno(), equalTo(64L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(64L));
        assertThat(status.getFollowerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testHandleResponse_receiveLessThanRequested() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 32, 0L, 31L);
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(32L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(1));
        assertThat(status.getLastRequestedSeqno(), equalTo(64L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(64L));
    }

    public void testHandleResponse_receiveNothingExpectedSomething() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 0, 0L, 0L);
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(0));
        assertThat(status.getLastRequestedSeqno(), equalTo(64L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(64L));
    }

    public void testHandleReadResponse_delayCoordinatesRead() {
        int[] counter = new int[]{0};
        scheduler = (delay, task) -> {
            counter[0]++;
            task.run();
        };
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 65, 0L, 64L);
        // Also invokes coordinateReads()
        task.innerHandleReadResponse(0L, 64L, response);
        response = generateShardChangesResponse(0, 0, 0L, 64L);
        task.innerHandleReadResponse(65L, 64L, response);
        assertThat(counter[0], equalTo(1));
    }

    public void testHandleReadResponse_mappingUpdate() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        imdVersions.add(1L);
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 1L, 64L);
        task.handleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getIndexMetadataVersion(), equalTo(1L));
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(1));
        assertThat(status.getLastRequestedSeqno(), equalTo(64L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(64L));
        assertThat(status.getFollowerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testHandleReadResponse_mappingUpdateRetryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        int max = randomIntBetween(1, 10);
        for (int i = 0; i < max; i++) {
            mappingUpdateFailures.add(new ConnectException());
        }
        imdVersions.add(1L);
        task.coordinateReads();
        ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 1L, 64L);
        task.handleReadResponse(0L, 64L, response);

        assertThat(mappingUpdateFailures.size(), equalTo(0));
        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(task.isStopped(), equalTo(false));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getIndexMetadataVersion(), equalTo(1L));
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(1));
        assertThat(status.getLastRequestedSeqno(), equalTo(64L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(64L));

    }

    public void testHandleReadResponse_mappingUpdateRetryableErrorRetriedTooManyTimes() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

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
        assertThat(status.getLastRequestedSeqno(), equalTo(64L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(64L));
    }

    public void testHandleReadResponse_mappingUpdateNonRetryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

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
        assertThat(status.getLastRequestedSeqno(), equalTo(64L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(64L));
    }

    public void testCoordinateWrites() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 0L, 64L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(1));
        assertThat(status.getLastRequestedSeqno(), equalTo(64L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(64L));
        assertThat(status.getFollowerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testCoordinateWrites_maxConcurrentWrites() {
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

    public void testCoordinateWrites_maxBatchOperationCount() {
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

    public void testCoordinateWrites_retryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        int max = randomIntBetween(1, 10);
        for (int i = 0; i < max; i++) {
            writeFailures.add(new ShardNotFoundException(new ShardId("leader_index", "", 0)));
        }
        ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 0L, 64L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 64L, response);

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

    public void testCoordinateWrites_retryableErrorRetriedTooManyTimes() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        int max = randomIntBetween(11, 32);
        for (int i = 0; i < max; i++) {
            writeFailures.add(new ShardNotFoundException(new ShardId("leader_index", "", 0)));
        }
        ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 0L, 64L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 64L, response);

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

    public void testCoordinateWrites_nonRetryableError() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        writeFailures.add(new RuntimeException());
        ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 0L, 64L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));
        assertThat(task.isStopped(), equalTo(true));
        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getNumberOfConcurrentWrites(), equalTo(1));
        assertThat(status.getFollowerGlobalCheckpoint(), equalTo(-1L));
    }

    public void testCoordinateWrites_maxBatchBytesLimit() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 128, Integer.MAX_VALUE, 1L);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 0L, 64L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(64));
    }

    public void testHandleWriteResponse() {
        ShardFollowNodeTask task = createShardFollowTask(64, 1, 1, Integer.MAX_VALUE, Long.MAX_VALUE);
        startTask(task, 64, -1);

        task.coordinateReads();
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(0L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        shardChangesRequests.clear();
        followerGlobalCheckpoints.add(64L);
        ShardChangesAction.Response response = generateShardChangesResponse(0, 64, 0L, 64L);
        // Also invokes coordinatesWrites()
        task.innerHandleReadResponse(0L, 64L, response);

        assertThat(bulkShardOperationRequests.size(), equalTo(1));
        assertThat(bulkShardOperationRequests.get(0), equalTo(Arrays.asList(response.getOperations())));

        // handleWrite() also delegates to coordinateReads
        assertThat(shardChangesRequests.size(), equalTo(1));
        assertThat(shardChangesRequests.get(0)[0], equalTo(65L));
        assertThat(shardChangesRequests.get(0)[1], equalTo(64L));

        ShardFollowNodeTask.Status status = task.getStatus();
        assertThat(status.getNumberOfConcurrentReads(), equalTo(1));
        assertThat(status.getLastRequestedSeqno(), equalTo(64L));
        assertThat(status.getLeaderGlobalCheckpoint(), equalTo(64L));
        assertThat(status.getFollowerGlobalCheckpoint(), equalTo(64L));
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
            protected void innerSendShardChangesRequest(long from, int maxOperationCount, Consumer<ShardChangesAction.Response> handler,
                                                        Consumer<Exception> errorHandler) {
                shardChangesRequests.add(new long[]{from, maxBatchOperationCount});
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

    private static ShardChangesAction.Response generateShardChangesResponse(long fromSeqNo, int size, long imdVersion,
                                                                            long leaderGlobalCheckPoint) {
        List<Translog.Operation> ops = new ArrayList<>();
        for (long seqNo = fromSeqNo; seqNo < size; seqNo++) {
            String id = UUIDs.randomBase64UUID();
            byte[] source = "{}".getBytes(StandardCharsets.UTF_8);
            ops.add(new Translog.Index("doc", id, seqNo, 0, source));
        }
        return new ShardChangesAction.Response(imdVersion, leaderGlobalCheckPoint, ops.toArray(new Translog.Operation[0]));
    }

    void startTask(ShardFollowNodeTask task, long leaderGlobalCheckpoint, long followerGlobalCheckpoint) {
        task.start(followerGlobalCheckpoint);
        // Shortcut to just set leaderGlobalCheckpoint, calling for example handleReadResponse() has side effects that
        // complicates testing in isolation.
        synchronized (task) {
            task.updateLeaderGlobalCheckpoint(leaderGlobalCheckpoint);
        }
    }


}
