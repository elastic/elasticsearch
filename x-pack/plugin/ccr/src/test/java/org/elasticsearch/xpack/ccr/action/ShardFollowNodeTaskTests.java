/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

public class ShardFollowNodeTaskTests extends ESTestCase {

    private ShardFollowNodeTask task;

    private AtomicLong leaderGlobalCheckPoint;
    private AtomicLong imdVersion;
    private AtomicInteger mappingUpdateCounter;

    private AtomicInteger truncatedRequests;
    private AtomicBoolean randomlyTruncateRequests;

    private AtomicInteger failedRequests;
    private AtomicBoolean randomlyFailWithRetryableError;

    private AtomicReference<Exception> failureHolder = new AtomicReference<>();

    public void testDefaults() throws Exception {
        long followGlobalCheckpoint = randomIntBetween(-1, 2048);
        task = createShardFollowTask(ShardFollowNodeTask.DEFAULT_MAX_BATCH_OPERATION_COUNT,
            ShardFollowNodeTask.DEFAULT_MAX_CONCURRENT_READ_BATCHES, ShardFollowNodeTask.DEFAULT_MAX_CONCURRENT_WRITE_BATCHES,
            10000, ShardFollowNodeTask.DEFAULT_MAX_WRITE_BUFFER_SIZE, followGlobalCheckpoint);
        task.start(followGlobalCheckpoint);

        assertBusy(() -> {
            assertThat(task.getStatus().getFollowerGlobalCheckpoint(), equalTo(10000L));
        });
        assertThat(mappingUpdateCounter.get(), equalTo(1));
    }

    public void testHitBufferLimit() throws Exception {
        // Setting buffer limit to 100, so that we are sure the limit will be met
        task = createShardFollowTask(ShardFollowNodeTask.DEFAULT_MAX_BATCH_OPERATION_COUNT, 3, 1, 10000, 100, -1);
        task.start(-1);

        assertBusy(() -> {
            assertThat(task.getStatus().getFollowerGlobalCheckpoint(), equalTo(10000L));
        });
    }

    public void testConcurrentReadsAndWrites() throws Exception {
        long followGlobalCheckpoint = randomIntBetween(-1, 2048);
        task = createShardFollowTask(randomIntBetween(32, 2048), randomIntBetween(2, 10),
            randomIntBetween(2, 10), 50000, 10240, followGlobalCheckpoint);
        task.start(followGlobalCheckpoint);

        assertBusy(() -> {
            assertThat(task.getStatus().getFollowerGlobalCheckpoint(), equalTo(50000L));
        });
    }

    public void testMappingUpdate() throws Exception {
        task = createShardFollowTask(1024, 1, 1, 1000, 1024, -1);
        task.start(-1);

        assertBusy(() -> {
            assertThat(task.getStatus().getFollowerGlobalCheckpoint(), greaterThanOrEqualTo(1000L));
        });
        imdVersion.set(2L);
        leaderGlobalCheckPoint.set(10000L);
        assertBusy(() -> {
            assertThat(task.getStatus().getFollowerGlobalCheckpoint(), equalTo(10000L));
        });
        assertThat(mappingUpdateCounter.get(), equalTo(2));
    }

    public void testOccasionalApiFailure() throws Exception {
        task = createShardFollowTask(1024, 1, 1, 10000, 1024, -1);
        task.start(-1);
        randomlyFailWithRetryableError.set(true);
        assertBusy(() -> {
            assertThat(task.getStatus().getFollowerGlobalCheckpoint(), equalTo(10000L));
        });
        assertThat(failedRequests.get(), greaterThan(0));
    }

    public void testNotAllExpectedOpsReturned() throws Exception {
        task = createShardFollowTask(1024, 1, 1, 10000, 1024, -1);
        task.start(-1);
        randomlyTruncateRequests.set(true);
        assertBusy(() -> {
            assertThat(task.getStatus().getFollowerGlobalCheckpoint(), equalTo(10000L));
        });
        assertThat(truncatedRequests.get(), greaterThan(0));
    }

    ShardFollowNodeTask createShardFollowTask(int maxBatchOperationCount, int maxConcurrentReadBatches, int maxConcurrentWriteBathces,
                                              int globalCheckpoint, int bufferWriteLimit, long followGlobalCheckpoint) {
        leaderGlobalCheckPoint = new AtomicLong(globalCheckpoint);
        imdVersion = new AtomicLong(1L);
        mappingUpdateCounter = new AtomicInteger(0);
        randomlyTruncateRequests = new AtomicBoolean(false);
        truncatedRequests = new AtomicInteger();
        randomlyFailWithRetryableError = new AtomicBoolean(false);
        failedRequests = new AtomicInteger(0);
        AtomicBoolean stopped = new AtomicBoolean(false);
        ShardFollowTask params = new ShardFollowTask(null, new ShardId("follow_index", "", 0),
            new ShardId("leader_index", "", 0), maxBatchOperationCount, maxConcurrentReadBatches,
            ShardFollowNodeTask.DEFAULT_MAX_BATCH_SIZE_IN_BYTES, maxConcurrentWriteBathces, bufferWriteLimit,
            TimeValue.timeValueMillis(500), TimeValue.timeValueMillis(10), Collections.emptyMap());

        BiConsumer<TimeValue, Runnable> scheduler = (delay, task) -> {
            try {
                Thread.sleep(delay.millis());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            Thread thread = new Thread(task);
            thread.start();
        };
        AtomicInteger readCounter = new AtomicInteger();
        AtomicInteger writeCounter = new AtomicInteger();
        LocalCheckpointTracker tracker = new LocalCheckpointTracker(followGlobalCheckpoint, followGlobalCheckpoint);
        return new ShardFollowNodeTask(1L, "type", ShardFollowTask.NAME, "description", null, Collections.emptyMap(), params, scheduler,
            TimeValue.timeValueMillis(10), TimeValue.timeValueMillis(500)) {

            @Override
            protected void innerUpdateMapping(LongConsumer handler, Consumer<Exception> errorHandler) {
                mappingUpdateCounter.incrementAndGet();
                handler.accept(imdVersion.get());
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(List<Translog.Operation> operations, LongConsumer handler,
                                                               Consumer<Exception> errorHandler) {
                if (randomlyFailWithRetryableError.get() && readCounter.incrementAndGet() % 5 == 0) {
                    failedRequests.incrementAndGet();
                    errorHandler.accept(new UnavailableShardsException(params.getFollowShardId(), "test error"));
                    return;
                }

                for(Translog.Operation op : operations) {
                    tracker.markSeqNoAsCompleted(op.seqNo());
                }

                // Emulate network thread and avoid SO:
                Thread thread = new Thread(() -> handler.accept(tracker.getCheckpoint()));
                thread.start();
            }

            @Override
            protected void innerSendShardChangesRequest(long from, int maxOperationCount, Consumer<ShardChangesAction.Response> handler,
                                                        Consumer<Exception> errorHandler) {
                if (randomlyFailWithRetryableError.get() && writeCounter.incrementAndGet() % 5 == 0) {
                    failedRequests.incrementAndGet();
                    errorHandler.accept(new UnavailableShardsException(params.getFollowShardId(), "test error"));
                    return;
                }

                if (from < 0) {
                    errorHandler.accept(new IllegalArgumentException());
                    return;
                }

                ShardChangesAction.Response response;
                if (from > leaderGlobalCheckPoint.get()) {
                    response = new ShardChangesAction.Response(imdVersion.get(), leaderGlobalCheckPoint.get(), new Translog.Operation[0]);
                } else {
                    if (randomlyTruncateRequests.get() && maxOperationCount > 10 && truncatedRequests.get() < 5) {
                        truncatedRequests.incrementAndGet();
                        maxOperationCount = maxOperationCount / 2;
                    }
                    List<Translog.Operation> ops = new ArrayList<>();
                    long maxSeqNo = Math.min(from + maxOperationCount, leaderGlobalCheckPoint.get());
                    for (long seqNo = from; seqNo <= maxSeqNo; seqNo++) {
                        String id = UUIDs.randomBase64UUID();
                        byte[] source = "{}".getBytes(StandardCharsets.UTF_8);
                        ops.add(new Translog.Index("doc", id, seqNo, 0, source));
                    }
                    response = new ShardChangesAction.Response(imdVersion.get(), leaderGlobalCheckPoint.get(),
                        ops.toArray(new Translog.Operation[0]));
                }
                // Emulate network thread and avoid SO:
                Thread thread = new Thread(() -> handler.accept(response));
                thread.start();
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
                stopped.set(true);
                failureHolder.set(e);
            }
        };
    }

    @After
    public void cancelNodeTask() throws Exception {
        if (task != null){
            task.markAsCompleted();
            assertThat(failureHolder.get(), nullValue());
            assertBusy(() -> {
                ShardFollowNodeTask.Status status = task.getStatus();
                assertThat(status.getNumberOfConcurrentReads(), equalTo(0));
                assertThat(status.getNumberOfConcurrentWrites(), equalTo(0));
            });
        }
    }

}
