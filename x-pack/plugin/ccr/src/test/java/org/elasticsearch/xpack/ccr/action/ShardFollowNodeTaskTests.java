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
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
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

@TestLogging("org.elasticsearch.xpack.ccr.action:TRACE")
public class ShardFollowNodeTaskTests extends ESTestCase {

    private ShardFollowNodeTask task;

    private AtomicLong imdVersion;
    private AtomicInteger mappingUpdateCounter;

    private AtomicInteger truncatedRequests;
    private AtomicBoolean randomlyTruncateRequests;

    private AtomicInteger failedRequests;
    private AtomicBoolean randomlyFailWithRetryableError;

    private AtomicReference<Exception> failureHolder = new AtomicReference<>();

    public void testDefaults() throws Exception {
        task = createShardFollowTask(ShardFollowNodeTask.DEFAULT_MAX_READ_SIZE, ShardFollowNodeTask.DEFAULT_MAX_CONCURRENT_READS,
            ShardFollowNodeTask.DEFAULT_MAX_WRITE_SIZE, ShardFollowNodeTask.DEFAULT_MAX_CONCURRENT_WRITES, 10000,
            ShardFollowNodeTask.DEFAULT_MAX_BUFFER_SIZE);
        task.start(randomIntBetween(-1, 2048), -1);

        assertBusy(() -> {
            assertThat(task.getStatus().getProcessedGlobalCheckpoint(), equalTo(10000L));
        });
        assertThat(mappingUpdateCounter.get(), equalTo(1));
    }

    public void testHitBufferLimit() throws Exception {
        // Setting buffer limit to 100, so that we are sure the limit will be met
        task = createShardFollowTask(ShardFollowNodeTask.DEFAULT_MAX_READ_SIZE, 3,
            ShardFollowNodeTask.DEFAULT_MAX_WRITE_SIZE, 1, 10000, 100);
        task.start(-1, -1);

        assertBusy(() -> {
            assertThat(task.getStatus().getProcessedGlobalCheckpoint(), equalTo(10000L));
        });
    }

    public void testConcurrentReadsAndWrites() throws Exception {
        task = createShardFollowTask(randomIntBetween(32, 2048), randomIntBetween(2, 10), randomIntBetween(32, 2048),
            randomIntBetween(2, 10), 50000, 10240);
        task.start(randomIntBetween(-1, 2048), -1);

        assertBusy(() -> {
            assertThat(task.getStatus().getProcessedGlobalCheckpoint(), equalTo(50000L));
        });
    }

    public void testMappingUpdate() throws Exception {
        task = createShardFollowTask(1024, 1, 1024, 1, 10000, 1024);
        task.start(-1, -1);

        assertBusy(() -> {
            assertThat(task.getStatus().getProcessedGlobalCheckpoint(), greaterThanOrEqualTo(1000L));
        });
        imdVersion.set(2L);
        assertBusy(() -> {
            assertThat(task.getStatus().getProcessedGlobalCheckpoint(), equalTo(10000L));
        });
        assertThat(mappingUpdateCounter.get(), equalTo(2));
    }

    public void testOccasionalApiFailure() throws Exception {
        task = createShardFollowTask(1024, 1, 1024, 1, 10000, 1024);
        task.start(-1, -1);
        randomlyFailWithRetryableError.set(true);
        assertBusy(() -> {
            assertThat(task.getStatus().getProcessedGlobalCheckpoint(), equalTo(10000L));
        });
        assertThat(failedRequests.get(), greaterThan(0));
    }

    public void testNotAllExpectedOpsReturned() throws Exception {
        task = createShardFollowTask(1024, 1, 1024, 1, 10000, 1024);
        task.start(-1, -1);
        randomlyTruncateRequests.set(true);
        assertBusy(() -> {
            assertThat(task.getStatus().getProcessedGlobalCheckpoint(), equalTo(10000L));
        });
        assertThat(truncatedRequests.get(), greaterThan(0));
    }

    ShardFollowNodeTask createShardFollowTask(int maxReadSize, int maxConcurrentReads, int maxWriteSize,
                                              int maxConcurrentWrites, int leaderGlobalCheckpoint, int bufferLimit) {
        imdVersion = new AtomicLong(1L);
        mappingUpdateCounter = new AtomicInteger(0);
        randomlyTruncateRequests = new AtomicBoolean(false);
        truncatedRequests = new AtomicInteger();
        randomlyFailWithRetryableError = new AtomicBoolean(false);
        failedRequests = new AtomicInteger(0);
        AtomicBoolean stopped = new AtomicBoolean(false);
        ShardFollowTask params = new ShardFollowTask(null, new ShardId("follow_index", "", 0),
            new ShardId("leader_index", "", 0), maxReadSize, maxConcurrentReads, ShardFollowNodeTask.DEFAULT_MAX_OPERATIONS_SIZE_IN_BYTES,
            maxWriteSize, maxConcurrentWrites, bufferLimit, Collections.emptyMap());

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
        LocalCheckpointTracker tracker = new LocalCheckpointTracker(SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.NO_OPS_PERFORMED);
        return new ShardFollowNodeTask(1L, "type", ShardFollowTask.NAME, "description", null,
            Collections.emptyMap(), null, null, params, scheduler) {

            @Override
            protected void updateMapping(LongConsumer handler) {
                mappingUpdateCounter.incrementAndGet();
                handler.accept(imdVersion.get());
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(Translog.Operation[] operations, LongConsumer handler,
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
            protected void innerSendShardChangesRequest(long from, Long to, Consumer<ShardChangesAction.Response> handler,
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
                if (from >= leaderGlobalCheckpoint) {
                    response = new ShardChangesAction.Response(1L, leaderGlobalCheckpoint, new Translog.Operation[0]);
                } else {
                    int size = to == null ? 100 : (int) (to - from + 1);
                    if (randomlyTruncateRequests.get() && size > 10 && truncatedRequests.get() < 5) {
                        truncatedRequests.incrementAndGet();
                        size = size / 2;
                    }
                    Translog.Operation[] ops = new Translog.Operation[size];
                    for (int i = 0; i < ops.length; i++) {
                        ops[i] = new Translog.Index("doc", UUIDs.randomBase64UUID(), from + i, 0, "{}".getBytes(StandardCharsets.UTF_8));
                    }
                    response = new ShardChangesAction.Response(imdVersion.get(), leaderGlobalCheckpoint, ops);
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
