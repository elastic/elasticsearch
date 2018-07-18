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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static org.hamcrest.Matchers.equalTo;

public class ShardFollowNodeTaskRandomTests extends ESTestCase {

    public void testSingleReaderWriter() throws Exception {
        TestRun testRun = createTestRun(randomNonNegativeLong(), randomNonNegativeLong(), randomIntBetween(1, 2048));
        ShardFollowNodeTask task = createShardFollowTask(1, testRun);
        startAndAssertAndStopTask(task, testRun);
    }

    public void testMultipleReaderWriter() throws Exception {
        int concurrency = randomIntBetween(2, 8);
        TestRun testRun = createTestRun(0, 0, 1024);
        ShardFollowNodeTask task = createShardFollowTask(concurrency, testRun);
        startAndAssertAndStopTask(task, testRun);
    }

    private void startAndAssertAndStopTask(ShardFollowNodeTask task, TestRun testRun) throws Exception {
        task.start(testRun.startSeqNo - 1);
        assertBusy(() -> {
            ShardFollowNodeTask.Status status = task.getStatus();
            assertThat(status.getLeaderGlobalCheckpoint(), equalTo(testRun.finalExpectedGlobalCheckpoint));
            assertThat(status.getFollowerGlobalCheckpoint(), equalTo(testRun.finalExpectedGlobalCheckpoint));
            assertThat(status.getIndexMetadataVersion(), equalTo(testRun.finalIndexMetaDataVerion));
        });

        task.markAsCompleted();
        assertBusy(() -> {
            ShardFollowNodeTask.Status status = task.getStatus();
            assertThat(status.getNumberOfConcurrentReads(), equalTo(0));
            assertThat(status.getNumberOfConcurrentWrites(), equalTo(0));
        });
    }

    private ShardFollowNodeTask createShardFollowTask(int concurrency, TestRun testRun) {
        AtomicBoolean stopped = new AtomicBoolean(false);
        ShardFollowTask params = new ShardFollowTask(null, new ShardId("follow_index", "", 0),
            new ShardId("leader_index", "", 0), testRun.maxOperationCount, concurrency,
            ShardFollowNodeTask.DEFAULT_MAX_BATCH_SIZE_IN_BYTES, concurrency, 10240,
            TimeValue.timeValueMillis(10), TimeValue.timeValueMillis(10), Collections.emptyMap());

        BiConsumer<TimeValue, Runnable> scheduler = (delay, task) -> {
            try {
                Thread.sleep(delay.millis());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            task.run();
        };
        LocalCheckpointTracker tracker = new LocalCheckpointTracker(testRun.startSeqNo - 1, testRun.startSeqNo - 1);
        Iterator<TestResponse> iterator = testRun.responses.iterator();
        return new ShardFollowNodeTask(1L, "type", ShardFollowTask.NAME, "description", null, Collections.emptyMap(), params, scheduler) {

            private long indexMetadataVersion = 0L;

            @Override
            protected void innerUpdateMapping(LongConsumer handler, Consumer<Exception> errorHandler) {
                handler.accept(indexMetadataVersion);
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(List<Translog.Operation> operations, LongConsumer handler,
                                                               Consumer<Exception> errorHandler) {
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

                // Emulate network thread and avoid SO:
                Runnable task = () -> {
                    ShardChangesAction.Response response;
                    synchronized (this) {
                        if (iterator.hasNext()) {
                            TestResponse testResponse = iterator.next();
                            if (testResponse.exception != null) {
                                errorHandler.accept(testResponse.exception);
                                return;
                            }

                            response = testResponse.response;
                            if (response.getOperations().length != 0) {
                                long firstSeqNo = response.getOperations()[0].seqNo();
                                if (from != firstSeqNo) {
                                    throw new AssertionError("unexpected from [" + from + "] expected [" + firstSeqNo + "] instead");
                                }
                            }
                            indexMetadataVersion = Math.max(indexMetadataVersion, response.getIndexMetadataVersion());
                        } else {
                            response = new ShardChangesAction.Response(0L, tracker.getCheckpoint(), new Translog.Operation[0]);
                        }
                    }
                    handler.accept(response);
                };
                Thread thread = new Thread(task);
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
            }
        };
    }

    private static TestRun createTestRun(long startSeqNo, long startIndexMetadataVersion, int maxOperationCount) {
        long prevGlobalCheckpoint = startSeqNo;
        long indexMetaDataVersion = startIndexMetadataVersion;
        int numResponses = randomIntBetween(16, 256);
        List<TestResponse> responses = new ArrayList<>(numResponses);
        for (int i = 0; i < numResponses; i++) {
            long nextGlobalCheckPoint = prevGlobalCheckpoint + maxOperationCount;
            if (randomIntBetween(0, 10) == 5) {
                indexMetaDataVersion++;
            }

            if (randomBoolean()) {
                List<Translog.Operation> ops = new ArrayList<>();
                for (long seqNo = prevGlobalCheckpoint; seqNo <= nextGlobalCheckPoint; seqNo++) {
                    String id = UUIDs.randomBase64UUID();
                    byte[] source = "{}".getBytes(StandardCharsets.UTF_8);
                    ops.add(new Translog.Index("doc", id, seqNo, 0, source));
                }
                responses.add(new TestResponse(null, new ShardChangesAction.Response(indexMetaDataVersion, nextGlobalCheckPoint,
                    ops.toArray(EMPTY))));
            } else {
                // Simulates a leader shard copy not having all the operations the shard follow task thinks it has by
                // splitting up a response into multiple responses:
                long toSeqNo;
                for (long fromSeqNo = prevGlobalCheckpoint; fromSeqNo <= nextGlobalCheckPoint; fromSeqNo = toSeqNo + 1) {
                    toSeqNo = randomLongBetween(fromSeqNo, nextGlobalCheckPoint);
                    List<Translog.Operation> ops = new ArrayList<>();
                    for (long seqNo = fromSeqNo; seqNo <= toSeqNo; seqNo++) {
                        String id = UUIDs.randomBase64UUID();
                        byte[] source = "{}".getBytes(StandardCharsets.UTF_8);
                        ops.add(new Translog.Index("doc", id, seqNo, 0, source));
                    }
                    ShardChangesAction.Response response = new ShardChangesAction.Response(indexMetaDataVersion, toSeqNo,
                        ops.toArray(EMPTY));
                    responses.add(new TestResponse(null, response));
                }
            }

            // Rarely add an empty shard changes response to also simulate a leader shard lagging behind
            if (rarely()) {
                ShardChangesAction.Response response = new ShardChangesAction.Response(indexMetaDataVersion, nextGlobalCheckPoint, EMPTY);
                responses.add(new TestResponse(null, response));
            }
            // Rarely add a random retryable error
            if (rarely()) {
                Exception error = new UnavailableShardsException(new ShardId("test", "test", 0), "");
                responses.add(new TestResponse(error, null));
            }
            prevGlobalCheckpoint = nextGlobalCheckPoint + 1;
        }
        return new TestRun(maxOperationCount, startSeqNo, startIndexMetadataVersion, indexMetaDataVersion,
            prevGlobalCheckpoint - 1, responses);
    }

    private static class TestRun {

        final int maxOperationCount;
        final long startSeqNo;
        final long startIndexMetadataVersion;

        final long finalIndexMetaDataVerion;
        final long finalExpectedGlobalCheckpoint;
        final List<TestResponse> responses;

        private TestRun(int maxOperationCount, long startSeqNo, long startIndexMetadataVersion, long finalIndexMetaDataVerion,
                        long finalExpectedGlobalCheckpoint, List<TestResponse> responses) {
            this.maxOperationCount = maxOperationCount;
            this.startSeqNo = startSeqNo;
            this.startIndexMetadataVersion = startIndexMetadataVersion;
            this.finalIndexMetaDataVerion = finalIndexMetaDataVerion;
            this.finalExpectedGlobalCheckpoint = finalExpectedGlobalCheckpoint;
            this.responses = responses;
        }
    }

    private static class TestResponse {

        final Exception exception;
        final ShardChangesAction.Response response;

        private TestResponse(Exception exception, ShardChangesAction.Response response) {
            this.exception = exception;
            this.response = response;
        }
    }

    private final static Translog.Operation[] EMPTY = new Translog.Operation[0];

}
