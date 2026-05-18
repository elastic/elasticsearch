/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogOperationsUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NoSeedNodeLeftException;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ShardFollowNodeTaskRandomTests extends ESTestCase {

    public void testSingleReaderWriter() throws Exception {
        TestRun testRun = createTestRun(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomIntBetween(1, 2048)
        );
        ShardFollowNodeTask task = createShardFollowTask(1, testRun);
        startAndAssertAndStopTask(task, testRun);
    }

    public void testMultipleReaderWriter() throws Exception {
        int concurrency = randomIntBetween(2, 8);
        TestRun testRun = createTestRun(0, 0, 0, 0, between(1, 1024));
        ShardFollowNodeTask task = createShardFollowTask(concurrency, testRun);
        startAndAssertAndStopTask(task, testRun);
    }

    private void startAndAssertAndStopTask(ShardFollowNodeTask task, TestRun testRun) throws Exception {
        task.start("uuid", testRun.startSeqNo - 1, testRun.startSeqNo - 1, testRun.startSeqNo - 1, testRun.startSeqNo - 1);
        assertBusy(() -> {
            ShardFollowNodeTaskStatus status = task.getStatus();
            assertThat(status.leaderGlobalCheckpoint(), equalTo(testRun.finalExpectedGlobalCheckpoint));
            assertThat(status.followerGlobalCheckpoint(), equalTo(testRun.finalExpectedGlobalCheckpoint));
            final long numberOfFailedFetches = testRun.responses.values()
                .stream()
                .flatMap(List::stream)
                .filter(f -> f.exception != null)
                .count();
            assertThat(status.failedReadRequests(), equalTo(numberOfFailedFetches));
            // the failures were able to be retried so fetch failures should have cleared
            assertThat(status.readExceptions().entrySet(), hasSize(0));
            assertThat(status.followerMappingVersion(), equalTo(testRun.finalMappingVersion));
        });

        task.markAsCompleted();
        assertBusy(() -> {
            ShardFollowNodeTaskStatus status = task.getStatus();
            assertThat(status.outstandingReadRequests(), equalTo(0));
            assertThat(status.outstandingWriteRequests(), equalTo(0));
        });
    }

    private ShardFollowNodeTask createShardFollowTask(int concurrency, TestRun testRun) {
        AtomicBoolean stopped = new AtomicBoolean(false);
        ShardFollowTask params = new ShardFollowTask(
            null,
            new ShardId("follow_index", "", 0),
            new ShardId("leader_index", "", 0),
            testRun.maxOperationCount,
            testRun.maxOperationCount,
            concurrency,
            concurrency,
            TransportResumeFollowAction.DEFAULT_MAX_READ_REQUEST_SIZE,
            TransportResumeFollowAction.DEFAULT_MAX_READ_REQUEST_SIZE,
            10240,
            ByteSizeValue.of(512, ByteSizeUnit.MB),
            TimeValue.timeValueMillis(10),
            TimeValue.timeValueMillis(10),
            Collections.emptyMap()
        );

        ThreadPool threadPool = new TestThreadPool(getClass().getSimpleName());
        BiConsumer<TimeValue, Runnable> scheduler = (delay, task) -> {
            assert delay.millis() < 100 : "The delay should be kept to a minimum, so that this test does not take to long to run";
            if (stopped.get() == false) {
                threadPool.schedule(task, delay, threadPool.generic());
            }
        };
        List<Translog.Operation> receivedOperations = Collections.synchronizedList(new ArrayList<>());
        LocalCheckpointTracker tracker = new LocalCheckpointTracker(testRun.startSeqNo - 1, testRun.startSeqNo - 1);
        return new ShardFollowNodeTask(
            1L,
            "type",
            ShardFollowTask.NAME,
            "description",
            null,
            Collections.emptyMap(),
            params,
            scheduler,
            System::nanoTime
        ) {

            private volatile long mappingVersion = 0L;
            private volatile long settingsVersion = 0L;
            private volatile long aliasesVersion = 0L;
            private final Map<Long, Integer> fromToSlot = new HashMap<>();

            @Override
            protected void innerUpdateMapping(long minRequiredMappingVersion, LongConsumer handler, Consumer<Exception> errorHandler) {
                handler.accept(mappingVersion);
            }

            @Override
            protected void innerUpdateSettings(LongConsumer handler, Consumer<Exception> errorHandler) {
                handler.accept(settingsVersion);
            }

            @Override
            protected void innerUpdateAliases(LongConsumer handler, Consumer<Exception> errorHandler) {
                handler.accept(aliasesVersion);
            }

            @Override
            protected void innerSendBulkShardOperationsRequest(
                String followerHistoryUUID,
                List<Translog.Operation> operations,
                long maxSeqNoOfUpdates,
                Consumer<BulkShardOperationsResponse> handler,
                Consumer<Exception> errorHandler
            ) {
                for (Translog.Operation op : operations) {
                    tracker.markSeqNoAsProcessed(op.seqNo());
                }
                receivedOperations.addAll(operations);

                // Emulate network thread and avoid SO:
                final BulkShardOperationsResponse response = new BulkShardOperationsResponse();
                response.setGlobalCheckpoint(tracker.getProcessedCheckpoint());
                response.setMaxSeqNo(tracker.getMaxSeqNo());
                threadPool.generic().execute(() -> handler.accept(response));
            }

            @Override
            protected void innerSendShardChangesRequest(
                long from,
                int maxOperationCount,
                Consumer<ShardChangesAction.Response> handler,
                Consumer<Exception> errorHandler
            ) {

                // Emulate network thread and avoid SO:
                Runnable task = () -> {
                    List<TestResponse> items = testRun.responses.get(from);
                    if (items != null) {
                        final TestResponse testResponse;
                        synchronized (fromToSlot) {
                            int slot;
                            if (fromToSlot.get(from) == null) {
                                slot = fromToSlot.getOrDefault(from, 0);
                                fromToSlot.put(from, slot);
                            } else {
                                slot = fromToSlot.get(from);
                            }
                            testResponse = items.get(slot);
                            fromToSlot.put(from, ++slot);
                            // if too many invocations occur with the same from then AOBE occurs, this ok and then something is wrong.
                        }
                        mappingVersion = testResponse.mappingVersion;
                        settingsVersion = testResponse.settingsVersion;
                        if (testResponse.exception != null) {
                            errorHandler.accept(testResponse.exception);
                        } else {
                            handler.accept(testResponse.response);
                        }
                    } else {
                        assert from >= testRun.finalExpectedGlobalCheckpoint;
                        final long globalCheckpoint = tracker.getProcessedCheckpoint();
                        final long maxSeqNo = tracker.getMaxSeqNo();
                        handler.accept(
                            new ShardChangesAction.Response(
                                0L,
                                0L,
                                0L,
                                globalCheckpoint,
                                maxSeqNo,
                                randomNonNegativeLong(),
                                new Translog.Operation[0],
                                1L
                            )
                        );
                    }
                };
                threadPool.generic().execute(task);
            }

            @Override
            protected Scheduler.Cancellable scheduleBackgroundRetentionLeaseRenewal(final LongSupplier followerGlobalCheckpoint) {
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

            @Override
            protected boolean isStopped() {
                return stopped.get();
            }

            @Override
            public void markAsCompleted() {
                stopped.set(true);
                tearDown();
            }

            @Override
            public void markAsFailed(Exception e) {
                stopped.set(true);
                tearDown();
            }

            private void tearDown() {
                threadPool.shutdown();
                List<Translog.Operation> expectedOperations = testRun.responses.values()
                    .stream()
                    .flatMap(List::stream)
                    .map(testResponse -> testResponse.response)
                    .filter(Objects::nonNull)
                    .flatMap(response -> Arrays.stream(response.getOperations()))
                    .sorted(Comparator.comparingLong(Translog.Operation::seqNo))
                    .collect(Collectors.toList());
                assertThat(receivedOperations.size(), equalTo(expectedOperations.size()));
                receivedOperations.sort(Comparator.comparingLong(Translog.Operation::seqNo));
                for (int i = 0; i < receivedOperations.size(); i++) {
                    Translog.Operation actual = receivedOperations.get(i);
                    Translog.Operation expected = expectedOperations.get(i);
                    assertThat(actual, equalTo(expected));
                }
            }
        };
    }

    private static TestRun createTestRun(
        final long startSeqNo,
        final long startMappingVersion,
        final long startSettingsVersion,
        final long startAliasesVersion,
        final int maxOperationCount
    ) {
        long prevGlobalCheckpoint = startSeqNo;
        long mappingVersion = startMappingVersion;
        long settingsVersion = startSettingsVersion;
        long aliasesVersion = startAliasesVersion;
        int numResponses = randomIntBetween(16, 256);
        Map<Long, List<TestResponse>> responses = Maps.newMapWithExpectedSize(numResponses);
        for (int i = 0; i < numResponses; i++) {
            long nextGlobalCheckPoint = prevGlobalCheckpoint + maxOperationCount;
            if (sometimes()) {
                mappingVersion++;
            }
            if (sometimes()) {
                settingsVersion++;
            }
            if (sometimes()) {
                aliasesVersion++;
            }
            if (sometimes()) {
                List<TestResponse> item = new ArrayList<>();
                // Sometimes add a random retryable error
                if (sometimes()) {
                    Exception error = new UnavailableShardsException(new ShardId("test", "test", 0), "");
                    item.add(new TestResponse(error, mappingVersion, settingsVersion, null));
                }
                List<Translog.Operation> ops = new ArrayList<>();
                for (long seqNo = prevGlobalCheckpoint; seqNo <= nextGlobalCheckPoint; seqNo++) {
                    String id = UUIDs.randomBase64UUID();
                    ops.add(TranslogOperationsUtils.indexOp(id, seqNo, 0));
                }
                item.add(
                    new TestResponse(
                        null,
                        mappingVersion,
                        settingsVersion,
                        new ShardChangesAction.Response(
                            mappingVersion,
                            settingsVersion,
                            aliasesVersion,
                            nextGlobalCheckPoint,
                            nextGlobalCheckPoint,
                            randomNonNegativeLong(),
                            ops.toArray(EMPTY),
                            randomNonNegativeLong()
                        )
                    )
                );
                responses.put(prevGlobalCheckpoint, item);
            } else {
                // Simulates a leader shard copy not having all the operations the shard follow task thinks it has by
                // splitting up a response into multiple responses AND simulates maxBatchSizeInBytes limit being reached:
                long toSeqNo;
                for (long fromSeqNo = prevGlobalCheckpoint; fromSeqNo <= nextGlobalCheckPoint; fromSeqNo = toSeqNo + 1) {
                    toSeqNo = randomLongBetween(fromSeqNo, nextGlobalCheckPoint);
                    List<TestResponse> item = new ArrayList<>();
                    // Sometimes add a random retryable error
                    if (sometimes()) {
                        Exception error = randomFrom(
                            new UnavailableShardsException(new ShardId("test", "test", 0), ""),
                            new NoSeedNodeLeftException("cluster_a"),
                            new CircuitBreakingException("test", randomInt(), randomInt(), randomFrom(CircuitBreaker.Durability.values())),
                            new EsRejectedExecutionException("test")
                        );
                        item.add(new TestResponse(error, mappingVersion, settingsVersion, null));
                    }
                    // Sometimes add an empty shard changes response to also simulate a leader shard lagging behind
                    if (sometimes()) {
                        ShardChangesAction.Response response = new ShardChangesAction.Response(
                            mappingVersion,
                            settingsVersion,
                            aliasesVersion,
                            prevGlobalCheckpoint,
                            prevGlobalCheckpoint,
                            randomNonNegativeLong(),
                            EMPTY,
                            randomNonNegativeLong()
                        );
                        item.add(new TestResponse(null, mappingVersion, settingsVersion, response));
                    }
                    List<Translog.Operation> ops = new ArrayList<>();
                    for (long seqNo = fromSeqNo; seqNo <= toSeqNo; seqNo++) {
                        String id = UUIDs.randomBase64UUID();
                        ops.add(TranslogOperationsUtils.indexOp(id, seqNo, 0));
                    }
                    // Report toSeqNo to simulate maxBatchSizeInBytes limit being met or last op to simulate a shard lagging behind:
                    long localLeaderGCP = randomBoolean() ? ops.get(ops.size() - 1).seqNo() : toSeqNo;
                    ShardChangesAction.Response response = new ShardChangesAction.Response(
                        mappingVersion,
                        settingsVersion,
                        aliasesVersion,
                        localLeaderGCP,
                        localLeaderGCP,
                        randomNonNegativeLong(),
                        ops.toArray(EMPTY),
                        randomNonNegativeLong()
                    );
                    item.add(new TestResponse(null, mappingVersion, settingsVersion, response));
                    responses.put(fromSeqNo, Collections.unmodifiableList(item));
                }
            }
            prevGlobalCheckpoint = nextGlobalCheckPoint + 1;
        }
        return new TestRun(maxOperationCount, startSeqNo, startMappingVersion, mappingVersion, prevGlobalCheckpoint - 1, responses);
    }

    // Instead of rarely(), which returns true very rarely especially not running in nightly mode or a multiplier have not been set
    private static boolean sometimes() {
        return randomIntBetween(0, 10) == 5;
    }

    private static class TestRun {

        final int maxOperationCount;
        final long startSeqNo;
        final long startMappingVersion;

        final long finalMappingVersion;
        final long finalExpectedGlobalCheckpoint;
        final Map<Long, List<TestResponse>> responses;

        private TestRun(
            int maxOperationCount,
            long startSeqNo,
            long startMappingVersion,
            long finalMappingVersion,
            long finalExpectedGlobalCheckpoint,
            Map<Long, List<TestResponse>> responses
        ) {
            this.maxOperationCount = maxOperationCount;
            this.startSeqNo = startSeqNo;
            this.startMappingVersion = startMappingVersion;
            this.finalMappingVersion = finalMappingVersion;
            this.finalExpectedGlobalCheckpoint = finalExpectedGlobalCheckpoint;
            this.responses = Collections.unmodifiableMap(responses);
        }
    }

    private static class TestResponse {

        final Exception exception;
        final long mappingVersion;
        final long settingsVersion;
        final ShardChangesAction.Response response;

        private TestResponse(Exception exception, long mappingVersion, long settingsVersion, ShardChangesAction.Response response) {
            this.exception = exception;
            this.mappingVersion = mappingVersion;
            this.settingsVersion = settingsVersion;
            this.response = response;
        }
    }

    private static final Translog.Operation[] EMPTY = new Translog.Operation[0];

}
