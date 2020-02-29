/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.index.shard.IndexShardTestCase;

public class BatchedShardExecutorTests extends IndexShardTestCase {

//    private static final int WRITE_THREADS = 2;
//
//    private ThreadPool threadPool;
//
//    @Override
//    public void setUp() throws Exception {
//        super.setUp();
//        Settings settings = Settings.builder().put("thread_pool.write.size", WRITE_THREADS).build();
//        threadPool = new TestThreadPool(getTestName(), settings);
//    }
//
//
//    @Override
//    public void tearDown() throws Exception {
//        try {
//            terminate(threadPool);
//        } finally {
//            super.tearDown();
//        }
//    }
//
//    public void testMaxScheduledTasksIsEqualToWriteThreads() throws Exception {
//        IndexShard shard = newStartedShard(true);
//
//        try {
//            BatchedShardExecutor batchedShardExecutor = new BatchedShardExecutor(primaryHandler(), replicaHandler(), threadPool);
//            batchedShardExecutor.afterIndexShardCreated(shard);
//
//            int numberOfOps = randomIntBetween(10, 20);
//            CountDownLatch flushedLatch = new CountDownLatch(numberOfOps);
//
//            try (Releasable ignore = blockWriteThreads(WRITE_THREADS)) {
//                for (int i = 0; i < numberOfOps; ++i) {
//                    BulkShardRequest request = bulkShardRequest(shard, i);
//
//                    batchedShardExecutor.primary(request, shard, noop(), noFailure(flushedLatch::countDown));
//                }
//
//                assertBusy(() -> {
//                    BatchedShardExecutor.ShardState shardState = batchedShardExecutor.getShardState(shard);
//                    assertEquals(WRITE_THREADS, shardState.scheduledTasks());
//                    assertEquals(numberOfOps, shardState.pendingOperations());
//                });
//            }
//            flushedLatch.await();
//
//            assertBusy(() -> {
//                BatchedShardExecutor.ShardState shardState = batchedShardExecutor.getShardState(shard);
//                assertEquals(0, shardState.scheduledTasks());
//            });
//        } finally {
//            closeShards(shard);
//        }
//    }
//
//    public void testOperationsAreIndexedOnPrimaryAndWrittenToTranslog() throws Exception {
//        IndexShard shard = newStartedShard(true);
//
//        try {
//            BatchedShardExecutor batchedShardExecutor = new BatchedShardExecutor(primaryHandler(), replicaHandler(), threadPool);
//            batchedShardExecutor.afterIndexShardCreated(shard);
//
//            int numberOfOps = randomIntBetween(200, 400);
//            CountDownLatch flushedLatch = new CountDownLatch(numberOfOps);
//
//            AtomicInteger successfulWrites = new AtomicInteger();
//
//            for (int i = 0; i < numberOfOps; ++i) {
//                BulkShardRequest request = bulkShardRequest(shard, i);
//                batchedShardExecutor.primary(request, shard, successCounter(successfulWrites), noFailure(flushedLatch::countDown));
//            }
//
//            flushedLatch.await();
//
//            assertEquals(numberOfOps, successfulWrites.get());
//            assertDocCount(shard, numberOfOps);
//
//            assertThat(getTranslog(shard).totalOperations(), greaterThanOrEqualTo(numberOfOps));
//        } finally {
//            closeShards(shard);
//        }
//    }
//
//    public void testOperationsAreIndexedOnPrimaryWrittenToTranslogAndRefreshIfRequested() throws Exception {
//        IndexShard shard = newStartedShard(true);
//
//        try {
//            BatchedShardExecutor batchedShardExecutor = new BatchedShardExecutor(primaryHandler(), replicaHandler(), threadPool);
//            batchedShardExecutor.afterIndexShardCreated(shard);
//
//            int numberOfOps = randomIntBetween(200, 400);
//            CountDownLatch flushedLatch = new CountDownLatch(numberOfOps);
//
//            AtomicInteger successfulWrites = new AtomicInteger();
//
//            for (int i = 0; i < numberOfOps; ++i) {
//                BulkShardRequest request = bulkShardRequest(shard, i);
//                request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
//                batchedShardExecutor.primary(request, shard, successCounter(successfulWrites), noFailure((result) -> {
//                    assertThat(result.forcedRefresh(), equalTo(true));
//                    flushedLatch.countDown();
//                }));
//            }
//
//            flushedLatch.await();
//
//            assertEquals(numberOfOps, successfulWrites.get());
//            assertDocCount(shard, numberOfOps);
//            assertThat(getTranslog(shard).totalOperations(), greaterThanOrEqualTo(numberOfOps));
//        } finally {
//            closeShards(shard);
//        }
//    }
//
//    public void testExceptionFromEngineFailsRequest() throws Exception {
//        IndexShard shard = newStartedShard(true);
//
//        try {
//            CheckedBiFunction<BatchedShardExecutor.PrimaryOp, Runnable, Boolean, Exception> primaryHandler = (primaryOp, rescheduler) -> {
//                throw new AlreadyClosedException(primaryOp.getIndexShard().shardId() + " engine is closed", new IOException());
//            };
//            BatchedShardExecutor batchedShardExecutor = new BatchedShardExecutor(primaryHandler, replicaHandler(), threadPool);
//            batchedShardExecutor.afterIndexShardCreated(shard);
//
//
//            BulkShardRequest request = bulkShardRequest(shard, 0);
//            PlainActionFuture<BatchedShardExecutor.WriteResult> writeListener = PlainActionFuture.newFuture();
//            // The flush listener is not called if the write fails
//            batchedShardExecutor.primary(request, shard, writeListener, ActionListener.wrap(() -> {
//                assert false : "Flush listener should not have been called";
//            }));
//
//            expectThrows(AlreadyClosedException.class, writeListener::actionGet);
//        } finally {
//            closeShards(shard);
//        }
//    }
//
//    public void testClosedIndexShardStateIsCleanedUp() throws Exception {
//        IndexShard shard = newStartedShard(true);
//        boolean closed = false;
//        try {
//            BatchedShardExecutor batchedShardExecutor = new BatchedShardExecutor(primaryHandler(), replicaHandler(), threadPool);
//            batchedShardExecutor.afterIndexShardCreated(shard);
//
//            CountDownLatch flushedLatch = new CountDownLatch(1);
//            AtomicInteger successfulWrites = new AtomicInteger();
//
//            BulkShardRequest request1 = bulkShardRequest(shard, 0);
//            batchedShardExecutor.primary(request1, shard, successCounter(successfulWrites), noFailure(flushedLatch::countDown));
//
//            flushedLatch.await();
//
//            assertEquals(1, successfulWrites.get());
//
//            // Close the shard
//            closeShards(shard);
//            closed = true;
//
//            batchedShardExecutor.beforeIndexShardClosed(shard.shardId(), shard, Settings.EMPTY);
//            assertBusy(() -> assertNull(batchedShardExecutor.getShardState(shard)));
//        } finally {
//            if (closed == false) {
//                closeShards(shard);
//            }
//        }
//    }
//
//    public void testOperationsAreIndexedOnReplicaAndWrittenToTranslog() throws Exception {
//        IndexShard primary = newStartedShard(true);
//        IndexShard replica = newStartedShard(false);
//
//        try {
//            BatchedShardExecutor batchedShardExecutor = new BatchedShardExecutor(primaryHandler(), replicaHandler(), threadPool);
//            batchedShardExecutor.afterIndexShardCreated(primary);
//            batchedShardExecutor.afterIndexShardCreated(replica);
//
//            int numberOfOps = randomIntBetween(200, 400);
//            CountDownLatch primaryFlushedLatch = new CountDownLatch(numberOfOps);
//
//            CopyOnWriteArrayList<BatchedShardExecutor.WriteResult> responses = new CopyOnWriteArrayList<>();
//
//            for (int i = 0; i < numberOfOps; ++i) {
//                BulkShardRequest request = bulkShardRequest(primary, i);
//                batchedShardExecutor.primary(request, primary, noFailure(responses::add), noFailure(primaryFlushedLatch::countDown));
//            }
//
//            primaryFlushedLatch.await();
//
//            CountDownLatch replicaFlushedLatch = new CountDownLatch(numberOfOps);
//            AtomicInteger successfulWrites = new AtomicInteger();
//
//            for (BatchedShardExecutor.WriteResult writeResult : responses) {
//                batchedShardExecutor.replica(writeResult.getReplicaRequest(), replica, successCounter(successfulWrites),
//                    noFailure(replicaFlushedLatch::countDown));
//            }
//
//            replicaFlushedLatch.await();
//
//            assertEquals(numberOfOps, successfulWrites.get());
//            assertDocCount(replica, numberOfOps);
//
//            assertThat(getTranslog(replica).totalOperations(), greaterThanOrEqualTo(numberOfOps));
//        } finally {
//            closeShards(primary, replica);
//        }
//    }
//
//    private BulkShardRequest bulkShardRequest(IndexShard shard, int id) {
//        BulkItemRequest[] items = new BulkItemRequest[1];
//        DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index")
//            .source(Requests.INDEX_CONTENT_TYPE)
//            .id(String.valueOf(id));
//        items[0] = new BulkItemRequest(0, writeRequest);
//        return new BulkShardRequest(shard.shardId(), WriteRequest.RefreshPolicy.NONE, items);
//    }
//
//    private CheckedBiFunction<BatchedShardExecutor.PrimaryOp, Runnable, Boolean, Exception> primaryHandler() {
//         return (primaryOp, rescheduler) -> {
//            boolean finished = TransportShardBulkAction.executeBulkItemRequest(primaryOp.getContext(), null,
//                threadPool::absoluteTimeInMillis, new TransportShardBulkActionTests.NoopMappingUpdatePerformer(), listener -> {},
//                rescheduler);
//            assert finished;
//            return finished;
//        };
//    }
//
//    private CheckedFunction<BatchedShardExecutor.ReplicaOp, Boolean, Exception> replicaHandler() {
//        return (replicaOp) -> {
//            Translog.Location location = TransportShardBulkAction.performOnReplica(replicaOp.getRequest(), replicaOp.getIndexShard());
//            replicaOp.setLocation(location);
//            return true;
//        };
//    }
//
//    private Releasable blockWriteThreads(int numberToBlock) throws InterruptedException {
//        CountDownLatch runningLatch = new CountDownLatch(numberToBlock);
//        CountDownLatch blockedLatch = new CountDownLatch(1);
//
//        for (int i = 0; i < numberToBlock; ++i) {
//            threadPool.executor(ThreadPool.Names.WRITE).execute(new AbstractRunnable() {
//                @Override
//                protected void doRun() throws Exception {
//                    runningLatch.countDown();
//                    blockedLatch.await();
//                }
//
//                @Override
//                public void onFailure(Exception e) {
//                    logger.error("uncaught exception on test thread pool", e);
//                }
//            });
//        }
//
//        runningLatch.await();
//        return blockedLatch::countDown;
//    }
//
//    private static <T> ActionListener<T> noop() {
//        return ActionListener.wrap(() -> {});
//    }
//
//    private <T> ActionListener<T> successCounter(AtomicInteger counter) {
//        return noFailure((v) -> counter.incrementAndGet());
//    }
//
//    private <T> ActionListener<T> noFailure(Runnable runnable) {
//        return noFailure((v) -> runnable.run());
//    }
//
//    private <T> ActionListener<T> noFailure(Consumer<T> consumer) {
//        return ActionListener.wrap(consumer::accept, (e) -> {
//            logger.error("unexpected onFailure call", e);
//            assert false;
//        });
//    }
}
