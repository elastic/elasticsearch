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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;

public class BatchedShardExecutorTests extends IndexShardTestCase {

    private static final int WRITE_THREADS = 2;

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder().put("thread_pool.write.size", WRITE_THREADS).build();
        threadPool = new TestThreadPool(getTestName(), settings);
    }


    @Override
    public void tearDown() throws Exception {
        try {
            terminate(threadPool);
        } finally {
            super.tearDown();
        }
    }

    public void testMaxScheduleTasksIsEqualToWriteThreads() throws Exception {
        IndexShard shard = newStartedShard(true);

        try {
            BulkItemRequest[] items = new BulkItemRequest[1];
            DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index")
                .id("id")
                .source(Requests.INDEX_CONTENT_TYPE)
                .create(randomBoolean());
            items[0] = new BulkItemRequest(0, writeRequest);
            BulkShardRequest request = new BulkShardRequest(shard.shardId(), WriteRequest.RefreshPolicy.NONE, items);

            BatchedShardExecutor batchedShardExecutor = new BatchedShardExecutor(primaryHandler(), replicaHandler(), threadPool);

            int numberOfOps = randomIntBetween(10, 20);
            CountDownLatch doneLatch = new CountDownLatch(numberOfOps);

            try (Releasable ignore = blockWriteThreads(WRITE_THREADS)) {
                for (int i = 0; i < numberOfOps; ++i) {
                    batchedShardExecutor.primary(request, shard, noop(), ActionListener.wrap(doneLatch::countDown));
                }

                assertBusy(() -> {
                    BatchedShardExecutor.ShardState shardState = batchedShardExecutor.getShardState(shard.shardId());
                    assertEquals(WRITE_THREADS, shardState.scheduledTasks());
                    assertEquals(numberOfOps, shardState.pendingOperations());
                });
            }
            doneLatch.await();

            assertBusy(() -> {
                BatchedShardExecutor.ShardState shardState = batchedShardExecutor.getShardState(shard.shardId());
                assertEquals(0, shardState.scheduledTasks());
            });
        } finally {
            closeShards(shard);
        }
    }

    private CheckedBiFunction<BatchedShardExecutor.PrimaryOp, Runnable, Boolean, Exception> primaryHandler() {
         return (primaryOp, rescheduler) -> {
            boolean finished = TransportShardBulkAction.executeBulkItemRequest(primaryOp.getContext(), null,
                threadPool::absoluteTimeInMillis, new TransportShardBulkActionTests.NoopMappingUpdatePerformer(), listener -> {},
                rescheduler);
            assert finished;
            return finished;
        };
    }

    private CheckedFunction<BatchedShardExecutor.ReplicaOp, Boolean, Exception> replicaHandler() {
        return (replicaOp) -> {
            Translog.Location location = TransportShardBulkAction.performOnReplica(replicaOp.getRequest(), replicaOp.getIndexShard());
            replicaOp.setLocation(location);
            return true;
        };
    }

    private Releasable blockWriteThreads(int numberToBlock) throws InterruptedException {
        CountDownLatch runningLatch = new CountDownLatch(numberToBlock);
        CountDownLatch blockedLatch = new CountDownLatch(1);

        for (int i = 0; i < numberToBlock; ++i) {
            threadPool.executor(ThreadPool.Names.WRITE).execute(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    runningLatch.countDown();
                    blockedLatch.await();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("uncaught exception on test thread pool", e);
                }
            });
        }

        runningLatch.await();
        return blockedLatch::countDown;
    }

    private static  <T> ActionListener<T> noop() {
        return ActionListener.wrap(() -> {});
    }
}
