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

package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

/**
 * Makes sure that tasks that attempt to store themselves on completion retry if
 * they don't succeed at first.
 */
public class TaskStorageRetryIT extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(TestTaskPlugin.class);
    }

    /**
     * Lower the queue sizes to be small enough that both bulk and searches will time out and have to be retried.
     */
    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
                .put(super.nodeSettings())
                .put("thread_pool.write.size", 2)
                .put("thread_pool.write.queue_size", 0)
                .build();
    }

    public void testRetry() throws Exception {
        logger.info("block the write executor");
        CyclicBarrier barrier = new CyclicBarrier(2);
        getInstanceFromNode(ThreadPool.class).executor(ThreadPool.Names.WRITE).execute(() -> {
            try {
                barrier.await();
                logger.info("blocking the write executor");
                barrier.await();
                logger.info("unblocked the write executor");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        barrier.await();
        Task task;
        PlainListenableActionFuture<TestTaskPlugin.NodesResponse> future =
                PlainListenableActionFuture.newListenableFuture();
        try {
            logger.info("start a task that will store its results");
            TestTaskPlugin.NodesRequest req = new TestTaskPlugin.NodesRequest("foo");
            req.setShouldStoreResult(true);
            req.setShouldBlock(false);
            task = nodeClient().executeLocally(TestTaskPlugin.TestTaskAction.INSTANCE, req, future);

            logger.info("verify that the task has started and is still running");
            assertBusy(() -> {
                GetTaskResponse runningTask = client().admin().cluster()
                        .prepareGetTask(new TaskId(nodeClient().getLocalNodeId(), task.getId()))
                        .get();
                assertNotNull(runningTask.getTask());
                assertFalse(runningTask.getTask().isCompleted());
                assertEquals(emptyMap(), runningTask.getTask().getErrorAsMap());
                assertEquals(emptyMap(), runningTask.getTask().getResponseAsMap());
                assertFalse(future.isDone());
            });
        } finally {
            logger.info("unblock the write executor");
            barrier.await();
        }

        logger.info("wait for the task to finish");
        future.get(10, TimeUnit.SECONDS);

        logger.info("check that it was written successfully");
        GetTaskResponse finishedTask = client().admin().cluster()
                .prepareGetTask(new TaskId(nodeClient().getLocalNodeId(), task.getId()))
                .get();
        assertTrue(finishedTask.getTask().isCompleted());
        assertEquals(emptyMap(), finishedTask.getTask().getErrorAsMap());
        assertEquals(singletonMap("failure_count", 0),
                finishedTask.getTask().getResponseAsMap());
    }

    /**
     * Get the {@linkplain NodeClient} local to the node being tested.
     */
    private NodeClient nodeClient() {
        /*
         * Luckilly our test infrastructure already returns it, but we can't
         * change the return type in the superclass because it is wrapped other
         * places.
         */
        return (NodeClient) client();
    }
}

