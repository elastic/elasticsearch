/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.support.ListenableActionFuture;
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
        ListenableActionFuture<TestTaskPlugin.NodesResponse> future = new ListenableActionFuture<>();
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

