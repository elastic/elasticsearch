/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.MockLog;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Semaphore;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

/**
 * Makes sure that tasks that attempt to store themselves on completion retry if they don't succeed at first.
 */
public class TaskStorageRetryIT extends ESSingleNodeTestCase {

    private static final Logger logger = LogManager.getLogger(TaskStorageRetryIT.class);

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.getPlugins(), TestTaskPlugin.class, WriteRejectingPlugin.class);
    }

    public void testRetry() {
        final var permits = getInstanceFromNode(WriteRejectionPermits.class);
        permits.rejectAlways = true;
        permits.release(2);

        final Task task;
        final var future = new PlainActionFuture<TestTaskPlugin.NodesResponse>();
        try {
            logger.info("start a task that will store its results");
            TestTaskPlugin.NodesRequest req = new TestTaskPlugin.NodesRequest("foo");
            req.setShouldStoreResult(true);
            req.setShouldBlock(false);

            logger.info("wait for failure log message");
            try (var mockLog = MockLog.capture(TaskResultsService.class)) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "task store failure message",
                        TaskResultsService.class.getCanonicalName(),
                        Level.WARN,
                        "failed to store task result, retrying in [*]"
                    )
                );
                task = nodeClient().executeLocally(TestTaskPlugin.TEST_TASK_ACTION, req, future);
                mockLog.awaitAllExpectationsMatched();
            }

            GetTaskResponse runningTask = clusterAdmin().prepareGetTask(new TaskId(nodeClient().getLocalNodeId(), task.getId())).get();
            assertNotNull(runningTask.getTask());
            assertFalse(runningTask.getTask().isCompleted());
            assertEquals(emptyMap(), runningTask.getTask().getErrorAsMap());
            assertEquals(emptyMap(), runningTask.getTask().getResponseAsMap());
            assertFalse(future.isDone());
        } finally {
            permits.rejectAlways = false;
        }

        logger.info("wait for the task to finish");
        safeGet(future);

        logger.info("check that it was written successfully");
        GetTaskResponse finishedTask = clusterAdmin().prepareGetTask(new TaskId(nodeClient().getLocalNodeId(), task.getId())).get();
        assertTrue(finishedTask.getTask().isCompleted());
        assertEquals(emptyMap(), finishedTask.getTask().getErrorAsMap());
        assertEquals(singletonMap("failure_count", 0), finishedTask.getTask().getResponseAsMap());
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

    public static class WriteRejectionPermits extends Semaphore {

        public volatile boolean rejectAlways;

        public WriteRejectionPermits() {
            super(0);
        }
    }

    public static class WriteRejectingPlugin extends Plugin implements ActionPlugin {

        private final WriteRejectionPermits writeRejectionPermits = new WriteRejectionPermits();

        @Override
        public Collection<?> createComponents(PluginServices services) {
            return List.of(writeRejectionPermits);
        }

        @Override
        public Collection<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter.Simple() {
                @Override
                protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
                    if (TransportIndexAction.NAME.equals(action) == false) {
                        return true;
                    }

                    assertArrayEquals(asInstanceOf(IndexRequest.class, request).indices(), new String[] { TaskResultsService.TASK_INDEX });

                    if (writeRejectionPermits.rejectAlways == false && writeRejectionPermits.tryAcquire() == false) {
                        return true;
                    }

                    listener.onFailure(new EsRejectedExecutionException("simulated"));
                    return false;
                }

                @Override
                public int order() {
                    return 0;
                }
            });
        }
    }
}
