/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.TransportService;

import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.elasticsearch.test.ESIntegTestCase.client;
import static org.elasticsearch.test.ESIntegTestCase.internalCluster;
import static org.elasticsearch.test.ESTestCase.assertBusy;

public class TaskAssertions {
    private static final Logger logger = LogManager.getLogger(TaskAssertions.class);

    private TaskAssertions() { }

    public static void awaitTaskWithPrefix(String actionPrefix) throws Exception {
        logger.info("--> waiting for task with prefix [{}] to start", actionPrefix);

        assertBusy(() -> {
            for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
                if (transportService.getTaskManager().getTasks().values().stream().anyMatch(t -> t.getAction().startsWith(actionPrefix))) {
                    return;
                }
            }
            fail("no task with prefix [" + actionPrefix + "] found");
        });
    }

    public static void assertAllCancellableTasksAreCancelled(String actionPrefix) throws Exception {
        logger.info("--> checking that all tasks with prefix {} are marked as cancelled", actionPrefix);

        assertBusy(() -> {
            boolean foundTask = false;
            for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
                for (CancellableTask cancellableTask : transportService.getTaskManager().getCancellableTasks().values()) {
                    if (cancellableTask.getAction().startsWith(actionPrefix)) {
                        foundTask = true;
                        assertTrue(
                            "task " + cancellableTask.getId() + "/" + cancellableTask.getAction() + " not cancelled",
                            cancellableTask.isCancelled());
                    }
                }
            }
            assertTrue("found no cancellable tasks", foundTask);
        });
    }

    public static void assertAllTasksHaveFinished(String actionPrefix) throws Exception {
        logger.info("--> checking that all tasks with prefix {} have finished", actionPrefix);
        assertBusy(() -> {
            final List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().get().getTasks();
            assertTrue(tasks.toString(), tasks.stream().noneMatch(t -> t.getAction().startsWith(actionPrefix)));
        });
    }
}
