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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.elasticsearch.test.ESIntegTestCase.client;
import static org.elasticsearch.test.ESIntegTestCase.internalCluster;
import static org.elasticsearch.test.ESTestCase.assertBusy;

public class TaskAssertions {
    private static final Logger logger = LogManager.getLogger(TaskAssertions.class);

    private TaskAssertions() {}

    public static void awaitTaskWithPrefix(String actionPrefix) throws Exception {
        awaitTaskWithPrefix(actionPrefix, internalCluster().getInstances(TransportService.class));
    }

    public static void awaitTaskWithPrefixOnMaster(String actionPrefix) throws Exception {
        awaitTaskWithPrefix(actionPrefix, List.of(internalCluster().getCurrentMasterNodeInstance(TransportService.class)));
    }

    private static void awaitTaskWithPrefix(String actionPrefix, Iterable<TransportService> transportServiceInstances) throws Exception {
        logger.info("--> waiting for task with prefix [{}] to start", actionPrefix);

        assertBusy(() -> {
            for (TransportService transportService : transportServiceInstances) {
                List<Task> matchingTasks = transportService.getTaskManager()
                    .getTasks()
                    .values()
                    .stream()
                    .filter(t -> t.getAction().startsWith(actionPrefix))
                    .collect(Collectors.toList());
                if (matchingTasks.isEmpty() == false) {
                    logger.trace("--> found {} tasks with prefix [{}]: {}", matchingTasks.size(), actionPrefix, matchingTasks);
                    return;
                }
            }
            fail("no task with prefix [" + actionPrefix + "] found");
        });
    }

    public static void assertAllCancellableTasksAreCancelled(String actionPrefix, @Nullable String opaqueId) throws Exception {
        logger.info("--> checking that all tasks with prefix {} are marked as cancelled", actionPrefix);

        assertBusy(() -> {
            var tasks = new ArrayList<CancellableTask>();
            for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
                var taskManager = transportService.getTaskManager();
                assertTrue(taskManager.assertCancellableTaskConsistency());
                taskManager.getCancellableTasks().values().stream().filter(t -> t.getAction().startsWith(actionPrefix)).forEach(tasks::add);
            }
            assertFalse("no tasks found for action: " + actionPrefix, tasks.isEmpty());
            assertTrue(
                tasks.toString(),
                tasks.stream().allMatch(t -> t.isCancelled() && Objects.equals(t.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER), opaqueId))
            );
        }, 30, TimeUnit.SECONDS);
    }

    public static void assertAllCancellableTasksAreCancelled(String actionPrefix) throws Exception {
        assertAllCancellableTasksAreCancelled(actionPrefix, null);
    }

    public static void assertAllTasksHaveFinished(String actionPrefix) throws Exception {
        logger.info("--> checking that all tasks with prefix {} have finished", actionPrefix);
        assertBusy(() -> {
            final List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().get().getTasks();
            assertTrue(tasks.toString(), tasks.stream().noneMatch(t -> t.action().startsWith(actionPrefix)));
        });
    }
}
