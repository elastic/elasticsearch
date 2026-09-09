/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.action.admin.cluster.node.tasks.TaskManagerTestCase;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * How large a {@code GET /_tasks} response gets is decided by how busy the cluster is, not by anything in the request.
 * Every matching task is turned into a {@link TaskInfo} and held on the coordinating node until all nodes have reported,
 * and nothing is written until then, so the request circuit breaker never sees any of it.
 *
 * <p>A serverless search node ran out of memory building one such response: 586,403 {@link TaskInfo} objects retaining
 * 564MB of a 992MB heap, from Kibana polling the endpoint with no filters while the write tier was backed up.
 *
 * <p>These tests cover the page being bounded, the pages joining up, and the default being small enough to fit.
 */
public class ListTasksPaginationTests extends TaskManagerTestCase {

    private static final String TEST_ACTION = "internal:test/list-tasks-pagination";

    /**
     * A ceiling one management call should not go past. Deliberately generous: the point is that some bound exists, not
     * that this is the right figure.
     */
    private static final long RESPONSE_BUDGET_BYTES = 16 * 1024 * 1024;

    /** Tasks collected by the single {@code GET /_tasks} call on the node that ran out of memory. */
    private static final int OBSERVED_TASK_COUNT = 586_403;

    public void testResponseIsNoLargerThanThePageSize() throws Exception {
        final int pageSize = 20;
        final TestNode coordinator = startCluster();
        final List<Task> registered = registerTasks(coordinator, pageSize + 5);
        try {
            final ListTasksResponse response = listTasks(coordinator, new ListTasksRequest().setSize(pageSize));

            assertThat(response.getTasks(), hasSize(pageSize));
            assertSortedByTaskId(response.getTasks());
        } finally {
            unregister(coordinator, registered);
        }
    }

    public void testPagesJoinUpToTheWholeList() throws Exception {
        final int pageSize = 10;
        final TestNode coordinator = startCluster();
        final List<Task> registered = registerTasks(coordinator, 25);
        try {
            // One more request than it should take, so that a cursor which fails to advance is a failure rather than a hang.
            final int allowedRequests = (registered.size() / pageSize) + 2;
            final List<TaskInfo> seen = new ArrayList<>();
            TaskId after = TaskId.EMPTY_TASK_ID;
            for (int request = 0; request <= allowedRequests; request++) {
                assertThat("the cursor is not advancing", request, lessThan(allowedRequests));
                final ListTasksResponse page = listTasks(coordinator, new ListTasksRequest().setSize(pageSize).setAfter(after));
                if (page.getTasks().isEmpty()) {
                    break;
                }
                assertThat(page.getTasks(), hasSize(lessThanOrEqualTo(pageSize)));
                assertSortedByTaskId(page.getTasks());
                seen.addAll(page.getTasks());
                after = page.getTasks().get(page.getTasks().size() - 1).taskId();
            }

            final Set<TaskId> seenIds = new HashSet<>();
            for (TaskInfo task : seen) {
                assertTrue("task [" + task.taskId() + "] came back on more than one page", seenIds.add(task.taskId()));
            }
            assertThat(seenIds, equalTo(taskIds(coordinator, registered)));
        } finally {
            unregister(coordinator, registered);
        }
    }

    public void testEverythingIsReturnedWhenPagingIsTurnedOff() throws Exception {
        final TestNode coordinator = startCluster();
        final List<Task> registered = registerTasks(coordinator, 25);
        try {
            final ListTasksResponse response = listTasks(coordinator, new ListTasksRequest().setSize(0));

            assertThat(response.getTasks(), hasSize(registered.size()));
        } finally {
            unregister(coordinator, registered);
        }
    }

    /**
     * Guards the default: a page size only helps if the page it permits actually fits. If someone raises the default, this
     * is where they find out what they have signed up for.
     */
    public void testTheDefaultPageFitsWithinBudget() {
        final int sample = 10_000;
        final long perTask = RamUsageTester.ramUsed(sampleTasks(sample)) / sample;

        logger.info(
            "{} bytes per task: a default page of {} tasks retains {} bytes, the {} tasks seen in the incident would have " + "retained {}",
            perTask,
            ListTasksRequest.DEFAULT_SIZE,
            perTask * ListTasksRequest.DEFAULT_SIZE,
            OBSERVED_TASK_COUNT,
            perTask * OBSERVED_TASK_COUNT
        );

        // These TaskInfo objects are simpler than the ones in the incident, which retained around 962 bytes each, so this
        // understates the real cost.
        assertThat(perTask * ListTasksRequest.DEFAULT_SIZE, lessThan(RESPONSE_BUDGET_BYTES));
    }

    private static void assertSortedByTaskId(List<TaskInfo> tasks) {
        for (int i = 1; i < tasks.size(); i++) {
            final TaskId previous = tasks.get(i - 1).taskId();
            final TaskId current = tasks.get(i).taskId();
            final int byNode = previous.getNodeId().compareTo(current.getNodeId());
            assertTrue(
                "tasks are not in page order: [" + previous + "] came before [" + current + "]",
                byNode < 0 || (byNode == 0 && previous.getId() < current.getId())
            );
        }
    }

    private TestNode startCluster() {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        return testNodes[0];
    }

    /** Matches only the tasks this test registers, which also keeps us off the double-listing path used for reindex. */
    private static ListTasksResponse listTasks(TestNode node, ListTasksRequest request) {
        return ActionTestUtils.executeBlocking(node.transportListTasksAction, request.setActions(TEST_ACTION));
    }

    private static List<Task> registerTasks(TestNode node, int count) {
        final TaskManager taskManager = node.transportService.getTaskManager();
        final List<Task> tasks = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            tasks.add(taskManager.register("transport", TEST_ACTION, new TaskAwareRequest() {
                @Override
                public void setParentTask(TaskId taskId) {}

                @Override
                public void setRequestId(long requestId) {}

                @Override
                public TaskId getParentTask() {
                    return TaskId.EMPTY_TASK_ID;
                }
            }));
        }
        return tasks;
    }

    private static Set<TaskId> taskIds(TestNode node, List<Task> tasks) {
        final Set<TaskId> ids = new HashSet<>();
        for (Task task : tasks) {
            ids.add(new TaskId(node.getNodeId(), task.getId()));
        }
        return ids;
    }

    private static void unregister(TestNode node, List<Task> tasks) {
        final TaskManager taskManager = node.transportService.getTaskManager();
        for (Task task : tasks) {
            taskManager.unregister(task);
        }
    }

    private static List<TaskInfo> sampleTasks(int count) {
        final List<TaskInfo> tasks = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            tasks.add(
                new TaskInfo(
                    new TaskId("node-1", i),
                    "transport",
                    "node-1",
                    "indices:data/write/bulk[s]",
                    "requests[1], indices[an-index-with-a-realistic-name]",
                    null,
                    System.nanoTime(),
                    0L,
                    true,
                    false,
                    TaskId.EMPTY_TASK_ID,
                    Map.of("X-Opaque-Id", "kibana-" + i),
                    new TaskId("node-1", i),
                    System.currentTimeMillis()
                )
            );
        }
        return tasks;
    }
}
