/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.persistent;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.persistent.TestPersistentTasksPlugin.removeClusterTask;
import static org.elasticsearch.persistent.TestPersistentTasksPlugin.removeProjectTask;
import static org.elasticsearch.persistent.TestPersistentTasksPlugin.startClusterTask;
import static org.elasticsearch.persistent.TestPersistentTasksPlugin.startProjectTask;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class ClusterAndProjectPersistentTasksSmokeIT extends ESIntegTestCase {

    private static final Set<String> TEST_TASK_NAMES = Set.of(TestPersistentTasksExecutor.NAME, TestPersistentTasksExecutor.CLUSTER_NAME);
    private static final Set<String> TEST_TASK_ACTIONS = Set.of(
        TestPersistentTasksExecutor.NAME + "[c]",
        TestPersistentTasksExecutor.CLUSTER_NAME + "[c]"
    );

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPersistentTasksPlugin.class);
    }

    public void testCoexistenceOfClusterAndProjectPersistentTasks() throws Exception {
        internalCluster().startNode();
        ensureGreen();
        final var persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        final var clusterService = internalCluster().getInstance(ClusterService.class);

        final var projectTask = safeAwait(startProjectTask(persistentTasksService, Metadata.DEFAULT_PROJECT_ID));
        final var clusterTask = safeAwait(startClusterTask(persistentTasksService));

        // Two tasks have different allocation IDs
        assertThat(projectTask.getAllocationId(), not(equalTo(clusterTask.getAllocationId())));
        // They are found in different section of the cluster state
        assertClusterStateHasTaskSize(clusterService, Metadata.DEFAULT_PROJECT_ID, 1);
        assertClusterStateHasTaskSize(clusterService, null, 1);

        // List tasks work correctly for both of them
        final List<TaskInfo> tasks = safeGet(clusterAdmin().prepareListTasks().execute()).getTasks()
            .stream()
            .filter(taskInfo -> "persistent".equals(taskInfo.type()) && TEST_TASK_ACTIONS.contains(taskInfo.action()))
            .toList();
        assertThat(tasks.toString(), tasks, hasSize(2));
        assertThat(
            tasks.stream().map(TaskInfo::action).toList(),
            containsInAnyOrder(TestPersistentTasksExecutor.NAME + "[c]", TestPersistentTasksExecutor.CLUSTER_NAME + "[c]")
        );
        assertThat(
            tasks.stream().map(taskinfo -> taskinfo.parentTaskId().getId()).toList(),
            containsInAnyOrder(projectTask.getAllocationId(), clusterTask.getAllocationId())
        );

        // Start remove the tasks
        if (randomBoolean()) {
            // Remove project task first
            safeAwait(removeProjectTask(persistentTasksService, Metadata.DEFAULT_PROJECT_ID, projectTask.getId()));

            assertBusy(() -> {
                final List<TaskInfo> remainingTasks = safeGet(clusterAdmin().prepareListTasks().execute()).getTasks()
                    .stream()
                    .filter(
                        taskInfo -> "persistent".equals(taskInfo.type())
                            && TEST_TASK_ACTIONS.contains(taskInfo.action())
                            && taskInfo.cancelled() == false
                    )
                    .toList();
                assertThat(remainingTasks.toString(), remainingTasks, hasSize(1));
                assertThat(remainingTasks.getFirst().parentTaskId().getId(), equalTo(clusterTask.getAllocationId()));

            });
            assertClusterStateHasTaskSize(clusterService, Metadata.DEFAULT_PROJECT_ID, 0);
            assertClusterStateHasTaskSize(clusterService, null, 1);

            safeAwait(removeClusterTask(persistentTasksService, clusterTask.getId()));
        } else {
            // Remove cluster task first
            safeAwait(removeClusterTask(persistentTasksService, clusterTask.getId()));

            assertBusy(() -> {
                final List<TaskInfo> remainingTasks = safeGet(clusterAdmin().prepareListTasks().execute()).getTasks()
                    .stream()
                    .filter(
                        taskInfo -> "persistent".equals(taskInfo.type())
                            && TEST_TASK_ACTIONS.contains(taskInfo.action())
                            && taskInfo.cancelled() == false
                    )
                    .toList();
                assertThat(remainingTasks.toString(), remainingTasks, hasSize(1));
                assertThat(remainingTasks.getFirst().parentTaskId().getId(), equalTo(projectTask.getAllocationId()));
            });

            assertClusterStateHasTaskSize(clusterService, Metadata.DEFAULT_PROJECT_ID, 1);
            assertClusterStateHasTaskSize(clusterService, null, 0);

            safeAwait(removeProjectTask(persistentTasksService, Metadata.DEFAULT_PROJECT_ID, projectTask.getId()));
        }

        assertClusterStateHasTaskSize(clusterService, Metadata.DEFAULT_PROJECT_ID, 0);
        assertClusterStateHasTaskSize(clusterService, null, 0);
    }

    private void assertClusterStateHasTaskSize(ClusterService clusterService, @Nullable ProjectId projectId, int size) {
        assertThat(
            PersistentTasks.getTasks(clusterService.state(), projectId)
                .tasks()
                .stream()
                .filter(task -> TEST_TASK_NAMES.contains(task.getTaskName()))
                .toList(),
            hasSize(size)
        );
    }
}
