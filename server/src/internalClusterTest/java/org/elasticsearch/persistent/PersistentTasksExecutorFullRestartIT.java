/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestParams;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, minNumDataNodes = 1)
public class PersistentTasksExecutorFullRestartIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPersistentTasksPlugin.class);
    }

    protected boolean ignoreExternalCluster() {
        return true;
    }

    public void testFullClusterRestart() throws Exception {
        PersistentTasksService service = internalCluster().getInstance(PersistentTasksService.class);
        int numberOfTasks = randomIntBetween(1, 10);
        String[] taskIds = new String[numberOfTasks];
        List<PlainActionFuture<PersistentTask<TestParams>>> futures = new ArrayList<>(numberOfTasks);

        for (int i = 0; i < numberOfTasks; i++) {
            PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
            futures.add(future);
            taskIds[i] = UUIDs.base64UUID();
            service.sendStartRequest(taskIds[i], TestPersistentTasksExecutor.NAME, new TestParams("Blah"), future);
        }

        for (int i = 0; i < numberOfTasks; i++) {
            assertThat(futures.get(i).get().getId(), equalTo(taskIds[i]));
        }

        List<PersistentTask<?>> tasksInProgress = findTasks(internalCluster().clusterService().state(), TestPersistentTasksExecutor.NAME);
        assertThat(tasksInProgress.size(), equalTo(numberOfTasks));

        // Make sure that at least one of the tasks is running
        assertBusy(() -> {
            // Wait for the task to start
            assertThat(
                clusterAdmin().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks().size(),
                greaterThan(0)
            );
        });

        // Restart cluster
        internalCluster().fullRestart();
        ensureYellow();

        tasksInProgress = findTasks(internalCluster().clusterService().state(), TestPersistentTasksExecutor.NAME);
        assertThat(tasksInProgress.size(), equalTo(numberOfTasks));
        Set<String> taskInProgressIds = tasksInProgress.stream().map(PersistentTask::getId).collect(Collectors.toSet());
        // Check that cluster state is correct
        for (int i = 0; i < numberOfTasks; i++) {
            assertTrue(taskInProgressIds.contains(taskIds[i]));
        }

        logger.info("Waiting for {} tasks to start", numberOfTasks);
        assertBusy(() -> {
            // Wait for all tasks to start
            assertThat(
                clusterAdmin().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get().getTasks().size(),
                equalTo(numberOfTasks)
            );
        });

        logger.info("Complete all tasks");
        // Complete the running task and make sure it finishes properly
        assertThat(
            new TestPersistentTasksPlugin.TestTasksRequestBuilder(client()).setOperation("finish").get().getTasks().size(),
            equalTo(numberOfTasks)
        );

        assertBusy(() -> {
            // Make sure the task is removed from the cluster state
            assertThat(findTasks(internalCluster().clusterService().state(), TestPersistentTasksExecutor.NAME), empty());
        });

    }
}
