/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;
import org.elasticsearch.xpack.persistent.TestPersistentActionPlugin.TestPersistentAction;
import org.elasticsearch.xpack.persistent.TestPersistentActionPlugin.TestRequest;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, minNumDataNodes = 1)
public class PersistentActionFullRestartIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPersistentActionPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    protected boolean ignoreExternalCluster() {
        return true;
    }

    @TestLogging("org.elasticsearch.xpack.persistent:TRACE,org.elasticsearch.cluster.service:DEBUG")
    public void testFullClusterRestart() throws Exception {
        int numberOfTasks = randomIntBetween(1, 10);
        long[] taskIds = new long[numberOfTasks];
        boolean[] stopped = new boolean[numberOfTasks];
        int runningTasks = 0;
        for (int i = 0; i < numberOfTasks; i++) {
            if (randomBoolean()) {
                runningTasks++;
                taskIds[i] = TestPersistentAction.INSTANCE.newRequestBuilder(client()).testParam("Blah").get().getTaskId();
                stopped[i] = false;
            } else {
                taskIds[i] = CreatePersistentTaskAction.INSTANCE.newRequestBuilder(client())
                        .setAction(TestPersistentAction.NAME)
                        .setRequest(new TestRequest("Blah"))
                        .setStopped(true)
                        .get().getTaskId();
                stopped[i] = true;
            }
        }
        final int numberOfRunningTasks = runningTasks;
        PersistentTasksInProgress tasksInProgress = internalCluster().clusterService().state().getMetaData()
                .custom(PersistentTasksInProgress.TYPE);
        assertThat(tasksInProgress.tasks().size(), equalTo(numberOfTasks));

        if (numberOfRunningTasks > 0) {
            // Make sure that at least one of the tasks is running
            assertBusy(() -> {
                // Wait for the task to start
                assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentAction.NAME + "[c]").get()
                                .getTasks().size(), greaterThan(0));
            });
        }

        // Restart cluster
        internalCluster().fullRestart();
        ensureYellow();

        tasksInProgress = internalCluster().clusterService().state().getMetaData().custom(PersistentTasksInProgress.TYPE);
        assertThat(tasksInProgress.tasks().size(), equalTo(numberOfTasks));
        // Check that cluster state is correct
        for (int i = 0; i < numberOfTasks; i++) {
            PersistentTaskInProgress<?> task = tasksInProgress.getTask(taskIds[i]);
            assertNotNull(task);
            assertThat(task.isStopped(), equalTo(stopped[i]));
        }

        logger.info("Waiting for {} original tasks to start", numberOfRunningTasks);
        assertBusy(() -> {
            // Wait for the running task to start automatically
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentAction.NAME + "[c]").get().getTasks().size(),
                    equalTo(numberOfRunningTasks));
        });

        // Start all other tasks
        tasksInProgress = internalCluster().clusterService().state().getMetaData().custom(PersistentTasksInProgress.TYPE);
        for (int i = 0; i < numberOfTasks; i++) {
            PersistentTaskInProgress<?> task = tasksInProgress.getTask(taskIds[i]);
            assertNotNull(task);
            logger.info("checking task with id {} stopped {} node {}", task.getId(), task.isStopped(), task.getExecutorNode());
            assertThat(task.isStopped(), equalTo(stopped[i]));
            assertThat(task.getExecutorNode(), stopped[i] ? nullValue() : notNullValue());
            if (stopped[i]) {
                assertAcked(StartPersistentTaskAction.INSTANCE.newRequestBuilder(client()).setTaskId(task.getId()).get());
            }
        }

        logger.info("Waiting for {} tasks to start", numberOfTasks);
        assertBusy(() -> {
            // Wait for all tasks to start
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentAction.NAME + "[c]").get().getTasks().size(),
                    equalTo(numberOfTasks));
        });

        logger.info("Complete all tasks");
        // Complete the running task and make sure it finishes properly
        assertThat(new TestPersistentActionPlugin.TestTasksRequestBuilder(client()).setOperation("finish").get().getTasks().size(),
                equalTo(numberOfTasks));

        assertBusy(() -> {
            // Make sure the task is removed from the cluster state
            assertThat(((PersistentTasksInProgress) internalCluster().clusterService().state().getMetaData()
                    .custom(PersistentTasksInProgress.TYPE)).tasks(), empty());
        });

    }
}
