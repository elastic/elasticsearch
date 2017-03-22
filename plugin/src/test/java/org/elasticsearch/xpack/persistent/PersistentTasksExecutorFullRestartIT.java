/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.persistent.PersistentTasksExecutorIT.PersistentTaskOperationFuture;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.xpack.persistent.TestPersistentTasksPlugin.TestRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, minNumDataNodes = 1)
public class PersistentTasksExecutorFullRestartIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPersistentTasksPlugin.class);
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
        PersistentTasksService service = internalCluster().getInstance(PersistentTasksService.class);
        int numberOfTasks = randomIntBetween(1, 10);
        long[] taskIds = new long[numberOfTasks];
        List<PersistentTaskOperationFuture> futures = new ArrayList<>(numberOfTasks);

        boolean[] stopped = new boolean[numberOfTasks];
        int runningTasks = 0;
        for (int i = 0; i < numberOfTasks; i++) {
            stopped[i] = randomBoolean();
            if (stopped[i] == false) {
                runningTasks++;
            }
            PersistentTaskOperationFuture future = new PersistentTaskOperationFuture();
            futures.add(future);
            service.createPersistentActionTask(TestPersistentTasksExecutor.NAME, new TestRequest("Blah"), stopped[i], true, future);
        }

        for (int i = 0; i < numberOfTasks; i++) {
            taskIds[i] = futures.get(i).get();
        }

        final int numberOfRunningTasks = runningTasks;
        PersistentTasksCustomMetaData tasksInProgress = internalCluster().clusterService().state().getMetaData()
                .custom(PersistentTasksCustomMetaData.TYPE);
        assertThat(tasksInProgress.tasks().size(), equalTo(numberOfTasks));

        if (numberOfRunningTasks > 0) {
            // Make sure that at least one of the tasks is running
            assertBusy(() -> {
                // Wait for the task to start
                assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get()
                        .getTasks().size(), greaterThan(0));
            });
        }

        // Restart cluster
        internalCluster().fullRestart();
        ensureYellow();

        tasksInProgress = internalCluster().clusterService().state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        assertThat(tasksInProgress.tasks().size(), equalTo(numberOfTasks));
        // Check that cluster state is correct
        for (int i = 0; i < numberOfTasks; i++) {
            PersistentTask<?> task = tasksInProgress.getTask(taskIds[i]);
            assertNotNull(task);
            assertThat(task.isStopped(), equalTo(stopped[i]));
        }

        logger.info("Waiting for {} original tasks to start", numberOfRunningTasks);
        assertBusy(() -> {
            // Wait for the running task to start automatically
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get()
                            .getTasks().size(), equalTo(numberOfRunningTasks));
        });

        // Start all other tasks
        tasksInProgress = internalCluster().clusterService().state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        service = internalCluster().getInstance(PersistentTasksService.class);
        for (int i = 0; i < numberOfTasks; i++) {
            PersistentTask<?> task = tasksInProgress.getTask(taskIds[i]);
            assertNotNull(task);
            logger.info("checking task with id {} stopped {} node {}", task.getId(), task.isStopped(), task.getExecutorNode());
            assertThat(task.isStopped(), equalTo(stopped[i]));
            assertThat(task.getExecutorNode(), stopped[i] ? nullValue() : notNullValue());
            if (stopped[i]) {
                PersistentTaskOperationFuture startFuture = new PersistentTaskOperationFuture();
                service.startTask(task.getId(), startFuture);
                assertEquals(startFuture.get(), (Long) task.getId());
            }
        }

        logger.info("Waiting for {} tasks to start", numberOfTasks);
        assertBusy(() -> {
            // Wait for all tasks to start
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get()
                            .getTasks().size(), equalTo(numberOfTasks));
        });

        logger.info("Complete all tasks");
        // Complete the running task and make sure it finishes properly
        assertThat(new TestPersistentTasksPlugin.TestTasksRequestBuilder(client()).setOperation("finish").get().getTasks().size(),
                equalTo(numberOfTasks));

        assertBusy(() -> {
            // Make sure the task is removed from the cluster state
            assertThat(((PersistentTasksCustomMetaData) internalCluster().clusterService().state().getMetaData()
                    .custom(PersistentTasksCustomMetaData.TYPE)).tasks(), empty());
        });

    }
}
