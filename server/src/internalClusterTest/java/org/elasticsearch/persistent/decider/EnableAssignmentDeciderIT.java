/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent.decider;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.persistent.TestPersistentTasksPlugin;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestParams;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static java.util.Collections.singletonList;
import static org.elasticsearch.persistent.decider.EnableAssignmentDecider.Allocation;
import static org.elasticsearch.persistent.decider.EnableAssignmentDecider.CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 1)
public class EnableAssignmentDeciderIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return singletonList(TestPersistentTasksPlugin.class);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    /**
     * Test that the {@link EnableAssignmentDecider#CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING} setting correctly
     * prevents persistent tasks to be assigned after a cluster restart.
     */
    public void testEnableAssignmentAfterRestart() throws Exception {
        final int numberOfTasks = randomIntBetween(1, 10);
        logger.trace("creating {} persistent tasks", numberOfTasks);

        final CountDownLatch latch = new CountDownLatch(numberOfTasks);
        for (int i = 0; i < numberOfTasks; i++) {
            PersistentTasksService service = internalCluster().getInstance(PersistentTasksService.class);
            service.sendStartRequest("task_" + i, TestPersistentTasksExecutor.NAME, new TestParams(randomAlphaOfLength(10)),
                ActionListener.wrap(latch::countDown));
        }
        latch.await();

        ClusterService clusterService = internalCluster().clusterService(internalCluster().getMasterName());
        PersistentTasksCustomMetadata tasks = clusterService.state().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        assertEquals(numberOfTasks, tasks.tasks().stream().filter(t -> TestPersistentTasksExecutor.NAME.equals(t.getTaskName())).count());

        logger.trace("waiting for the tasks to be running");
        assertBusy(() -> {
            ListTasksResponse listTasks = client().admin().cluster().prepareListTasks()
                                                                    .setActions(TestPersistentTasksExecutor.NAME + "[c]")
                                                                    .get();
            assertThat(listTasks.getTasks().size(), equalTo(numberOfTasks));
        });

        try {
            logger.trace("disable persistent tasks assignment");
            disablePersistentTasksAssignment();

            logger.trace("restart the cluster");
            internalCluster().fullRestart();
            ensureYellow();

            logger.trace("persistent tasks assignment is still disabled");
            assertEnableAssignmentSetting(Allocation.NONE);

            logger.trace("persistent tasks are not assigned");
            tasks = internalCluster().clusterService().state().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
            assertEquals(numberOfTasks, tasks.tasks().stream()
                .filter(t -> TestPersistentTasksExecutor.NAME.equals(t.getTaskName()))
                .filter(t -> t.isAssigned() == false)
                .count());

            ListTasksResponse runningTasks = client().admin().cluster().prepareListTasks()
                .setActions(TestPersistentTasksExecutor.NAME + "[c]")
                .get();
            assertThat(runningTasks.getTasks().size(), equalTo(0));

            logger.trace("enable persistent tasks assignment");
            if (randomBoolean()) {
                enablePersistentTasksAssignment();
            } else {
                resetPersistentTasksAssignment();
            }

            assertBusy(() -> {
                ListTasksResponse listTasks = client().admin().cluster().prepareListTasks()
                    .setActions(TestPersistentTasksExecutor.NAME + "[c]")
                    .get();
                assertThat(listTasks.getTasks().size(), equalTo(numberOfTasks));
            });

        } finally {
            resetPersistentTasksAssignment();
        }
    }

    private void assertEnableAssignmentSetting(final Allocation expected) {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().setMetadata(true).get();
        Settings settings = clusterStateResponse.getState().getMetadata().settings();

        String value = settings.get(CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey());
        assertThat(Allocation.fromString(value), equalTo(expected));
    }

    private void disablePersistentTasksAssignment() {
        Settings.Builder settings = Settings.builder().put(CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.NONE);
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
    }

    private void enablePersistentTasksAssignment() {
        Settings.Builder settings = Settings.builder().put(CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.ALL);
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
    }

    private void resetPersistentTasksAssignment() {
        Settings.Builder settings = Settings.builder().putNull(CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey());
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
    }

}
