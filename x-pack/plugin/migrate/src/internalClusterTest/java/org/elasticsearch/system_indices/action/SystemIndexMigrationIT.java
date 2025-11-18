/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.InternalTestCluster;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.system_indices.task.SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME;
import static org.hamcrest.Matchers.equalTo;

/**
 * This class is for testing that when restarting a node, SystemIndexMigrationTaskState can be read.
 */
public class SystemIndexMigrationIT extends AbstractFeatureMigrationIntegTest {
    private static Logger logger = LogManager.getLogger(SystemIndexMigrationIT.class);

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).build();
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        // We need to be able to set the index creation version manually.
        return false;
    }

    public void testSystemIndexMigrationCanBeInterruptedWithShutdown() throws Exception {
        CyclicBarrier taskCreated = new CyclicBarrier(2);
        CyclicBarrier shutdownCompleted = new CyclicBarrier(2);
        AtomicBoolean hasBlocked = new AtomicBoolean();

        createSystemIndexForDescriptor(INTERNAL_MANAGED);

        final ClusterStateListener clusterStateListener = event -> {
            PersistentTasksCustomMetadata.PersistentTask<?> task = PersistentTasksCustomMetadata.getTaskWithId(
                event.state(),
                SYSTEM_INDEX_UPGRADE_TASK_NAME
            );

            if (task != null
                && task.getState() != null // Make sure the task is really started
                && hasBlocked.compareAndSet(false, true)) {

                logger.info("Task created");
                safeAwait(taskCreated); // now we can test internalCluster().restartNode
                safeAwait(shutdownCompleted); // waiting until the node has stopped
            }

        };
        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        clusterService.addListener(clusterStateListener);

        // create task by calling API
        final PostFeatureUpgradeRequest req = new PostFeatureUpgradeRequest(TEST_REQUEST_TIMEOUT);
        client().execute(PostFeatureUpgradeAction.INSTANCE, req);
        logger.info("migrate feature api called");

        safeAwait(taskCreated); // waiting when the task is created

        internalCluster().restartNode(masterAndDataNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                safeAwait(shutdownCompleted); // now we can release the master thread
                return super.onNodeStopped(nodeName);
            }
        });

        assertBusy(() -> {
            // Wait for the node we restarted to completely rejoin the cluster
            ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
            assertThat("expected restarted node to rejoin cluster", clusterState.getNodes().size(), equalTo(2));

            GetFeatureUpgradeStatusResponse statusResponse = client().execute(
                GetFeatureUpgradeStatusAction.INSTANCE,
                new GetFeatureUpgradeStatusRequest(TEST_REQUEST_TIMEOUT)
            ).get();
            assertThat(
                "expected migration to fail due to restarting only data node",
                statusResponse.getUpgradeStatus(),
                equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.ERROR)
            );
        });
    }
}
