/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.migration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusAction;
import org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusRequest;
import org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse;
import org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeAction;
import org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.InternalTestCluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.upgrades.SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME;
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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestPlugin.class);
        plugins.add(ReindexPlugin.class);
        return plugins;
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
                try {
                    logger.info("Task created");
                    taskCreated.await(10, TimeUnit.SECONDS); // now we can test internalCluster().restartNode

                    shutdownCompleted.await(10, TimeUnit.SECONDS); // waiting until the node has stopped
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new AssertionError(e);
                }
            }

        };
        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        clusterService.addListener(clusterStateListener);

        // create task by calling API
        final PostFeatureUpgradeRequest req = new PostFeatureUpgradeRequest();
        client().execute(PostFeatureUpgradeAction.INSTANCE, req);
        logger.info("migrate feature api called");

        taskCreated.await(10, TimeUnit.SECONDS); // waiting when the task is created

        internalCluster().restartNode(masterAndDataNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                shutdownCompleted.await(10, TimeUnit.SECONDS);// now we can release the master thread
                return super.onNodeStopped(nodeName);
            }
        });

        assertBusy(() -> {
            // Wait for the node we restarted to completely rejoin the cluster
            ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            assertThat("expected restarted node to rejoin cluster", clusterState.getNodes().getSize(), equalTo(2));

            GetFeatureUpgradeStatusResponse statusResponse = client().execute(
                GetFeatureUpgradeStatusAction.INSTANCE,
                new GetFeatureUpgradeStatusRequest()
            ).get();
            assertThat(
                "expected migration to fail due to restarting only data node",
                statusResponse.getUpgradeStatus(),
                equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.ERROR)
            );
        });
    }
}
