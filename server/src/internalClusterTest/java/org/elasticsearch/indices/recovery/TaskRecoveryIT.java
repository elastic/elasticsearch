/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TaskRecoveryIT extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TaskRecoveryIT.EngineTestPlugin.class);
    }

    /**
     * Checks that the parent / child task hierarchy is correct for tasks that are initiated by a recovery task.
     * We use an engine plugin that stalls translog recovery, which gives us the opportunity to inspect the
     * task hierarchy.
     */
    public void testTaskForOngoingRecovery() throws Exception {
        String indexName = "test";
        internalCluster().startMasterOnlyNode();
        String nodeWithPrimary = internalCluster().startDataOnlyNode();
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.include._name", nodeWithPrimary))
        );
        try {
            String nodeWithReplica = internalCluster().startDataOnlyNode();

            // Create an index so that there is something to recover
            updateIndexSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put("index.routing.allocation.include._name", nodeWithPrimary + "," + nodeWithReplica),
                indexName
            );
            // Translog recovery is stalled, so we can inspect the running tasks.
            assertBusy(() -> {
                List<TaskInfo> primaryTasks = clusterAdmin().prepareListTasks(nodeWithPrimary)
                    .setActions(PeerRecoverySourceService.Actions.START_RECOVERY)
                    .get()
                    .getTasks();
                assertThat("Expected a single primary task", primaryTasks.size(), equalTo(1));
                List<TaskInfo> replicaTasks = clusterAdmin().prepareListTasks(nodeWithReplica)
                    .setActions(PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG)
                    .get()
                    .getTasks();
                assertThat("Expected a single replica task", replicaTasks.size(), equalTo(1));
                assertThat(
                    "Replica task's parent task ID was incorrect",
                    replicaTasks.get(0).parentTaskId(),
                    equalTo(primaryTasks.get(0).taskId())
                );
            });
        } finally {
            // Release the EngineTestPlugin, which will allow translog recovery to complete
            StreamSupport.stream(internalCluster().getInstances(PluginsService.class).spliterator(), false)
                .flatMap(ps -> ps.filterPlugins(EnginePlugin.class).stream())
                .map(EngineTestPlugin.class::cast)
                .forEach(EngineTestPlugin::release);
        }
        ensureGreen(indexName);
    }

    /**
     * An engine plugin that defers translog recovery until the engine is released via {@link #release()}.
     */
    public static class EngineTestPlugin extends Plugin implements EnginePlugin {
        private final CountDownLatch latch = new CountDownLatch(1);

        public void release() {
            latch.countDown();
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> new InternalEngine(config) {

                @Override
                public void skipTranslogRecovery() {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    super.skipTranslogRecovery();
                }
            });
        }
    }
}
