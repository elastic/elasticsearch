/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchableSnapshotsRelocationIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateSearchableSnapshots.class, MockRepository.Plugin.class);
    }

    public void testRelocationWaitsForPreWarm() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String firstDataNode = internalCluster().startDataOnlyNode();
        final String index = "test-idx";
        createIndexWithContent(index, indexSettingsNoReplicas(1).build());
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String snapshotName = "test-snapshot";
        createSnapshot(repoName, snapshotName, List.of(index));
        assertAcked(client().admin().indices().prepareDelete(index));
        final String restoredIndex = mountSnapshot(repoName, snapshotName, index, Settings.EMPTY);
        ensureGreen(restoredIndex);
        final String secondDataNode = internalCluster().startDataOnlyNode();
        blockDataNode(repoName, secondDataNode);
        logger.info("--> force index [{}] to relocate to [{}]", index, secondDataNode);
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(restoredIndex)
                .setSettings(
                    Settings.builder()
                        .put(
                            IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(),
                            secondDataNode
                        )
                )
        );
        waitForBlock(secondDataNode, repoName);
        final List<RecoveryState> recoveryStates = getActiveRestores(restoredIndex);
        assertThat(recoveryStates, Matchers.hasSize(1));
        final RecoveryState shardRecoveryState = recoveryStates.get(0);
        assertEquals(firstDataNode, shardRecoveryState.getSourceNode().getName());
        assertEquals(secondDataNode, shardRecoveryState.getTargetNode().getName());
        assertBusy(() -> assertSame(getActiveRestores(restoredIndex).get(0).getStage(), RecoveryState.Stage.INDEX), 30L, TimeUnit.SECONDS);
        logger.info("--> sleep for 5s to make sure we are actually stuck at the INDEX stage");
        TimeUnit.SECONDS.sleep(5L);
        final RecoveryState recoveryState = getActiveRestores(restoredIndex).get(0);
        assertSame(recoveryState.getStage(), RecoveryState.Stage.INDEX);
        unblockAllDataNodes(repoName);
        assertFalse(
            client().admin()
                .cluster()
                .prepareHealth(restoredIndex)
                .setWaitForNoRelocatingShards(true)
                .setWaitForEvents(Priority.LANGUID)
                .get()
                .isTimedOut()
        );
        assertThat(getActiveRestores(restoredIndex), Matchers.empty());
    }

    private static List<RecoveryState> getActiveRestores(String restoredIndex) {
        return client().admin()
            .indices()
            .prepareRecoveries(restoredIndex)
            .setDetailed(true)
            .setActiveOnly(true)
            .get()
            .shardRecoveryStates()
            .get(restoredIndex);
    }
}
