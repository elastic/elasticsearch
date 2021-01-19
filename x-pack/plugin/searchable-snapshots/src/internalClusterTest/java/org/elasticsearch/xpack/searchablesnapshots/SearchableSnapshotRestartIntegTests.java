/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchableSnapshotRestartIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // ensure the cache is definitely used
            .put(CacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(1L, ByteSizeUnit.GB))
            .build();
    }

    @TestLogging(value = "org.elasticsearch.xpack.searchablesnapshots:DEBUG", reason = "investigate issue")
    public void testFullClusterRestart() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(3);
        final String index = "test-idx";
        final String index2 = "test-idx2";
        int numShards = 23;
        createIndex(index, indexSettingsNoReplicas(numShards).put(INDEX_SOFT_DELETES_SETTING.getKey(), true).build());
        createIndex(index2, indexSettingsNoReplicas(numShards).put(INDEX_SOFT_DELETES_SETTING.getKey(), true).build());
        indexRandomDocs(index, 1000);
        indexRandomDocs(index2, 1000);
        final String repoName = "test-repo";
        createRepository(repoName, "fs");
        final String snapshotName = "test-snapshot";
        createSnapshot(repoName, snapshotName, List.of(index, index2));
        assertAcked(client().admin().indices().prepareDelete(index, index2));
        final String restoredIndex = mountSnapshot(repoName, snapshotName, index, Settings.EMPTY);
        final String restoredIndex2 = mountSnapshot(repoName, snapshotName, index2, Settings.EMPTY);

        /*assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none").build()).get());*/

        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "primaries").build()).get());

        ensureGreen();

        assertBusy(() -> {
            RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(restoredIndex).get();
            Map<String, List<RecoveryState>> shardRecoveries = recoveryResponse.shardRecoveryStates();
            assertThat(shardRecoveries.containsKey(restoredIndex), equalTo(true));
            List<RecoveryState> recoveryStates = shardRecoveries.get(restoredIndex);
            assertThat(recoveryStates.size(), equalTo(numShards));
            for (RecoveryState recoveryState : recoveryStates) {
                assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));
            }
        });

        assertBusy(() -> {
            RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(restoredIndex2).get();
            Map<String, List<RecoveryState>> shardRecoveries = recoveryResponse.shardRecoveryStates();
            assertThat(shardRecoveries.containsKey(restoredIndex2), equalTo(true));
            List<RecoveryState> recoveryStates = shardRecoveries.get(restoredIndex2);
            assertThat(recoveryStates.size(), equalTo(numShards));
            for (RecoveryState recoveryState : recoveryStates) {
                assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));
            }
        });

        final ClusterState previousState = client().admin().cluster().prepareState().get().getState();

        logger.info("Previous state: {}", previousState.routingTable());

        for (CacheService cacheService : internalCluster().getDataNodeInstances(CacheService.class)) {
            cacheService.synchronizeCache();
            logger.info("After synchronizing cache");
            //assertThat(cacheService.getPersistentCache().getNumDocs(), greaterThan(0L));
        }
        internalCluster().fullRestart();
        ensureStableCluster(internalCluster().numDataAndMasterNodes());

        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder()
            .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "all").build()).get());

        ensureGreen();

        assertBusy(() -> {
            RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(restoredIndex).get();
            Map<String, List<RecoveryState>> shardRecoveries = recoveryResponse.shardRecoveryStates();
            assertThat(shardRecoveries.containsKey(restoredIndex), equalTo(true));
            List<RecoveryState> recoveryStates = shardRecoveries.get(restoredIndex);
            assertThat(recoveryStates.size(), equalTo(numShards));
            for (RecoveryState recoveryState : recoveryStates) {
                assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));
            }
        });

        final ClusterState state = client().admin().cluster().prepareState().get().getState();

        logger.info("Current state: {}", state.routingTable());

        for (int shardId = 0; shardId < numShards; shardId++) {
            assertEquals(previousState.routingTable().index(restoredIndex).shard(shardId).primaryShard().currentNodeId(),
                state.routingTable().index(restoredIndex).shard(shardId).primaryShard().currentNodeId()
            );
        }
    }
}
