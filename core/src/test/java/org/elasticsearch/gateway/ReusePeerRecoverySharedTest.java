/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.indices.recovery.RecoveryState;

import static org.elasticsearch.test.ESIntegTestCase.client;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Test of file reuse on recovery shared between integration tests and backwards
 * compatibility tests.
 */
public class ReusePeerRecoverySharedTest {
    /**
     * Test peer reuse on recovery. This is shared between RecoverFromGatewayIT
     * and RecoveryBackwardsCompatibilityIT.
     *
     * @param indexSettings
     *            settings for the index to test
     * @param restartCluster
     *            runnable that will restart the cluster under test
     * @param logger
     *            logger for logging
     * @param useSyncIds
     *            should this use synced flush? can't use synced from in the bwc
     *            tests
     */
    public static void testCase(Settings indexSettings, Runnable restartCluster, Logger logger, boolean useSyncIds) {
        /*
         * prevent any rebalance actions during the peer recovery if we run into
         * a relocation the reuse count will be 0 and this fails the test. We
         * are testing here if we reuse the files on disk after full restarts
         * for replicas.
         */
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put(indexSettings)
                .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)));
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("30s").get();
        logger.info("--> indexing docs");
        for (int i = 0; i < 1000; i++) {
            client().prepareIndex("test", "type").setSource("field", "value").execute().actionGet();
            if ((i % 200) == 0) {
                client().admin().indices().prepareFlush().execute().actionGet();
            }
        }
        if (randomBoolean()) {
            client().admin().indices().prepareFlush().execute().actionGet();
        }
        logger.info("--> running cluster health");
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("30s").get();
        // just wait for merges
        client().admin().indices().prepareForceMerge("test").setMaxNumSegments(100).get();
        client().admin().indices().prepareFlush().setForce(true).get();

        if (useSyncIds == false) {
            logger.info("--> disabling allocation while the cluster is shut down");

            // Disable allocations while we are closing nodes
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                    .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.NONE)).get();
            logger.info("--> full cluster restart");
            restartCluster.run();

            logger.info("--> waiting for cluster to return to green after first shutdown");
            client().admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("30s").get();
        } else {
            logger.info("--> trying to sync flush");
            assertEquals(client().admin().indices().prepareSyncedFlush("test").get().failedShards(), 0);
            assertSyncIdsNotNull();
        }

        logger.info("--> disabling allocation while the cluster is shut down{}", useSyncIds ? "" : " a second time");
        // Disable allocations while we are closing nodes
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.NONE))
                .get();
        logger.info("--> full cluster restart");
        restartCluster.run();

        logger.info("--> waiting for cluster to return to green after {}shutdown", useSyncIds ? "" : "second ");
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("30s").get();

        if (useSyncIds) {
            assertSyncIdsNotNull();
        }
        RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries("test").get();
        for (RecoveryState recoveryState : recoveryResponse.shardRecoveryStates().get("test")) {
            long recovered = 0;
            for (RecoveryState.File file : recoveryState.getIndex().fileDetails()) {
                if (file.name().startsWith("segments")) {
                    recovered += file.length();
                }
            }
            if (!recoveryState.getPrimary() && (useSyncIds == false)) {
                logger.info("--> replica shard {} recovered from {} to {}, recovered {}, reuse {}", recoveryState.getShardId().getId(),
                        recoveryState.getSourceNode().getName(), recoveryState.getTargetNode().getName(),
                        recoveryState.getIndex().recoveredBytes(), recoveryState.getIndex().reusedBytes());
                assertThat("no bytes should be recovered", recoveryState.getIndex().recoveredBytes(), equalTo(recovered));
                assertThat("data should have been reused", recoveryState.getIndex().reusedBytes(), greaterThan(0L));
                // we have to recover the segments file since we commit the translog ID on engine startup
                assertThat("all bytes should be reused except of the segments file", recoveryState.getIndex().reusedBytes(),
                        equalTo(recoveryState.getIndex().totalBytes() - recovered));
                assertThat("no files should be recovered except of the segments file", recoveryState.getIndex().recoveredFileCount(),
                        equalTo(1));
                assertThat("all files should be reused except of the segments file", recoveryState.getIndex().reusedFileCount(),
                        equalTo(recoveryState.getIndex().totalFileCount() - 1));
                assertThat("> 0 files should be reused", recoveryState.getIndex().reusedFileCount(), greaterThan(0));
            } else {
                if (useSyncIds && !recoveryState.getPrimary()) {
                    logger.info("--> replica shard {} recovered from {} to {} using sync id, recovered {}, reuse {}",
                            recoveryState.getShardId().getId(), recoveryState.getSourceNode().getName(), recoveryState.getTargetNode().getName(),
                            recoveryState.getIndex().recoveredBytes(), recoveryState.getIndex().reusedBytes());
                }
                assertThat(recoveryState.getIndex().recoveredBytes(), equalTo(0L));
                assertThat(recoveryState.getIndex().reusedBytes(), equalTo(recoveryState.getIndex().totalBytes()));
                assertThat(recoveryState.getIndex().recoveredFileCount(), equalTo(0));
                assertThat(recoveryState.getIndex().reusedFileCount(), equalTo(recoveryState.getIndex().totalFileCount()));
            }
        }
    }

    public static void assertSyncIdsNotNull() {
        IndexStats indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNotNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }
    }
}
