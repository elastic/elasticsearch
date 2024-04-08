/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.recovery;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class FullRollingRestartIT extends ESIntegTestCase {
    protected void assertTimeout(ClusterHealthRequestBuilder requestBuilder) {
        ClusterHealthResponse clusterHealth = requestBuilder.get();
        if (clusterHealth.isTimedOut()) {
            logger.info("cluster health request timed out:\n{}", clusterHealth);
            fail("cluster health request timed out");
        }
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    public void testFullRollingRestart() throws Exception {
        internalCluster().startNode();
        createIndex("test");

        final String healthTimeout = "1m";

        for (int i = 0; i < 1000; i++) {
            prepareIndex("test").setId(Long.toString(i)).setSource(Map.<String, Object>of("test", "value" + i)).get();
        }
        flush();
        for (int i = 1000; i < 2000; i++) {
            prepareIndex("test").setId(Long.toString(i)).setSource(Map.<String, Object>of("test", "value" + i)).get();
        }

        logger.info("--> now start adding nodes");
        internalCluster().startNode();
        internalCluster().startNode();

        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("3")
        );

        logger.info("--> add two more nodes");
        internalCluster().startNode();
        internalCluster().startNode();

        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("5")
        );

        logger.info("--> refreshing and checking data");
        refresh();
        for (int i = 0; i < 10; i++) {
            assertHitCount(prepareSearch().setSize(0).setQuery(matchAllQuery()), 2000L);
        }

        // now start shutting nodes down
        internalCluster().stopRandomDataNode();
        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("4")
        );

        internalCluster().stopRandomDataNode();
        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("3")
        );

        logger.info("--> stopped two nodes, verifying data");
        refresh();
        for (int i = 0; i < 10; i++) {
            assertHitCount(prepareSearch().setSize(0).setQuery(matchAllQuery()), 2000L);
        }

        // closing the 3rd node
        internalCluster().stopRandomDataNode();
        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("2")
        );

        internalCluster().stopRandomDataNode();

        // make sure the cluster state is yellow, and all has been recovered
        assertTimeout(
            clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForYellowStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("1")
        );

        logger.info("--> one node left, verifying data");
        refresh();
        for (int i = 0; i < 10; i++) {
            assertHitCount(prepareSearch().setSize(0).setQuery(matchAllQuery()), 2000L);
        }
    }

    public void testNoRebalanceOnRollingRestart() throws Exception {
        // see https://github.com/elastic/elasticsearch/issues/14387
        internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNodes(3);
        /**
         * We start 3 nodes and a dedicated master. Restart one of the data-nodes and ensure that we got no relocations.
         * Yet we have 6 shards 0 replica so that means if the restarting node comes back both other nodes are subject
         * to relocating to the restarting node since all had 2 shards and now one node has nothing allocated.
         * We have a fix for this to wait until we have allocated unallocated shards now so this shouldn't happen.
         */
        prepareCreate("test").setSettings(
            indexSettings(6, 0).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMinutes(1))
        ).get();

        for (int i = 0; i < 100; i++) {
            prepareIndex("test").setId(Long.toString(i)).setSource(Map.<String, Object>of("test", "value" + i)).get();
        }
        ensureGreen();
        ClusterState state = clusterAdmin().prepareState().get().getState();
        RecoveryResponse recoveryResponse = indicesAdmin().prepareRecoveries("test").get();
        for (RecoveryState recoveryState : recoveryResponse.shardRecoveryStates().get("test")) {
            assertNotEquals(
                "relocated "
                    + recoveryState.getShardId()
                    + " from: "
                    + recoveryState.getSourceNode()
                    + " to: "
                    + recoveryState.getTargetNode()
                    + "\n"
                    + state,
                recoveryState.getRecoverySource().getType(),
                RecoverySource.Type.PEER
            );
        }
        internalCluster().restartRandomDataNode();
        ensureGreen();
        clusterAdmin().prepareState().get();

        recoveryResponse = indicesAdmin().prepareRecoveries("test").get();
        for (RecoveryState recoveryState : recoveryResponse.shardRecoveryStates().get("test")) {
            assertNotEquals(
                "relocated "
                    + recoveryState.getShardId()
                    + " from: "
                    + recoveryState.getSourceNode()
                    + " to: "
                    + recoveryState.getTargetNode()
                    + "-- \nbefore: \n"
                    + state,
                recoveryState.getRecoverySource().getType(),
                RecoverySource.Type.PEER
            );
        }
    }
}
