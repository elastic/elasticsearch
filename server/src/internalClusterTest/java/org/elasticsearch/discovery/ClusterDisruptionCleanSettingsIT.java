/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockIndexEventListener;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.elasticsearch.core.Nullable;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterDisruptionCleanSettingsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockIndexEventListener.TestPlugin.class);
    }

    /**
     * This test creates a scenario where a primary shard (0 replicas) relocates and is in POST_RECOVERY on the target
     * node but already deleted on the source node. Search request should still work.
     */
    public void testSearchWithRelocationAndSlowClusterStateProcessing() throws Exception {
        // Don't use AbstractDisruptionTestCase.DEFAULT_SETTINGS as settings
        // (which can cause node disconnects on a slow CI machine)
        internalCluster().startMasterOnlyNode();
        final String node_1 = internalCluster().startDataOnlyNode();

        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
        );
        ensureGreen("test");

        final String node_2 = internalCluster().startDataOnlyNode();
        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            indexRequestBuilderList.add(prepareIndex("test").setSource("{\"int_field\":1}", XContentType.JSON));
        }
        indexRandom(true, indexRequestBuilderList);

        // Block cluster state processing on target node so shard stays in POST_RECOVERY
        BlockClusterStateProcessing disruption = new BlockClusterStateProcessing(node_2, random());
        internalCluster().setDisruptionScheme(disruption);

        // Detect when the shard reaches POST_RECOVERY on the target node
        CountDownLatch postRecoveryLatch = new CountDownLatch(1);
        for (MockIndexEventListener.TestEventListener eventListener : internalCluster().getInstances(
            MockIndexEventListener.TestEventListener.class
        )) {
            eventListener.setNewDelegate(new IndexEventListener() {
                @Override
                public void indexShardStateChanged(
                    IndexShard indexShard,
                    @Nullable IndexShardState previousState,
                    IndexShardState currentState,
                    @Nullable String reason
                ) {
                    if (currentState == IndexShardState.POST_RECOVERY) {
                        postRecoveryLatch.countDown();
                    }
                }
            });
        }

        ClusterRerouteUtils.reroute(internalCluster().client(), new MoveAllocationCommand("test", 0, node_1, node_2));
        postRecoveryLatch.await();
        disruption.startDisrupting();

        // now search for the documents and see if we get a reply
        assertHitCount(prepareSearch().setSize(0), 100);
        disruption.stopDisrupting();
    }
}
