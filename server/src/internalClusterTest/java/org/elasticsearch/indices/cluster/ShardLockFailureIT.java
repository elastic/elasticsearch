/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.cluster;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLogAppender;

public class ShardLockFailureIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IndicesClusterStateService.SHARD_LOCK_RETRY_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(10))
            .build();
    }

    public void testShardLockFailure() throws Exception {
        final var node = internalCluster().startDataOnlyNode();

        final var indexName = "testindex";
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", node)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
                .build()
        );
        ensureGreen(indexName);

        final var shardId = client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setRoutingTable(true)
            .get()
            .getState()
            .routingTable()
            .shardRoutingTable(indexName, 0)
            .shardId();

        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> {
            try {
                assertTrue(
                    event.state()
                        .routingTable()
                        .shardRoutingTable(shardId)
                        .allShards()
                        .noneMatch(sr -> sr.unassigned() && sr.unassignedInfo().getNumFailedAllocations() > 0)
                );
            } catch (IndexNotFoundException e) {
                // ok
            }
        });

        var mockLogAppender = new MockLogAppender();
        try (
            var ignored1 = internalCluster().getInstance(NodeEnvironment.class, node).shardLock(shardId, "blocked for test");
            var ignored2 = mockLogAppender.capturing(IndicesClusterStateService.class)
        ) {
            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "warning",
                    "org.elasticsearch.indices.cluster.IndicesClusterStateService",
                    Level.WARN,
                    "shard lock currently unavailable"
                )
            );

            updateIndexSettings(Settings.builder().putNull(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name"), indexName);
            ensureYellow(indexName);
            assertBusy(mockLogAppender::assertAllExpectationsMatched);
            assertEquals(ClusterHealthStatus.YELLOW, client().admin().cluster().prepareHealth(indexName).get().getStatus());
        }

        ensureGreen(indexName);
    }
}
