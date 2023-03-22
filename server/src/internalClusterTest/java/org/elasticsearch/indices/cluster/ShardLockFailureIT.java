/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.cluster;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ShardLockFailureIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IndicesClusterStateService.SHARD_LOCK_RETRY_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(10))
            .build();
    }

    @TestLogging(reason = "checking DEBUG logs from ICSS", value = "org.elasticsearch.indices.cluster.IndicesClusterStateService:DEBUG")
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
            var ignored2 = mockLogAppender.capturing(IndicesClusterStateService.class);
        ) {
            final CountDownLatch countDownLatch = new CountDownLatch(1);

            mockLogAppender.addExpectation(new MockLogAppender.LoggingExpectation() {
                int debugMessagesSeen = 0;
                int warnMessagesSeen = 0;

                @Override
                public synchronized void match(LogEvent event) {
                    try {
                        assertEquals("org.elasticsearch.indices.cluster.IndicesClusterStateService", event.getLoggerName());
                        if (event.getMessage().getFormattedMessage().contains("shard lock currently unavailable")) {
                            if (event.getLevel() == Level.WARN) {
                                warnMessagesSeen += 1;
                                assertEquals(29L * warnMessagesSeen - 24, debugMessagesSeen);
                                if (warnMessagesSeen == 3) {
                                    countDownLatch.countDown();
                                }
                            } else if (event.getLevel() == Level.DEBUG) {
                                debugMessagesSeen += 1;
                            } else {
                                fail("unexpected log level: " + event.getLevel());
                            }
                        }
                    } catch (Throwable t) {
                        ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError("unexpected", t));
                    }
                }

                @Override
                public void assertMatched() {}
            });

            updateIndexSettings(Settings.builder().putNull(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name"), indexName);
            ensureYellow(indexName);
            assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
            assertEquals(ClusterHealthStatus.YELLOW, client().admin().cluster().prepareHealth(indexName).get().getStatus());
            mockLogAppender.assertAllExpectationsMatched();
        }

        ensureGreen(indexName);
    }
}
