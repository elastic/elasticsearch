/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ShardLockFailureIT extends ESIntegTestCase {

    @TestLogging(reason = "checking DEBUG logs from ICSS", value = "org.elasticsearch.indices.cluster.IndicesClusterStateService:DEBUG")
    public void testShardLockFailure() throws Exception {
        final var node = internalCluster().startDataOnlyNode(
            Settings.builder()
                .put(IndicesClusterStateService.SHARD_LOCK_RETRY_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(10))
                .put(IndicesClusterStateService.SHARD_LOCK_RETRY_TIMEOUT_SETTING.getKey(), TimeValue.timeValueDays(10))
                .build()
        );

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

        final var shardId = new ShardId(resolveIndex(indexName), 0);

        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> {
            try {
                assertTrue(
                    event.state()
                        .routingTable()
                        .shardRoutingTable(shardId)
                        .allShards()
                        .noneMatch(sr -> sr.unassigned() && sr.unassignedInfo().failedAllocations() > 0)
                );
            } catch (IndexNotFoundException e) {
                // ok
            }
        });

        try (
            var ignored1 = internalCluster().getInstance(NodeEnvironment.class, node).shardLock(shardId, "blocked for test");
            var mockLog = MockLog.capture(IndicesClusterStateService.class);
        ) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "hot threads",
                    "org.elasticsearch.indices.cluster.IndicesClusterStateService",
                    Level.WARN,
                    "[testindex][0]: acquire shard lock for create"
                )
            );
            mockLog.addExpectation(new MockLog.LoggingExpectation() {
                private final CountDownLatch countDownLatch = new CountDownLatch(1);
                int debugMessagesSeen = 0;
                int warnMessagesSeen = 0;

                @Override
                public synchronized void match(LogEvent event) {
                    try {
                        assertEquals("org.elasticsearch.indices.cluster.IndicesClusterStateService", event.getLoggerName());
                        if (event.getMessage().getFormattedMessage().matches("shard lock for .* has been unavailable for at least .*")) {
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
                public void assertMatched() {
                    fail("unused");
                }

                @Override
                public void awaitMatched(long millis) throws InterruptedException {
                    assertTrue(countDownLatch.await(millis, TimeUnit.MILLISECONDS));
                }
            });

            updateIndexSettings(Settings.builder().putNull(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name"), indexName);
            ensureYellow(indexName);
            mockLog.awaitAllExpectationsMatched();
            assertEquals(ClusterHealthStatus.YELLOW, clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).get().getStatus());
        }

        ensureGreen(indexName);
    }

    @TestLogging(reason = "checking WARN logs from ICSS", value = "org.elasticsearch.indices.cluster.IndicesClusterStateService:WARN")
    public void testShardLockTimeout() throws Exception {
        final var node = internalCluster().startDataOnlyNode(
            Settings.builder()
                .put(IndicesClusterStateService.SHARD_LOCK_RETRY_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(10))
                .put(IndicesClusterStateService.SHARD_LOCK_RETRY_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(100))
                .build()
        );

        final var indexName = "testindex";
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", node)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
                .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 1)
                .build()
        );
        ensureGreen(indexName);

        final var shardId = new ShardId(resolveIndex(indexName), 0);

        try (
            var ignored1 = internalCluster().getInstance(NodeEnvironment.class, node).shardLock(shardId, "blocked for test");
            var mockLog = MockLog.capture(IndicesClusterStateService.class);
        ) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "hot threads",
                    "org.elasticsearch.indices.cluster.IndicesClusterStateService",
                    Level.WARN,
                    "[testindex][0]: acquire shard lock for create"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "timeout message",
                    "org.elasticsearch.indices.cluster.IndicesClusterStateService",
                    Level.WARN,
                    """
                        timed out after [indices.store.shard_lock_retry.timeout=100ms/100ms] \
                        while waiting to acquire shard lock for [testindex][0]"""
                )
            );

            updateIndexSettings(Settings.builder().putNull(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name"), indexName);
            mockLog.awaitAllExpectationsMatched();
            final var clusterHealthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName)
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(TimeValue.timeValueSeconds(10))
                .setWaitForNoInitializingShards(true)
                .setWaitForNoRelocatingShards(true)
                .get();
            assertFalse(clusterHealthResponse.isTimedOut());
            assertEquals(ClusterHealthStatus.YELLOW, clusterHealthResponse.getStatus());
            assertEquals(1, clusterHealthResponse.getUnassignedShards());
        }

        ClusterRerouteUtils.rerouteRetryFailed(client());
        ensureGreen(indexName);
    }
}
