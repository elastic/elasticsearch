/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshot;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.SnapshotShutdownProgressTracker;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.junit.Before;

import static org.elasticsearch.snapshots.SnapshotShutdownProgressTracker.SNAPSHOT_PROGRESS_DURING_SHUTDOWN_LOG_INTERVAL_SETTING;
import static org.elasticsearch.test.NodeShutdownTestUtils.clearShutdownMetadata;
import static org.elasticsearch.test.NodeShutdownTestUtils.putShutdownForRemovalMetadata;

public class StatelessSnapshotShutdownIT extends AbstractStatelessPluginIntegTestCase {

    private MockLog mockLog;

    @Before
    public void init() {
        startMasterOnlyNode();
    }

    public void setUp() throws Exception {
        super.setUp();
        mockLog = MockLog.capture(SnapshotShutdownProgressTracker.class);
    }

    public void tearDown() throws Exception {
        mockLog.close();
        super.tearDown();
    }

    /**
     * Tests that on stateless search nodes we do not log snapshot shutdown progress
     */
    public void testStatelessSearchNodesDoNotLogSnapshotShuttingDownProgress() throws InterruptedException {
        String searchNodeName = startSearchNode(
            Settings.builder()
                // Speed up the logging frequency, so that the test doesn't have to wait too long to check for log messages.
                .put(SNAPSHOT_PROGRESS_DURING_SHUTDOWN_LOG_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(200))
                .build()
        );

        MockLog.PatternNotSeenEventExpectation snapshotShutdownProgressTrackerToNotRunExpectation =
            new MockLog.PatternNotSeenEventExpectation(
                "Expect SnapshotShutdownProgressTracker to not run for search node",
                SnapshotShutdownProgressTracker.class.getCanonicalName(),
                Level.INFO,
                "Shard snapshot completion stats since shutdown began*"
            );
        mockLog.addExpectation(snapshotShutdownProgressTrackerToNotRunExpectation);
        snapshotShutdownProgressTrackerToNotRunExpectation.awaitMatched(1000);

        // Put shutdown metadata to trigger shutdown progress tracker
        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        putShutdownForRemovalMetadata(searchNodeName, clusterService);

        // Wait for log expectation to be matched
        mockLog.assertAllExpectationsMatched();
        clearShutdownMetadata(clusterService);
    }

    /**
     * Tests that on stateless index nodes we log snapshot shutdown progress
     */
    public void testStatelessIndexNodesDoLogSnapshotShuttingDownProgress() {
        String indexNodeName = startIndexNode(
            Settings.builder()
                // Speed up the logging frequency, so that the test doesn't have to wait too long to check for log messages.
                .put(SNAPSHOT_PROGRESS_DURING_SHUTDOWN_LOG_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(200))
                .build()
        );

        mockLog.addExpectation(
            new MockLog.SeenEventExpectation(
                "Expect SnapshotShutdownProgressTracker to run for index node",
                SnapshotShutdownProgressTracker.class.getCanonicalName(),
                Level.INFO,
                "*Shard snapshot completion stats since shutdown began*"
            )
        );

        // Put shutdown metadata to trigger shutdown progress tracker
        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        putShutdownForRemovalMetadata(indexNodeName, clusterService);

        // Wait for log expectation to be matched
        mockLog.awaitAllExpectationsMatched();
        clearShutdownMetadata(clusterService);
    }
}
