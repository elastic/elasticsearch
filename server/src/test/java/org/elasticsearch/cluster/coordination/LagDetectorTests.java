/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.logging.ChunkedLoggingStreamTestUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.cluster.coordination.LagDetector.CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class LagDetectorTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;
    private Set<DiscoveryNode> failedNodes;
    private LagDetector lagDetector;
    private DiscoveryNode node1, node2, localNode;
    private TimeValue followerLagTimeout;

    @Before
    public void setupFixture() {
        deterministicTaskQueue = new DeterministicTaskQueue();

        failedNodes = new HashSet<>();

        Settings.Builder settingsBuilder = Settings.builder();
        if (randomBoolean()) {
            followerLagTimeout = TimeValue.timeValueMillis(randomLongBetween(2, 100000));
            settingsBuilder.put(CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING.getKey(), followerLagTimeout.millis() + "ms");
        } else {
            followerLagTimeout = CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING.get(Settings.EMPTY);
        }

        lagDetector = new LagDetector(
            settingsBuilder.build(),
            deterministicTaskQueue.getThreadPool(),
            (discoveryNode, appliedVersion, expectedVersion) -> failedNodes.add(discoveryNode),
            () -> localNode
        );

        localNode = CoordinationStateTests.createNode("local");
        node1 = CoordinationStateTests.createNode("node1");
        node2 = CoordinationStateTests.createNode("node2");
    }

    public void testLagDetectorNotStartedIfNodeHasAlreadyAppliedVersion() {
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.setAppliedVersion(node1, 1);
        lagDetector.startLagDetector(1);
        final long startTime = deterministicTaskQueue.getCurrentTimeMillis();
        deterministicTaskQueue.runAllTasks();
        assertThat("no lag detector started", deterministicTaskQueue.getCurrentTimeMillis(), is(startTime));
        assertThat(failedNodes, empty());
    }

    public void testNoLagDetectedIfNodeAppliesVersionAfterLagDetectorStarted() {
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.startLagDetector(1);
        lagDetector.setAppliedVersion(node1, 1);
        final long startTime = deterministicTaskQueue.getCurrentTimeMillis();
        deterministicTaskQueue.runAllTasks();
        assertThat("lag detector started", deterministicTaskQueue.getCurrentTimeMillis(), greaterThan(startTime));
        assertThat(failedNodes, empty());
    }

    public void testNoLagDetectedIfNodeAppliesVersionJustBeforeTimeout() {
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.startLagDetector(1);
        deterministicTaskQueue.scheduleAt(
            deterministicTaskQueue.getCurrentTimeMillis() + followerLagTimeout.millis() - 1,
            () -> lagDetector.setAppliedVersion(node1, 1)
        );
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertThat(failedNodes, empty());
    }

    public void testLagDetectedIfNodeAppliesVersionJustAfterTimeout() {
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.startLagDetector(1);
        deterministicTaskQueue.scheduleAt(
            deterministicTaskQueue.getCurrentTimeMillis() + followerLagTimeout.millis() + 1,
            () -> lagDetector.setAppliedVersion(node1, 1)
        );
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertThat(failedNodes, contains(node1));
    }

    public void testNoLagDetectedOnLocalNode() {
        lagDetector.setTrackedNodes(Collections.singletonList(localNode));
        lagDetector.startLagDetector(1);
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertThat(failedNodes, empty());
    }

    public void testNoLagDetectedIfNodeAppliesLaterVersionAfterLagDetectorStarted() {
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.startLagDetector(1);
        lagDetector.setAppliedVersion(node1, 2);
        deterministicTaskQueue.runAllTasks();
        assertThat(failedNodes, empty());
    }

    public void testLagDetectedIfNodeAppliesEarlierVersionAfterLagDetectorStarted() {
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.startLagDetector(2);
        lagDetector.setAppliedVersion(node1, 1);
        deterministicTaskQueue.runAllTasks();
        assertThat(failedNodes, contains(node1));
    }

    public void testNoLagDetectedIfNodeIsRemovedAfterLagDetectorStarted() {
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.startLagDetector(1);
        lagDetector.setTrackedNodes(Collections.singletonList(node2));
        deterministicTaskQueue.runAllTasks();
        assertThat(failedNodes, empty());

        lagDetector.startLagDetector(2);
        deterministicTaskQueue.runAllTasks();
        assertThat(failedNodes, contains(node2));
    }

    public void testNoLagDetectedIfDetectorIsClearedAfterLagDetectorStarted() {
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.startLagDetector(1);
        lagDetector.clearTrackedNodes();
        deterministicTaskQueue.runAllTasks();
        assertThat(failedNodes, empty());

        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.startLagDetector(2);
        deterministicTaskQueue.runAllTasks();
        assertThat(failedNodes, contains(node1));
    }

    public void testDetectorIgnoresNodesAddedAfterStarted() {
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.startLagDetector(1);
        lagDetector.setTrackedNodes(Arrays.asList(node1, node2));
        lagDetector.setAppliedVersion(node1, 1);
        deterministicTaskQueue.runAllTasks();
        assertThat(failedNodes, empty());
    }

    public void testDetectorKeepsTrackingExistingNodesEvenIfNewOnesAdded() {
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.startLagDetector(1);
        lagDetector.setTrackedNodes(Arrays.asList(node1, node2));
        deterministicTaskQueue.runAllTasks();
        assertThat(failedNodes, contains(node1));
    }

    public void testDetectorIgnoresApplicationsFromUnknownNodes() {
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.startLagDetector(1);
        lagDetector.setAppliedVersion(node2, 1);
        deterministicTaskQueue.runAllTasks();
        assertThat(failedNodes, contains(node1));

        failedNodes.clear();
        lagDetector.startLagDetector(2);
        deterministicTaskQueue.runAllTasks();
        assertThat(failedNodes, contains(node1));
    }

    public void testLagDetection() {
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.setAppliedVersion(node1, 1);
        lagDetector.startLagDetector(1);
        {
            final long startTime = deterministicTaskQueue.getCurrentTimeMillis();
            deterministicTaskQueue.runAllTasks();
            assertThat("no lag detector started", deterministicTaskQueue.getCurrentTimeMillis(), is(startTime));
        }
        assertThat(failedNodes, empty());

        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.startLagDetector(2);
        lagDetector.setAppliedVersion(node1, 2);
        {
            final long startTime = deterministicTaskQueue.getCurrentTimeMillis();
            deterministicTaskQueue.runAllTasks();
            assertThat("lag detector started", deterministicTaskQueue.getCurrentTimeMillis(), greaterThan(startTime));
        }
        assertThat(failedNodes, empty());

        lagDetector.startLagDetector(3);
        deterministicTaskQueue.scheduleAt(
            deterministicTaskQueue.getCurrentTimeMillis() + followerLagTimeout.millis() - 1,
            () -> lagDetector.setAppliedVersion(node1, 3)
        );
        assertThat(failedNodes, empty());

        lagDetector.startLagDetector(4);
        deterministicTaskQueue.scheduleAt(
            deterministicTaskQueue.getCurrentTimeMillis() + followerLagTimeout.millis() + 1,
            () -> lagDetector.setAppliedVersion(node1, 4)
        );
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertThat(failedNodes, contains(node1));
        failedNodes.clear();

        lagDetector.startLagDetector(5);
        lagDetector.clearTrackedNodes();
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertThat(failedNodes, empty()); // clearing tracked nodes cancels earlier lag detector ...

        lagDetector.startLagDetector(6);
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertThat(failedNodes, contains(node1));
        failedNodes.clear(); // ... but later lag detectors still work

        lagDetector.setTrackedNodes(Collections.singletonList(node2));
        lagDetector.setAppliedVersion(node2, 7);
        lagDetector.startLagDetector(7);
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertThat(failedNodes, empty()); // removing a node from the tracked set means it is not tracked

        lagDetector.startLagDetector(8);
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertThat(failedNodes, contains(node2));
        failedNodes.clear();

        lagDetector.startLagDetector(9);
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertThat(failedNodes, empty()); // nodes added after a lag detector was started are also ignored
    }

    // literal name because it appears in the docs so must not be changed without care
    private static final String LOGGER_NAME = "org.elasticsearch.cluster.coordination.LagDetector";

    @TestLogging(reason = "testing LagDetector logging", value = LOGGER_NAME + ":DEBUG")
    public void testHotThreadsChunkedLoggingEncoding() {
        final var node = DiscoveryNodeUtils.create("test");
        final var expectedBody = randomUnicodeOfLengthBetween(1, 20000);
        assertEquals(
            expectedBody,
            ChunkedLoggingStreamTestUtils.getDecodedLoggedBody(
                LogManager.getLogger(LOGGER_NAME),
                Level.DEBUG,
                "hot threads from node ["
                    + node.descriptionWithoutAttributes()
                    + "] lagging at version [1] despite commit of cluster state version [2]",
                ReferenceDocs.LAGGING_NODE_TROUBLESHOOTING,
                new LagDetector.HotThreadsLoggingTask(node, 1, 2, expectedBody, () -> {})::run
            ).utf8ToString()
        );
    }

}
