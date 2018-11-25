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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.cluster.coordination.LagDetector.CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
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
        deterministicTaskQueue = new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build(), random());

        failedNodes = new HashSet<>();

        Settings.Builder settingsBuilder = Settings.builder();
        if (randomBoolean()) {
            followerLagTimeout = TimeValue.timeValueMillis(randomLongBetween(2, 100000));
            settingsBuilder.put(CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING.getKey(), followerLagTimeout.millis() + "ms");
        } else {
            followerLagTimeout = CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING.get(Settings.EMPTY);
        }

        lagDetector = new LagDetector(settingsBuilder.build(), deterministicTaskQueue.getThreadPool(), failedNodes::add, () -> localNode);

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
        deterministicTaskQueue.scheduleAt(deterministicTaskQueue.getCurrentTimeMillis() + followerLagTimeout.millis() - 1,
            () -> lagDetector.setAppliedVersion(node1, 1));
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertThat(failedNodes, empty());
    }

    public void testLagDetectedIfNodeAppliesVersionJustAfterTimeout() {
        lagDetector.setTrackedNodes(Collections.singletonList(node1));
        lagDetector.startLagDetector(1);
        deterministicTaskQueue.scheduleAt(deterministicTaskQueue.getCurrentTimeMillis() + followerLagTimeout.millis() + 1,
            () -> lagDetector.setAppliedVersion(node1, 1));
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
        deterministicTaskQueue.scheduleAt(deterministicTaskQueue.getCurrentTimeMillis() + followerLagTimeout.millis() - 1,
            () -> lagDetector.setAppliedVersion(node1, 3));
        assertThat(failedNodes, empty());

        lagDetector.startLagDetector(4);
        deterministicTaskQueue.scheduleAt(deterministicTaskQueue.getCurrentTimeMillis() + followerLagTimeout.millis() + 1,
            () -> lagDetector.setAppliedVersion(node1, 4));
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
}
