/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NodeAndClusterAlias;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class TaskCancellationServiceTests extends ESTestCase {

    public void testSendHeartbeatRequest() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(builder().put(NODE_NAME_SETTING.getKey(), "node").build(), random());
        Set<NodeAndClusterAlias> receivedHeartbeats = new HashSet<>();
        TaskCancellationService.SendHeartbeatTask sendingTask = new TaskCancellationService.SendHeartbeatTask(
            logger, taskQueue.getThreadPool(), receivedHeartbeats::add, TimeValue.timeValueMillis(between(1, 1000)));
        assertFalse(taskQueue.hasRunnableTasks());
        assertFalse(taskQueue.hasDeferredTasks());
        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);
        sendingTask.register(Collections.singletonList(new NodeAndClusterAlias(node1, null)));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        assertFalse(taskQueue.hasDeferredTasks());
        assertTrue(taskQueue.hasRunnableTasks());
        taskQueue.runRandomTask();
        assertTrue(taskQueue.hasDeferredTasks());
        assertThat(receivedHeartbeats, containsInAnyOrder(new NodeAndClusterAlias(node1, null)));
        receivedHeartbeats.clear();

        if (randomBoolean()) {
            taskQueue.advanceTime();
            sendingTask.register(Collections.singletonList(new NodeAndClusterAlias(node2, "remote")));
        } else {
            sendingTask.register(Collections.singletonList(new NodeAndClusterAlias(node2, "remote")));
            taskQueue.advanceTime();
        }
        assertTrue(taskQueue.hasRunnableTasks());
        taskQueue.runRandomTask();
        assertThat(receivedHeartbeats, containsInAnyOrder(new NodeAndClusterAlias(node1, null), new NodeAndClusterAlias(node2, "remote")));
        receivedHeartbeats.clear();

        if (randomBoolean()) {
            sendingTask.unregister(Collections.singletonList(new NodeAndClusterAlias(node2, "remote")));
            taskQueue.advanceTime();
        } else {
            taskQueue.advanceTime();
            sendingTask.unregister(Collections.singletonList(new NodeAndClusterAlias(node2, "remote")));
        }
        assertTrue(taskQueue.hasRunnableTasks());
        taskQueue.runRandomTask();
        assertThat(receivedHeartbeats, containsInAnyOrder(new NodeAndClusterAlias(node1, null)));
        receivedHeartbeats.clear();

        if (randomBoolean()) {
            taskQueue.advanceTime();
        }
        sendingTask.unregister(Collections.singletonList(new NodeAndClusterAlias(node1, null)));
        taskQueue.runAllTasks();
        assertThat(receivedHeartbeats, empty());
    }
}
