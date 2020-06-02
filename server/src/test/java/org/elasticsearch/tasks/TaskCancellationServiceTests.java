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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;

public class TaskCancellationServiceTests extends ESTestCase {

    public void testSendHeartbeatRequest() {
        TaskId task1 = new TaskId(randomAlphaOfLength(4), randomNonNegativeLong());
        TaskId task2 = new TaskId(randomAlphaOfLength(4), randomNonNegativeLong());
        NodeAndClusterAlias node1 = new NodeAndClusterAlias(
            new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT), "remote");
        NodeAndClusterAlias node2 = new NodeAndClusterAlias(
            new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT), null);
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(builder().put(NODE_NAME_SETTING.getKey(), "node").build(), random());
        Map<NodeAndClusterAlias, Collection<TaskId>> receivedHeartbeats = new HashMap<>();
        BiConsumer<NodeAndClusterAlias, TaskCancellationService.HeartbeatRequest> sendFunction =
            (node, req) -> assertNull(receivedHeartbeats.put(node, req.taskIds));

        TaskCancellationService.SendHeartbeatTask sendingTask = new TaskCancellationService.SendHeartbeatTask(
            logger, taskQueue.getThreadPool(), sendFunction, TimeValue.timeValueMillis(between(1, 1000)));
        assertFalse(taskQueue.hasRunnableTasks());
        assertFalse(taskQueue.hasDeferredTasks());

        sendingTask.register(task1, Collections.singletonList(node1));
        assertFalse(taskQueue.hasRunnableTasks());
        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        assertFalse(taskQueue.hasDeferredTasks());
        assertTrue(taskQueue.hasRunnableTasks());
        taskQueue.runRandomTask();
        assertTrue(taskQueue.hasDeferredTasks());
        assertThat(receivedHeartbeats, hasEntry(node1, Set.of(task1)));
        receivedHeartbeats.clear();

        if (randomBoolean()) {
            taskQueue.advanceTime();
            sendingTask.register(task2, Set.of(node1, node2));
        } else {
            sendingTask.register(task2, Set.of(node1, node2));
            taskQueue.advanceTime();
        }
        assertTrue(taskQueue.hasRunnableTasks());
        taskQueue.runRandomTask();
        assertThat(receivedHeartbeats, hasEntry(node1, Set.of(task1, task2)));
        assertThat(receivedHeartbeats, hasEntry(node2, Set.of(task2)));
        receivedHeartbeats.clear();

        if (randomBoolean()) {
            sendingTask.unregister(task2, Set.of(node1, node2));
            taskQueue.advanceTime();
        } else {
            taskQueue.advanceTime();
            sendingTask.unregister(task2, Set.of(node1, node2));
        }
        assertTrue(taskQueue.hasRunnableTasks());
        taskQueue.runRandomTask();
        assertThat(receivedHeartbeats, hasEntry(node1, Set.of(task1)));
        receivedHeartbeats.clear();

        if (randomBoolean()) {
            taskQueue.advanceTime();
        }
        sendingTask.unregister(task1, Set.of(node1));
        taskQueue.runAllTasks();
        assertThat(receivedHeartbeats.entrySet(), empty());
    }
}
