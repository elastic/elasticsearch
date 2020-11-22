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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class TaskCancellationServiceTests extends ESTestCase {

    public void testSendBanMarkerHeartbeats() throws Exception {
        final DeterministicTaskQueue queue = new DeterministicTaskQueue(
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build(), random());
        final Map<TaskId, List<DiscoveryNode>> childNodes = new HashMap<>();
        final List<Tuple<TaskId, DiscoveryNode>> sentMessages = new ArrayList<>();

        final TaskCancellationService.BanParentMarkerHeartbeatSender sender = new TaskCancellationService.BanParentMarkerHeartbeatSender(
            queue.getThreadPool(),
            TimeValue.timeValueMillis(randomIntBetween(1, 1000)),
            taskId -> childNodes.getOrDefault(taskId, List.of()),
            (taskId, node) -> sentMessages.add(Tuple.tuple(taskId, node)));

        assertFalse(sender.isRunningOrScheduled());
        final TaskId task1 = new TaskId("node", 1);
        final Releasable releasable1 = sender.registerCancellingTask(task1);
        assertTrue(sender.isRunningOrScheduled());
        assertTrue(queue.hasDeferredTasks());
        assertFalse(queue.hasRunnableTasks());
        queue.advanceTime();
        queue.runAllRunnableTasks();
        assertThat(sentMessages, Matchers.empty());

        final DiscoveryNode node1 = new DiscoveryNode("node-1", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode node2 = new DiscoveryNode("node-2", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode node3 = new DiscoveryNode("node-3", buildNewFakeTransportAddress(), Version.CURRENT);

        childNodes.put(task1, List.of(node1, node2));
        assertTrue(queue.hasDeferredTasks());
        assertFalse(queue.hasRunnableTasks());
        queue.advanceTime();
        queue.runAllRunnableTasks();
        assertThat(sentMessages, equalTo(List.of(Tuple.tuple(task1, node1), Tuple.tuple(task1, node2))));
        sentMessages.clear();

        final Releasable releasable2 = sender.registerCancellingTask(task1);
        assertTrue(sender.isRunningOrScheduled());
        assertTrue(queue.hasDeferredTasks());
        assertFalse(queue.hasRunnableTasks());
        assertThat(sentMessages, empty());
        queue.advanceTime();
        queue.runAllRunnableTasks();
        assertThat(sentMessages, equalTo(List.of(Tuple.tuple(task1, node1), Tuple.tuple(task1, node2))));
        sentMessages.clear();

        if (randomBoolean()) {
            releasable1.close();
        } else {
            releasable2.close();
        }
        final TaskId task2 = new TaskId("node", 2);
        childNodes.put(task2, List.of(node3));
        final Releasable releasable3 = sender.registerCancellingTask(task2);
        assertTrue(sender.isRunningOrScheduled());
        assertTrue(queue.hasDeferredTasks());
        assertFalse(queue.hasRunnableTasks());
        queue.advanceTime();
        queue.runAllRunnableTasks();
        assertThat(sentMessages, containsInAnyOrder(Tuple.tuple(task1, node1), Tuple.tuple(task1, node2), Tuple.tuple(task2, node3)));
        sentMessages.clear();

        releasable1.close();
        releasable2.close();
        assertTrue(sender.isRunningOrScheduled());
        assertTrue(queue.hasDeferredTasks());
        assertFalse(queue.hasRunnableTasks());
        queue.advanceTime();
        queue.runAllRunnableTasks();
        assertThat(sentMessages, equalTo(List.of(Tuple.tuple(task2, node3))));
        sentMessages.clear();

        releasable3.close();
        assertFalse(sender.isRunningOrScheduled());
        assertTrue(queue.hasDeferredTasks());
        queue.advanceTime();
        queue.runAllRunnableTasks();
        assertThat(sentMessages, empty());

        assertFalse(queue.hasDeferredTasks());
        assertFalse(queue.hasRunnableTasks());
    }
}
