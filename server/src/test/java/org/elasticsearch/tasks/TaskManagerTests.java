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

package org.elasticsearch.tasks;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.TaskManagerTestCase;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.FakeTcpChannel;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TransportRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TaskManagerTests extends TaskManagerTestCase {

    /**
     * Makes sure that tasks that attempt to store themselves on completion retry if
     * they don't succeed at first.
     */
    public void testResultsServiceRetryTotalTime() {
        Iterator<TimeValue> times = TaskResultsService.STORE_BACKOFF_POLICY.iterator();
        long total = 0;
        while (times.hasNext()) {
            total += times.next().millis();
        }
        assertEquals(600000L, total);
    }

    public void testTrackingChannelTask() throws Exception {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        final TaskManager taskManager = testNodes[0].transportService.getTaskManager();
        Set<CancellableTask> cancelledTasks = new HashSet<>();
        taskManager.registerOrphanedTasksOnChannelCloseListener(tasks -> {
            for (CancellableTask task : tasks) {
                assertTrue("task [" + task + "] was cancelled already", cancelledTasks.add(task));
            }
        });
        Map<TcpChannel, Set<Task>> pendingTasks = new HashMap<>();
        Set<Task> expectedCancelledTasks = new HashSet<>();
        MockTcpChannel[] channels = new MockTcpChannel[randomIntBetween(1, 10)];
        List<Releasable> stopTrackingTasks = new ArrayList<>();
        for (int i = 0; i < channels.length; i++) {
            channels[i] = new MockTcpChannel();
        }
        int iterations = randomIntBetween(1, 200);
        for (int i = 0; i < iterations; i++) {
            final List<Releasable> subset = randomSubsetOf(stopTrackingTasks);
            stopTrackingTasks.removeAll(subset);
            Releasables.close(subset);
            final MockTcpChannel channel = randomFrom(channels);
            final Task task = taskManager.register("transport", "test", new CancellableRequest(Integer.toString(i)));
            if (channel.isOpen() && randomBoolean()) {
                channel.close();
                expectedCancelledTasks.addAll(pendingTasks.getOrDefault(channel, Collections.emptySet()));
            }
            final Releasable stopTracking = taskManager.startTrackingCancellableChannelTask(channel, (CancellableTask) task);
            if (channel.isOpen()) {
                pendingTasks.computeIfAbsent(channel, k -> new HashSet<>()).add(task);
                stopTrackingTasks.add(() -> {
                    stopTracking.close();
                    pendingTasks.get(channel).remove(task);
                });
            } else {
                expectedCancelledTasks.add(task);
            }
            assertThat(expectedCancelledTasks, equalTo(expectedCancelledTasks));
        }
        assertThat(expectedCancelledTasks, equalTo(expectedCancelledTasks));
    }

    static class CancellableRequest extends TransportRequest {
        private final String requestId;

        CancellableRequest(String requestId) {
            this.requestId = requestId;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "request-" + requestId, parentTaskId, headers) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return false;
                }

                @Override
                public String toString() {
                    return getDescription();
                }
            };
        }
    }

    static class MockTcpChannel extends FakeTcpChannel {
        private boolean registeredListener = false;

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            if (isOpen()) {
                assertFalse("listener was registered already", registeredListener);
                registeredListener = true;
                super.addCloseListener(listener);
            } else {
                listener.onResponse(null);
            }
        }
    }
}
