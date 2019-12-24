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
package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher.AckListener;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.lucene.util.LuceneTestCase.random;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FakeThreadPoolMasterService extends MasterService {
    private static final Logger logger = LogManager.getLogger(FakeThreadPoolMasterService.class);

    private final String name;
    private final List<Runnable> pendingTasks = new ArrayList<>();
    private final Consumer<Runnable> onTaskAvailableToRun;
    private boolean scheduledNextTask = false;
    private boolean taskInProgress = false;
    private boolean waitForPublish = false;

    public FakeThreadPoolMasterService(String nodeName, String serviceName, Consumer<Runnable> onTaskAvailableToRun) {
        super(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), nodeName).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            createMockThreadPool());
        this.name = serviceName;
        this.onTaskAvailableToRun = onTaskAvailableToRun;
    }

    private static ThreadPool createMockThreadPool() {
        final ThreadContext context = new ThreadContext(Settings.EMPTY);
        final ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(context);
        return mockThreadPool;
    }

    @Override
    protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
        return new PrioritizedEsThreadPoolExecutor(name, 1, 1, 1, TimeUnit.SECONDS, EsExecutors.daemonThreadFactory(name),
            null, null) {

            @Override
            public void execute(Runnable command, final TimeValue timeout, final Runnable timeoutCallback) {
                execute(command);
            }

            @Override
            public void execute(Runnable command) {
                pendingTasks.add(command);
                scheduleNextTaskIfNecessary();
            }
        };
    }

    public int getFakeMasterServicePendingTaskCount() {
        return pendingTasks.size();
    }

    private void scheduleNextTaskIfNecessary() {
        if (taskInProgress == false && pendingTasks.isEmpty() == false && scheduledNextTask == false) {
            scheduledNextTask = true;
            onTaskAvailableToRun.accept(new Runnable() {
                @Override
                public String toString() {
                    return "master service scheduling next task";
                }

                @Override
                public void run() {
                    assert taskInProgress == false;
                    assert waitForPublish == false;
                    assert scheduledNextTask;
                    final int taskIndex = randomInt(pendingTasks.size() - 1);
                    logger.debug("next master service task: choosing task {} of {}", taskIndex, pendingTasks.size());
                    final Runnable task = pendingTasks.remove(taskIndex);
                    taskInProgress = true;
                    scheduledNextTask = false;
                    task.run();
                    if (waitForPublish == false) {
                        taskInProgress = false;
                    }
                    FakeThreadPoolMasterService.this.scheduleNextTaskIfNecessary();
                }
            });
        }
    }

    @Override
    public ClusterState.Builder incrementVersion(ClusterState clusterState) {
        // generate cluster UUID deterministically for repeatable tests
        return ClusterState.builder(clusterState).incrementVersion().stateUUID(UUIDs.randomBase64UUID(random()));
    }

    @Override
    protected void publish(ClusterChangedEvent clusterChangedEvent, TaskOutputs taskOutputs, long startTimeMillis) {
        assert waitForPublish == false;
        waitForPublish = true;
        final AckListener ackListener = taskOutputs.createAckListener(threadPool, clusterChangedEvent.state());
        clusterStatePublisher.publish(clusterChangedEvent, new ActionListener<Void>() {

            private boolean listenerCalled = false;

            @Override
            public void onResponse(Void aVoid) {
                assert listenerCalled == false;
                listenerCalled = true;
                assert waitForPublish;
                waitForPublish = false;
                try {
                    onPublicationSuccess(clusterChangedEvent, taskOutputs);
                } finally {
                    taskInProgress = false;
                    scheduleNextTaskIfNecessary();
                }
            }

            @Override
            public void onFailure(Exception e) {
                assert listenerCalled == false;
                listenerCalled = true;
                assert waitForPublish;
                waitForPublish = false;
                try {
                    onPublicationFailed(clusterChangedEvent, taskOutputs, startTimeMillis, e);
                } finally {
                    taskInProgress = false;
                    scheduleNextTaskIfNecessary();
                }
            }
        }, wrapAckListener(ackListener));
    }

    protected AckListener wrapAckListener(AckListener ackListener) {
        return ackListener;
    }
}
