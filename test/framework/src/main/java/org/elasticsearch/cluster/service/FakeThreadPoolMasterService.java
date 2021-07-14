/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.core.TimeValue;
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

public class FakeThreadPoolMasterService extends MasterService {
    private static final Logger logger = LogManager.getLogger(FakeThreadPoolMasterService.class);

    private final String name;
    private final List<Runnable> pendingTasks = new ArrayList<>();
    private final Consumer<Runnable> onTaskAvailableToRun;
    private boolean scheduledNextTask = false;
    private boolean taskInProgress = false;
    private boolean waitForPublish = false;

    public FakeThreadPoolMasterService(String nodeName, String serviceName, ThreadPool threadPool,
                                       Consumer<Runnable> onTaskAvailableToRun) {
        super(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), nodeName).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), threadPool);
        this.name = serviceName;
        this.onTaskAvailableToRun = onTaskAvailableToRun;
    }

    @Override
    protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
        return new PrioritizedEsThreadPoolExecutor(name, 1, 1, 1, TimeUnit.SECONDS, EsExecutors.daemonThreadFactory(name),
            null, null, PrioritizedEsThreadPoolExecutor.StarvationWatcher.NOOP_STARVATION_WATCHER) {

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
                    final ThreadContext threadContext = threadPool.getThreadContext();
                    try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                        threadContext.markAsSystemContext();
                        task.run();
                    }
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
        final ActionListener<Void> publishListener = new ActionListener<>() {

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
        };
        threadPool.generic().execute(threadPool.getThreadContext().preserveContext(new Runnable() {
            @Override
            public void run() {
                clusterStatePublisher.publish(clusterChangedEvent, publishListener, wrapAckListener(ackListener));
            }

            @Override
            public String toString() {
                return "publish change of cluster state from version [" + clusterChangedEvent.previousState().version() + "] in term [" +
                        clusterChangedEvent.previousState().term() + "] to version [" + clusterChangedEvent.state().version()
                        + "] in term [" + clusterChangedEvent.state().term() + "]";
            }
        }));
    }

    protected AckListener wrapAckListener(AckListener ackListener) {
        return ackListener;
    }
}
