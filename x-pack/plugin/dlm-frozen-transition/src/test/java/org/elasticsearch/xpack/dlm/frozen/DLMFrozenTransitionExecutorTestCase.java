/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Shared harness for tests that need a real {@link DLMFrozenTransitionExecutor} backed by a real thread pool and
 * cluster service, so that different test classes covering different aspects of the executor (submission/status
 * tracking, the extension-point provider that reads from it) don't each hand-roll the same wiring.
 */
abstract class DLMFrozenTransitionExecutorTestCase extends ESTestCase {

    protected ThreadPool threadPool;
    protected ClusterService clusterService;
    protected DLMFrozenTransitionSettings transitionSettings;

    protected void setupExecutorTestCase() throws Exception {
        this.threadPool = new TestThreadPool("test-dlm-frozen-transition-executor");
        Set<Setting<?>> settingSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(DLMFrozenTransitionSettings.TRANSITION_ENABLED_SETTING);
        this.clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            DiscoveryNodeUtils.create("node", "node"),
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, settingSet)
        );
        this.clusterService.getMasterService().setClusterStatePublisher((event, publishListener, ackListener) -> {
            ClusterServiceUtils.setAllElapsedMillis(event);
            ackListener.onCommit(TimeValue.ZERO);
            clusterService.getClusterApplierService()
                .onNewClusterState("mock_publish_to_self[" + event.getSummary() + "]", event::getNewState, ActionListener.wrap(ignored -> {
                    ackListener.onNodeAck(event.getNewState().nodes().getLocalNode(), null);
                    publishListener.onResponse(null);
                }, publishListener::onFailure));
        });
        this.transitionSettings = DLMFrozenTransitionSettings.create(clusterService);
    }

    protected void tearDownExecutorTestCase() throws Exception {
        if (this.clusterService != null) {
            this.clusterService.close();
        }
        if (this.threadPool != null) {
            terminate(threadPool);
        }
    }

    /**
     * Pairs a {@link TestThreadPool} carrying the DLM-frozen-transition executor with the
     * {@link DLMFrozenTransitionExecutor} that consumes it, so each test can own its own correctly-sized
     * executor and clean it up via try-with-resources.
     */
    record TestExecutorHandle(TestThreadPool pool, DLMFrozenTransitionExecutor executor) implements Closeable {
        @Override
        public void close() {
            ThreadPool.terminate(pool, 10, TimeUnit.SECONDS);
        }
    }

    protected TestExecutorHandle newExecutor(int maxConcurrency, int maxQueueSize) {
        return newExecutor(maxConcurrency, maxQueueSize, makeErrorStore());
    }

    protected TestExecutorHandle newExecutor(int maxConcurrency, int maxQueueSize, DataStreamLifecycleErrorStore errorStore) {
        TestThreadPool pool = new TestThreadPool(
            "test-dlm-frozen-transition-pool",
            new FixedExecutorBuilder(
                Settings.EMPTY,
                DLMFrozenTransitionPlugin.EXECUTOR_NAME,
                maxConcurrency,
                maxQueueSize,
                "dlm.frozen.transition.thread_pool",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
        DLMFrozenTransitionExecutor exec = new DLMFrozenTransitionExecutor(
            clusterService,
            maxConcurrency + maxQueueSize,
            transitionSettings,
            errorStore,
            pool.executor(DLMFrozenTransitionPlugin.EXECUTOR_NAME)
        );
        return new TestExecutorHandle(pool, exec);
    }

    protected static DataStreamLifecycleErrorStore makeErrorStore() {
        return new DataStreamLifecycleErrorStore(System::currentTimeMillis);
    }

    /**
     * Minimal test double implementing {@link DLMFrozenTransitionRunnable} with deterministic, test-controlled behavior.
     * The {@code started} latch always counts down when the task begins. Set {@code blockUntil} to a non-released latch
     * to hold the task, or leave it at the default (already released) for tasks that complete immediately.
     */
    static class TestDLMFrozenTransitionRunnable implements DLMFrozenTransitionRunnable {
        private final String indexName;
        private final ProjectId projectId;
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch blockUntil = new CountDownLatch(0);
        Throwable throwOnRun;

        TestDLMFrozenTransitionRunnable(String indexName, ProjectId projectId) {
            this.indexName = indexName;
            this.projectId = projectId;
        }

        @Override
        public String getIndexName() {
            return indexName;
        }

        @Override
        public ProjectId getProjectId() {
            return projectId;
        }

        @Override
        public void run() {
            started.countDown();
            try {
                blockUntil.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (throwOnRun instanceof RuntimeException rte) {
                throw rte;
            } else if (throwOnRun instanceof Error error) {
                throw error;
            }
        }
    }
}
