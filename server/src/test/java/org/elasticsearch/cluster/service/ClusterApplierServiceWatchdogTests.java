/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.TimeoutClusterStateListener;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.service.ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_INTERVAL;
import static org.elasticsearch.cluster.service.ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_QUIET_TIME;
import static org.mockito.Mockito.mock;

public class ClusterApplierServiceWatchdogTests extends ESTestCase {

    public void testThreadWatchdogLogging() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();

        final var settingsBuilder = Settings.builder();

        final long intervalMillis;
        if (randomBoolean()) {
            intervalMillis = randomLongBetween(1, 1_000_000);
            settingsBuilder.put(CLUSTER_APPLIER_THREAD_WATCHDOG_INTERVAL.getKey(), TimeValue.timeValueMillis(intervalMillis));
        } else {
            intervalMillis = CLUSTER_APPLIER_THREAD_WATCHDOG_INTERVAL.get(Settings.EMPTY).millis();
        }

        final long quietTimeMillis;
        if (randomBoolean()) {
            quietTimeMillis = randomLongBetween(intervalMillis, 3 * intervalMillis);
            settingsBuilder.put(CLUSTER_APPLIER_THREAD_WATCHDOG_QUIET_TIME.getKey(), TimeValue.timeValueMillis(quietTimeMillis));
        } else {
            quietTimeMillis = CLUSTER_APPLIER_THREAD_WATCHDOG_QUIET_TIME.get(Settings.EMPTY).millis();
        }

        final var settings = settingsBuilder.build();

        try (
            var clusterApplierService = new ClusterApplierService(
                randomIdentifier(),
                settings,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                deterministicTaskQueue.getThreadPool()
            ) {
                @Override
                protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                    return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
                }
            };
            var mockLog = MockLog.capture(ClusterApplierService.class)
        ) {
            clusterApplierService.setNodeConnectionsService(mock(NodeConnectionsService.class));
            clusterApplierService.setInitialState(ClusterState.EMPTY_STATE);
            clusterApplierService.start();

            final AtomicBoolean completedTask = new AtomicBoolean();

            final Runnable hotThreadsDumpsAsserter = () -> {
                final var startMillis = deterministicTaskQueue.getCurrentTimeMillis();

                for (int i = 0; i < 3; i++) {
                    mockLog.addExpectation(
                        new MockLog.SeenEventExpectation(
                            "hot threads dump [" + i + "]",
                            ClusterApplierService.class.getCanonicalName(),
                            Level.WARN,
                            "hot threads dump due to active threads not making progress"
                        )
                    );

                    while (deterministicTaskQueue.getCurrentTimeMillis() < startMillis + 2 * intervalMillis + i * quietTimeMillis) {
                        deterministicTaskQueue.advanceTime();
                        deterministicTaskQueue.runAllRunnableTasks();
                    }

                    mockLog.assertAllExpectationsMatched();
                }
            };

            if (randomBoolean()) {
                clusterApplierService.runOnApplierThread(
                    "slow task",
                    randomFrom(Priority.values()),
                    ignored -> hotThreadsDumpsAsserter.run(),
                    ActionListener.running(() -> assertTrue(completedTask.compareAndSet(false, true)))
                );
            } else {
                class TestListener implements TimeoutClusterStateListener {
                    @Override
                    public void postAdded() {
                        hotThreadsDumpsAsserter.run();
                    }

                    @Override
                    public void onClose() {
                        fail("should time out before closing");
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        assertTrue(completedTask.compareAndSet(false, true));
                        clusterApplierService.removeTimeoutListener(TestListener.this);
                    }

                    @Override
                    public void clusterChanged(ClusterChangedEvent event) {
                        fail("no cluster state updates expected");
                    }
                }

                clusterApplierService.addTimeoutListener(
                    // timeout sufficiently short that it elapses while postAdded() is still running
                    TimeValue.timeValueMillis(randomLongBetween(0, 2 * intervalMillis + 2 * quietTimeMillis)),
                    new TestListener()
                );
            }

            deterministicTaskQueue.runAllRunnableTasks();

            assertTrue(completedTask.get());
        }
    }
}
