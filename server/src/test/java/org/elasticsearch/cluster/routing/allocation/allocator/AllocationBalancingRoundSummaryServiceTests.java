/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

public class AllocationBalancingRoundSummaryServiceTests extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(AllocationBalancingRoundSummaryServiceTests.class);

    private static final String BALANCING_SUMMARY_MSG_PREFIX = "Balancing round summaries:*";

    final Settings enabledSummariesSettings = Settings.builder()
        .put(AllocationBalancingRoundSummaryService.ENABLE_BALANCER_ROUND_SUMMARIES_SETTING.getKey(), true)
        .build();
    final Settings disabledDefaultEmptySettings = Settings.builder().build();
    final Settings enabledButNegativeIntervalSettings = Settings.builder()
        .put(AllocationBalancingRoundSummaryService.ENABLE_BALANCER_ROUND_SUMMARIES_SETTING.getKey(), true)
        .put(AllocationBalancingRoundSummaryService.BALANCER_ROUND_SUMMARIES_LOG_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
        .build();

    ClusterSettings enabledClusterSettings = new ClusterSettings(enabledSummariesSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    ClusterSettings disabledDefaultEmptyClusterSettings = new ClusterSettings(
        disabledDefaultEmptySettings,
        ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
    );
    ClusterSettings enabledButNegativeIntervalClusterSettings = new ClusterSettings(
        enabledButNegativeIntervalSettings,
        ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
    );

    // Construction parameters for the service.

    DeterministicTaskQueue deterministicTaskQueue;
    ThreadPool testThreadPool;

    @Before
    public void setUpThreadPool() {
        deterministicTaskQueue = new DeterministicTaskQueue();
        testThreadPool = deterministicTaskQueue.getThreadPool();
    }

    /**
     * Test that the service is disabled and no logging occurs when
     * {@link AllocationBalancingRoundSummaryService#ENABLE_BALANCER_ROUND_SUMMARIES_SETTING} defaults to false.
     */
    public void testServiceDisabledByDefault() {
        var service = new AllocationBalancingRoundSummaryService(testThreadPool, disabledDefaultEmptyClusterSettings);

        try (var mockLog = MockLog.capture(AllocationBalancingRoundSummaryService.class)) {
            /**
             * Add a summary and check it is not logged.
             */

            service.addBalancerRoundSummary(new BalancingRoundSummary(50));
            service.verifyNumberOfSummaries(0); // when summaries are disabled, summaries are not retained when added.
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "Running balancer summary logging",
                    AllocationBalancingRoundSummaryService.class.getName(),
                    Level.INFO,
                    "*"
                )
            );

            if (deterministicTaskQueue.hasDeferredTasks()) {
                deterministicTaskQueue.advanceTime();
            }
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
            service.verifyNumberOfSummaries(0);
        }
    }

    public void testEnabledService() {
        var service = new AllocationBalancingRoundSummaryService(testThreadPool, enabledClusterSettings);

        try (var mockLog = MockLog.capture(AllocationBalancingRoundSummaryService.class)) {
            /**
             * Add a summary and check the service logs a report on it.
             */

            service.addBalancerRoundSummary(new BalancingRoundSummary(50));
            service.verifyNumberOfSummaries(1);
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "Running balancer summary logging",
                    AllocationBalancingRoundSummaryService.class.getName(),
                    Level.INFO,
                    BALANCING_SUMMARY_MSG_PREFIX
                )
            );

            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
            service.verifyNumberOfSummaries(0);

            /**
             * Add a second summary, check for more logging.
             */

            service.addBalancerRoundSummary(new BalancingRoundSummary(200));
            service.verifyNumberOfSummaries(1);
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "Running balancer summary logging a second time",
                    AllocationBalancingRoundSummaryService.class.getName(),
                    Level.INFO,
                    BALANCING_SUMMARY_MSG_PREFIX
                )
            );

            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
            service.verifyNumberOfSummaries(0);
        }
    }

    /**
     * The service should combine multiple summaries together into a single report when multiple summaries were added since the last report.
     */
    public void testCombinedSummary() {
        var service = new AllocationBalancingRoundSummaryService(testThreadPool, enabledClusterSettings);

        try (var mockLog = MockLog.capture(AllocationBalancingRoundSummaryService.class)) {
            service.addBalancerRoundSummary(new BalancingRoundSummary(50));
            service.addBalancerRoundSummary(new BalancingRoundSummary(100));
            service.verifyNumberOfSummaries(2);
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "Running balancer summary logging of combined summaries",
                    AllocationBalancingRoundSummaryService.class.getName(),
                    Level.INFO,
                    "*150*"
                )
            );

            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
            service.verifyNumberOfSummaries(0);
        }
    }

    /**
     * The service shouldn't log anything when there haven't been any summaries added since the last report.
     */
    public void testNoSummariesToReport() {
        var service = new AllocationBalancingRoundSummaryService(testThreadPool, enabledClusterSettings);

        try (var mockLog = MockLog.capture(AllocationBalancingRoundSummaryService.class)) {
            /**
             * First add some summaries to report, ensuring that the logging is active.
             */

            service.addBalancerRoundSummary(new BalancingRoundSummary(50));
            service.verifyNumberOfSummaries(1);
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "Running balancer summary logging of combined summaries",
                    AllocationBalancingRoundSummaryService.class.getName(),
                    Level.INFO,
                    BALANCING_SUMMARY_MSG_PREFIX
                )
            );

            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
            service.verifyNumberOfSummaries(0);

            /**
             * Now check that there are no further log messages because there were no further summaries added.
             */

            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "No balancer round summary to log",
                    AllocationBalancingRoundSummaryService.class.getName(),
                    Level.INFO,
                    "*"
                )
            );

            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
            service.verifyNumberOfSummaries(0);
        }
    }

    /**
     * Test that the service is disabled by setting {@link AllocationBalancingRoundSummaryService#ENABLE_BALANCER_ROUND_SUMMARIES_SETTING}
     * to false.
     */
    public void testEnableAndThenDisableService() {
        var disabledSettingsUpdate = Settings.builder()
            .put(AllocationBalancingRoundSummaryService.ENABLE_BALANCER_ROUND_SUMMARIES_SETTING.getKey(), false)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(enabledSummariesSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        var service = new AllocationBalancingRoundSummaryService(testThreadPool, clusterSettings);

        try (var mockLog = MockLog.capture(AllocationBalancingRoundSummaryService.class)) {
            /**
             * Add some summaries, but then disable the service before logging occurs. Disabling the service should drain and discard any
             * summaries waiting to be reported.
             */

            service.addBalancerRoundSummary(new BalancingRoundSummary(50));
            service.verifyNumberOfSummaries(1);

            clusterSettings.applySettings(disabledSettingsUpdate);
            service.verifyNumberOfSummaries(0);

            /**
             * Verify that any additional summaries are not retained, since the service is disabled.
             */

            service.addBalancerRoundSummary(new BalancingRoundSummary(50));
            service.verifyNumberOfSummaries(0);

            // Check that the service never logged anything.
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "Running balancer summary logging",
                    AllocationBalancingRoundSummaryService.class.getName(),
                    Level.INFO,
                    "*"
                )
            );
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
            service.verifyNumberOfSummaries(0);
        }
    }

}
