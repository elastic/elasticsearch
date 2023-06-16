/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class HealthPeriodicLoggerTests extends ESTestCase {
    private ThreadPool threadPool;

    private Client client;
    private ClusterService clusterService;

    private Client getTestClient() {
        return mock(Client.class);
    }

    private HealthService getTestHealthService() {
        return new HealthService(null, null, this.threadPool);
    }

    @Before
    public void setupServices() {
        threadPool = new TestThreadPool(getTestName());

        Set<Setting<?>> builtInClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        builtInClusterSettings.add(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_POLL_INTERVAL_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, builtInClusterSettings);
        this.clusterService = createClusterService(this.threadPool, clusterSettings);

        this.client = getTestClient();

    }

    @After
    public void cleanup() {
        clusterService.close();
        threadPool.shutdownNow();
        client.close();
    }

    private List<HealthIndicatorResult> getTestIndicatorResults() {
        var networkLatency = new HealthIndicatorResult("network_latency", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("slow_task_assignment", YELLOW, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", GREEN, null, null, null, null);

        return List.of(networkLatency, slowTasks, shardsAvailable);
    }

    public void testHealthPeriodicLoggerResultToMap() {
        var results = getTestIndicatorResults();
        var overallStatus = HealthStatus.merge(results.stream().map(HealthIndicatorResult::status));

        HealthPeriodicLogger.HealthPeriodicLoggerResult result = new HealthPeriodicLogger.HealthPeriodicLoggerResult(results);
        Map<String, Object> loggerResults = result.toMap();

        assertEquals(results.size() + 1, loggerResults.size());

        // test indicator status
        assertEquals(
            "green",
            loggerResults.get(String.format(Locale.ROOT, "%s.network_latency.status", HealthPeriodicLogger.HEALTH_FIELD_PREFIX))
        );
        assertEquals(
            "yellow",
            loggerResults.get(String.format(Locale.ROOT, "%s.slow_task_assignment.status", HealthPeriodicLogger.HEALTH_FIELD_PREFIX))
        );
        assertEquals(
            "green",
            loggerResults.get(String.format(Locale.ROOT, "%s.shards_availability.status", HealthPeriodicLogger.HEALTH_FIELD_PREFIX))
        );

        // test calculated overall status
        assertEquals(
            overallStatus.xContentValue(),
            loggerResults.get(String.format(Locale.ROOT, "%s.status", HealthPeriodicLogger.HEALTH_FIELD_PREFIX))
        );
    }

    public void testTriggeredJobCallsGetHealth() throws Exception {
        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        ActionListener<List<HealthIndicatorResult>> testListener = new ActionListener<List<HealthIndicatorResult>>() {
            @Override
            public void onResponse(List<HealthIndicatorResult> indicatorResults) {
                listenerCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                failureCalled.set(true);
            }
        };
        HealthService testHealthService = getTestHealthService();

        HealthPeriodicLogger testHealthPeriodicLogger = new HealthPeriodicLogger(Settings.EMPTY, clusterService, client, testHealthService);
        testHealthPeriodicLogger.init();

        HealthPeriodicLogger spyHealthPeriodicLogger = spy(testHealthPeriodicLogger);
        when(spyHealthPeriodicLogger.getIsHealthNode()).thenReturn(true);
        doAnswer(invocation -> {
            testListener.onResponse(getTestIndicatorResults());
            return null;
        }).when(spyHealthPeriodicLogger).callGetHealth();

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);
        spyHealthPeriodicLogger.triggered(event);

        assertFalse(failureCalled.get());
        assertTrue(listenerCalled.get());
    }

}
