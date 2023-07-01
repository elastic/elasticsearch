/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class HealthPeriodicLoggerTests extends ESTestCase {
    private ThreadPool threadPool;

    private NodeClient client;
    private ClusterService clusterService;

    private HealthPeriodicLogger testHealthPeriodicLogger;
    private ClusterService testClusterService;

    private NodeClient getTestClient() {
        return mock(NodeClient.class);
    }

    private HealthService getMockedHealthService() {
        return mock(HealthService.class);
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
        if (testClusterService != null) {
            testClusterService.close();
        }
        if (testHealthPeriodicLogger != null) {
            testHealthPeriodicLogger.close();
        }
        threadPool.shutdownNow();
        client.close();
    }

    private List<HealthIndicatorResult> getTestIndicatorResults() {
        var networkLatency = new HealthIndicatorResult("network_latency", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("slow_task_assignment", YELLOW, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", GREEN, null, null, null, null);

        return List.of(networkLatency, slowTasks, shardsAvailable);
    }

    private String makeHealthStatusString(String key) {
        return String.format(Locale.ROOT, "%s.%s.status", HealthPeriodicLogger.HEALTH_FIELD_PREFIX, key);
    }

    public void testHealthPeriodicLoggerResultToMap() {
        var results = getTestIndicatorResults();
        var overallStatus = HealthStatus.merge(results.stream().map(HealthIndicatorResult::status));

        HealthService testHealthService = getMockedHealthService();
        testHealthPeriodicLogger = new HealthPeriodicLogger(Settings.EMPTY, this.clusterService, client, testHealthService);
        testHealthPeriodicLogger.init();

        Map<String, Object> loggerResults = testHealthPeriodicLogger.toLoggedFields(results);

        assertEquals(results.size() + 1, loggerResults.size());

        // test indicator status
        assertEquals("green", loggerResults.get(makeHealthStatusString("network_latency")));
        assertEquals("yellow", loggerResults.get(makeHealthStatusString("slow_task_assignment")));
        assertEquals("green", loggerResults.get(makeHealthStatusString("shards_availability")));

        // test calculated overall status
        assertEquals(overallStatus.xContentValue(), loggerResults.get(makeHealthStatusString("overall")));

        // test empty results
        {
            List<HealthIndicatorResult> empty = new ArrayList<HealthIndicatorResult>();
            Map<String, Object> emptyResults = testHealthPeriodicLogger.toLoggedFields(empty);

            assertEquals(0, emptyResults.size());
        }
    }

    public void testHealthNodeIsSelected() throws Exception {
        HealthService testHealthService = this.getMockedHealthService();

        // create a cluster topology
        final DiscoveryNode node1 = DiscoveryNodeUtils.builder("node_1").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        final DiscoveryNode node2 = DiscoveryNodeUtils.builder("node_2")
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
            .build();
        ClusterState current = ClusterStateCreationUtils.state(node2, node1, node2, new DiscoveryNode[] { node1, node2 });

        testClusterService = createClusterService(current, this.threadPool);
        testHealthPeriodicLogger = new HealthPeriodicLogger(Settings.EMPTY, testClusterService, client, testHealthService);
        testHealthPeriodicLogger.init();

        // test that it knows that it's not initially the health node
        assertFalse(testHealthPeriodicLogger.isHealthNode);

        // trigger a cluster change and recheck
        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", current, ClusterState.EMPTY_STATE));
        assertTrue(testHealthPeriodicLogger.isHealthNode);
    }

    public void testJobScheduling() {
        HealthService testHealthService = this.getMockedHealthService();

        // create a cluster topology
        final DiscoveryNode node1 = DiscoveryNodeUtils.builder("node_1").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        final DiscoveryNode node2 = DiscoveryNodeUtils.builder("node_2")
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
            .build();
        ClusterState current = ClusterStateCreationUtils.state(node2, node1, node2, new DiscoveryNode[] { node1, node2 });

        testClusterService = createClusterService(current, this.threadPool);
        testHealthPeriodicLogger = new HealthPeriodicLogger(Settings.EMPTY, testClusterService, client, testHealthService);
        testHealthPeriodicLogger.init();

        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", current, ClusterState.EMPTY_STATE));
        assertTrue(testHealthPeriodicLogger.isHealthNode);

        SchedulerEngine scheduler = testHealthPeriodicLogger.getScheduler();
        assertTrue(scheduler.scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME));

        ClusterState noHealthNode = ClusterStateCreationUtils.state(node2, node1, new DiscoveryNode[] { node1, node2 });
        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", noHealthNode, current));
        assertFalse(testHealthPeriodicLogger.isHealthNode);
        assertFalse(scheduler.scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME));
    }

    public void testTriggeredJobCallsTryToLogHealth() throws Exception {
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
        HealthService testHealthService = this.getMockedHealthService();

        testHealthPeriodicLogger = new HealthPeriodicLogger(Settings.EMPTY, clusterService, client, testHealthService);
        testHealthPeriodicLogger.init();

        HealthPeriodicLogger spyHealthPeriodicLogger = spy(testHealthPeriodicLogger);
        spyHealthPeriodicLogger.isHealthNode = true;
        doAnswer(invocation -> {
            testListener.onResponse(getTestIndicatorResults());
            return null;
        }).when(spyHealthPeriodicLogger).tryToLogHealth();

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);
        spyHealthPeriodicLogger.triggered(event);

        assertFalse(failureCalled.get());
        assertTrue(listenerCalled.get());
    }

    public void testTryToLogHealthConcurrencyControlWithResults() {
        AtomicBoolean getHealthCalled = new AtomicBoolean(false);

        HealthService testHealthService = this.getMockedHealthService();
        doAnswer(invocation -> {
            // get and call the results listener provided to getHealth
            ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
            listener.onResponse(getTestIndicatorResults());
            getHealthCalled.set(true);
            return null;
        }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());

        testHealthPeriodicLogger = new HealthPeriodicLogger(Settings.EMPTY, clusterService, client, testHealthService);
        testHealthPeriodicLogger.init();

        HealthPeriodicLogger spyHealthPeriodicLogger = spy(testHealthPeriodicLogger);
        spyHealthPeriodicLogger.isHealthNode = true;

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);

        // run it once, verify getHealth is called
        {
            spyHealthPeriodicLogger.triggered(event);
            assertTrue(getHealthCalled.get());
        }

        // run it again, verify getHealth is called, because we are calling the results listener
        {
            getHealthCalled.set(false);
            spyHealthPeriodicLogger.triggered(event);
            assertTrue(getHealthCalled.get());
        }
    }

    public void testTryToLogHealthConcurrencyControl() {
        AtomicBoolean getHealthCalled = new AtomicBoolean(false);

        HealthService testHealthService = this.getMockedHealthService();
        doAnswer(invocation -> {
            // get but do not call the provided listener
            ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
            assertNotNull(listener);

            // note that we received the getHealth call
            getHealthCalled.set(true);
            return null;
        }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());

        testHealthPeriodicLogger = new HealthPeriodicLogger(Settings.EMPTY, clusterService, client, testHealthService);
        testHealthPeriodicLogger.init();

        HealthPeriodicLogger spyHealthPeriodicLogger = spy(testHealthPeriodicLogger);
        spyHealthPeriodicLogger.isHealthNode = true;

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);

        // call it once, and verify that getHealth is called
        {
            spyHealthPeriodicLogger.triggered(event);
            assertTrue(getHealthCalled.get());
        }

        // trigger it again, and verify that getHealth is not called
        // it's not called because the results listener was never called by getHealth
        // this is simulating a double invocation of getHealth, due perhaps to a lengthy getHealth call
        {
            getHealthCalled.set(false);
            spyHealthPeriodicLogger.triggered(event);
            assertFalse(getHealthCalled.get());
        }
    }

    public void testTryToLogHealthConcurrencyControlWithException() {
        AtomicBoolean getHealthCalled = new AtomicBoolean(false);

        HealthService testHealthService = this.getMockedHealthService();

        testHealthPeriodicLogger = new HealthPeriodicLogger(Settings.EMPTY, clusterService, client, testHealthService);
        testHealthPeriodicLogger.init();

        HealthPeriodicLogger spyHealthPeriodicLogger = spy(testHealthPeriodicLogger);
        spyHealthPeriodicLogger.isHealthNode = true;

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);

        // run it once and trigger an exception during the getHealth call
        {
            getHealthCalled.set(false);
            doThrow(new ResourceNotFoundException("No preflight indicators")).when(testHealthService)
                .getHealth(any(), isNull(), anyBoolean(), anyInt(), any());
            getHealthCalled.set(false);
            spyHealthPeriodicLogger.triggered(event);
            assertFalse(getHealthCalled.get());
        }

        // run it again and have getHealth work. This tests that the RunOnce still sets the currentlyRunning variable.
        {
            getHealthCalled.set(false);
            doAnswer(invocation -> {
                ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
                listener.onResponse(getTestIndicatorResults());
                getHealthCalled.set(true);
                return null;
            }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());
            getHealthCalled.set(false);
            spyHealthPeriodicLogger.triggered(event);
            assertTrue(getHealthCalled.get());
        }
    }

    public void testLoggingHappens() {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "overall",
                HealthPeriodicLogger.class.getCanonicalName(),
                Level.INFO,
                String.format(Locale.ROOT, "%s=\"yellow\"", makeHealthStatusString("overall"))
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "network_latency",
                HealthPeriodicLogger.class.getCanonicalName(),
                Level.INFO,
                String.format(Locale.ROOT, "%s=\"green\"", makeHealthStatusString("network_latency"))
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "slow_task_assignment",
                HealthPeriodicLogger.class.getCanonicalName(),
                Level.INFO,
                String.format(Locale.ROOT, "%s=\"yellow\"", makeHealthStatusString("slow_task_assignment"))
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "ilm",
                HealthPeriodicLogger.class.getCanonicalName(),
                Level.INFO,
                String.format(Locale.ROOT, "%s=\"red\"", makeHealthStatusString("ilm"))
            )
        );
        Logger periodicLoggerLogger = LogManager.getLogger(HealthPeriodicLogger.class);
        Loggers.addAppender(periodicLoggerLogger, mockAppender);

        HealthService testHealthService = this.getMockedHealthService();

        testHealthPeriodicLogger = new HealthPeriodicLogger(Settings.EMPTY, clusterService, client, testHealthService);
        testHealthPeriodicLogger.init();

        HealthPeriodicLogger spyHealthPeriodicLogger = spy(testHealthPeriodicLogger);
        spyHealthPeriodicLogger.isHealthNode = true;
        doAnswer(invocation -> {
            spyHealthPeriodicLogger.resultsListener.onResponse(getTestIndicatorResults());
            return null;
        }).when(spyHealthPeriodicLogger).tryToLogHealth();

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);
        spyHealthPeriodicLogger.triggered(event);

        try {
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(periodicLoggerLogger, mockAppender);
            mockAppender.stop();
        }

    }

}
