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
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongGaugeMetric;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class HealthPeriodicLoggerTests extends ESTestCase {
    private ThreadPool threadPool;

    private NodeClient client;
    private ClusterService clusterService;

    private HealthPeriodicLogger testHealthPeriodicLogger;
    private ClusterSettings clusterSettings;
    private final DiscoveryNode node1 = DiscoveryNodeUtils.builder("node_1").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
    private final DiscoveryNode node2 = DiscoveryNodeUtils.builder("node_2")
        .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
        .build();
    private ClusterState stateWithLocalHealthNode;

    private NodeClient getTestClient() {
        return mock(NodeClient.class);
    }

    private HealthService getMockedHealthService() {
        return mock(HealthService.class);
    }

    private MeterRegistry getMockedMeterRegistry() {
        return mock(MeterRegistry.class);
    }

    private TelemetryProvider getMockedTelemetryProvider() {
        return mock(TelemetryProvider.class);
    }

    @Before
    public void setupServices() {
        threadPool = new TestThreadPool(getTestName());
        stateWithLocalHealthNode = ClusterStateCreationUtils.state(node2, node1, node2, new DiscoveryNode[] { node1, node2 });
        this.clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        this.clusterService = createClusterService(stateWithLocalHealthNode, this.threadPool, clusterSettings);
        this.client = getTestClient();
    }

    @After
    public void cleanup() {
        clusterService.close();
        if (testHealthPeriodicLogger != null) {
            testHealthPeriodicLogger.close();
        }
        threadPool.shutdownNow();
    }

    public void testConvertToLoggedFields() {
        var results = getTestIndicatorResults();
        var overallStatus = HealthStatus.merge(results.stream().map(HealthIndicatorResult::status));

        Map<String, Object> loggerResults = HealthPeriodicLogger.convertToLoggedFields(results);

        // verify that the number of fields is the number of indicators + 2 (for overall and for message)
        assertThat(loggerResults.size(), equalTo(results.size() + 2));

        // test indicator status
        assertThat(loggerResults.get(makeHealthStatusString("master_is_stable")), equalTo("green"));
        assertThat(loggerResults.get(makeHealthStatusString("disk")), equalTo("yellow"));
        assertThat(loggerResults.get(makeHealthStatusString("shards_availability")), equalTo("yellow"));

        // test calculated overall status
        assertThat(loggerResults.get(makeHealthStatusString("overall")), equalTo(overallStatus.xContentValue()));

        // test calculated message
        assertThat(
            loggerResults.get(HealthPeriodicLogger.MESSAGE_FIELD),
            equalTo(String.format(Locale.ROOT, "health=%s [disk,shards_availability]", overallStatus.xContentValue()))
        );

        // test empty results
        {
            List<HealthIndicatorResult> empty = new ArrayList<>();
            Map<String, Object> emptyResults = HealthPeriodicLogger.convertToLoggedFields(empty);

            assertThat(emptyResults.size(), equalTo(0));
        }

        // test all-green results
        {
            results = getTestIndicatorResultsAllGreen();
            loggerResults = HealthPeriodicLogger.convertToLoggedFields(results);
            overallStatus = HealthStatus.merge(results.stream().map(HealthIndicatorResult::status));

            // test calculated message
            assertThat(
                loggerResults.get(HealthPeriodicLogger.MESSAGE_FIELD),
                equalTo(String.format(Locale.ROOT, "health=%s", overallStatus.xContentValue()))
            );
        }
    }

    public void testHealthNodeIsSelected() {
        HealthService testHealthService = this.getMockedHealthService();
        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(clusterService, testHealthService, true);

        // test that it knows that it's not initially the health node
        assertFalse(testHealthPeriodicLogger.isHealthNode);

        // trigger a cluster change and recheck
        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
        assertTrue(testHealthPeriodicLogger.isHealthNode);
    }

    public void testJobScheduling() {
        HealthService testHealthService = this.getMockedHealthService();

        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(clusterService, testHealthService, true);

        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
        assertTrue(testHealthPeriodicLogger.isHealthNode);

        SchedulerEngine scheduler = testHealthPeriodicLogger.getScheduler();
        assertNotNull(scheduler);
        assertTrue(scheduler.scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME));

        ClusterState noHealthNode = ClusterStateCreationUtils.state(node2, node1, new DiscoveryNode[] { node1, node2 });
        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", noHealthNode, stateWithLocalHealthNode));
        assertFalse(testHealthPeriodicLogger.isHealthNode);
        assertFalse(scheduler.scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME));
    }

    public void testEnabled() {
        HealthService testHealthService = this.getMockedHealthService();
        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(clusterService, testHealthService, true);

        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
        assertTrue(testHealthPeriodicLogger.isHealthNode);

        // disable it and then verify that the job is gone
        {
            this.clusterSettings.applySettings(Settings.builder().put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), false).build());
            assertFalse(
                testHealthPeriodicLogger.getScheduler().scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME)
            );
        }

        // enable it and then verify that the job is created
        {
            this.clusterSettings.applySettings(Settings.builder().put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), true).build());
            assertTrue(
                testHealthPeriodicLogger.getScheduler().scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME)
            );
        }
    }

    public void testUpdatePollInterval() {
        HealthService testHealthService = this.getMockedHealthService();

        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(clusterService, testHealthService, false);
        assertNull(testHealthPeriodicLogger.getScheduler());

        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
        assertTrue(testHealthPeriodicLogger.isHealthNode);

        // verify that updating the polling interval doesn't schedule the job
        {
            this.clusterSettings.applySettings(
                Settings.builder().put(HealthPeriodicLogger.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(15)).build()
            );
            assertNull(testHealthPeriodicLogger.getScheduler());
        }
    }

    public void testTriggeredJobCallsTryToLogHealth() throws Exception {
        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        ActionListener<List<HealthIndicatorResult>> testListener = new ActionListener<>() {
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

        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, true);

        HealthPeriodicLogger spyHealthPeriodicLogger = spy(testHealthPeriodicLogger);
        spyHealthPeriodicLogger.isHealthNode = true;
        doAnswer(invocation -> {
            testListener.onResponse(getTestIndicatorResults());
            return null;
        }).when(spyHealthPeriodicLogger).tryToLogHealth();

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);
        spyHealthPeriodicLogger.triggered(event);

        assertBusy(() -> assertFalse(failureCalled.get()));
        assertBusy(() -> assertTrue(listenerCalled.get()));
    }

    public void testResultFailureHandling() throws Exception {
        AtomicInteger getHealthCalled = new AtomicInteger(0);

        HealthService testHealthService = this.getMockedHealthService();

        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, true);

        HealthPeriodicLogger spyHealthPeriodicLogger = spy(testHealthPeriodicLogger);
        spyHealthPeriodicLogger.isHealthNode = true;

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);

        // run it and call the listener's onFailure
        {
            doAnswer(invocation -> {
                ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
                listener.onFailure(new Exception("fake failure"));
                getHealthCalled.incrementAndGet();
                return null;
            }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());
            spyHealthPeriodicLogger.triggered(event);
            assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(1)));
        }

        // run it again and verify that the concurrency control is reset and the getHealth is called
        {
            doAnswer(invocation -> {
                ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
                listener.onResponse(getTestIndicatorResults());
                getHealthCalled.incrementAndGet();
                return null;
            }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());
            spyHealthPeriodicLogger.triggered(event);
            assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(2)));
        }
    }

    public void testTryToLogHealthConcurrencyControlWithResults() throws Exception {
        AtomicInteger getHealthCalled = new AtomicInteger(0);

        HealthService testHealthService = this.getMockedHealthService();
        doAnswer(invocation -> {
            // get and call the results listener provided to getHealth
            ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
            listener.onResponse(getTestIndicatorResults());
            getHealthCalled.incrementAndGet();
            return null;
        }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());

        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, true);

        HealthPeriodicLogger spyHealthPeriodicLogger = spy(testHealthPeriodicLogger);
        spyHealthPeriodicLogger.isHealthNode = true;

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);

        // run it once, verify getHealth is called
        {
            spyHealthPeriodicLogger.triggered(event);
            assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(1)));
        }

        // run it again, verify getHealth is called, because we are calling the results listener
        {
            spyHealthPeriodicLogger.triggered(event);
            assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(2)));
        }
    }

    public void testTryToLogHealthConcurrencyControl() throws Exception {
        AtomicInteger getHealthCalled = new AtomicInteger(0);

        HealthService testHealthService = this.getMockedHealthService();
        doAnswer(invocation -> {
            // get but do not call the provided listener
            ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
            assertNotNull(listener);

            // note that we received the getHealth call
            getHealthCalled.incrementAndGet();
            return null;
        }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());

        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, true);

        HealthPeriodicLogger spyHealthPeriodicLogger = spy(testHealthPeriodicLogger);
        spyHealthPeriodicLogger.isHealthNode = true;

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);

        // call it once, and verify that getHealth is called
        {
            spyHealthPeriodicLogger.triggered(event);
            assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(1)));
        }

        // trigger it again, and verify that getHealth is not called
        // it's not called because the results listener was never called by getHealth
        // this is simulating a double invocation of getHealth, due perhaps to a lengthy getHealth call
        {
            spyHealthPeriodicLogger.triggered(event);
            assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(1)));
        }
    }

    public void testTryToLogHealthConcurrencyControlWithException() throws Exception {
        AtomicInteger getHealthCalled = new AtomicInteger(0);

        HealthService testHealthService = this.getMockedHealthService();

        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, true);

        HealthPeriodicLogger spyHealthPeriodicLogger = spy(testHealthPeriodicLogger);
        spyHealthPeriodicLogger.isHealthNode = true;

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);

        // run it once and trigger an exception during the getHealth call
        {
            doThrow(new ResourceNotFoundException("No preflight indicators")).when(testHealthService)
                .getHealth(any(), isNull(), anyBoolean(), anyInt(), any());
            spyHealthPeriodicLogger.triggered(event);
            assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(0)));
        }

        // run it again and have getHealth work. This tests that the RunOnce still sets the currentlyRunning variable.
        {
            doAnswer(invocation -> {
                ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
                listener.onResponse(getTestIndicatorResults());
                getHealthCalled.incrementAndGet();
                return null;
            }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());
            spyHealthPeriodicLogger.triggered(event);
            assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(1)));
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
                "master_is_stable",
                HealthPeriodicLogger.class.getCanonicalName(),
                Level.INFO,
                String.format(Locale.ROOT, "%s=\"green\"", makeHealthStatusString("master_is_stable"))
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "disk",
                HealthPeriodicLogger.class.getCanonicalName(),
                Level.INFO,
                String.format(Locale.ROOT, "%s=\"yellow\"", makeHealthStatusString("disk"))
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

        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, true);

        // switch to Log only mode
        this.clusterSettings.applySettings(
            Settings.builder()
                .put(HealthPeriodicLogger.OUTPUT_MODE_SETTING.getKey(), HealthPeriodicLogger.OutputMode.LOGS)
                .put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), true)
                .build()
        );

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

    public void testOutputModeNoLogging() {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "overall",
                HealthPeriodicLogger.class.getCanonicalName(),
                Level.INFO,
                String.format(Locale.ROOT, "%s=\"yellow\"", makeHealthStatusString("overall"))
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "master_is_stable",
                HealthPeriodicLogger.class.getCanonicalName(),
                Level.INFO,
                String.format(Locale.ROOT, "%s=\"green\"", makeHealthStatusString("master_is_stable"))
            )
        );
        mockAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "disk",
                HealthPeriodicLogger.class.getCanonicalName(),
                Level.INFO,
                String.format(Locale.ROOT, "%s=\"yellow\"", makeHealthStatusString("disk"))
            )
        );
        Logger periodicLoggerLogger = LogManager.getLogger(HealthPeriodicLogger.class);
        Loggers.addAppender(periodicLoggerLogger, mockAppender);

        HealthService testHealthService = this.getMockedHealthService();

        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, true);

        // switch to Metrics only mode
        this.clusterSettings.applySettings(
            Settings.builder()
                .put(HealthPeriodicLogger.OUTPUT_MODE_SETTING.getKey(), HealthPeriodicLogger.OutputMode.METRICS)
                .put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), true)
                .build()
        );

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

    public void testMetricsMode() {
        List<String> logs = new ArrayList<>();
        List<Long> metrics = new ArrayList<>();

        BiConsumer<LongGaugeMetric, Long> metricWriter = (metric, value) -> metrics.add(value);
        Consumer<ESLogMessage> logWriter = msg -> logs.add(msg.asString());

        HealthService testHealthService = this.getMockedHealthService();
        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, true, metricWriter, logWriter);

        // switch to Metrics only mode
        this.clusterSettings.applySettings(
            Settings.builder()
                .put(HealthPeriodicLogger.OUTPUT_MODE_SETTING.getKey(), HealthPeriodicLogger.OutputMode.METRICS)
                .put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), true)
                .build()
        );

        HealthPeriodicLogger spyHealthPeriodicLogger = spy(testHealthPeriodicLogger);
        spyHealthPeriodicLogger.isHealthNode = true;
        List<HealthIndicatorResult> results = getTestIndicatorResultsWithRed();

        doAnswer(invocation -> {
            spyHealthPeriodicLogger.resultsListener.onResponse(results);
            return null;
        }).when(spyHealthPeriodicLogger).tryToLogHealth();

        assertEquals(0, metrics.size());

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);
        spyHealthPeriodicLogger.triggered(event);

        assertEquals(0, logs.size());
        assertEquals(4, metrics.size());
    }

    private List<HealthIndicatorResult> getTestIndicatorResults() {
        var networkLatency = new HealthIndicatorResult("master_is_stable", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("disk", YELLOW, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", YELLOW, null, null, null, null);

        return List.of(networkLatency, slowTasks, shardsAvailable);
    }

    private List<HealthIndicatorResult> getTestIndicatorResultsAllGreen() {
        var networkLatency = new HealthIndicatorResult("master_is_stable", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("disk", GREEN, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", GREEN, null, null, null, null);

        return List.of(networkLatency, slowTasks, shardsAvailable);
    }

    private List<HealthIndicatorResult> getTestIndicatorResultsWithRed() {
        var networkLatency = new HealthIndicatorResult("master_is_stable", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("disk", GREEN, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult("shards_availability", RED, null, null, null, null);

        return List.of(networkLatency, slowTasks, shardsAvailable);
    }

    private String makeHealthStatusString(String key) {
        return String.format(Locale.ROOT, "%s.%s.status", HealthPeriodicLogger.HEALTH_FIELD_PREFIX, key);
    }

    private HealthPeriodicLogger createAndInitHealthPeriodicLogger(
        ClusterService clusterService,
        HealthService testHealthService,
        boolean enabled
    ) {
        return createAndInitHealthPeriodicLogger(clusterService, testHealthService, enabled, null, null);
    }

    private HealthPeriodicLogger createAndInitHealthPeriodicLogger(
        ClusterService clusterService,
        HealthService testHealthService,
        boolean enabled,
        BiConsumer<LongGaugeMetric, Long> metricWriter,
        Consumer<ESLogMessage> logWriter
    ) {
        var provider = getMockedTelemetryProvider();
        var registry = getMockedMeterRegistry();
        doReturn(registry).when(provider).getMeterRegistry();
        if (metricWriter != null || logWriter != null) {
            testHealthPeriodicLogger = HealthPeriodicLogger.create(
                Settings.EMPTY,
                clusterService,
                this.client,
                testHealthService,
                provider,
                metricWriter,
                logWriter
            );
        } else {
            testHealthPeriodicLogger = HealthPeriodicLogger.create(
                Settings.EMPTY,
                clusterService,
                this.client,
                testHealthService,
                provider
            );
        }
        if (enabled) {
            clusterSettings.applySettings(Settings.builder().put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), true).build());
        }

        return testHealthPeriodicLogger;
    }
}
