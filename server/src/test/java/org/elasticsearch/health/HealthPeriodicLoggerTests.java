/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health;

import org.apache.logging.log4j.Level;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorServiceTests;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.node.DiskHealthIndicatorService;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongGaugeMetric;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.PRIMARY_UNASSIGNED_IMPACT_ID;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.REPLICA_UNASSIGNED_IMPACT_ID;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.node.DiskHealthIndicatorService.IMPACT_INGEST_UNAVAILABLE_ID;
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
            if (testHealthPeriodicLogger.lifecycleState() == Lifecycle.State.STARTED) {
                testHealthPeriodicLogger.stop();
            }
            if (testHealthPeriodicLogger.lifecycleState() == Lifecycle.State.INITIALIZED
                || testHealthPeriodicLogger.lifecycleState() == Lifecycle.State.STOPPED) {
                testHealthPeriodicLogger.close();
            }
        }
        threadPool.shutdownNow();
    }

    public void testConvertToLoggedFields() {
        var results = getTestIndicatorResults();
        var overallStatus = HealthStatus.merge(results.stream().map(HealthIndicatorResult::status));

        Map<String, Object> loggerResults = HealthPeriodicLogger.convertToLoggedFields(results);

        // verify that the number of fields is the number of indicators + 7
        // (for overall and for message, plus details for the two yellow indicators, plus three impact)
        assertThat(loggerResults.size(), equalTo(results.size() + 7));

        // test indicator status
        assertThat(loggerResults.get(makeHealthStatusString("master_is_stable")), equalTo("green"));
        assertThat(loggerResults.get(makeHealthStatusString("disk")), equalTo("yellow"));
        assertThat(
            loggerResults.get(makeHealthDetailsString("disk")),
            equalTo(
                getTestIndicatorResults().stream()
                    .filter(i -> i.name().equals("disk"))
                    .findFirst()
                    .map(HealthIndicatorResult::details)
                    .map(Strings::toString)
                    .orElseThrow()
            )
        );
        assertThat(loggerResults.get(makeHealthStatusString("shards_availability")), equalTo("yellow"));
        assertThat(
            loggerResults.get(makeHealthDetailsString("shards_availability")),
            equalTo(
                getTestIndicatorResults().stream()
                    .filter(i -> i.name().equals("shards_availability"))
                    .findFirst()
                    .map(HealthIndicatorResult::details)
                    .map(Strings::toString)
                    .orElseThrow()
            )
        );

        // test calculated overall status
        assertThat(loggerResults.get(makeHealthStatusString("overall")), equalTo(overallStatus.xContentValue()));

        // test calculated message
        assertThat(
            loggerResults.get(HealthPeriodicLogger.MESSAGE_FIELD),
            equalTo(String.format(Locale.ROOT, "health=%s [disk,shards_availability]", overallStatus.xContentValue()))
        );

        // test impact
        assertThat(loggerResults.get(makeHealthImpactString(DiskHealthIndicatorService.NAME, IMPACT_INGEST_UNAVAILABLE_ID)), equalTo(true));
        assertThat(
            loggerResults.get(makeHealthImpactString(ShardsAvailabilityHealthIndicatorService.NAME, PRIMARY_UNASSIGNED_IMPACT_ID)),
            equalTo(true)
        );
        assertThat(
            loggerResults.get(makeHealthImpactString(ShardsAvailabilityHealthIndicatorService.NAME, REPLICA_UNASSIGNED_IMPACT_ID)),
            equalTo(true)
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
        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(clusterService, testHealthService, randomBoolean());

        // test that it knows that it's not initially the health node
        assertFalse(testHealthPeriodicLogger.isHealthNode());

        // trigger a cluster change and recheck
        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
        assertTrue(testHealthPeriodicLogger.isHealthNode());
    }

    public void testJobScheduling() throws Exception {
        HealthService testHealthService = this.getMockedHealthService();

        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(clusterService, testHealthService, false);

        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
        assertTrue("local node should be the health node", testHealthPeriodicLogger.isHealthNode());
        assertTrue("health logger should be enabled", testHealthPeriodicLogger.enabled());

        // Even if this is the health node, we do not schedule a job because the service is not started yet
        assertNull(testHealthPeriodicLogger.getScheduler());
        // Starting the service should schedule a try to schedule a run
        testHealthPeriodicLogger.start();
        AtomicReference<SchedulerEngine> scheduler = new AtomicReference<>();
        assertBusy(() -> {
            var s = testHealthPeriodicLogger.getScheduler();
            assertNotNull(s);
            scheduler.set(s);
        });
        assertTrue(scheduler.get().scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME));

        // Changing the health node should cancel the run
        ClusterState noHealthNode = ClusterStateCreationUtils.state(node2, node1, new DiscoveryNode[] { node1, node2 });
        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", noHealthNode, stateWithLocalHealthNode));
        assertFalse(testHealthPeriodicLogger.isHealthNode());
        assertFalse(scheduler.get().scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME));
    }

    public void testEnabled() {
        HealthService testHealthService = this.getMockedHealthService();
        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(clusterService, testHealthService, true);

        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
        verifyLoggerIsReadyToRun(testHealthPeriodicLogger);

        // disable it and then verify that the job is gone
        {
            this.clusterSettings.applySettings(Settings.builder().put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), false).build());
            assertFalse(testHealthPeriodicLogger.enabled());
            assertFalse(
                testHealthPeriodicLogger.getScheduler().scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME)
            );
        }

        // enable it and then verify that the job is created
        {
            this.clusterSettings.applySettings(Settings.builder().put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), true).build());
            assertTrue(testHealthPeriodicLogger.enabled());
            assertTrue(
                testHealthPeriodicLogger.getScheduler().scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME)
            );
        }
        // ensure the job is not recreated during enabling if the service has stopped
        {
            testHealthPeriodicLogger.stop();
            assertFalse(
                testHealthPeriodicLogger.getScheduler().scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME)
            );
            this.clusterSettings.applySettings(Settings.builder().put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), true).build());
            assertTrue(testHealthPeriodicLogger.enabled());
            assertFalse(
                testHealthPeriodicLogger.getScheduler().scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME)
            );
        }
    }

    public void testUpdatePollInterval() {
        HealthService testHealthService = this.getMockedHealthService();
        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(clusterService, testHealthService, false);
        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
        assertTrue("local node should be the health node", testHealthPeriodicLogger.isHealthNode());
        assertTrue("health logger should be enabled", testHealthPeriodicLogger.enabled());
        // Ensure updating the poll interval won't trigger a job when service not started
        {
            TimeValue pollInterval = TimeValue.timeValueSeconds(randomIntBetween(15, 59));
            this.clusterSettings.applySettings(
                Settings.builder()
                    .put(HealthPeriodicLogger.POLL_INTERVAL_SETTING.getKey(), pollInterval)
                    // Since the default value of enabled is false, if we do not set it here it disable it
                    .put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), true)
                    .build()
            );
            assertTrue("health logger should be enabled", testHealthPeriodicLogger.enabled());
            assertEquals(pollInterval, testHealthPeriodicLogger.getPollInterval());
            assertNull(testHealthPeriodicLogger.getScheduler());
        }

        testHealthPeriodicLogger.start();
        // Start the service and check it's scheduled
        {
            TimeValue pollInterval = TimeValue.timeValueSeconds(randomIntBetween(15, 59));
            this.clusterSettings.applySettings(
                Settings.builder()
                    .put(HealthPeriodicLogger.POLL_INTERVAL_SETTING.getKey(), pollInterval)
                    // Since the default value of enabled is false, if we do not set it here it disable it
                    .put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), true)
                    .build()
            );
            assertEquals(pollInterval, testHealthPeriodicLogger.getPollInterval());
            verifyLoggerIsReadyToRun(testHealthPeriodicLogger);
            assertNotNull(testHealthPeriodicLogger.getScheduler());
            assertTrue(
                testHealthPeriodicLogger.getScheduler().scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME)
            );
        }

        // Poll interval doesn't schedule a job when disabled
        {
            TimeValue pollInterval = TimeValue.timeValueSeconds(randomIntBetween(15, 59));
            this.clusterSettings.applySettings(
                Settings.builder()
                    .put(HealthPeriodicLogger.POLL_INTERVAL_SETTING.getKey(), pollInterval)
                    .put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), false)
                    .build()
            );
            assertFalse(testHealthPeriodicLogger.enabled());
            assertEquals(pollInterval, testHealthPeriodicLogger.getPollInterval());
            assertFalse(
                testHealthPeriodicLogger.getScheduler().scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME)
            );
            // Re-enable
            this.clusterSettings.applySettings(
                Settings.builder().put(HealthPeriodicLogger.POLL_INTERVAL_SETTING.getKey(), pollInterval).build()
            );
        }

        testHealthPeriodicLogger.stop();
        // verify that updating the polling interval doesn't schedule the job if it's stopped
        {
            this.clusterSettings.applySettings(
                Settings.builder()
                    .put(HealthPeriodicLogger.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(30))
                    .put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), true)
                    .build()
            );
            assertTrue("health logger should be enabled", testHealthPeriodicLogger.enabled());
            assertFalse(
                testHealthPeriodicLogger.getScheduler().scheduledJobIds().contains(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME)
            );
        }
    }

    public void testTriggeredJobCallsTryToLogHealth() throws Exception {
        AtomicBoolean calledGetHealth = new AtomicBoolean();
        HealthService testHealthService = this.getMockedHealthService();
        doAnswer(invocation -> {
            ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
            assertNotNull(listener);
            calledGetHealth.set(true);
            listener.onResponse(getTestIndicatorResults());
            return null;
        }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());
        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, true);
        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));

        verifyLoggerIsReadyToRun(testHealthPeriodicLogger);

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);
        testHealthPeriodicLogger.triggered(event);
        assertBusy(() -> assertTrue(calledGetHealth.get()));
    }

    public void testResultFailureHandling() throws Exception {
        AtomicInteger getHealthCalled = new AtomicInteger(0);

        HealthService testHealthService = this.getMockedHealthService();

        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, true);
        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
        assertTrue("local node should be the health node", testHealthPeriodicLogger.isHealthNode());

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);

        // run it and call the listener's onFailure
        {
            doAnswer(invocation -> {
                ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
                listener.onFailure(new Exception("fake failure"));
                getHealthCalled.incrementAndGet();
                return null;
            }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());
            testHealthPeriodicLogger.triggered(event);
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
            testHealthPeriodicLogger.triggered(event);
            assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(2)));
        }
    }

    public void testTryToLogHealthConcurrencyControlWithResults() throws Exception {
        AtomicInteger getHealthCalled = new AtomicInteger(0);

        CountDownLatch waitForSecondRun = new CountDownLatch(1);
        CountDownLatch waitForRelease = new CountDownLatch(1);
        HealthService testHealthService = this.getMockedHealthService();
        doAnswer(invocation -> {
            // get and call the results listener provided to getHealth
            ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
            getHealthCalled.incrementAndGet();
            waitForSecondRun.await();
            listener.onResponse(getTestIndicatorResults());
            waitForRelease.countDown();
            return null;
        }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());

        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, true);
        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
        verifyLoggerIsReadyToRun(testHealthPeriodicLogger);

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);

        // run it once, verify getHealth is called
        {
            Thread logHealthThread = new Thread(() -> testHealthPeriodicLogger.triggered(event));
            logHealthThread.start();
            // We wait to verify that the triggered even is in progress, then we block, so it will rename in progress
            assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(1)));
            // We try to log again while it's in progress, we expect this run to be skipped
            assertFalse(testHealthPeriodicLogger.tryToLogHealth());
            // Unblock the first execution
            waitForSecondRun.countDown();
        }

        // run it again, verify getHealth is called, because we are calling the results listener
        {
            waitForRelease.await();
            testHealthPeriodicLogger.triggered(event);
            assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(2)));
        }
    }

    public void testTryToLogHealthConcurrencyControl() throws Exception {
        AtomicInteger getHealthCalled = new AtomicInteger(0);

        CountDownLatch waitForSecondRun = new CountDownLatch(1);
        CountDownLatch waitForRelease = new CountDownLatch(1);

        HealthService testHealthService = this.getMockedHealthService();
        doAnswer(invocation -> {
            // get but do not call the provided listener immediately
            ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
            assertNotNull(listener);

            // note that we received the getHealth call
            getHealthCalled.incrementAndGet();

            // wait for the next run that should be skipped
            waitForSecondRun.await();
            // we can continue now
            listener.onResponse(getTestIndicatorResults());
            waitForRelease.countDown();
            return null;
        }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());

        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, false);
        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
        assertTrue("local node should be the health node", testHealthPeriodicLogger.isHealthNode());

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);

        // call it and verify that getHealth is called
        {
            Thread logHealthThread = new Thread(() -> testHealthPeriodicLogger.triggered(event));
            logHealthThread.start();
            assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(1)));
        }

        // run it again, verify that it's skipped because the other one is in progress
        {
            assertFalse(testHealthPeriodicLogger.tryToLogHealth());
            // Unblock the first execution
            waitForSecondRun.countDown();
        }

        // run it again, verify getHealth is called, because we are calling the results listener
        {
            waitForRelease.await();
            testHealthPeriodicLogger.triggered(event);
            assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(2)));
        }
    }

    public void testTryToLogHealthConcurrencyControlWithException() throws Exception {
        AtomicInteger getHealthCalled = new AtomicInteger(0);

        HealthService testHealthService = this.getMockedHealthService();

        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, false);
        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
        assertTrue("local node should be the health node", testHealthPeriodicLogger.isHealthNode());

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);

        // run it once and trigger an exception during the getHealth call
        {
            doThrow(new ResourceNotFoundException("No preflight indicators")).when(testHealthService)
                .getHealth(any(), isNull(), anyBoolean(), anyInt(), any());
            testHealthPeriodicLogger.triggered(event);
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
            testHealthPeriodicLogger.triggered(event);
            assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(1)));
        }
    }

    public void testClosingWhenRunInProgress() throws Exception {
        // Check that closing will still happen even if the run doesn't finish
        {
            AtomicInteger getHealthCalled = new AtomicInteger(0);

            HealthService testHealthService = this.getMockedHealthService();
            doAnswer(invocation -> {
                ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
                assertNotNull(listener);

                // note that we received the getHealth call
                getHealthCalled.incrementAndGet();
                return null;
            }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());

            HealthPeriodicLogger healthLoggerThatWillNotFinish = createAndInitHealthPeriodicLogger(
                this.clusterService,
                testHealthService,
                true
            );
            healthLoggerThatWillNotFinish.clusterChanged(
                new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE)
            );
            assertTrue("local node should be the health node", healthLoggerThatWillNotFinish.isHealthNode());
            assertTrue("health logger should be enabled", healthLoggerThatWillNotFinish.enabled());

            SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);

            // call it and verify that it's in progress
            {
                healthLoggerThatWillNotFinish.triggered(event);
                assertBusy(() -> assertThat(getHealthCalled.get(), equalTo(1)));
            }
            healthLoggerThatWillNotFinish.stop();
            assertEquals(Lifecycle.State.STOPPED, healthLoggerThatWillNotFinish.lifecycleState());
            // Close and wait out the timeout
            healthLoggerThatWillNotFinish.close();
            assertBusy(() -> assertEquals(Lifecycle.State.CLOSED, healthLoggerThatWillNotFinish.lifecycleState()), 5, TimeUnit.SECONDS);
        }

        // Ensure it will wait until it finishes before it closes
        {
            AtomicInteger getHealthCalled = new AtomicInteger(0);

            CountDownLatch waitForCloseToBeTriggered = new CountDownLatch(1);
            CountDownLatch waitForRelease = new CountDownLatch(1);

            HealthService testHealthService = this.getMockedHealthService();
            doAnswer(invocation -> {
                // get but do not call the provided listener immediately
                ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
                assertNotNull(listener);

                // note that we received the getHealth call
                getHealthCalled.incrementAndGet();

                // wait for the close signal
                waitForCloseToBeTriggered.await();
                // we can continue now
                listener.onResponse(getTestIndicatorResults());
                waitForRelease.countDown();
                return null;
            }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());

            testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, true);
            testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
            verifyLoggerIsReadyToRun(testHealthPeriodicLogger);

            SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);

            // call it and verify that getHealth is called
            {
                Thread logHealthThread = new Thread(() -> testHealthPeriodicLogger.triggered(event));
                logHealthThread.start();
                assertBusy(() -> assertTrue(testHealthPeriodicLogger.currentlyRunning()));
            }

            // stop and close it
            {
                testHealthPeriodicLogger.stop();
                assertEquals(Lifecycle.State.STOPPED, testHealthPeriodicLogger.lifecycleState());
                assertTrue(testHealthPeriodicLogger.currentlyRunning());
                Thread closeHealthLogger = new Thread(() -> testHealthPeriodicLogger.close());
                closeHealthLogger.start();
                assertBusy(() -> assertTrue(testHealthPeriodicLogger.waitingToFinishCurrentRun()));
                waitForCloseToBeTriggered.countDown();
                assertBusy(() -> assertEquals(Lifecycle.State.CLOSED, testHealthPeriodicLogger.lifecycleState()));
            }
        }
    }

    public void testLoggingHappens() {
        try (var mockLog = MockLog.capture(HealthPeriodicLogger.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "overall",
                    HealthPeriodicLogger.class.getCanonicalName(),
                    Level.INFO,
                    String.format(Locale.ROOT, "%s=\"yellow\"", makeHealthStatusString("overall"))
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "master_is_stable",
                    HealthPeriodicLogger.class.getCanonicalName(),
                    Level.INFO,
                    String.format(Locale.ROOT, "%s=\"green\"", makeHealthStatusString("master_is_stable"))
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "disk",
                    HealthPeriodicLogger.class.getCanonicalName(),
                    Level.INFO,
                    String.format(Locale.ROOT, "%s=\"yellow\"", makeHealthStatusString("disk"))
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "ilm",
                    HealthPeriodicLogger.class.getCanonicalName(),
                    Level.INFO,
                    String.format(Locale.ROOT, "%s=\"red\"", makeHealthStatusString("ilm"))
                )
            );

            HealthService testHealthService = this.getMockedHealthService();
            doAnswer(invocation -> {
                ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
                assertNotNull(listener);
                listener.onResponse(getTestIndicatorResults());
                return null;
            }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());
            testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, false);

            // switch to Log only mode
            this.clusterSettings.applySettings(
                Settings.builder()
                    .put(HealthPeriodicLogger.OUTPUT_MODE_SETTING.getKey(), HealthPeriodicLogger.OutputMode.LOGS)
                    .put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), true)
                    .build()
            );
            testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
            assertTrue("local node should be the health node", testHealthPeriodicLogger.isHealthNode());

            SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);
            testHealthPeriodicLogger.triggered(event);
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testOutputModeNoLogging() {
        try (var mockLog = MockLog.capture(HealthPeriodicLogger.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "overall",
                    HealthPeriodicLogger.class.getCanonicalName(),
                    Level.INFO,
                    String.format(Locale.ROOT, "%s=\"yellow\"", makeHealthStatusString("overall"))
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "master_is_stable",
                    HealthPeriodicLogger.class.getCanonicalName(),
                    Level.INFO,
                    String.format(Locale.ROOT, "%s=\"green\"", makeHealthStatusString("master_is_stable"))
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "disk",
                    HealthPeriodicLogger.class.getCanonicalName(),
                    Level.INFO,
                    String.format(Locale.ROOT, "%s=\"yellow\"", makeHealthStatusString("disk"))
                )
            );

            HealthService testHealthService = this.getMockedHealthService();
            doAnswer(invocation -> {
                ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
                assertNotNull(listener);
                listener.onResponse(getTestIndicatorResults());
                return null;
            }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());
            testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(this.clusterService, testHealthService, false);

            // switch to Metrics only mode
            this.clusterSettings.applySettings(
                Settings.builder()
                    .put(HealthPeriodicLogger.OUTPUT_MODE_SETTING.getKey(), HealthPeriodicLogger.OutputMode.METRICS)
                    .put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), true)
                    .build()
            );
            testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
            assertTrue("local node should be the health node", testHealthPeriodicLogger.isHealthNode());

            SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);
            testHealthPeriodicLogger.triggered(event);

            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testMetricsMode() {
        List<String> logs = new ArrayList<>();
        List<Long> metrics = new ArrayList<>();

        BiConsumer<LongGaugeMetric, Long> metricWriter = (metric, value) -> metrics.add(value);
        Consumer<ESLogMessage> logWriter = msg -> logs.add(msg.asString());
        List<HealthIndicatorResult> results = getTestIndicatorResultsWithRed();
        HealthService testHealthService = this.getMockedHealthService();
        doAnswer(invocation -> {
            ActionListener<List<HealthIndicatorResult>> listener = invocation.getArgument(4);
            assertNotNull(listener);
            listener.onResponse(results);
            return null;
        }).when(testHealthService).getHealth(any(), isNull(), anyBoolean(), anyInt(), any());
        testHealthPeriodicLogger = createAndInitHealthPeriodicLogger(
            this.clusterService,
            testHealthService,
            false,
            metricWriter,
            logWriter
        );

        // switch to Metrics only mode
        this.clusterSettings.applySettings(
            Settings.builder()
                .put(HealthPeriodicLogger.OUTPUT_MODE_SETTING.getKey(), HealthPeriodicLogger.OutputMode.METRICS)
                .put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), true)
                .build()
        );
        testHealthPeriodicLogger.clusterChanged(new ClusterChangedEvent("test", stateWithLocalHealthNode, ClusterState.EMPTY_STATE));
        assertTrue("local node should be the health node", testHealthPeriodicLogger.isHealthNode());

        assertEquals(0, metrics.size());

        SchedulerEngine.Event event = new SchedulerEngine.Event(HealthPeriodicLogger.HEALTH_PERIODIC_LOGGER_JOB_NAME, 0, 0);
        testHealthPeriodicLogger.triggered(event);

        assertEquals(0, logs.size());
        assertEquals(4, metrics.size());
    }

    private void verifyLoggerIsReadyToRun(HealthPeriodicLogger healthPeriodicLogger) {
        assertTrue("local node should be the health node", healthPeriodicLogger.isHealthNode());
        assertTrue("health logger should be enabled", healthPeriodicLogger.enabled());
        assertEquals("health logger is started", Lifecycle.State.STARTED, healthPeriodicLogger.lifecycleState());
    }

    private List<HealthIndicatorResult> getTestIndicatorResults() {
        var networkLatency = new HealthIndicatorResult("master_is_stable", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult(
            "disk",
            YELLOW,
            null,
            new SimpleHealthIndicatorDetails(
                Map.of(
                    "indices_with_readonly_block",
                    0,
                    "nodes_with_enough_disk_space",
                    1,
                    "nodes_with_unknown_disk_status",
                    0,
                    "nodes_over_high_watermark",
                    0,
                    "nodes_over_flood_stage_watermark",
                    1
                )
            ),
            List.of(
                new HealthIndicatorImpact(
                    DiskHealthIndicatorService.NAME,
                    IMPACT_INGEST_UNAVAILABLE_ID,
                    2,
                    "description",
                    List.of(ImpactArea.INGEST)
                )
            ),
            null
        );
        var shardsAvailable = new HealthIndicatorResult(
            "shards_availability",
            YELLOW,
            null,
            new SimpleHealthIndicatorDetails(ShardsAvailabilityHealthIndicatorServiceTests.addDefaults(Map.of())),
            List.of(
                new HealthIndicatorImpact(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    PRIMARY_UNASSIGNED_IMPACT_ID,
                    2,
                    "description",
                    List.of(ImpactArea.SEARCH)
                ),
                new HealthIndicatorImpact(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    REPLICA_UNASSIGNED_IMPACT_ID,
                    2,
                    "description",
                    List.of(ImpactArea.SEARCH)
                )
            ),
            null
        );

        return List.of(networkLatency, slowTasks, shardsAvailable);
    }

    private List<HealthIndicatorResult> getTestIndicatorResultsAllGreen() {
        var networkLatency = new HealthIndicatorResult("master_is_stable", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("disk", GREEN, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult(
            "shards_availability",
            GREEN,
            null,
            new SimpleHealthIndicatorDetails(ShardsAvailabilityHealthIndicatorServiceTests.addDefaults(Map.of())),
            null,
            null
        );

        return List.of(networkLatency, slowTasks, shardsAvailable);
    }

    private List<HealthIndicatorResult> getTestIndicatorResultsWithRed() {
        var networkLatency = new HealthIndicatorResult("master_is_stable", GREEN, null, null, null, null);
        var slowTasks = new HealthIndicatorResult("disk", GREEN, null, null, null, null);
        var shardsAvailable = new HealthIndicatorResult(
            "shards_availability",
            RED,
            null,
            new SimpleHealthIndicatorDetails(ShardsAvailabilityHealthIndicatorServiceTests.addDefaults(Map.of("unassigned_primaries", 1))),
            null,
            null
        );

        return List.of(networkLatency, slowTasks, shardsAvailable);
    }

    private String makeHealthStatusString(String key) {
        return String.format(Locale.ROOT, "%s.%s.status", HealthPeriodicLogger.HEALTH_FIELD_PREFIX, key);
    }

    private String makeHealthDetailsString(String key) {
        return String.format(Locale.ROOT, "%s.%s.details", HealthPeriodicLogger.HEALTH_FIELD_PREFIX, key);
    }

    private String makeHealthImpactString(String indicatorName, String impact) {
        return String.format(Locale.ROOT, "%s.%s.%s.impacted", HealthPeriodicLogger.HEALTH_FIELD_PREFIX, indicatorName, impact);
    }

    private HealthPeriodicLogger createAndInitHealthPeriodicLogger(
        ClusterService clusterService,
        HealthService testHealthService,
        boolean started
    ) {
        return createAndInitHealthPeriodicLogger(clusterService, testHealthService, started, null, null);
    }

    private HealthPeriodicLogger createAndInitHealthPeriodicLogger(
        ClusterService clusterService,
        HealthService testHealthService,
        boolean started,
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
        if (started) {
            testHealthPeriodicLogger.start();
        }
        // Reset cluster setting
        clusterSettings.applySettings(Settings.EMPTY);
        // enable
        clusterSettings.applySettings(Settings.builder().put(HealthPeriodicLogger.ENABLED_SETTING.getKey(), true).build());

        return testHealthPeriodicLogger;
    }
}
