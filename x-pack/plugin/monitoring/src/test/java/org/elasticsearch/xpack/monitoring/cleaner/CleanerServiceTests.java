/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.cleaner;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.monitoring.MonitoringField;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.time.Clock;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class CleanerServiceTests extends ESTestCase {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final XPackLicenseState licenseState = mock(XPackLicenseState.class);
    private ClusterSettings clusterSettings;
    private ThreadPool threadPool;

    @Before
    public void start() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, Collections.singleton(MonitoringField.HISTORY_DURATION));
        threadPool = new TestThreadPool("CleanerServiceTests");
    }

    @After
    public void stop() throws InterruptedException {
        terminate(threadPool);
    }

    public void testConstructorWithInvalidRetention() {
        // invalid setting
        expectedException.expect(IllegalArgumentException.class);

        try {
            TimeValue expected = TimeValue.timeValueHours(1);
            Settings settings = Settings.builder().put(MonitoringField.HISTORY_DURATION.getKey(), expected.getStringRep()).build();

            new CleanerService(settings, clusterSettings, threadPool, licenseState);
        } finally {
            assertWarnings(
                "[xpack.monitoring.history.duration] setting was deprecated in Elasticsearch and will be removed in a future release."
            );
        }
    }

    public void testGetRetention() {
        TimeValue expected = TimeValue.timeValueHours(25);
        Settings settings = Settings.builder().put(MonitoringField.HISTORY_DURATION.getKey(), expected.getStringRep()).build();

        assertEquals(expected, new CleanerService(settings, clusterSettings, threadPool, licenseState).getRetention());

        assertWarnings(
            "[xpack.monitoring.history.duration] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testSetGlobalRetention() {
        // Note: I used this value to ensure we're not double-validating the setter; the cluster state should be the
        // only thing calling this method and it will use the settings object to validate the time value
        TimeValue expected = TimeValue.timeValueHours(2);

        CleanerService service = new CleanerService(Settings.EMPTY, clusterSettings, threadPool, licenseState);
        service.setGlobalRetention(expected);
        assertEquals(expected, service.getRetention());
    }

    public void testNextExecutionDelay() {
        CleanerService.ExecutionScheduler scheduler = new CleanerService.DefaultExecutionScheduler();

        ZonedDateTime now = ZonedDateTime.of(2015, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(TimeValue.timeValueHours(1).millis()));

        now = ZonedDateTime.of(2015, 1, 1, 1, 0, 0, 0, ZoneOffset.UTC);
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(TimeValue.timeValueHours(24).millis()));

        now = ZonedDateTime.of(2015, 1, 1, 0, 59, 0, 0, ZoneOffset.UTC);
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(TimeValue.timeValueMinutes(1).millis()));

        now = ZonedDateTime.of(2015, 1, 1, 23, 59, 0, 0, ZoneOffset.UTC);
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(TimeValue.timeValueMinutes(60 + 1).millis()));

        ZoneId defaultZone = Clock.systemDefaultZone().getZone();
        now = ZonedDateTime.of(2015, 1, 1, 12, 34, 56, 0, defaultZone);
        long nextScheduledMillis = ZonedDateTime.of(2015, 1, 2, 1, 0, 0, 0, defaultZone).toInstant().toEpochMilli();
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(nextScheduledMillis - now.toInstant().toEpochMilli()));

    }

    public void testExecution() throws InterruptedException {
        final int nbExecutions = randomIntBetween(1, 3);
        CountDownLatch latch = new CountDownLatch(nbExecutions);

        logger.debug("--> creates a cleaner service that cleans every second");
        XPackLicenseState mockLicenseState = mock(XPackLicenseState.class);
        CleanerService service = new CleanerService(
            Settings.EMPTY,
            clusterSettings,
            mockLicenseState,
            threadPool,
            new TestExecutionScheduler(1_000)
        );

        logger.debug("--> registers cleaning listener");
        TestListener listener = new TestListener(latch);
        service.add(listener);

        try {
            logger.debug("--> starts cleaning service");
            service.start();

            logger.debug("--> waits for listener to be executed");
            if (latch.await(10, TimeUnit.SECONDS) == false) {
                fail("waiting too long for test to complete. Expected listener was not executed");
            }
        } finally {
            service.stop();
        }
        assertThat(latch.getCount(), equalTo(0L));
    }

    public void testLifecycle() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var mockLicenseState = mock(XPackLicenseState.class);

        CleanerService service = new CleanerService(
            Settings.EMPTY,
            clusterSettings,
            mockLicenseState,
            threadPool,
            new TestExecutionScheduler(1_000)
        );

        final var cleanupCount = new AtomicInteger();
        service.add(ignored -> cleanupCount.incrementAndGet());

        service.start();
        while (cleanupCount.get() < 10) {
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
        }

        service.stop();
        if (randomBoolean()) {
            service.close();
        }
        deterministicTaskQueue.runAllTasks(); // ensures the scheduling stops
        assertEquals(10, cleanupCount.get());
    }

    class TestListener implements CleanerService.Listener {

        final CountDownLatch latch;

        TestListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onCleanUpIndices(TimeValue retention) {
            latch.countDown();
        }
    }

    class TestExecutionScheduler implements CleanerService.ExecutionScheduler {

        final long offset;

        TestExecutionScheduler(long offset) {
            this.offset = offset;
        }

        @Override
        public TimeValue nextExecutionDelay(ZonedDateTime now) {
            return TimeValue.timeValueMillis(offset);
        }
    }
}
