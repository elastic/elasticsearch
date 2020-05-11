/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.cleaner;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
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

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

        TimeValue expected = TimeValue.timeValueHours(1);
        Settings settings = Settings.builder().put(MonitoringField.HISTORY_DURATION.getKey(), expected.getStringRep()).build();

        new CleanerService(settings, clusterSettings, threadPool, licenseState);
    }

    public void testGetRetentionWithSettingWithUpdatesAllowed() {
        TimeValue expected = TimeValue.timeValueHours(25);
        Settings settings = Settings.builder().put(MonitoringField.HISTORY_DURATION.getKey(), expected.getStringRep()).build();

        when(licenseState.isAllowed(Feature.MONITORING_UPDATE_RETENTION)).thenReturn(true);

        assertEquals(expected, new CleanerService(settings, clusterSettings, threadPool, licenseState).getRetention());

        verify(licenseState).isAllowed(Feature.MONITORING_UPDATE_RETENTION);
    }

    public void testGetRetentionDefaultValueWithNoSettings() {
        when(licenseState.isAllowed(Feature.MONITORING_UPDATE_RETENTION)).thenReturn(true);

        assertEquals(MonitoringField.HISTORY_DURATION.get(Settings.EMPTY),
                     new CleanerService(Settings.EMPTY, clusterSettings, threadPool, licenseState).getRetention());

        verify(licenseState).isAllowed(Feature.MONITORING_UPDATE_RETENTION);
    }

    public void testGetRetentionDefaultValueWithSettingsButUpdatesNotAllowed() {
        TimeValue notExpected = TimeValue.timeValueHours(25);
        Settings settings = Settings.builder().put(MonitoringField.HISTORY_DURATION.getKey(), notExpected.getStringRep()).build();

        when(licenseState.isAllowed(Feature.MONITORING_UPDATE_RETENTION)).thenReturn(false);

        assertEquals(MonitoringField.HISTORY_DURATION.get(Settings.EMPTY),
                     new CleanerService(settings, clusterSettings, threadPool, licenseState).getRetention());

        verify(licenseState).isAllowed(Feature.MONITORING_UPDATE_RETENTION);
    }

    public void testSetGlobalRetention() {
        // Note: I used this value to ensure we're not double-validating the setter; the cluster state should be the
        // only thing calling this method and it will use the settings object to validate the time value
        TimeValue expected = TimeValue.timeValueHours(2);

        when(licenseState.isAllowed(Feature.MONITORING_UPDATE_RETENTION)).thenReturn(true);

        CleanerService service = new CleanerService(Settings.EMPTY, clusterSettings, threadPool, licenseState);

        service.setGlobalRetention(expected);

        assertEquals(expected, service.getRetention());

        verify(licenseState, times(2)).isAllowed(Feature.MONITORING_UPDATE_RETENTION); // once by set, once by get
    }

    public void testSetGlobalRetentionAppliesEvenIfLicenseDisallows() {
        // Note: I used this value to ensure we're not double-validating the setter; the cluster state should be the
        // only thing calling this method and it will use the settings object to validate the time value
        TimeValue expected = TimeValue.timeValueHours(2);

        // required to be true on the second call for it to see it take effect
        when(licenseState.isAllowed(Feature.MONITORING_UPDATE_RETENTION)).thenReturn(false).thenReturn(true);

        CleanerService service = new CleanerService(Settings.EMPTY, clusterSettings, threadPool, licenseState);

        // uses allow=false
        service.setGlobalRetention(expected);

        // uses allow=true
        assertEquals(expected, service.getRetention());

        verify(licenseState, times(2)).isAllowed(Feature.MONITORING_UPDATE_RETENTION);
    }

    public void testNextExecutionDelay() {
        CleanerService.ExecutionScheduler scheduler = new CleanerService.DefaultExecutionScheduler();

        ZonedDateTime now = ZonedDateTime.of(2015, 1, 1, 0, 0,0,0, ZoneOffset.UTC);
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(TimeValue.timeValueHours(1).millis()));

        now = ZonedDateTime.of(2015, 1, 1, 1, 0, 0, 0, ZoneOffset.UTC);
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(TimeValue.timeValueHours(24).millis()));

        now = ZonedDateTime.of(2015, 1, 1, 0, 59, 0, 0, ZoneOffset.UTC);
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(TimeValue.timeValueMinutes(1).millis()));

        now = ZonedDateTime.of(2015, 1, 1, 23, 59, 0, 0, ZoneOffset.UTC);
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(TimeValue.timeValueMinutes(60 + 1).millis()));

        ZoneId defaultZone = Clock.systemDefaultZone().getZone();
        now = ZonedDateTime.of(2015, 1, 1, 12, 34, 56, 0, defaultZone);
        long nextScheduledMillis = ZonedDateTime.of(2015, 1, 2, 1, 0, 0,0,
            defaultZone).toInstant().toEpochMilli();
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(nextScheduledMillis - now.toInstant().toEpochMilli()));

    }

    public void testExecution() throws InterruptedException {
        final int nbExecutions = randomIntBetween(1, 3);
        CountDownLatch latch = new CountDownLatch(nbExecutions);

        logger.debug("--> creates a cleaner service that cleans every second");
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isAllowed(Feature.MONITORING)).thenReturn(true);
        CleanerService service = new CleanerService(Settings.EMPTY, clusterSettings, licenseState, threadPool,
                new TestExecutionScheduler(1_000));

        logger.debug("--> registers cleaning listener");
        TestListener listener = new TestListener(latch);
        service.add(listener);

        try {
            logger.debug("--> starts cleaning service");
            service.start();

            logger.debug("--> waits for listener to be executed");
            if (!latch.await(10, TimeUnit.SECONDS)) {
                fail("waiting too long for test to complete. Expected listener was not executed");
            }
        } finally {
            service.stop();
        }
        assertThat(latch.getCount(), equalTo(0L));
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
