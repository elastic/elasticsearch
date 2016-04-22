/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.cleaner;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.MonitoringLicensee;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

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

    private final MonitoringLicensee licensee = mock(MonitoringLicensee.class);
    private ClusterSettings clusterSettings;
    private ThreadPool threadPool;

    @Before
    public void start() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, Collections.singleton(MonitoringSettings.HISTORY_DURATION));
        threadPool = new ThreadPool("CleanerServiceTests");
    }

    @After
    public void stop() throws InterruptedException {
        terminate(threadPool);
    }

    public void testConstructorWithInvalidRetention() {
        // invalid setting
        expectedException.expect(IllegalArgumentException.class);

        TimeValue expected = TimeValue.timeValueHours(1);
        Settings settings = Settings.builder().put(MonitoringSettings.HISTORY_DURATION.getKey(), expected.getStringRep()).build();

        new CleanerService(settings, clusterSettings, threadPool, licensee);
    }

    public void testGetRetentionWithSettingWithUpdatesAllowed() {
        TimeValue expected = TimeValue.timeValueHours(25);
        Settings settings = Settings.builder().put(MonitoringSettings.HISTORY_DURATION.getKey(), expected.getStringRep()).build();

        when(licensee.allowUpdateRetention()).thenReturn(true);

        assertEquals(expected, new CleanerService(settings, clusterSettings, threadPool, licensee).getRetention());

        verify(licensee).allowUpdateRetention();
    }

    public void testGetRetentionDefaultValueWithNoSettings() {
        when(licensee.allowUpdateRetention()).thenReturn(true);

        assertEquals(MonitoringSettings.HISTORY_DURATION.get(Settings.EMPTY),
                     new CleanerService(Settings.EMPTY, clusterSettings, threadPool, licensee).getRetention());

        verify(licensee).allowUpdateRetention();
    }

    public void testGetRetentionDefaultValueWithSettingsButUpdatesNotAllowed() {
        TimeValue notExpected = TimeValue.timeValueHours(25);
        Settings settings = Settings.builder().put(MonitoringSettings.HISTORY_DURATION.getKey(), notExpected.getStringRep()).build();

        when(licensee.allowUpdateRetention()).thenReturn(false);

        assertEquals(MonitoringSettings.HISTORY_DURATION.get(Settings.EMPTY),
                     new CleanerService(settings, clusterSettings, threadPool, licensee).getRetention());

        verify(licensee).allowUpdateRetention();
    }

    public void testSetGlobalRetention() {
        // Note: I used this value to ensure we're not double-validating the setter; the cluster state should be the
        // only thing calling this method and it will use the settings object to validate the time value
        TimeValue expected = TimeValue.timeValueHours(2);

        when(licensee.allowUpdateRetention()).thenReturn(true);

        CleanerService service = new CleanerService(Settings.EMPTY, clusterSettings, threadPool, licensee);

        service.setGlobalRetention(expected);

        assertEquals(expected, service.getRetention());

        verify(licensee, times(2)).allowUpdateRetention(); // once by set, once by get
    }

    public void testSetGlobalRetentionAppliesEvenIfLicenseDisallows() {
        // Note: I used this value to ensure we're not double-validating the setter; the cluster state should be the
        // only thing calling this method and it will use the settings object to validate the time value
        TimeValue expected = TimeValue.timeValueHours(2);

        // required to be true on the second call for it to see it take effect
        when(licensee.allowUpdateRetention()).thenReturn(false).thenReturn(true);

        CleanerService service = new CleanerService(Settings.EMPTY, clusterSettings, threadPool, licensee);

        // uses allow=false
        service.setGlobalRetention(expected);

        // uses allow=true
        assertEquals(expected, service.getRetention());

        verify(licensee, times(2)).allowUpdateRetention();
    }

    public void testNextExecutionDelay() {
        CleanerService.ExecutionScheduler scheduler = new CleanerService.DefaultExecutionScheduler();

        DateTime now = new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC);
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(TimeValue.timeValueHours(1).millis()));

        now = new DateTime(2015, 1, 1, 1, 0, DateTimeZone.UTC);
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(TimeValue.timeValueHours(24).millis()));

        now = new DateTime(2015, 1, 1, 0, 59, DateTimeZone.UTC);
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(TimeValue.timeValueMinutes(1).millis()));

        now = new DateTime(2015, 1, 1, 23, 59, DateTimeZone.UTC);
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(TimeValue.timeValueMinutes(60 + 1).millis()));

        now = new DateTime(2015, 1, 1, 12, 34, 56);
        assertThat(scheduler.nextExecutionDelay(now).millis(), equalTo(new DateTime(2015, 1, 2, 1, 0, 0).getMillis() - now.getMillis()));

    }

    public void testExecution() throws InterruptedException {
        final int nbExecutions = randomIntBetween(1, 3);
        CountDownLatch latch = new CountDownLatch(nbExecutions);

        logger.debug("--> creates a cleaner service that cleans every second");
        MonitoringLicensee licensee = mock(MonitoringLicensee.class);
        when(licensee.cleaningEnabled()).thenReturn(true);
        CleanerService service = new CleanerService(Settings.EMPTY, clusterSettings, licensee, threadPool,
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
        public TimeValue nextExecutionDelay(DateTime now) {
            return TimeValue.timeValueMillis(offset);
        }
    }
}
