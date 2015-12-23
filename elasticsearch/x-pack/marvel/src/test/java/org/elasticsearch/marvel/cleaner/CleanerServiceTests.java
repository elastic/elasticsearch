/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.cleaner;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.marvel.license.MarvelLicensee;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CleanerServiceTests extends ESTestCase {

    private ClusterSettings clusterSettings;
    private TimeValue defaultRetention;
    private ThreadPool threadPool;

    @Before
    public void start() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<Setting<?>>(Arrays.asList(CleanerService.HISTORY_SETTING)));
        defaultRetention = TimeValue.parseTimeValue("7d", null, "");
        threadPool = new ThreadPool("CleanerServiceTests");
    }

    @After
    public void stop() throws InterruptedException {
        terminate(threadPool);
    }

    public void testRetentionDefaultValue() {
        MarvelLicensee licensee = mock(MarvelLicensee.class);
        when(licensee.allowUpdateRetention()).thenReturn(false);
        assertNull(new CleanerService(Settings.EMPTY, clusterSettings, threadPool, licensee).getRetention());
    }

    public void testRetentionUpdateAllowed() {
        TimeValue randomRetention = TimeValue.parseTimeValue(randomTimeValue(), null, "");

        MarvelLicensee licensee = mock(MarvelLicensee.class);
        when(licensee.allowUpdateRetention()).thenReturn(true);

        CleanerService service = new CleanerService(Settings.EMPTY, clusterSettings, threadPool, licensee);
        service.setRetention(randomRetention);
        assertThat(service.getRetention(), equalTo(randomRetention));

        try {
            service.validateRetention(randomRetention);
        } catch (IllegalArgumentException e) {
            fail("fail to validate new value of retention");
        }
    }

    public void testRetentionUpdateBlocked() {
        TimeValue randomRetention = TimeValue.parseTimeValue(randomTimeValue(), null, "");

        MarvelLicensee licensee = mock(MarvelLicensee.class);
        when(licensee.allowUpdateRetention()).thenReturn(false);

        CleanerService service = new CleanerService(Settings.EMPTY, clusterSettings, threadPool, licensee);
        try {
            service.setRetention(randomRetention);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("license does not allow the history duration setting to be updated to value"));
            assertNull(service.getRetention());
        }

        try {
            service.validateRetention(randomRetention);
            fail("exception should have been thrown");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("license does not allow the history duration setting to be updated to value"));
        }
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
        MarvelLicensee licensee = mock(MarvelLicensee.class);
        when(licensee.cleaningEnabled()).thenReturn(true);
        CleanerService service = new CleanerService(Settings.EMPTY, clusterSettings, licensee, threadPool, new TestExecutionScheduler(1_000));

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
