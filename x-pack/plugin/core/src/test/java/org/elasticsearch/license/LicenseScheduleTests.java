/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class LicenseScheduleTests extends ESTestCase {

    private License license;
    private SchedulerEngine.Schedule schedule;

    @Before
    public void setup() throws Exception {
        license = TestUtils.generateSignedLicense(TimeValue.timeValueDays(12));
        final ClusterStateLicenseService service = new ClusterStateLicenseService(
            Settings.EMPTY,
            mock(ThreadPool.class),
            mock(ClusterService.class),
            mock(Clock.class),
            mock(XPackLicenseState.class)
        );
        schedule = service.nextLicenseCheck(license);
    }

    public void testExpiredLicenseSchedule() throws Exception {
        long triggeredTime = license.expiryDate() + randomIntBetween(1, 1000);
        assertThat(schedule.nextScheduledTimeAfter(license.issueDate(), triggeredTime), equalTo(-1L));
    }

    public void testInvalidLicenseSchedule() throws Exception {
        long triggeredTime = license.issueDate() - randomIntBetween(1, 1000);
        assertThat(schedule.nextScheduledTimeAfter(triggeredTime, triggeredTime), equalTo(license.issueDate()));
    }

    public void testDailyWarningPeriod() {

        long millisInDay = TimeValue.timeValueDays(1).getMillis();
        long warningOffset = LicenseSettings.LICENSE_EXPIRATION_WARNING_PERIOD.getMillis();
        do {
            long nextOffset = license.expiryDate() - warningOffset;
            long triggeredTime = nextOffset + randomLongBetween(1, millisInDay);
            long expectedTime = nextOffset + millisInDay;
            long scheduledTime = schedule.nextScheduledTimeAfter(triggeredTime, triggeredTime);
            assertThat(
                String.format(
                    Locale.ROOT,
                    "Incorrect schedule:\nexpected  [%s]\ngot       [%s]\ntriggered [%s]\nexpiry    [%s]",
                    DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(expectedTime)),
                    DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(scheduledTime)),
                    DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(triggeredTime)),
                    DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(license.expiryDate()))
                ),
                scheduledTime,
                equalTo(expectedTime)
            );

            warningOffset -= millisInDay;
        } while (warningOffset > 0);
    }
}
