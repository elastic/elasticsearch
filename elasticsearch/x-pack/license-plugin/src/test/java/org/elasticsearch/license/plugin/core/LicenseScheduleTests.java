/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.TestUtils;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class LicenseScheduleTests extends ESTestCase {

    public void testEnabledLicenseSchedule() throws Exception {
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(12));
        final LicenseSchedule schedule = new LicenseSchedule(license);
        int expiryDuration = (int) (license.expiryDate() - license.issueDate());
        long triggeredTime = license.issueDate() + between(0, expiryDuration);
        assertThat(schedule.nextScheduledTimeAfter(license.issueDate(), triggeredTime), equalTo(license.expiryDate()));
    }

    public void testGraceLicenseSchedule() throws Exception {
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(12));
        final LicenseSchedule schedule = new LicenseSchedule(license);
        long triggeredTime = license.expiryDate() + between(1,
                ((int) LicensesService.GRACE_PERIOD_DURATION.getMillis()));
        assertThat(schedule.nextScheduledTimeAfter(license.issueDate(), triggeredTime),
                equalTo(license.expiryDate() + LicensesService.GRACE_PERIOD_DURATION.getMillis()));
    }

    public void testExpiredLicenseSchedule() throws Exception {
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(12));
        final LicenseSchedule schedule = new LicenseSchedule(license);
        long triggeredTime = license.expiryDate() + LicensesService.GRACE_PERIOD_DURATION.getMillis() +
                randomIntBetween(1, 1000);
        assertThat(schedule.nextScheduledTimeAfter(license.issueDate(), triggeredTime),
                equalTo(-1L));
    }

    public void testInvalidLicenseSchedule() throws Exception {
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(12));
        final LicenseSchedule schedule = new LicenseSchedule(license);
        long triggeredTime = license.issueDate() - randomIntBetween(1, 1000);
        assertThat(schedule.nextScheduledTimeAfter(triggeredTime, triggeredTime),
                equalTo(license.issueDate()));
    }
}