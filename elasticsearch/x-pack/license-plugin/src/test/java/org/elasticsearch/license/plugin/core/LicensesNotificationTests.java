/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import java.util.List;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.TestUtils;
import org.elasticsearch.license.plugin.TestUtils.AssertingLicensee;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static org.hamcrest.Matchers.equalTo;

public class LicensesNotificationTests extends AbstractLicenseServiceTestCase {

    public void testLicenseNotification() throws Exception {
        final License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(48));
        int nLicensee = randomIntBetween(1, 3);
        AssertingLicensee[] assertingLicensees = new AssertingLicensee[nLicensee];
        for (int i = 0; i < assertingLicensees.length; i++) {
            assertingLicensees[i] = new AssertingLicensee("testLicenseNotification" + i, logger);
        }
        setInitialState(license, assertingLicensees);
        licenseService.start();
        for (int i = 0; i < assertingLicensees.length; i++) {
            assertLicenseStates(assertingLicensees[i], LicenseState.ENABLED);
        }
        clock.fastForward(TimeValue.timeValueMillis(license.expiryDate() - clock.millis()));
        final LicensesMetaData licensesMetaData = new LicensesMetaData(license);
        licenseService.onUpdate(licensesMetaData);
        for (AssertingLicensee assertingLicensee : assertingLicensees) {
            assertLicenseStates(assertingLicensee, LicenseState.ENABLED, LicenseState.GRACE_PERIOD);
        }
        clock.fastForward(TimeValue.timeValueMillis((license.expiryDate() +
                LicenseState.GRACE_PERIOD_DURATION.getMillis()) - clock.millis()));
        licenseService.onUpdate(licensesMetaData);
        for (AssertingLicensee assertingLicensee : assertingLicensees) {
            assertLicenseStates(assertingLicensee, LicenseState.ENABLED, LicenseState.GRACE_PERIOD, LicenseState.DISABLED);
        }
        clock.setTime(new DateTime(DateTimeZone.UTC));
        final License newLicense = TestUtils.generateSignedLicense(TimeValue.timeValueHours(2));
        clock.fastForward(TimeValue.timeValueHours(1));
        LicensesMetaData licensesMetaData1 = new LicensesMetaData(newLicense);
        licenseService.onUpdate(licensesMetaData1);
        for (AssertingLicensee assertingLicensee : assertingLicensees) {
            assertLicenseStates(assertingLicensee, LicenseState.ENABLED, LicenseState.GRACE_PERIOD, LicenseState.DISABLED,
                    LicenseState.ENABLED);
        }
    }

    private void assertLicenseStates(AssertingLicensee licensee, LicenseState... states) {
        StringBuilder msg = new StringBuilder();
        msg.append("Actual: ");
        msg.append(dumpLicensingStates(licensee.statuses));
        msg.append(" Expected: ");
        msg.append(dumpLicensingStates(states));
        assertThat(msg.toString(), licensee.statuses.size(), equalTo(states.length));
        for (int i = 0; i < states.length; i++) {
            assertThat(msg.toString(), licensee.statuses.get(i).getLicenseState(), equalTo(states[i]));
        }
    }

    private String dumpLicensingStates(List<Licensee.Status> statuses) {
        return dumpLicensingStates(statuses.toArray(new Licensee.Status[statuses.size()]));
    }

    private String dumpLicensingStates(Licensee.Status... statuses) {
        LicenseState[] states = new LicenseState[statuses.length];
        for (int i = 0; i < statuses.length; i++) {
            states[i] = statuses[i].getLicenseState();
        }
        return dumpLicensingStates(states);
    }

    private String dumpLicensingStates(LicenseState... states) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < states.length; i++) {
            sb.append(states[i].name());
            if (i != states.length - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
