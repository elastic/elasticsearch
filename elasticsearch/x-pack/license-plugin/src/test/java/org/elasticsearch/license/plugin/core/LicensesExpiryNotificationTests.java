/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.apache.lucene.util.LuceneTestCase.BadApple;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.plugin.TestUtils.AssertingLicensee;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.List;

import static org.elasticsearch.license.plugin.TestUtils.awaitNoBlock;
import static org.elasticsearch.license.plugin.TestUtils.awaitNoPendingTasks;
import static org.elasticsearch.license.plugin.TestUtils.generateSignedLicense;
import static org.elasticsearch.license.plugin.TestUtils.registerAndAckSignedLicenses;
import static org.hamcrest.Matchers.equalTo;

//test is just too slow, please fix it to not be sleep-based
@BadApple(bugUrl = "https://github.com/elastic/x-plugins/issues/1007")
public class LicensesExpiryNotificationTests extends ESSingleNodeTestCase {
    static {
        MetaData.registerPrototype(LicensesMetaData.TYPE, LicensesMetaData.PROTO);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testTrialLicenseEnforcement() throws Exception {
        LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        licensesService.setTrialLicenseDuration(TimeValue.timeValueSeconds(5));
        licensesService.setGracePeriodDuration(TimeValue.timeValueSeconds(3));
        licensesService.start();
        String id1 = "testTrialLicenseEnforcement";
        final AssertingLicensee licensee = new AssertingLicensee(id1, logger);
        awaitNoBlock(client());
        licensesService.register(licensee);
        awaitNoPendingTasks(client());
        boolean success = awaitBusy(() -> licensee.licenseStates.size() == 3);
        // trail license: enable, grace, disabled
        assertLicenseStates(licensee, LicenseState.ENABLED, LicenseState.GRACE_PERIOD, LicenseState.DISABLED);
        assertTrue(dumpLicensingStates(licensee.licenseStates), success);
        licensesService.stop();
    }

    public void testTrialLicenseEnforcementMultipleLicensees() throws Exception {
        LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        licensesService.setTrialLicenseDuration(TimeValue.timeValueSeconds(5));
        licensesService.setGracePeriodDuration(TimeValue.timeValueSeconds(3));
        licensesService.start();
        String id1 = "testTrialLicenseEnforcementMultipleLicensees_1";
        final AssertingLicensee licensee1 = new AssertingLicensee(id1, logger);
        String id12 = "testTrialLicenseEnforcementMultipleLicensees_2";
        final AssertingLicensee licensee2 = new AssertingLicensee(id12, logger);
        awaitNoBlock(client());
        licensesService.register(licensee1);
        licensesService.register(licensee2);
        awaitNoPendingTasks(client());
        boolean success = awaitBusy(() -> licensee1.licenseStates.size() == 3);
        assertTrue(dumpLicensingStates(licensee1.licenseStates), success);
        success = awaitBusy(() -> licensee2.licenseStates.size() == 3);
        assertTrue(dumpLicensingStates(licensee2.licenseStates), success);
        // trail license: enable, grace, disabled
        assertLicenseStates(licensee1, LicenseState.ENABLED, LicenseState.GRACE_PERIOD, LicenseState.DISABLED);
        assertLicenseStates(licensee2, LicenseState.ENABLED, LicenseState.GRACE_PERIOD, LicenseState.DISABLED);
        licensesService.stop();
    }

    public void testTrialSignedLicenseEnforcement() throws Exception {
        LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        licensesService.setTrialLicenseDuration(TimeValue.timeValueSeconds(2));
        licensesService.setGracePeriodDuration(TimeValue.timeValueSeconds(3));
        licensesService.start();
        String id1 = "testTrialSignedLicenseEnforcement";
        final AssertingLicensee licensee = new AssertingLicensee(id1, logger);
        awaitNoBlock(client());
        licensesService.register(licensee);
        awaitNoPendingTasks(client());
        boolean success = awaitBusy(() -> licensee.licenseStates.size() == 1);
        assertTrue(dumpLicensingStates(licensee.licenseStates), success);
        registerAndAckSignedLicenses(licensesService, generateSignedLicense(TimeValue.timeValueSeconds(4)), LicensesStatus.VALID);
        success = awaitBusy(() -> licensee.licenseStates.size() == 4);
        // trial: enable, signed: enable, signed: grace, signed: disabled
        assertLicenseStates(licensee, LicenseState.ENABLED, LicenseState.ENABLED, LicenseState.GRACE_PERIOD, LicenseState.DISABLED);
        assertTrue(dumpLicensingStates(licensee.licenseStates), success);
        licensesService.stop();
    }

    public void testSignedLicenseEnforcement() throws Exception {
        LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        licensesService.setTrialLicenseDuration(TimeValue.timeValueSeconds(4));
        licensesService.setGracePeriodDuration(TimeValue.timeValueSeconds(3));
        licensesService.start();
        String id1 = "testSignedLicenseEnforcement";
        final AssertingLicensee licensee = new AssertingLicensee(id1, logger);
        awaitNoBlock(client());
        registerAndAckSignedLicenses(licensesService, generateSignedLicense(TimeValue.timeValueSeconds(2)), LicensesStatus.VALID);
        licensesService.register(licensee);
        awaitNoPendingTasks(client());
        boolean success = awaitBusy(() -> licensee.licenseStates.size() == 3);
        // signed: enable, signed: grace, signed: disabled
        assertLicenseStates(licensee, LicenseState.ENABLED, LicenseState.GRACE_PERIOD, LicenseState.DISABLED);
        assertTrue(dumpLicensingStates(licensee.licenseStates), success);
        licensesService.stop();
    }

    public void testSingedLicenseEnforcementMultipleLicensees() throws Exception {
        LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        licensesService.setTrialLicenseDuration(TimeValue.timeValueSeconds(4));
        licensesService.setGracePeriodDuration(TimeValue.timeValueSeconds(3));
        licensesService.start();
        String id1 = "testSingedLicenseEnforcementMultipleLicensees_1";
        final AssertingLicensee licensee1 = new AssertingLicensee(id1, logger);
        String id12 = "testSingedLicenseEnforcementMultipleLicensees_2";
        final AssertingLicensee licensee2 = new AssertingLicensee(id12, logger);
        awaitNoBlock(client());
        registerAndAckSignedLicenses(licensesService, generateSignedLicense(TimeValue.timeValueSeconds(2)), LicensesStatus.VALID);
        licensesService.register(licensee1);
        licensesService.register(licensee2);
        awaitNoPendingTasks(client());
        boolean success = awaitBusy(() -> licensee1.licenseStates.size() == 3);
        assertTrue(dumpLicensingStates(licensee1.licenseStates), success);
        success = awaitBusy(() -> licensee2.licenseStates.size() == 3);
        assertTrue(dumpLicensingStates(licensee2.licenseStates), success);
        // signed license: enable, grace, disabled
        assertLicenseStates(licensee1, LicenseState.ENABLED, LicenseState.GRACE_PERIOD, LicenseState.DISABLED);
        assertLicenseStates(licensee2, LicenseState.ENABLED, LicenseState.GRACE_PERIOD, LicenseState.DISABLED);
        licensesService.stop();
    }

    public void testMultipleSignedLicenseEnforcement() throws Exception {
        // register with trial license and assert onEnable and onDisable notification
        LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        licensesService.setTrialLicenseDuration(TimeValue.timeValueSeconds(4));
        licensesService.setGracePeriodDuration(TimeValue.timeValueSeconds(1));
        licensesService.start();
        String id1 = "testMultipleSignedLicenseEnforcement";
        final AssertingLicensee licensee = new AssertingLicensee(id1, logger);
        awaitNoBlock(client());
        licensesService.register(licensee);
        awaitNoPendingTasks(client());
        // trial license enabled
        boolean success = awaitBusy(() -> licensee.licenseStates.size() == 1);
        assertTrue(dumpLicensingStates(licensee.licenseStates), success);
        registerAndAckSignedLicenses(licensesService, generateSignedLicense("basic", TimeValue.timeValueSeconds(3)), LicensesStatus.VALID);
        // signed license enabled
        success = awaitBusy(() -> licensee.licenseStates.size() == 2);
        assertTrue(dumpLicensingStates(licensee.licenseStates), success);
        registerAndAckSignedLicenses(licensesService, generateSignedLicense("gold", TimeValue.timeValueSeconds(2)), LicensesStatus.VALID);
        // second signed license enabled, grace and expired
        success = awaitBusy(() ->licensee.licenseStates.size() == 5);
        assertLicenseStates(licensee, LicenseState.ENABLED, LicenseState.ENABLED, LicenseState.ENABLED, LicenseState.GRACE_PERIOD,
                LicenseState.DISABLED);
        assertTrue(dumpLicensingStates(licensee.licenseStates), success);
        licensesService.stop();
    }

    public void testNonOverlappingMultipleLicensesEnforcement() throws Exception {
        // register with trial license and assert onEnable and onDisable notification
        LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        licensesService.setTrialLicenseDuration(TimeValue.timeValueSeconds(3));
        licensesService.setGracePeriodDuration(TimeValue.timeValueSeconds(1));
        licensesService.start();
        String id1 = "testNonOverlappingMultipleLicensesEnforcement";
        final AssertingLicensee licensee = new AssertingLicensee(id1, logger);
        awaitNoBlock(client());
        licensesService.register(licensee);
        // trial license: enabled, grace, disabled
        boolean success = awaitBusy(() -> licensee.licenseStates.size() == 3);

        assertTrue(dumpLicensingStates(licensee.licenseStates), success);
        // install license
        registerAndAckSignedLicenses(licensesService, generateSignedLicense("basic", TimeValue.timeValueSeconds(2)), LicensesStatus.VALID);
        // trial license: enabled, grace, disabled, signed license: enabled, grace, disabled
        success = awaitBusy(() -> licensee.licenseStates.size() == 6);
        assertLicenseStates(licensee, LicenseState.ENABLED, LicenseState.GRACE_PERIOD, LicenseState.DISABLED, LicenseState.ENABLED,
                LicenseState.GRACE_PERIOD, LicenseState.DISABLED);
        assertTrue(dumpLicensingStates(licensee.licenseStates), success);
        licensesService.stop();
    }

    private void assertLicenseStates(AssertingLicensee licensee, LicenseState... states) {
        StringBuilder msg = new StringBuilder();
        msg.append("Actual: ");
        msg.append(dumpLicensingStates(licensee.licenseStates));
        msg.append(" Expected: ");
        msg.append(dumpLicensingStates(states));
        assertThat(msg.toString(), licensee.licenseStates.size(), equalTo(states.length));
        for (int i = 0; i < states.length; i++) {
            assertThat(msg.toString(), licensee.licenseStates.get(i), equalTo(states[i]));
        }
    }

    private String dumpLicensingStates(List<LicenseState> states) {
        return dumpLicensingStates(states.toArray(new LicenseState[states.size()]));
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
