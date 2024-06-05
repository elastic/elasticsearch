/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackField;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.license.License.OperationMode.BASIC;
import static org.elasticsearch.license.License.OperationMode.ENTERPRISE;
import static org.elasticsearch.license.License.OperationMode.GOLD;
import static org.elasticsearch.license.License.OperationMode.PLATINUM;
import static org.elasticsearch.license.License.OperationMode.STANDARD;
import static org.elasticsearch.license.License.OperationMode.TRIAL;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.hamcrest.core.IsNot.not;

/**
 * Unit tests for the {@link XPackLicenseState}
 */
public class XPackLicenseStateTests extends ESTestCase {

    /** Creates a license state with the given license type and active state, and checks the given method returns expected. */
    void assertAllowed(OperationMode mode, boolean active, Predicate<XPackLicenseState> predicate, boolean expected) {
        XPackLicenseState licenseState = buildLicenseState(mode, active);
        assertEquals(expected, predicate.test(licenseState));
    }

    XPackLicenseState buildLicenseState(OperationMode mode, boolean active) {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        licenseState.update(new XPackLicenseStatus(mode, active, null));
        return licenseState;
    }

    /**
     * Checks the ack message going from the  {@code from} license type to {@code to} license type.
     * TODO: check the actual messages, not just the number of them! This was copied from previous license tests...
     */
    void assertAckMessages(String feature, OperationMode from, OperationMode to, int expectedMessages) {
        String[] gotMessages = XPackLicenseState.ACKNOWLEDGMENT_MESSAGES.get(feature).apply(from, to);
        assertEquals(expectedMessages, gotMessages.length);
    }

    static <T> T randomFrom(T[] values, Predicate<T> filter) {
        return randomFrom(Arrays.stream(values).filter(filter).collect(Collectors.toList()));
    }

    static OperationMode randomMode() {
        return randomFrom(OperationMode.values());
    }

    public static OperationMode randomTrialStandardGoldOrPlatinumMode() {
        return randomFrom(TRIAL, STANDARD, GOLD, PLATINUM);
    }

    public static OperationMode randomTrialOrPlatinumMode() {
        return randomFrom(TRIAL, PLATINUM);
    }

    public static OperationMode randomTrialGoldOrPlatinumMode() {
        return randomFrom(TRIAL, GOLD, PLATINUM);
    }

    public static OperationMode randomTrialBasicStandardGoldOrPlatinumMode() {
        return randomFrom(TRIAL, BASIC, STANDARD, GOLD, PLATINUM);
    }

    public static OperationMode randomBasicStandardOrGold() {
        return randomFrom(BASIC, STANDARD, GOLD);
    }

    public void testSecurityAckBasicToNotGoldOrStandard() {
        OperationMode toMode = randomFrom(OperationMode.values(), mode -> mode != GOLD && mode != STANDARD);
        assertAckMessages(XPackField.SECURITY, BASIC, toMode, 0);
    }

    public void testSecurityAckAnyToTrialOrPlatinum() {
        assertAckMessages(XPackField.SECURITY, randomMode(), randomTrialOrPlatinumMode(), 0);
    }

    public void testSecurityAckTrialGoldOrPlatinumToBasic() {
        assertAckMessages(XPackField.SECURITY, randomTrialGoldOrPlatinumMode(), BASIC, 6);
    }

    public void testSecurityAckStandardToBasic() {
        assertAckMessages(XPackField.SECURITY, STANDARD, BASIC, 1);
    }

    public void testSecurityAckAnyToStandard() {
        OperationMode from = randomFrom(BASIC, GOLD, PLATINUM, TRIAL);
        assertAckMessages(XPackField.SECURITY, from, STANDARD, 5);
    }

    public void testSecurityAckBasicStandardTrialOrPlatinumToGold() {
        OperationMode from = randomFrom(BASIC, PLATINUM, TRIAL, STANDARD);
        assertAckMessages(XPackField.SECURITY, from, GOLD, 3);
    }

    public void testMonitoringAckBasicToAny() {
        assertAckMessages(XPackField.MONITORING, BASIC, randomMode(), 0);
    }

    public void testMonitoringAckAnyToTrialGoldOrPlatinum() {
        assertAckMessages(XPackField.MONITORING, randomMode(), randomTrialStandardGoldOrPlatinumMode(), 0);
    }

    public void testMonitoringAckNotBasicToBasic() {
        OperationMode from = randomFrom(STANDARD, GOLD, PLATINUM, TRIAL);
        assertAckMessages(XPackField.MONITORING, from, BASIC, 1);
    }

    public void testSqlAckAnyToTrialOrPlatinum() {
        assertAckMessages(XPackField.SQL, randomMode(), randomTrialOrPlatinumMode(), 0);
    }

    public void testSqlAckTrialOrPlatinumToNotTrialOrPlatinum() {
        assertAckMessages(XPackField.SQL, randomTrialOrPlatinumMode(), randomBasicStandardOrGold(), 1);
    }

    public void testCcrAckAnyToTrialOrPlatinum() {
        assertAckMessages(XPackField.CCR, randomMode(), randomTrialOrPlatinumMode(), 0);
    }

    public void testCcrAckTrialOrPlatinumToNotTrialOrPlatinum() {
        assertAckMessages(XPackField.CCR, randomTrialOrPlatinumMode(), randomBasicStandardOrGold(), 1);
    }

    public void testExpiredLicense() {
        // use standard feature which would normally be allowed at all license levels
        LicensedFeature feature = LicensedFeature.momentary("family", "enterpriseFeature", STANDARD);
        assertAllowed(STANDARD, false, s -> s.isAllowed(feature), false);
        assertAllowed(GOLD, false, s -> s.isAllowed(feature), false);
        assertAllowed(PLATINUM, false, s -> s.isAllowed(feature), false);
        assertAllowed(ENTERPRISE, false, s -> s.isAllowed(feature), false);
        assertAllowed(TRIAL, false, s -> s.isAllowed(feature), false);
    }

    public void testStandardFeature() {
        LicensedFeature feature = LicensedFeature.momentary("family", "standardFeature", STANDARD);
        assertAllowed(BASIC, true, s -> s.isAllowed(feature), false);
        assertAllowed(STANDARD, true, s -> s.isAllowed(feature), true);
        assertAllowed(GOLD, true, s -> s.isAllowed(feature), true);
        assertAllowed(PLATINUM, true, s -> s.isAllowed(feature), true);
        assertAllowed(ENTERPRISE, true, s -> s.isAllowed(feature), true);
        assertAllowed(TRIAL, true, s -> s.isAllowed(feature), true);
    }

    public void testGoldFeature() {
        LicensedFeature feature = LicensedFeature.momentary("family", "goldFeature", GOLD);
        assertAllowed(BASIC, true, s -> s.isAllowed(feature), false);
        assertAllowed(STANDARD, true, s -> s.isAllowed(feature), false);
        assertAllowed(GOLD, true, s -> s.isAllowed(feature), true);
        assertAllowed(PLATINUM, true, s -> s.isAllowed(feature), true);
        assertAllowed(ENTERPRISE, true, s -> s.isAllowed(feature), true);
        assertAllowed(TRIAL, true, s -> s.isAllowed(feature), true);
    }

    public void testPlatinumFeature() {
        LicensedFeature feature = LicensedFeature.momentary("family", "platinumFeature", PLATINUM);
        assertAllowed(BASIC, true, s -> s.isAllowed(feature), false);
        assertAllowed(STANDARD, true, s -> s.isAllowed(feature), false);
        assertAllowed(GOLD, true, s -> s.isAllowed(feature), false);
        assertAllowed(PLATINUM, true, s -> s.isAllowed(feature), true);
        assertAllowed(ENTERPRISE, true, s -> s.isAllowed(feature), true);
        assertAllowed(TRIAL, true, s -> s.isAllowed(feature), true);
    }

    public void testEnterpriseFeature() {
        LicensedFeature feature = LicensedFeature.momentary("family", "enterpriseFeature", ENTERPRISE);
        assertAllowed(BASIC, true, s -> s.isAllowed(feature), false);
        assertAllowed(STANDARD, true, s -> s.isAllowed(feature), false);
        assertAllowed(GOLD, true, s -> s.isAllowed(feature), false);
        assertAllowed(PLATINUM, true, s -> s.isAllowed(feature), false);
        assertAllowed(ENTERPRISE, true, s -> s.isAllowed(feature), true);
        assertAllowed(TRIAL, true, s -> s.isAllowed(feature), true);
    }

    public void testStatusDescription() {
        assertThat(buildLicenseState(BASIC, true).statusDescription(), is("active basic license"));
        assertThat(buildLicenseState(STANDARD, true).statusDescription(), is("active standard license"));
        assertThat(buildLicenseState(GOLD, false).statusDescription(), is("expired gold license"));
        assertThat(buildLicenseState(PLATINUM, false).statusDescription(), is("expired platinum license"));
        assertThat(buildLicenseState(ENTERPRISE, true).statusDescription(), is("active enterprise license"));
        assertThat(buildLicenseState(TRIAL, false).statusDescription(), is("expired trial license"));
    }

    public void testLastUsedMomentaryFeature() {
        LicensedFeature.Momentary goldFeature = LicensedFeature.momentary("family", "goldFeature", GOLD);
        AtomicInteger currentTime = new AtomicInteger(100); // non zero start time
        XPackLicenseState licenseState = new XPackLicenseState(currentTime::get);
        Map<XPackLicenseState.FeatureUsage, Long> lastUsed = licenseState.getLastUsed();
        assertThat("initial epoch time", lastUsed, not(hasKey(goldFeature)));

        licenseState.isAllowed(goldFeature);
        lastUsed = licenseState.getLastUsed();
        assertThat("isAllowed does not track", lastUsed, not(hasKey(goldFeature)));

        goldFeature.check(licenseState);
        lastUsed = licenseState.getLastUsed();
        assertThat("feature.check tracks usage", lastUsed, aMapWithSize(1));

        XPackLicenseState.FeatureUsage usage = Iterables.get(lastUsed.keySet(), 0);
        assertThat(usage.feature().getName(), equalTo("goldFeature"));
        assertThat(usage.contextName(), nullValue());
        assertThat(lastUsed.get(usage), equalTo(100L));

        currentTime.set(200);
        goldFeature.check(licenseState);
        lastUsed = licenseState.getLastUsed();
        assertThat("feature.check updates usage", lastUsed.keySet(), containsInAnyOrder(usage));
        assertThat(lastUsed.get(usage), equalTo(200L));
    }

    public void testLastUsedPersistentFeature() {
        LicensedFeature.Persistent goldFeature = LicensedFeature.persistent("family", "goldFeature", GOLD);
        AtomicInteger currentTime = new AtomicInteger(100); // non zero start time
        XPackLicenseState licenseState = new XPackLicenseState(currentTime::get);
        Map<XPackLicenseState.FeatureUsage, Long> lastUsed = licenseState.getLastUsed();
        assertThat("initial epoch time", lastUsed, not(hasKey(goldFeature)));

        licenseState.isAllowed(goldFeature);
        lastUsed = licenseState.getLastUsed();
        assertThat("isAllowed does not track", lastUsed, not(hasKey(goldFeature)));

        goldFeature.checkAndStartTracking(licenseState, "somecontext");
        currentTime.set(200); // advance time after starting tracking
        lastUsed = licenseState.getLastUsed();
        assertThat(lastUsed, aMapWithSize(1));

        XPackLicenseState.FeatureUsage usage = Iterables.get(lastUsed.keySet(), 0);
        assertThat(usage.feature().getName(), equalTo("goldFeature"));
        assertThat(usage.contextName(), equalTo("somecontext"));
        assertThat(lastUsed.get(usage), equalTo(200L));

        currentTime.set(300);
        goldFeature.stopTracking(licenseState, "somecontext");
        lastUsed = licenseState.getLastUsed();
        assertThat("stopTracking sets time to current", lastUsed.keySet(), containsInAnyOrder(usage));
        assertThat(lastUsed.get(usage), equalTo(300L));

        currentTime.set(400);
        lastUsed = licenseState.getLastUsed();
        assertThat("last used no longer returns current", lastUsed.keySet(), containsInAnyOrder(usage));
        assertThat(lastUsed.get(usage), equalTo(300L));
    }

    public void testWarningHeader() {
        XPackLicenseState licenseState = new XPackLicenseState(() -> 0);
        License.OperationMode licenseLevel = randomFrom(STANDARD, GOLD, PLATINUM, ENTERPRISE);
        LicensedFeature.Momentary feature = LicensedFeature.momentary(null, "testfeature", licenseLevel);

        licenseState.update(new XPackLicenseStatus(licenseLevel, true, null));
        feature.check(licenseState);
        ensureNoWarnings();

        String warningSoon = "warning: license expiring soon";
        licenseState.update(new XPackLicenseStatus(licenseLevel, true, warningSoon));
        feature.check(licenseState);
        assertCriticalWarnings(warningSoon);

        /*
        TODO: this does not yet work because the active comes before expiry check as a chance
        String warningExpired = "warning: license expired";
        licenseState.update(licenseLevel, false, warningExpired);
        feature.check(licenseState);
        assertWarnings(warningExpired);
         */
    }
}
