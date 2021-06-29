/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.license.License.OperationMode.BASIC;
import static org.elasticsearch.license.License.OperationMode.ENTERPRISE;
import static org.elasticsearch.license.License.OperationMode.GOLD;
import static org.elasticsearch.license.License.OperationMode.MISSING;
import static org.elasticsearch.license.License.OperationMode.PLATINUM;
import static org.elasticsearch.license.License.OperationMode.STANDARD;
import static org.elasticsearch.license.License.OperationMode.TRIAL;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.hamcrest.core.IsNot.not;

/**
 * Unit tests for the {@link XPackLicenseState}
 */
public class XPackLicenseStateTests extends ESTestCase {

    /** Creates a license state with the given license type and active state, and checks the given method returns expected. */
    void assertAllowed(OperationMode mode, boolean active, Predicate<XPackLicenseState> predicate, boolean expected) {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        licenseState.update(mode, active, Long.MAX_VALUE, null);
        assertEquals(expected, predicate.test(licenseState));
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

    public void testSecurityDefaults() {
        Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
        XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_IP_FILTERING), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_AUDITING), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_DLS_FLS), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_ALL_REALMS), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(true));

        licenseState = TestUtils.newTestLicenseState();
        assertSecurityNotAllowed(licenseState);
    }

    public void testTransportSslDoesNotAutomaticallyEnableSecurityOnTrialLicense() {
        Settings settings = Settings.builder().put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), true).build();
        final XPackLicenseState licenseState= new XPackLicenseState(settings, () -> 0);
        assertSecurityNotAllowed(licenseState);
    }

    public void testSecurityBasicWithoutExplicitSecurityEnabled() {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        licenseState.update(BASIC, true, Long.MAX_VALUE, null);

        assertThat(licenseState.isSecurityEnabled(), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_IP_FILTERING), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_AUDITING), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_DLS_FLS), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_TOKEN_SERVICE), is(false));

        assertThat(licenseState.isSecurityEnabled(), is(false));
    }

    public void testSecurityBasicWithExplicitSecurityEnabled() {
        final Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
        XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        licenseState.update(BASIC, true, Long.MAX_VALUE, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_IP_FILTERING), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_AUDITING), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_DLS_FLS), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_TOKEN_SERVICE), is(false));

        assertThat(licenseState.isSecurityEnabled(), is(true));
    }

    public void testSecurityStandard() {
        Settings settings = randomFrom(Settings.EMPTY,
            Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build());
        XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        licenseState.update(STANDARD, true, Long.MAX_VALUE, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_IP_FILTERING), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_AUDITING), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_DLS_FLS), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_TOKEN_SERVICE), is(true));
    }

    public void testSecurityStandardExpired() {
        Settings settings = randomFrom(Settings.EMPTY,
            Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build());
        XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        licenseState.update(STANDARD, false, Long.MAX_VALUE, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_IP_FILTERING), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_AUDITING), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_DLS_FLS), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_TOKEN_SERVICE), is(true));
    }

    public void testSecurityGold() {
        Settings settings = randomFrom(Settings.EMPTY,
            Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build());
        XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        licenseState.update(GOLD, true, Long.MAX_VALUE, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_IP_FILTERING), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_AUDITING), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_DLS_FLS), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_STANDARD_REALMS), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_TOKEN_SERVICE), is(true));
    }

    public void testSecurityGoldExpired() {
        Settings settings = randomFrom(Settings.EMPTY,
            Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build());
        XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        licenseState.update(GOLD, false, Long.MAX_VALUE, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_IP_FILTERING), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_AUDITING), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_DLS_FLS), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_STANDARD_REALMS), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_TOKEN_SERVICE), is(true));
    }

    public void testSecurityPlatinum() {
        Settings settings = randomFrom(Settings.EMPTY,
            Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build());
        XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        licenseState.update(PLATINUM, true, Long.MAX_VALUE, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_IP_FILTERING), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_AUDITING), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_DLS_FLS), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_ALL_REALMS), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_TOKEN_SERVICE), is(true));
    }

    public void testSecurityPlatinumExpired() {
        Settings settings = randomFrom(Settings.EMPTY,
            Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build());
        XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        licenseState.update(PLATINUM, false, Long.MAX_VALUE, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_IP_FILTERING), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_AUDITING), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_DLS_FLS), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_ALL_REALMS), is(true));
        assertThat(licenseState.checkFeature(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
        assertThat(licenseState.checkFeature(Feature.SECURITY_TOKEN_SERVICE), is(true));
    }

    public void testNewTrialDefaultsSecurityOff() {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        licenseState.update(TRIAL, true, Long.MAX_VALUE, VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));

        assertThat(licenseState.isSecurityEnabled(), is(false));
        assertSecurityNotAllowed(licenseState);
    }

    private void assertSecurityNotAllowed(XPackLicenseState licenseState) {
        assertThat(licenseState.isSecurityEnabled(), is(false));
    }

    public void testSecurityAckBasicToNotGoldOrStandard() {
        OperationMode toMode = randomFrom(OperationMode.values(), mode -> mode != GOLD && mode != STANDARD);
        assertAckMessages(XPackField.SECURITY, BASIC, toMode, 0);
    }

    public void testSecurityAckAnyToTrialOrPlatinum() {
        assertAckMessages(XPackField.SECURITY, randomMode(), randomTrialOrPlatinumMode(), 0);
    }

    public void testSecurityAckTrialGoldOrPlatinumToBasic() {
        assertAckMessages(XPackField.SECURITY, randomTrialGoldOrPlatinumMode(), BASIC, 7);
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
        assertAckMessages(XPackField.MONITORING, from, BASIC, 2);
    }

    public void testMonitoringUpdateRetention() {
        assertAllowed(STANDARD, true, s -> s.checkFeature(Feature.MONITORING_UPDATE_RETENTION), true);
        assertAllowed(GOLD, true, s -> s.checkFeature(Feature.MONITORING_UPDATE_RETENTION), true);
        assertAllowed(PLATINUM, true, s -> s.checkFeature(Feature.MONITORING_UPDATE_RETENTION), true);
        assertAllowed(TRIAL, true, s -> s.checkFeature(Feature.MONITORING_UPDATE_RETENTION), true);
        assertAllowed(BASIC, true, s -> s.checkFeature(Feature.MONITORING_UPDATE_RETENTION), false);
        assertAllowed(MISSING, false, s -> s.checkFeature(Feature.MONITORING_UPDATE_RETENTION), false);
    }

    public void testWatcherPlatinumGoldTrialStandard() throws Exception {
        assertAllowed(TRIAL, true, s -> s.checkFeature(Feature.WATCHER), true);
        assertAllowed(GOLD, true, s -> s.checkFeature(Feature.WATCHER), true);
        assertAllowed(PLATINUM, true, s -> s.checkFeature(Feature.WATCHER), true);
        assertAllowed(STANDARD, true, s -> s.checkFeature(Feature.WATCHER), true);
    }

    public void testWatcherBasicLicense() throws Exception {
        assertAllowed(BASIC, true, s -> s.checkFeature(Feature.WATCHER), false);
    }

    public void testWatcherInactive() {
        assertAllowed(BASIC, false, s -> s.checkFeature(Feature.WATCHER), false);
    }

    public void testWatcherInactivePlatinumGoldTrial() throws Exception {
        assertAllowed(TRIAL, false, s -> s.checkFeature(Feature.WATCHER), false);
        assertAllowed(GOLD, false, s -> s.checkFeature(Feature.WATCHER), false);
        assertAllowed(PLATINUM, false, s -> s.checkFeature(Feature.WATCHER), false);
        assertAllowed(STANDARD, false, s -> s.checkFeature(Feature.WATCHER), false);
    }

    public void testEncryptedSnapshotsWithInactiveLicense() {
        assertAllowed(BASIC, false, s -> s.checkFeature(Feature.ENCRYPTED_SNAPSHOT), false);
        assertAllowed(TRIAL, false, s -> s.checkFeature(Feature.ENCRYPTED_SNAPSHOT), false);
        assertAllowed(GOLD, false, s -> s.checkFeature(Feature.ENCRYPTED_SNAPSHOT), false);
        assertAllowed(PLATINUM, false, s -> s.checkFeature(Feature.ENCRYPTED_SNAPSHOT), false);
        assertAllowed(ENTERPRISE, false, s -> s.checkFeature(Feature.ENCRYPTED_SNAPSHOT), false);
        assertAllowed(STANDARD, false, s -> s.checkFeature(Feature.ENCRYPTED_SNAPSHOT), false);
    }

    public void testEncryptedSnapshotsWithActiveLicense() {
        assertAllowed(BASIC, true, s -> s.checkFeature(Feature.ENCRYPTED_SNAPSHOT), false);
        assertAllowed(TRIAL, true, s -> s.checkFeature(Feature.ENCRYPTED_SNAPSHOT), true);
        assertAllowed(GOLD, true, s -> s.checkFeature(Feature.ENCRYPTED_SNAPSHOT), false);
        assertAllowed(PLATINUM, true, s -> s.checkFeature(Feature.ENCRYPTED_SNAPSHOT), true);
        assertAllowed(ENTERPRISE, true, s -> s.checkFeature(Feature.ENCRYPTED_SNAPSHOT), true);
        assertAllowed(STANDARD, true, s -> s.checkFeature(Feature.ENCRYPTED_SNAPSHOT), false);
    }

    public void testGraphPlatinumTrial() throws Exception {
        assertAllowed(TRIAL, true, s -> s.checkFeature(Feature.GRAPH), true);
        assertAllowed(PLATINUM, true, s -> s.checkFeature(Feature.GRAPH), true);
    }

    public void testGraphBasic() throws Exception {
        assertAllowed(BASIC, true, s -> s.checkFeature(Feature.GRAPH), false);
    }

    public void testGraphStandard() throws Exception {
        assertAllowed(STANDARD, true, s -> s.checkFeature(Feature.GRAPH), false);
    }

    public void testGraphInactiveBasic() {
        assertAllowed(BASIC, false, s -> s.checkFeature(Feature.GRAPH), false);
    }

    public void testGraphInactivePlatinumTrial() throws Exception {
        assertAllowed(TRIAL, false, s -> s.checkFeature(Feature.MACHINE_LEARNING), false);
        assertAllowed(PLATINUM, false, s -> s.checkFeature(Feature.MACHINE_LEARNING), false);
    }

    public void testMachineLearningPlatinumTrial() throws Exception {
        assertAllowed(TRIAL, true, s -> s.checkFeature(Feature.MACHINE_LEARNING), true);
        assertAllowed(PLATINUM, true, s -> s.checkFeature(Feature.MACHINE_LEARNING), true);
    }

    public void testMachineLearningBasic() throws Exception {
        assertAllowed(BASIC, true, s -> s.checkFeature(Feature.MACHINE_LEARNING), false);
    }

    public void testMachineLearningStandard() throws Exception {
        assertAllowed(STANDARD, true, s -> s.checkFeature(Feature.MACHINE_LEARNING), false);
    }

    public void testMachineLearningInactiveBasic() {
        assertAllowed(BASIC, false, s -> s.checkFeature(Feature.MACHINE_LEARNING), false);
    }

    public void testMachineLearningInactivePlatinumTrial() throws Exception {
        assertAllowed(TRIAL, false, s -> s.checkFeature(Feature.MACHINE_LEARNING), false);
        assertAllowed(PLATINUM, false, s -> s.checkFeature(Feature.MACHINE_LEARNING), false);
    }

    public void testLogstashPlatinumGoldTrialStandard() throws Exception {
        assertAllowed(TRIAL, true, s -> s.checkFeature(Feature.LOGSTASH), true);
        assertAllowed(GOLD, true, s -> s.checkFeature(Feature.LOGSTASH), true);
        assertAllowed(PLATINUM, true, s -> s.checkFeature(Feature.LOGSTASH), true);
        assertAllowed(STANDARD, true, s -> s.checkFeature(Feature.LOGSTASH), true);
    }

    public void testLogstashBasicLicense() throws Exception {
        assertAllowed(BASIC, true, s -> s.checkFeature(Feature.LOGSTASH), false);
    }

    public void testLogstashInactive() {
        assertAllowed(BASIC, false, s -> s.checkFeature(Feature.LOGSTASH), false);
        assertAllowed(TRIAL, false, s -> s.checkFeature(Feature.LOGSTASH), false);
        assertAllowed(GOLD, false, s -> s.checkFeature(Feature.LOGSTASH), false);
        assertAllowed(PLATINUM, false, s -> s.checkFeature(Feature.LOGSTASH), false);
        assertAllowed(STANDARD, false, s -> s.checkFeature(Feature.LOGSTASH), false);
    }

    public void testJdbcDefaults() {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        assertThat(licenseState.checkFeature(XPackLicenseState.Feature.JDBC), is(true));
    }

    public void testJdbcBasic() {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        licenseState.update(BASIC, true, Long.MAX_VALUE, null);
        assertThat(licenseState.checkFeature(XPackLicenseState.Feature.JDBC), is(false));
    }

    public void testJdbcStandard() {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        licenseState.update(STANDARD, true, Long.MAX_VALUE, null);

        assertThat(licenseState.checkFeature(XPackLicenseState.Feature.JDBC), is(false));
    }

    public void testJdbcStandardExpired() {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        licenseState.update(STANDARD, false, Long.MAX_VALUE, null);

        assertThat(licenseState.checkFeature(XPackLicenseState.Feature.JDBC), is(false));
    }

    public void testJdbcGold() {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        licenseState.update(GOLD, true, Long.MAX_VALUE, null);

        assertThat(licenseState.checkFeature(XPackLicenseState.Feature.JDBC), is(false));
    }

    public void testJdbcGoldExpired() {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        licenseState.update(GOLD, false, Long.MAX_VALUE, null);

        assertThat(licenseState.checkFeature(XPackLicenseState.Feature.JDBC), is(false));
    }

    public void testJdbcPlatinum() {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        licenseState.update(PLATINUM, true, Long.MAX_VALUE, null);

        assertThat(licenseState.checkFeature(XPackLicenseState.Feature.JDBC), is(true));
    }

    public void testJdbcPlatinumExpired() {
        XPackLicenseState licenseState = TestUtils.newTestLicenseState();
        licenseState.update(PLATINUM, false, Long.MAX_VALUE, null);

        assertThat(licenseState.checkFeature(XPackLicenseState.Feature.JDBC), is(false));
    }

    public void testSqlAckAnyToTrialOrPlatinum() {
        assertAckMessages(XPackField.SQL, randomMode(), randomTrialOrPlatinumMode(), 0);
    }

    public void testSqlAckTrialOrPlatinumToNotTrialOrPlatinum() {
        assertAckMessages(XPackField.SQL, randomTrialOrPlatinumMode(), randomBasicStandardOrGold(), 1);
    }

    public void testCcrDefaults() {
        final XPackLicenseState state = TestUtils.newTestLicenseState();
        assertTrue(state.checkFeature(XPackLicenseState.Feature.CCR));
    }

    public void testCcrBasic() {
        final XPackLicenseState state = TestUtils.newTestLicenseState();
        state.update(BASIC, true, Long.MAX_VALUE, null);

        assertThat(state.checkFeature(XPackLicenseState.Feature.CCR), is(false));
    }

    public void testCcrBasicExpired() {
        final XPackLicenseState state = TestUtils.newTestLicenseState();
        state.update(BASIC, false, Long.MAX_VALUE, null);

        assertThat(state.checkFeature(XPackLicenseState.Feature.CCR), is(false));
    }

    public void testCcrStandard() {
        final XPackLicenseState state = TestUtils.newTestLicenseState();
        state.update(STANDARD, true, Long.MAX_VALUE, null);

        assertThat(state.checkFeature(XPackLicenseState.Feature.CCR), is(false));
    }

    public void testCcrStandardExpired() {
        final XPackLicenseState state = TestUtils.newTestLicenseState();
        state.update(STANDARD, false, Long.MAX_VALUE, null);

        assertThat(state.checkFeature(XPackLicenseState.Feature.CCR), is(false));
    }

    public void testCcrGold() {
        final XPackLicenseState state = TestUtils.newTestLicenseState();
        state.update(GOLD, true, Long.MAX_VALUE, null);

        assertThat(state.checkFeature(XPackLicenseState.Feature.CCR), is(false));
    }

    public void testCcrGoldExpired() {
        final XPackLicenseState state = TestUtils.newTestLicenseState();
        state.update(GOLD, false, Long.MAX_VALUE, null);

        assertThat(state.checkFeature(XPackLicenseState.Feature.CCR), is(false));
    }

    public void testCcrPlatinum() {
        final XPackLicenseState state = TestUtils.newTestLicenseState();
        state.update(PLATINUM, true, Long.MAX_VALUE, null);

        assertTrue(state.checkFeature(XPackLicenseState.Feature.CCR));
    }

    public void testCcrPlatinumExpired() {
        final XPackLicenseState state = TestUtils.newTestLicenseState();
        state.update(PLATINUM, false, Long.MAX_VALUE, null);

        assertFalse(state.checkFeature(XPackLicenseState.Feature.CCR));
    }

    public void testCcrAckAnyToTrialOrPlatinum() {
        assertAckMessages(XPackField.CCR, randomMode(), randomTrialOrPlatinumMode(), 0);
    }

    public void testCcrAckTrialOrPlatinumToNotTrialOrPlatinum() {
        assertAckMessages(XPackField.CCR, randomTrialOrPlatinumMode(), randomBasicStandardOrGold(), 1);
    }

    public void testLastUsed() {
        Feature goldFeature = Feature.SECURITY_DLS_FLS;
        AtomicInteger currentTime = new AtomicInteger(100); // non zero start time
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, currentTime::get);
        assertThat("initial epoch time", licenseState.getLastUsed(), not(hasKey(goldFeature)));
        licenseState.isAllowed(goldFeature);
        assertThat("isAllowed does not track", licenseState.getLastUsed(), not(hasKey(goldFeature)));
        licenseState.checkFeature(goldFeature);
        assertThat("checkFeature tracks used time", licenseState.getLastUsed(), hasEntry(goldFeature, 100L));
        currentTime.set(200);
        licenseState.checkFeature(goldFeature);
        assertThat("checkFeature updates tracked time", licenseState.getLastUsed(), hasEntry(goldFeature, 200L));
    }
}
