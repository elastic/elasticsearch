/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.license.License.OperationMode.BASIC;
import static org.elasticsearch.license.License.OperationMode.GOLD;
import static org.elasticsearch.license.License.OperationMode.MISSING;
import static org.elasticsearch.license.License.OperationMode.PLATINUM;
import static org.elasticsearch.license.License.OperationMode.STANDARD;
import static org.elasticsearch.license.License.OperationMode.TRIAL;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for the {@link XPackLicenseState}
 */
public class XPackLicenseStateTests extends ESTestCase {

    /** Creates a license state with the given license type and active state, and checks the given method returns expected. */
    void assertAllowed(OperationMode mode, boolean active, Predicate<XPackLicenseState> predicate, boolean expected) {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(mode, active, null);
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
        XPackLicenseState licenseState =
                new XPackLicenseState(Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build());
        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_IP_FILTERING), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_AUDITING), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_STATS_AND_HEALTH), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_DLS_FLS), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_ALL_REALMS), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(true));

        licenseState = new XPackLicenseState(Settings.EMPTY);
        assertSecurityNotAllowed(licenseState);
    }

    public void testTransportSslDoesNotAutomaticallyEnableSecurityOnTrialLicense() {
        final XPackLicenseState licenseState;
        licenseState =
            new XPackLicenseState(Settings.builder().put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), true).build());
        assertSecurityNotAllowed(licenseState);
    }

    public void testSecurityBasicWithoutExplicitSecurityEnabled() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(BASIC, true, null);

        assertThat(licenseState.isSecurityEnabled(), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_IP_FILTERING), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_AUDITING), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_STATS_AND_HEALTH), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_DLS_FLS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_TOKEN_SERVICE), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_API_KEY_SERVICE), is(true));

        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.SECURITY), is(true));
        assertThat(licenseState.isSecurityEnabled(), is(false));
    }

    public void testSecurityBasicWithExplicitSecurityEnabled() {
        final Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
        XPackLicenseState licenseState = new XPackLicenseState(settings);
        licenseState.update(BASIC, true, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_IP_FILTERING), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_AUDITING), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_STATS_AND_HEALTH), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_DLS_FLS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_TOKEN_SERVICE), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_API_KEY_SERVICE), is(true));

        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.SECURITY), is(true));
        assertThat(licenseState.isSecurityEnabled(), is(true));
    }

    public void testSecurityDefaultBasicExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(BASIC, false, null);

        assertThat(licenseState.isSecurityEnabled(), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_IP_FILTERING), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_AUDITING), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_STATS_AND_HEALTH), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_DLS_FLS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_TOKEN_SERVICE), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_API_KEY_SERVICE), is(true));
    }

    public void testSecurityEnabledBasicExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(
            Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build());
        licenseState.update(BASIC, false, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_IP_FILTERING), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_AUDITING), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_STATS_AND_HEALTH), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_DLS_FLS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_TOKEN_SERVICE), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_API_KEY_SERVICE), is(true));
    }

    public void testSecurityStandard() {
        XPackLicenseState licenseState = new XPackLicenseState(randomFrom(Settings.EMPTY,
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build()));
        licenseState.update(STANDARD, true, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_IP_FILTERING), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_AUDITING), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_STATS_AND_HEALTH), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_DLS_FLS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
    }

    public void testSecurityStandardExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(randomFrom(Settings.EMPTY,
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build()));
        licenseState.update(STANDARD, false, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_IP_FILTERING), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_AUDITING), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_STATS_AND_HEALTH), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_DLS_FLS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
    }

    public void testSecurityGold() {
        XPackLicenseState licenseState = new XPackLicenseState(randomFrom(Settings.EMPTY,
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build()));
        licenseState.update(GOLD, true, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_IP_FILTERING), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_AUDITING), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_STATS_AND_HEALTH), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_DLS_FLS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_STANDARD_REALMS), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_TOKEN_SERVICE), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_API_KEY_SERVICE), is(true));
    }

    public void testSecurityGoldExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(randomFrom(Settings.EMPTY,
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build()));
        licenseState.update(GOLD, false, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_IP_FILTERING), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_AUDITING), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_STATS_AND_HEALTH), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_DLS_FLS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_STANDARD_REALMS), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_TOKEN_SERVICE), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_API_KEY_SERVICE), is(true));
    }

    public void testSecurityPlatinum() {
        XPackLicenseState licenseState = new XPackLicenseState(randomFrom(Settings.EMPTY,
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build()));
        licenseState.update(PLATINUM, true, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_IP_FILTERING), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_AUDITING), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_STATS_AND_HEALTH), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_DLS_FLS), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_ALL_REALMS), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_TOKEN_SERVICE), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_API_KEY_SERVICE), is(true));
    }

    public void testSecurityPlatinumExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(randomFrom(Settings.EMPTY,
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build()));
        licenseState.update(PLATINUM, false, null);

        assertThat(licenseState.isSecurityEnabled(), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_IP_FILTERING), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_AUDITING), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_STATS_AND_HEALTH), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_DLS_FLS), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_ALL_REALMS), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS), is(false));
        assertThat(licenseState.isAllowed(Feature.SECURITY_TOKEN_SERVICE), is(true));
        assertThat(licenseState.isAllowed(Feature.SECURITY_API_KEY_SERVICE), is(true));
    }

    public void testNewTrialDefaultsSecurityOff() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(TRIAL, true, VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));

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

    public void testMonitoringAllowed() {
        assertAllowed(randomMode(), true, s -> s.isAllowed(Feature.MONITORING), true);
        assertAllowed(randomMode(), false, s -> s.isAllowed(Feature.MONITORING), false);
    }

    public void testMonitoringUpdateRetention() {
        assertAllowed(STANDARD, true, s -> s.isAllowed(Feature.MONITORING_UPDATE_RETENTION), true);
        assertAllowed(GOLD, true, s -> s.isAllowed(Feature.MONITORING_UPDATE_RETENTION), true);
        assertAllowed(PLATINUM, true, s -> s.isAllowed(Feature.MONITORING_UPDATE_RETENTION), true);
        assertAllowed(TRIAL, true, s -> s.isAllowed(Feature.MONITORING_UPDATE_RETENTION), true);
        assertAllowed(BASIC, true, s -> s.isAllowed(Feature.MONITORING_UPDATE_RETENTION), false);
        assertAllowed(MISSING, false, s -> s.isAllowed(Feature.MONITORING_UPDATE_RETENTION), false);
    }

    public void testWatcherPlatinumGoldTrialStandard() throws Exception {
        assertAllowed(TRIAL, true, s -> s.isAllowed(Feature.WATCHER), true);
        assertAllowed(GOLD, true, s -> s.isAllowed(Feature.WATCHER), true);
        assertAllowed(PLATINUM, true, s -> s.isAllowed(Feature.WATCHER), true);
        assertAllowed(STANDARD, true, s -> s.isAllowed(Feature.WATCHER), true);
    }

    public void testWatcherBasicLicense() throws Exception {
        assertAllowed(BASIC, true, s -> s.isAllowed(Feature.WATCHER), false);
    }

    public void testWatcherInactive() {
        assertAllowed(BASIC, false, s -> s.isAllowed(Feature.WATCHER), false);
    }

    public void testWatcherInactivePlatinumGoldTrial() throws Exception {
        assertAllowed(TRIAL, false, s -> s.isAllowed(Feature.WATCHER), false);
        assertAllowed(GOLD, false, s -> s.isAllowed(Feature.WATCHER), false);
        assertAllowed(PLATINUM, false, s -> s.isAllowed(Feature.WATCHER), false);
        assertAllowed(STANDARD, false, s -> s.isAllowed(Feature.WATCHER), false);
    }

    public void testGraphPlatinumTrial() throws Exception {
        assertAllowed(TRIAL, true, s -> s.isAllowed(Feature.GRAPH), true);
        assertAllowed(PLATINUM, true, s -> s.isAllowed(Feature.GRAPH), true);
    }

    public void testGraphBasic() throws Exception {
        assertAllowed(BASIC, true, s -> s.isAllowed(Feature.GRAPH), false);
    }

    public void testGraphStandard() throws Exception {
        assertAllowed(STANDARD, true, s -> s.isAllowed(Feature.GRAPH), false);
    }

    public void testGraphInactiveBasic() {
        assertAllowed(BASIC, false, s -> s.isAllowed(Feature.GRAPH), false);
    }

    public void testGraphInactivePlatinumTrial() throws Exception {
        assertAllowed(TRIAL, false, s -> s.isAllowed(Feature.MACHINE_LEARNING), false);
        assertAllowed(PLATINUM, false, s -> s.isAllowed(Feature.MACHINE_LEARNING), false);
    }

    public void testMachineLearningPlatinumTrial() throws Exception {
        assertAllowed(TRIAL, true, s -> s.isAllowed(Feature.MACHINE_LEARNING), true);
        assertAllowed(PLATINUM, true, s -> s.isAllowed(Feature.MACHINE_LEARNING), true);
    }

    public void testMachineLearningBasic() throws Exception {
        assertAllowed(BASIC, true, s -> s.isAllowed(Feature.MACHINE_LEARNING), false);
    }

    public void testMachineLearningStandard() throws Exception {
        assertAllowed(STANDARD, true, s -> s.isAllowed(Feature.MACHINE_LEARNING), false);
    }

    public void testMachineLearningInactiveBasic() {
        assertAllowed(BASIC, false, s -> s.isAllowed(Feature.MACHINE_LEARNING), false);
    }

    public void testMachineLearningInactivePlatinumTrial() throws Exception {
        assertAllowed(TRIAL, false, s -> s.isAllowed(Feature.MACHINE_LEARNING), false);
        assertAllowed(PLATINUM, false, s -> s.isAllowed(Feature.MACHINE_LEARNING), false);
    }

    public void testLogstashPlatinumGoldTrialStandard() throws Exception {
        assertAllowed(TRIAL, true, s -> s.isAllowed(Feature.LOGSTASH), true);
        assertAllowed(GOLD, true, s -> s.isAllowed(Feature.LOGSTASH), true);
        assertAllowed(PLATINUM, true, s -> s.isAllowed(Feature.LOGSTASH), true);
        assertAllowed(STANDARD, true, s -> s.isAllowed(Feature.LOGSTASH), true);
    }

    public void testLogstashBasicLicense() throws Exception {
        assertAllowed(BASIC, true, s -> s.isAllowed(Feature.LOGSTASH), false);
    }

    public void testLogstashInactive() {
        assertAllowed(BASIC, false, s -> s.isAllowed(Feature.LOGSTASH), false);
        assertAllowed(TRIAL, false, s -> s.isAllowed(Feature.LOGSTASH), false);
        assertAllowed(GOLD, false, s -> s.isAllowed(Feature.LOGSTASH), false);
        assertAllowed(PLATINUM, false, s -> s.isAllowed(Feature.LOGSTASH), false);
        assertAllowed(STANDARD, false, s -> s.isAllowed(Feature.LOGSTASH), false);
    }

    public void testSqlDefaults() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.SQL), is(true));
        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.JDBC), is(true));
    }

    public void testSqlBasic() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(BASIC, true, null);

        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.SQL), is(true));
        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.JDBC), is(false));
    }

    public void testSqlBasicExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(BASIC, false, null);

        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.SQL), is(false));
        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.JDBC), is(false));
    }

    public void testSqlStandard() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(STANDARD, true, null);

        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.SQL), is(true));
        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.JDBC), is(false));
    }

    public void testSqlStandardExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(STANDARD, false, null);

        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.SQL), is(false));
        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.JDBC), is(false));
    }

    public void testSqlGold() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(GOLD, true, null);

        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.SQL), is(true));
        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.JDBC), is(false));
    }

    public void testSqlGoldExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(GOLD, false, null);

        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.SQL), is(false));
        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.JDBC), is(false));
    }

    public void testSqlPlatinum() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(PLATINUM, true, null);

        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.SQL), is(true));
        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.JDBC), is(true));
    }

    public void testSqlPlatinumExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(PLATINUM, false, null);

        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.SQL), is(false));
        assertThat(licenseState.isAllowed(XPackLicenseState.Feature.JDBC), is(false));
    }

    public void testSqlAckAnyToTrialOrPlatinum() {
        assertAckMessages(XPackField.SQL, randomMode(), randomTrialOrPlatinumMode(), 0);
    }

    public void testSqlAckTrialOrPlatinumToNotTrialOrPlatinum() {
        assertAckMessages(XPackField.SQL, randomTrialOrPlatinumMode(), randomBasicStandardOrGold(), 1);
    }

    public void testCcrDefaults() {
        final XPackLicenseState state = new XPackLicenseState(Settings.EMPTY);
        assertTrue(state.isAllowed(XPackLicenseState.Feature.CCR));
    }

    public void testCcrBasic() {
        final XPackLicenseState state = new XPackLicenseState(Settings.EMPTY);
        state.update(BASIC, true, null);

        assertThat(state.isAllowed(XPackLicenseState.Feature.CCR), is(false));
    }

    public void testCcrBasicExpired() {
        final XPackLicenseState state = new XPackLicenseState(Settings.EMPTY);
        state.update(BASIC, false, null);

        assertThat(state.isAllowed(XPackLicenseState.Feature.CCR), is(false));
    }

    public void testCcrStandard() {
        final XPackLicenseState state = new XPackLicenseState(Settings.EMPTY);
        state.update(STANDARD, true, null);

        assertThat(state.isAllowed(XPackLicenseState.Feature.CCR), is(false));
    }

    public void testCcrStandardExpired() {
        final XPackLicenseState state = new XPackLicenseState(Settings.EMPTY);
        state.update(STANDARD, false, null);

        assertThat(state.isAllowed(XPackLicenseState.Feature.CCR), is(false));
    }

    public void testCcrGold() {
        final XPackLicenseState state = new XPackLicenseState(Settings.EMPTY);
        state.update(GOLD, true, null);

        assertThat(state.isAllowed(XPackLicenseState.Feature.CCR), is(false));
    }

    public void testCcrGoldExpired() {
        final XPackLicenseState state = new XPackLicenseState(Settings.EMPTY);
        state.update(GOLD, false, null);

        assertThat(state.isAllowed(XPackLicenseState.Feature.CCR), is(false));
    }

    public void testCcrPlatinum() {
        final XPackLicenseState state = new XPackLicenseState(Settings.EMPTY);
        state.update(PLATINUM, true, null);

        assertTrue(state.isAllowed(XPackLicenseState.Feature.CCR));
    }

    public void testCcrPlatinumExpired() {
        final XPackLicenseState state = new XPackLicenseState(Settings.EMPTY);
        state.update(PLATINUM, false, null);

        assertFalse(state.isAllowed(XPackLicenseState.Feature.CCR));
    }

    public void testCcrAckAnyToTrialOrPlatinum() {
        assertAckMessages(XPackField.CCR, randomMode(), randomTrialOrPlatinumMode(), 0);
    }

    public void testCcrAckTrialOrPlatinumToNotTrialOrPlatinum() {
        assertAckMessages(XPackField.CCR, randomTrialOrPlatinumMode(), randomBasicStandardOrGold(), 1);
    }

    public void testTransformBasic() throws Exception {
        assertAllowed(BASIC, true, s -> s.isAllowed(Feature.TRANSFORM), true);
    }

    public void testTransformStandard() throws Exception {
        assertAllowed(STANDARD, true, s -> s.isAllowed(Feature.TRANSFORM), true);
    }

    public void testTransformInactiveBasic() {
        assertAllowed(BASIC, false, s -> s.isAllowed(Feature.TRANSFORM), false);
    }
}
