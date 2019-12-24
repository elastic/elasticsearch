/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License.OperationMode;
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
    void assertAckMesssages(String feature, OperationMode from, OperationMode to, int expectedMessages) {
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
        assertThat(licenseState.isAuthAllowed(), is(true));
        assertThat(licenseState.isIpFilteringAllowed(), is(true));
        assertThat(licenseState.isAuditingAllowed(), is(true));
        assertThat(licenseState.isStatsAndHealthAllowed(), is(true));
        assertThat(licenseState.isDocumentAndFieldLevelSecurityAllowed(), is(true));
        assertThat(licenseState.allowedRealmType(), is(XPackLicenseState.AllowedRealmType.ALL));
        assertThat(licenseState.isCustomRoleProvidersAllowed(), is(true));

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

        assertThat(licenseState.isAuthAllowed(), is(false));
        assertThat(licenseState.isIpFilteringAllowed(), is(false));
        assertThat(licenseState.isAuditingAllowed(), is(false));
        assertThat(licenseState.isStatsAndHealthAllowed(), is(true));
        assertThat(licenseState.isDocumentAndFieldLevelSecurityAllowed(), is(false));
        assertThat(licenseState.allowedRealmType(), is(XPackLicenseState.AllowedRealmType.NONE));
        assertThat(licenseState.isCustomRoleProvidersAllowed(), is(false));
        assertThat(licenseState.isTokenServiceAllowed(), is(false));
        assertThat(licenseState.isApiKeyServiceAllowed(), is(false));

        assertThat(licenseState.isSecurityAvailable(), is(true));
        assertThat(licenseState.isSecurityDisabledByLicenseDefaults(), is(true));
    }

    public void testSecurityBasicWithExplicitSecurityEnabled() {
        final Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
        XPackLicenseState licenseState = new XPackLicenseState(settings);
        licenseState.update(BASIC, true, null);

        assertThat(licenseState.isAuthAllowed(), is(true));
        assertThat(licenseState.isIpFilteringAllowed(), is(false));
        assertThat(licenseState.isAuditingAllowed(), is(false));
        assertThat(licenseState.isStatsAndHealthAllowed(), is(true));
        assertThat(licenseState.isDocumentAndFieldLevelSecurityAllowed(), is(false));
        assertThat(licenseState.allowedRealmType(), is(XPackLicenseState.AllowedRealmType.NATIVE));
        assertThat(licenseState.isCustomRoleProvidersAllowed(), is(false));
        assertThat(licenseState.isTokenServiceAllowed(), is(false));
        assertThat(licenseState.isApiKeyServiceAllowed(), is(true));

        assertThat(licenseState.isSecurityAvailable(), is(true));
        assertThat(licenseState.isSecurityDisabledByLicenseDefaults(), is(false));
    }

    public void testSecurityDefaultBasicExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(BASIC, false, null);

        assertThat(licenseState.isAuthAllowed(), is(false));
        assertThat(licenseState.isIpFilteringAllowed(), is(false));
        assertThat(licenseState.isAuditingAllowed(), is(false));
        assertThat(licenseState.isStatsAndHealthAllowed(), is(false));
        assertThat(licenseState.isDocumentAndFieldLevelSecurityAllowed(), is(false));
        assertThat(licenseState.allowedRealmType(), is(XPackLicenseState.AllowedRealmType.NONE));
        assertThat(licenseState.isCustomRoleProvidersAllowed(), is(false));
        assertThat(licenseState.isTokenServiceAllowed(), is(false));
        assertThat(licenseState.isApiKeyServiceAllowed(), is(false));
    }

    public void testSecurityEnabledBasicExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(
            Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build());
        licenseState.update(BASIC, false, null);

        assertThat(licenseState.isAuthAllowed(), is(true));
        assertThat(licenseState.isIpFilteringAllowed(), is(false));
        assertThat(licenseState.isAuditingAllowed(), is(false));
        assertThat(licenseState.isStatsAndHealthAllowed(), is(false));
        assertThat(licenseState.isDocumentAndFieldLevelSecurityAllowed(), is(false));
        assertThat(licenseState.allowedRealmType(), is(XPackLicenseState.AllowedRealmType.NATIVE));
        assertThat(licenseState.isCustomRoleProvidersAllowed(), is(false));
        assertThat(licenseState.isTokenServiceAllowed(), is(false));
        assertThat(licenseState.isApiKeyServiceAllowed(), is(true));
    }

    public void testSecurityStandard() {
        XPackLicenseState licenseState = new XPackLicenseState(randomFrom(Settings.EMPTY,
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build()));
        licenseState.update(STANDARD, true, null);

        assertThat(licenseState.isAuthAllowed(), is(true));
        assertThat(licenseState.isIpFilteringAllowed(), is(false));
        assertThat(licenseState.isAuditingAllowed(), is(false));
        assertThat(licenseState.isStatsAndHealthAllowed(), is(true));
        assertThat(licenseState.isDocumentAndFieldLevelSecurityAllowed(), is(false));
        assertThat(licenseState.allowedRealmType(), is(XPackLicenseState.AllowedRealmType.NATIVE));
        assertThat(licenseState.isCustomRoleProvidersAllowed(), is(false));
    }

    public void testSecurityStandardExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(randomFrom(Settings.EMPTY,
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build()));
        licenseState.update(STANDARD, false, null);

        assertThat(licenseState.isAuthAllowed(), is(true));
        assertThat(licenseState.isIpFilteringAllowed(), is(false));
        assertThat(licenseState.isAuditingAllowed(), is(false));
        assertThat(licenseState.isStatsAndHealthAllowed(), is(false));
        assertThat(licenseState.isDocumentAndFieldLevelSecurityAllowed(), is(false));
        assertThat(licenseState.allowedRealmType(), is(XPackLicenseState.AllowedRealmType.NATIVE));
        assertThat(licenseState.isCustomRoleProvidersAllowed(), is(false));
    }

    public void testSecurityGold() {
        XPackLicenseState licenseState = new XPackLicenseState(randomFrom(Settings.EMPTY,
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build()));
        licenseState.update(GOLD, true, null);

        assertThat(licenseState.isAuthAllowed(), is(true));
        assertThat(licenseState.isIpFilteringAllowed(), is(true));
        assertThat(licenseState.isAuditingAllowed(), is(true));
        assertThat(licenseState.isStatsAndHealthAllowed(), is(true));
        assertThat(licenseState.isDocumentAndFieldLevelSecurityAllowed(), is(false));
        assertThat(licenseState.allowedRealmType(), is(XPackLicenseState.AllowedRealmType.DEFAULT));
        assertThat(licenseState.isCustomRoleProvidersAllowed(), is(false));
        assertThat(licenseState.isTokenServiceAllowed(), is(true));
        assertThat(licenseState.isApiKeyServiceAllowed(), is(true));
    }

    public void testSecurityGoldExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(randomFrom(Settings.EMPTY,
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build()));
        licenseState.update(GOLD, false, null);

        assertThat(licenseState.isAuthAllowed(), is(true));
        assertThat(licenseState.isIpFilteringAllowed(), is(true));
        assertThat(licenseState.isAuditingAllowed(), is(true));
        assertThat(licenseState.isStatsAndHealthAllowed(), is(false));
        assertThat(licenseState.isDocumentAndFieldLevelSecurityAllowed(), is(false));
        assertThat(licenseState.allowedRealmType(), is(XPackLicenseState.AllowedRealmType.DEFAULT));
        assertThat(licenseState.isCustomRoleProvidersAllowed(), is(false));
        assertThat(licenseState.isTokenServiceAllowed(), is(true));
        assertThat(licenseState.isApiKeyServiceAllowed(), is(true));
    }

    public void testSecurityPlatinum() {
        XPackLicenseState licenseState = new XPackLicenseState(randomFrom(Settings.EMPTY,
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build()));
        licenseState.update(PLATINUM, true, null);

        assertThat(licenseState.isAuthAllowed(), is(true));
        assertThat(licenseState.isIpFilteringAllowed(), is(true));
        assertThat(licenseState.isAuditingAllowed(), is(true));
        assertThat(licenseState.isStatsAndHealthAllowed(), is(true));
        assertThat(licenseState.isDocumentAndFieldLevelSecurityAllowed(), is(true));
        assertThat(licenseState.allowedRealmType(), is(XPackLicenseState.AllowedRealmType.ALL));
        assertThat(licenseState.isCustomRoleProvidersAllowed(), is(true));
        assertThat(licenseState.isTokenServiceAllowed(), is(true));
        assertThat(licenseState.isApiKeyServiceAllowed(), is(true));
    }

    public void testSecurityPlatinumExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(randomFrom(Settings.EMPTY,
                Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build()));
        licenseState.update(PLATINUM, false, null);

        assertThat(licenseState.isAuthAllowed(), is(true));
        assertThat(licenseState.isIpFilteringAllowed(), is(true));
        assertThat(licenseState.isAuditingAllowed(), is(true));
        assertThat(licenseState.isStatsAndHealthAllowed(), is(false));
        assertThat(licenseState.isDocumentAndFieldLevelSecurityAllowed(), is(true));
        assertThat(licenseState.allowedRealmType(), is(XPackLicenseState.AllowedRealmType.ALL));
        assertThat(licenseState.isCustomRoleProvidersAllowed(), is(false));
        assertThat(licenseState.isTokenServiceAllowed(), is(true));
        assertThat(licenseState.isApiKeyServiceAllowed(), is(true));
    }

    public void testNewTrialDefaultsSecurityOff() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(TRIAL, true, VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));

        assertThat(licenseState.isSecurityDisabledByLicenseDefaults(), is(true));
        assertSecurityNotAllowed(licenseState);
    }

    private void assertSecurityNotAllowed(XPackLicenseState licenseState) {
        assertThat(licenseState.isAuthAllowed(), is(false));
        assertThat(licenseState.isIpFilteringAllowed(), is(false));
        assertThat(licenseState.isAuditingAllowed(), is(false));
        assertThat(licenseState.isStatsAndHealthAllowed(), is(true));
        assertThat(licenseState.isDocumentAndFieldLevelSecurityAllowed(), is(false));
        assertThat(licenseState.allowedRealmType(), is(XPackLicenseState.AllowedRealmType.NONE));
        assertThat(licenseState.isCustomRoleProvidersAllowed(), is(false));
    }

    public void testSecurityAckBasicToNotGoldOrStandard() {
        OperationMode toMode = randomFrom(OperationMode.values(), mode -> mode != GOLD && mode != STANDARD);
        assertAckMesssages(XPackField.SECURITY, BASIC, toMode, 0);
    }

    public void testSecurityAckAnyToTrialOrPlatinum() {
        assertAckMesssages(XPackField.SECURITY, randomMode(), randomTrialOrPlatinumMode(), 0);
    }

    public void testSecurityAckTrialGoldOrPlatinumToBasic() {
        assertAckMesssages(XPackField.SECURITY, randomTrialGoldOrPlatinumMode(), BASIC, 7);
    }

    public void testSecurityAckStandardToBasic() {
        assertAckMesssages(XPackField.SECURITY, STANDARD, BASIC, 1);
    }

    public void testSecurityAckAnyToStandard() {
        OperationMode from = randomFrom(BASIC, GOLD, PLATINUM, TRIAL);
        assertAckMesssages(XPackField.SECURITY, from, STANDARD, 5);
    }

    public void testSecurityAckBasicStandardTrialOrPlatinumToGold() {
        OperationMode from = randomFrom(BASIC, PLATINUM, TRIAL, STANDARD);
        assertAckMesssages(XPackField.SECURITY, from, GOLD, 3);
    }

    public void testMonitoringAckBasicToAny() {
        assertAckMesssages(XPackField.MONITORING, BASIC, randomMode(), 0);
    }

    public void testMonitoringAckAnyToTrialGoldOrPlatinum() {
        assertAckMesssages(XPackField.MONITORING, randomMode(), randomTrialStandardGoldOrPlatinumMode(), 0);
    }

    public void testMonitoringAckNotBasicToBasic() {
        OperationMode from = randomFrom(STANDARD, GOLD, PLATINUM, TRIAL);
        assertAckMesssages(XPackField.MONITORING, from, BASIC, 2);
    }

    public void testMonitoringAllowed() {
        assertAllowed(randomMode(), true, XPackLicenseState::isMonitoringAllowed, true);
        assertAllowed(randomMode(), false, XPackLicenseState::isMonitoringAllowed, false);
    }

    public void testMonitoringUpdateRetention() {
        assertAllowed(STANDARD, true, XPackLicenseState::isUpdateRetentionAllowed, true);
        assertAllowed(GOLD, true, XPackLicenseState::isUpdateRetentionAllowed, true);
        assertAllowed(PLATINUM, true, XPackLicenseState::isUpdateRetentionAllowed, true);
        assertAllowed(TRIAL, true, XPackLicenseState::isUpdateRetentionAllowed, true);
        assertAllowed(BASIC, true, XPackLicenseState::isUpdateRetentionAllowed, false);
        assertAllowed(MISSING, false, XPackLicenseState::isUpdateRetentionAllowed, false);
    }

    public void testWatcherPlatinumGoldTrialStandard() throws Exception {
        assertAllowed(TRIAL, true, XPackLicenseState::isWatcherAllowed, true);
        assertAllowed(GOLD, true, XPackLicenseState::isWatcherAllowed, true);
        assertAllowed(PLATINUM, true, XPackLicenseState::isWatcherAllowed, true);
        assertAllowed(STANDARD, true, XPackLicenseState::isWatcherAllowed, true);
    }

    public void testWatcherBasicLicense() throws Exception {
        assertAllowed(BASIC, true, XPackLicenseState::isWatcherAllowed, false);
    }

    public void testWatcherInactive() {
        assertAllowed(BASIC, false, XPackLicenseState::isWatcherAllowed, false);
    }

    public void testWatcherInactivePlatinumGoldTrial() throws Exception {
        assertAllowed(TRIAL, false, XPackLicenseState::isWatcherAllowed, false);
        assertAllowed(GOLD, false, XPackLicenseState::isWatcherAllowed, false);
        assertAllowed(PLATINUM, false, XPackLicenseState::isWatcherAllowed, false);
        assertAllowed(STANDARD, false, XPackLicenseState::isWatcherAllowed, false);
    }

    public void testGraphPlatinumTrial() throws Exception {
        assertAllowed(TRIAL, true, XPackLicenseState::isGraphAllowed, true);
        assertAllowed(PLATINUM, true, XPackLicenseState::isGraphAllowed, true);
    }

    public void testGraphBasic() throws Exception {
        assertAllowed(BASIC, true, XPackLicenseState::isGraphAllowed, false);
    }

    public void testGraphStandard() throws Exception {
        assertAllowed(STANDARD, true, XPackLicenseState::isGraphAllowed, false);
    }

    public void testGraphInactiveBasic() {
        assertAllowed(BASIC, false, XPackLicenseState::isGraphAllowed, false);
    }

    public void testGraphInactivePlatinumTrial() throws Exception {
        assertAllowed(TRIAL, false, XPackLicenseState::isMachineLearningAllowed, false);
        assertAllowed(PLATINUM, false, XPackLicenseState::isMachineLearningAllowed, false);
    }

    public void testMachineLearningPlatinumTrial() throws Exception {
        assertAllowed(TRIAL, true, XPackLicenseState::isMachineLearningAllowed, true);
        assertAllowed(PLATINUM, true, XPackLicenseState::isMachineLearningAllowed, true);
    }

    public void testMachineLearningBasic() throws Exception {
        assertAllowed(BASIC, true, XPackLicenseState::isMachineLearningAllowed, false);
    }

    public void testMachineLearningStandard() throws Exception {
        assertAllowed(STANDARD, true, XPackLicenseState::isMachineLearningAllowed, false);
    }

    public void testMachineLearningInactiveBasic() {
        assertAllowed(BASIC, false, XPackLicenseState::isMachineLearningAllowed, false);
    }

    public void testMachineLearningInactivePlatinumTrial() throws Exception {
        assertAllowed(TRIAL, false, XPackLicenseState::isMachineLearningAllowed, false);
        assertAllowed(PLATINUM, false, XPackLicenseState::isMachineLearningAllowed, false);
    }

    public void testLogstashPlatinumGoldTrialStandard() throws Exception {
        assertAllowed(TRIAL, true, XPackLicenseState::isLogstashAllowed, true);
        assertAllowed(GOLD, true, XPackLicenseState::isLogstashAllowed, true);
        assertAllowed(PLATINUM, true, XPackLicenseState::isLogstashAllowed, true);
        assertAllowed(STANDARD, true, XPackLicenseState::isLogstashAllowed, true);
    }

    public void testLogstashBasicLicense() throws Exception {
        assertAllowed(BASIC, true, XPackLicenseState::isLogstashAllowed, false);
    }

    public void testLogstashInactive() {
        assertAllowed(BASIC, false, XPackLicenseState::isLogstashAllowed, false);
        assertAllowed(TRIAL, false, XPackLicenseState::isLogstashAllowed, false);
        assertAllowed(GOLD, false, XPackLicenseState::isLogstashAllowed, false);
        assertAllowed(PLATINUM, false, XPackLicenseState::isLogstashAllowed, false);
        assertAllowed(STANDARD, false, XPackLicenseState::isLogstashAllowed, false);
    }

    public void testSqlDefaults() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        assertThat(licenseState.isSqlAllowed(), is(true));
        assertThat(licenseState.isJdbcAllowed(), is(true));
    }

    public void testSqlBasic() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(BASIC, true, null);

        assertThat(licenseState.isSqlAllowed(), is(true));
        assertThat(licenseState.isJdbcAllowed(), is(false));
    }

    public void testSqlBasicExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(BASIC, false, null);

        assertThat(licenseState.isSqlAllowed(), is(false));
        assertThat(licenseState.isJdbcAllowed(), is(false));
    }

    public void testSqlStandard() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(STANDARD, true, null);

        assertThat(licenseState.isSqlAllowed(), is(true));
        assertThat(licenseState.isJdbcAllowed(), is(false));
    }

    public void testSqlStandardExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(STANDARD, false, null);

        assertThat(licenseState.isSqlAllowed(), is(false));
        assertThat(licenseState.isJdbcAllowed(), is(false));
    }

    public void testSqlGold() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(GOLD, true, null);

        assertThat(licenseState.isSqlAllowed(), is(true));
        assertThat(licenseState.isJdbcAllowed(), is(false));
    }

    public void testSqlGoldExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(GOLD, false, null);

        assertThat(licenseState.isSqlAllowed(), is(false));
        assertThat(licenseState.isJdbcAllowed(), is(false));
    }

    public void testSqlPlatinum() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(PLATINUM, true, null);

        assertThat(licenseState.isSqlAllowed(), is(true));
        assertThat(licenseState.isJdbcAllowed(), is(true));
    }

    public void testSqlPlatinumExpired() {
        XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY);
        licenseState.update(PLATINUM, false, null);

        assertThat(licenseState.isSqlAllowed(), is(false));
        assertThat(licenseState.isJdbcAllowed(), is(false));
    }

    public void testSqlAckAnyToTrialOrPlatinum() {
        assertAckMesssages(XPackField.SQL, randomMode(), randomTrialOrPlatinumMode(), 0);
    }

    public void testSqlAckTrialOrPlatinumToNotTrialOrPlatinum() {
        assertAckMesssages(XPackField.SQL, randomTrialOrPlatinumMode(), randomBasicStandardOrGold(), 1);
    }

    public void testTransformBasic() throws Exception {
        assertAllowed(BASIC, true, XPackLicenseState::isTransformAllowed, true);
    }

    public void testTransformStandard() throws Exception {
        assertAllowed(STANDARD, true, XPackLicenseState::isTransformAllowed, true);
    }

    public void testTransformInactiveBasic() {
        assertAllowed(BASIC, false, XPackLicenseState::isTransformAllowed, false);
    }
}
