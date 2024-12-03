/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.elasticsearch.license.TestUtils.dateMath;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SyntheticSourceLicenseServiceTests extends ESTestCase {

    private LicenseService mockLicenseService;
    private SyntheticSourceLicenseService licenseService;

    @Before
    public void setup() throws Exception {
        mockLicenseService = mock(LicenseService.class);
        License license = createEnterpriseLicense();
        when(mockLicenseService.getLicense()).thenReturn(license);
        licenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
    }

    public void testLicenseAllowsSyntheticSource() {
        MockLicenseState licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(same(SyntheticSourceLicenseService.SYNTHETIC_SOURCE_FEATURE))).thenReturn(true);
        licenseService.setLicenseState(licenseState);
        licenseService.setLicenseService(mockLicenseService);
        assertFalse(
            "synthetic source is allowed, so not fallback to stored source",
            licenseService.fallbackToStoredSource(false, randomBoolean())
        );
        Mockito.verify(licenseState, Mockito.times(1)).featureUsed(any());
    }

    public void testLicenseAllowsSyntheticSourceTemplateValidation() {
        MockLicenseState licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(same(SyntheticSourceLicenseService.SYNTHETIC_SOURCE_FEATURE))).thenReturn(true);
        licenseService.setLicenseState(licenseState);
        licenseService.setLicenseService(mockLicenseService);
        assertFalse(
            "synthetic source is allowed, so not fallback to stored source",
            licenseService.fallbackToStoredSource(true, randomBoolean())
        );
        Mockito.verify(licenseState, Mockito.never()).featureUsed(any());
    }

    public void testDefaultDisallow() {
        MockLicenseState licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(same(SyntheticSourceLicenseService.SYNTHETIC_SOURCE_FEATURE))).thenReturn(false);
        licenseService.setLicenseState(licenseState);
        licenseService.setLicenseService(mockLicenseService);
        assertTrue(
            "synthetic source is not allowed, so fallback to stored source",
            licenseService.fallbackToStoredSource(false, randomBoolean())
        );
        Mockito.verify(licenseState, Mockito.never()).featureUsed(any());
    }

    public void testFallback() {
        MockLicenseState licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(same(SyntheticSourceLicenseService.SYNTHETIC_SOURCE_FEATURE))).thenReturn(true);
        licenseService.setLicenseState(licenseState);
        licenseService.setLicenseService(mockLicenseService);
        licenseService.setSyntheticSourceFallback(true);
        assertTrue(
            "synthetic source is allowed, but fallback has been enabled, so fallback to stored source",
            licenseService.fallbackToStoredSource(false, randomBoolean())
        );
        Mockito.verifyNoInteractions(licenseState);
        Mockito.verifyNoInteractions(mockLicenseService);
    }

    public void testGoldOrPlatinumLicense() throws Exception {
        mockLicenseService = mock(LicenseService.class);
        License license = createGoldOrPlatinumLicense();
        when(mockLicenseService.getLicense()).thenReturn(license);

        MockLicenseState licenseState = MockLicenseState.createMock();
        when(licenseState.getOperationMode()).thenReturn(license.operationMode());
        when(licenseState.isAllowed(same(SyntheticSourceLicenseService.SYNTHETIC_SOURCE_FEATURE_LEGACY))).thenReturn(true);
        licenseService.setLicenseState(licenseState);
        licenseService.setLicenseService(mockLicenseService);
        assertFalse(
            "legacy licensed usage is allowed, so not fallback to stored source",
            licenseService.fallbackToStoredSource(false, true)
        );
        Mockito.verify(licenseState, Mockito.times(1)).featureUsed(any());
    }

    public void testGoldOrPlatinumLicenseLegacyLicenseNotAllowed() throws Exception {
        mockLicenseService = mock(LicenseService.class);
        License license = createGoldOrPlatinumLicense();
        when(mockLicenseService.getLicense()).thenReturn(license);

        MockLicenseState licenseState = MockLicenseState.createMock();
        when(licenseState.getOperationMode()).thenReturn(license.operationMode());
        when(licenseState.isAllowed(same(SyntheticSourceLicenseService.SYNTHETIC_SOURCE_FEATURE))).thenReturn(false);
        licenseService.setLicenseState(licenseState);
        licenseService.setLicenseService(mockLicenseService);
        assertTrue(
            "legacy licensed usage is not allowed, so fallback to stored source",
            licenseService.fallbackToStoredSource(false, false)
        );
        Mockito.verify(licenseState, Mockito.never()).featureUsed(any());
        Mockito.verify(licenseState, Mockito.times(1)).isAllowed(same(SyntheticSourceLicenseService.SYNTHETIC_SOURCE_FEATURE));
    }

    public void testGoldOrPlatinumLicenseBeyondCutoffDate() throws Exception {
        long start = LocalDateTime.of(2025, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        License license = createGoldOrPlatinumLicense(start);
        mockLicenseService = mock(LicenseService.class);
        when(mockLicenseService.getLicense()).thenReturn(license);

        MockLicenseState licenseState = MockLicenseState.createMock();
        when(licenseState.getOperationMode()).thenReturn(license.operationMode());
        when(licenseState.isAllowed(same(SyntheticSourceLicenseService.SYNTHETIC_SOURCE_FEATURE))).thenReturn(false);
        licenseService.setLicenseState(licenseState);
        licenseService.setLicenseService(mockLicenseService);
        assertTrue("beyond cutoff date, so fallback to stored source", licenseService.fallbackToStoredSource(false, true));
        Mockito.verify(licenseState, Mockito.never()).featureUsed(any());
        Mockito.verify(licenseState, Mockito.times(1)).isAllowed(same(SyntheticSourceLicenseService.SYNTHETIC_SOURCE_FEATURE));
    }

    public void testGoldOrPlatinumLicenseCustomCutoffDate() throws Exception {
        licenseService = new SyntheticSourceLicenseService(Settings.EMPTY, "2025-01-02T00:00");

        long start = LocalDateTime.of(2025, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        License license = createGoldOrPlatinumLicense(start);
        mockLicenseService = mock(LicenseService.class);
        when(mockLicenseService.getLicense()).thenReturn(license);

        MockLicenseState licenseState = MockLicenseState.createMock();
        when(licenseState.getOperationMode()).thenReturn(license.operationMode());
        when(licenseState.isAllowed(same(SyntheticSourceLicenseService.SYNTHETIC_SOURCE_FEATURE_LEGACY))).thenReturn(true);
        licenseService.setLicenseState(licenseState);
        licenseService.setLicenseService(mockLicenseService);
        assertFalse("custom cutoff date, so fallback to stored source", licenseService.fallbackToStoredSource(false, true));
        Mockito.verify(licenseState, Mockito.times(1)).featureUsed(any());
        Mockito.verify(licenseState, Mockito.times(1)).isAllowed(same(SyntheticSourceLicenseService.SYNTHETIC_SOURCE_FEATURE_LEGACY));
    }

    static License createEnterpriseLicense() throws Exception {
        long start = LocalDateTime.of(2024, 11, 12, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        return createEnterpriseLicense(start);
    }

    static License createEnterpriseLicense(long start) throws Exception {
        String uid = UUID.randomUUID().toString();
        long currentTime = System.currentTimeMillis();
        final License.Builder builder = License.builder()
            .uid(uid)
            .version(License.VERSION_CURRENT)
            .expiryDate(dateMath("now+2d", currentTime))
            .startDate(start)
            .issueDate(currentTime)
            .type("enterprise")
            .issuedTo("customer")
            .issuer("elasticsearch")
            .maxResourceUnits(10);
        return TestUtils.generateSignedLicense(builder);
    }

    static License createGoldOrPlatinumLicense() throws Exception {
        long start = LocalDateTime.of(2024, 11, 12, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        return createGoldOrPlatinumLicense(start);
    }

    static License createGoldOrPlatinumLicense(long start) throws Exception {
        String uid = UUID.randomUUID().toString();
        long currentTime = System.currentTimeMillis();
        final License.Builder builder = License.builder()
            .uid(uid)
            .version(License.VERSION_CURRENT)
            .expiryDate(dateMath("now+100d", currentTime))
            .startDate(start)
            .issueDate(currentTime)
            .type(randomBoolean() ? "gold" : "platinum")
            .issuedTo("customer")
            .issuer("elasticsearch")
            .maxNodes(5);
        return TestUtils.generateSignedLicense(builder);
    }
}
