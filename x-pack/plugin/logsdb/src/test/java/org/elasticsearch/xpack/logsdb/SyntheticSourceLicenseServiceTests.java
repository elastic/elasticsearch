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

import java.util.UUID;

import static org.elasticsearch.license.TestUtils.dateMath;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SyntheticSourceLicenseServiceTests extends ESTestCase {

    private LicenseService mockLicenseService;
    private SyntheticSourceLicenseService licenseService;

    @Before
    public void setup() throws Exception {
        mockLicenseService = mock(LicenseService.class);
        License license = createDummyLicense();
        when(mockLicenseService.getLicense()).thenReturn(license);
        licenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
    }

    public void testLicenseAllowsSyntheticSource() {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(any())).thenReturn(true);
        licenseService.setLicenseState(licenseState);
        licenseService.setLicenseService(mockLicenseService);
        assertFalse("synthetic source is allowed, so not fallback to stored source", licenseService.fallbackToStoredSource(false, false));
        Mockito.verify(licenseState, Mockito.times(1)).featureUsed(any());
    }

    public void testLicenseAllowsSyntheticSourceTemplateValidation() {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(any())).thenReturn(true);
        licenseService.setLicenseState(licenseState);
        licenseService.setLicenseService(mockLicenseService);
        assertFalse("synthetic source is allowed, so not fallback to stored source", licenseService.fallbackToStoredSource(true, false));
        Mockito.verify(licenseState, Mockito.never()).featureUsed(any());
    }

    public void testDefaultDisallow() {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(any())).thenReturn(false);
        licenseService.setLicenseState(licenseState);
        licenseService.setLicenseService(mockLicenseService);
        assertTrue("synthetic source is not allowed, so fallback to stored source", licenseService.fallbackToStoredSource(false, false));
        Mockito.verify(licenseState, Mockito.never()).featureUsed(any());
    }

    public void testFallback() {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(any())).thenReturn(true);
        licenseService.setLicenseState(licenseState);
        licenseService.setLicenseService(mockLicenseService);
        licenseService.setSyntheticSourceFallback(true);
        assertTrue(
            "synthetic source is allowed, but fallback has been enabled, so fallback to stored source",
            licenseService.fallbackToStoredSource(false, false)
        );
        Mockito.verifyNoInteractions(licenseState);
    }

    static License createDummyLicense() throws Exception {
        long now = System.currentTimeMillis();
        String uid = UUID.randomUUID().toString();
        final License.Builder builder = License.builder()
            .uid(uid)
            .version(License.VERSION_CURRENT)
            .expiryDate(dateMath("now+2h", now))
            .startDate(now)
            .issueDate(now)
            .type("basic")
            .issuedTo("customer")
            .issuer("elasticsearch")
            .maxNodes(5);
        License license = TestUtils.generateSignedLicense(builder);
        return license;
    }
}
