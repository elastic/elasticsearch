/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SyntheticSourceLicenseServiceTests extends ESTestCase {

    public void testLicenseAllowsSyntheticSource() {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(any())).thenReturn(true);
        var licenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
        licenseService.setLicenseState(licenseState);
        assertFalse("synthetic source is allowed, so not fallback to stored source", licenseService.fallbackToStoredSource(false));
        Mockito.verify(licenseState, Mockito.times(1)).featureUsed(any());
    }

    public void testLicenseAllowsSyntheticSourceTemplateValidation() {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(any())).thenReturn(true);
        var licenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
        licenseService.setLicenseState(licenseState);
        assertFalse("synthetic source is allowed, so not fallback to stored source", licenseService.fallbackToStoredSource(true));
        Mockito.verify(licenseState, Mockito.never()).featureUsed(any());
    }

    public void testDefaultDisallow() {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(any())).thenReturn(false);
        var licenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
        licenseService.setLicenseState(licenseState);
        assertTrue("synthetic source is not allowed, so fallback to stored source", licenseService.fallbackToStoredSource(false));
        Mockito.verify(licenseState, Mockito.never()).featureUsed(any());
    }

    public void testFallback() {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(any())).thenReturn(true);
        var licenseService = new SyntheticSourceLicenseService(Settings.EMPTY);
        licenseService.setLicenseState(licenseState);
        licenseService.setSyntheticSourceFallback(true);
        assertTrue(
            "synthetic source is allowed, but fallback has been enabled, so fallback to stored source",
            licenseService.fallbackToStoredSource(false)
        );
        Mockito.verifyNoInteractions(licenseState);
    }

}
