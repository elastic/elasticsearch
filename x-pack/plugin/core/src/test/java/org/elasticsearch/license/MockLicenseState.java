/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.mockito.Mockito;

import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.mockito.Mockito.doAnswer;

/** A license state that may be mocked by testing because the internal methods are made public */
public class MockLicenseState extends XPackLicenseState {

    public MockLicenseState(LongSupplier epochMillisProvider) {
        super(epochMillisProvider);
    }

    @Override
    public boolean isAllowed(LicensedFeature feature) {
        return super.isAllowed(feature);
    }

    @Override
    public void enableUsageTracking(LicensedFeature feature, String contextName) {
        super.enableUsageTracking(feature, contextName);
    }

    @Override
    public void disableUsageTracking(LicensedFeature feature, String contextName) {
        super.disableUsageTracking(feature, contextName);
    }

    public static MockLicenseState createMock() {
        MockLicenseState mock = Mockito.mock(MockLicenseState.class);
        Mockito.when(mock.copyCurrentLicenseState()).thenReturn(mock);
        return mock;
    }

    public static void acceptListeners(MockLicenseState licenseState, Consumer<LicenseStateListener> addListener) {
        doAnswer(inv -> {
            final LicenseStateListener listener = (LicenseStateListener) inv.getArguments()[0];
            addListener.accept(listener);
            return null;
        }).when(licenseState).addListener(Mockito.any());
    }
}
