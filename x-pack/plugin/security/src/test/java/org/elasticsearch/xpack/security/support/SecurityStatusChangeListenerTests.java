/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class SecurityStatusChangeListenerTests extends ESTestCase {

    private XPackLicenseState licenseState;
    private SecurityStatusChangeListener listener;
    private MockLogAppender logAppender;
    private Logger listenerLogger;

    @Before
    public void setup() throws IllegalAccessException {
        licenseState = Mockito.mock(XPackLicenseState.class);
        when(licenseState.isAllowed(XPackLicenseState.Feature.SECURITY)).thenReturn(true);

        listener = new SecurityStatusChangeListener(licenseState);

        logAppender = new MockLogAppender();
        logAppender.start();
        listenerLogger = LogManager.getLogger(listener.getClass());
        Loggers.addAppender(listenerLogger, logAppender);
    }

    @After
    public void cleanup() {
        Loggers.removeAppender(listenerLogger, logAppender);
        logAppender.stop();
    }

    public void testSecurityEnabledToDisabled() {
        when(licenseState.isSecurityEnabled()).thenReturn(true);

        when(licenseState.getOperationMode()).thenReturn(License.OperationMode.GOLD);
        logAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
            "initial change",
            listener.getClass().getName(),
            Level.INFO,
            "Active license is now [GOLD]; Security is enabled"
        ));
        listener.licenseStateChanged();

        when(licenseState.getOperationMode()).thenReturn(License.OperationMode.PLATINUM);
        logAppender.addExpectation(new MockLogAppender.UnseenEventExpectation(
            "no-op change",
            listener.getClass().getName(),
            Level.INFO,
            "Active license is now [PLATINUM]; Security is enabled"
        ));

        when(licenseState.isSecurityEnabled()).thenReturn(false);
        when(licenseState.getOperationMode()).thenReturn(License.OperationMode.BASIC);
        logAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
            "change to basic",
            listener.getClass().getName(),
            Level.INFO,
            "Active license is now [BASIC]; Security is disabled"
        ));
        listener.licenseStateChanged();

        logAppender.assertAllExpectationsMatched();
    }

    public void testSecurityDisabledToEnabled() {
        when(licenseState.isSecurityEnabled()).thenReturn(false);

        when(licenseState.getOperationMode()).thenReturn(License.OperationMode.TRIAL);
        logAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
            "initial change",
            listener.getClass().getName(),
            Level.INFO,
            "Active license is now [TRIAL]; Security is disabled"
        ));
        listener.licenseStateChanged();

        when(licenseState.getOperationMode()).thenReturn(License.OperationMode.BASIC);
        logAppender.addExpectation(new MockLogAppender.UnseenEventExpectation(
            "no-op change",
            listener.getClass().getName(),
            Level.INFO,
            "Active license is now [BASIC]; Security is disabled"
        ));

        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.getOperationMode()).thenReturn(License.OperationMode.PLATINUM);
        logAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
            "change to platinum",
            listener.getClass().getName(),
            Level.INFO,
            "Active license is now [PLATINUM]; Security is enabled"
        ));
        listener.licenseStateChanged();

        logAppender.assertAllExpectationsMatched();
    }

}
