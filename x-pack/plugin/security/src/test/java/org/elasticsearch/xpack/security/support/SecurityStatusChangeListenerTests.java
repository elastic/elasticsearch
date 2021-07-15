/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
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
        logAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
            "built-in security features are not enabled",
            listener.getClass().getName(),
            Level.WARN,
            "Elasticsearch built-in security features are not enabled. Without authentication, your cluster could be accessible " +
                "to anyone. See https://www.elastic.co/guide/en/elasticsearch/reference/" + Version.CURRENT.major + "." +
                Version.CURRENT.minor + "/security-minimal-setup.html to enable security."
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
        assertWarnings("The default behavior of disabling security on basic"
            + " licenses is deprecated. In a later version of Elasticsearch, the value of [xpack.security.enabled] will "
            + "default to \"true\" , regardless of the license level. "
            + "See https://www.elastic.co/guide/en/elasticsearch/reference/7.15/security-minimal-setup.html to enable security, "
            + "or explicitly disable security by setting [xpack.security.enabled] to false in elasticsearch.yml");

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
        logAppender.addExpectation(new MockLogAppender.SeenEventExpectation(
            "built-in security features are not enabled",
            listener.getClass().getName(),
            Level.WARN,
            "Elasticsearch built-in security features are not enabled. Without authentication, your cluster could be accessible " +
                "to anyone. See https://www.elastic.co/guide/en/elasticsearch/reference/" + Version.CURRENT.major + "." +
                Version.CURRENT.minor + "/security-minimal-setup.html to enable security."
        ));
        listener.licenseStateChanged();
        assertWarnings("The default behavior of disabling security on trial"
            + " licenses is deprecated. In a later version of Elasticsearch, the value of [xpack.security.enabled] will "
            + "default to \"true\" , regardless of the license level. "
            + "See https://www.elastic.co/guide/en/elasticsearch/reference/7.15/security-minimal-setup.html to enable security, "
            + "or explicitly disable security by setting [xpack.security.enabled] to false in elasticsearch.yml");

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

    public void testWarningForImplicitlyDisabledSecurity() {
        when(licenseState.isSecurityEnabled()).thenReturn(false);
        when(licenseState.getOperationMode()).thenReturn(License.OperationMode.TRIAL);
        listener.licenseStateChanged();
        assertWarnings("The default behavior of disabling security on trial"
            + " licenses is deprecated. In a later version of Elasticsearch, the value of [xpack.security.enabled] will "
            + "default to \"true\" , regardless of the license level. "
            + "See https://www.elastic.co/guide/en/elasticsearch/reference/7.15/security-minimal-setup.html to enable security, "
            + "or explicitly disable security by setting [xpack.security.enabled] to false in elasticsearch.yml");
    }

}
