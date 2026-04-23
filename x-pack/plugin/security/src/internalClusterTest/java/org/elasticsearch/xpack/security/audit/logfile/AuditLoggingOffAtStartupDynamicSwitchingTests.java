/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.logging.log4j.Level;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.io.IOException;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class AuditLoggingOffAtStartupDynamicSwitchingTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.AUDIT_ENABLED.getKey(), false)
            .build();
    }

    public void testFlippingAuditLogFalseToTrueToFalse() throws IOException {
        checkAuditLoggingDisabled("no audit event when disabled");
        setAuditLogsSetting(true);

        checkAuditLoggingEnabled("access_granted event when enabled");

        setAuditLogsSetting(false);
        checkAuditLoggingDisabled("no audit event when disabled (again)");
    }

    void checkAuditLoggingDisabled(String name) throws IOException {
        try (var mockLog = MockLog.capture(LoggingAuditTrail.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(name, LoggingAuditTrail.class.getName(), Level.INFO, "*access_granted*")
            );
            authenticate();
            mockLog.assertAllExpectationsMatched();
        }
    }

    void authenticate() throws IOException {
        Request req = new Request("GET", "/_security/_authenticate");
        RequestOptions.Builder options = req.getOptions().toBuilder();
        options.addHeader(
            "Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(
                SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
            )
        );
        req.setOptions(options);
        getRestClient().performRequest(req);
    }

    protected void setAuditLogsSetting(final boolean settingValue) throws IOException {
        final Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{ \"persistent\": { \"" + XPackSettings.AUDIT_ENABLED.getKey() + "\": " + settingValue + " } }");
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .addHeader(
                    "Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(
                        SecuritySettingsSource.TEST_USER_NAME,
                        new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
                    )
                )
        );
        getRestClient().performRequest(request);
    }

    void checkAuditLoggingEnabled(String name) throws IOException {
        try (var mockLog = MockLog.capture(LoggingAuditTrail.class)) {
            mockLog.addExpectation(
                new MockLog.PatternSeenEventExpectation(
                    name,
                    LoggingAuditTrail.class.getName(),
                    Level.INFO,
                    ".*action=\"cluster:admin/xpack/security/user/authenticate\".*access_granted.*"
                )
            );
            authenticate();
            mockLog.assertAllExpectationsMatched();
        }
    }
}
