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
import java.util.Optional;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public abstract class AbstractAuditLoggingDynamicSwitchingTestCase extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected final Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        Optional<Boolean> auditEnabled = startupAuditEnabled();
        if (auditEnabled.isPresent()) {
            builder.put(XPackSettings.AUDIT_ENABLED.getKey(), auditEnabled.get());
        } else {
            // Remove any value that SecuritySettingsSource (which uses randomBoolean()) may have set
            builder.remove(XPackSettings.AUDIT_ENABLED.getKey());
        }
        return builder.build();
    }

    /**
     * Returns the value to set for {@code xpack.security.audit.enabled} at node startup.
     * An empty optional means the setting is omitted entirely (exercising the default behavior).
     */
    protected abstract Optional<Boolean> startupAuditEnabled();

    void checkAuditLoggingDisabled(String name) throws IOException {
        try (var mockLog = MockLog.capture(LoggingAuditTrail.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(name, LoggingAuditTrail.class.getName(), Level.INFO, "*access_granted*")
            );
            authenticate();
            mockLog.assertAllExpectationsMatched();
        }
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
}
