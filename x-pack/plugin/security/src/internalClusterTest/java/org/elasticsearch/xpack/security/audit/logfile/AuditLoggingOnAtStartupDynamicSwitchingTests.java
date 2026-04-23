/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.XPackSettings;

import java.io.IOException;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class AuditLoggingOnAtStartupDynamicSwitchingTests extends AuditLoggingOffAtStartupDynamicSwitchingTests {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.AUDIT_ENABLED.getKey(), true)
            .build();
    }

    public void testFlippingAuditLogFalseToTrueToFalse() throws IOException {
        checkAuditLoggingEnabled("access_granted event when enabled");
        setAuditLogsSetting(false);
        checkAuditLoggingDisabled("no audit event when disabled");
        setAuditLogsSetting(true);
        checkAuditLoggingEnabled("access_granted event when enabled (again)");
    }

}
