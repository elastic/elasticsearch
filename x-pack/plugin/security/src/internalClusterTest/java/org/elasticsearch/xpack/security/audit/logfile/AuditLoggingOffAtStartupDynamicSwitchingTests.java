/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import java.io.IOException;
import java.util.Optional;

public class AuditLoggingOffAtStartupDynamicSwitchingTests extends AbstractAuditLoggingDynamicSwitchingTestCase {

    @Override
    protected Optional<Boolean> startupAuditEnabled() {
        return Optional.of(false);
    }

    public void testFlippingAuditLogFalseToTrueToFalse() throws IOException {
        checkAuditLoggingDisabled("no audit event when disabled");
        setAuditLogsSetting(true);
        checkAuditLoggingEnabled("access_granted event when enabled");
        setAuditLogsSetting(false);
        checkAuditLoggingDisabled("no audit event when disabled (again)");
    }
}
