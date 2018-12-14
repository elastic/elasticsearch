/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.EnumSet;

/**
 * A bootstrap check which enforces the licensing of FIPS
 */
final class FIPS140LicenseBootstrapCheck implements BootstrapCheck {

    static final EnumSet<License.OperationMode> ALLOWED_LICENSE_OPERATION_MODES =
        EnumSet.of(License.OperationMode.PLATINUM, License.OperationMode.TRIAL);

    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        if (XPackSettings.FIPS_MODE_ENABLED.get(context.settings())) {
            License license = LicenseService.getLicense(context.metaData());
            if (license != null && ALLOWED_LICENSE_OPERATION_MODES.contains(license.operationMode()) == false) {
                return BootstrapCheckResult.failure("FIPS mode is only allowed with a Platinum or Trial license");
            }
        }
        return BootstrapCheckResult.success();
    }
}
