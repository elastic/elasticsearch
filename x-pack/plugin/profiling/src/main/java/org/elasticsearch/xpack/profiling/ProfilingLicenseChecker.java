/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;

import java.util.function.Supplier;

public final class ProfilingLicenseChecker {
    private static final LicensedFeature.Momentary UNIVERSAL_PROFILING_FEATURE = LicensedFeature.momentary(
        null,
        "universal_profiling",
        License.OperationMode.ENTERPRISE
    );

    private final Supplier<XPackLicenseState> licenseStateResolver;

    public ProfilingLicenseChecker(Supplier<XPackLicenseState> licenseStateResolver) {
        this.licenseStateResolver = licenseStateResolver;
    }

    public boolean isSupportedLicense() {
        return UNIVERSAL_PROFILING_FEATURE.checkWithoutTracking(licenseStateResolver.get());
    }

    public void requireSupportedLicense() {
        if (UNIVERSAL_PROFILING_FEATURE.check(licenseStateResolver.get()) == false) {
            throw LicenseUtils.newComplianceException(UNIVERSAL_PROFILING_FEATURE.getName());
        }
    }
}
