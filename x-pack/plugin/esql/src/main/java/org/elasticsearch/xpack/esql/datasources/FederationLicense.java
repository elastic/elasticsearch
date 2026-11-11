/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.session.EsqlLicenseChecker;

import java.util.function.Supplier;

/**
 * Wraps the plugin's overridable license supplier for data federation operations. Registered as a
 * component by {@code EsqlPlugin} so that federation transport actions can inject it and call
 * {@link #check()} or {@link #isAllowed()} rather than directly using
 * {@code XPackPlugin.getSharedLicenseState()}, which is not overridable in integration tests.
 */
public class FederationLicense {

    private final Supplier<XPackLicenseState> licenseStateSupplier;

    public FederationLicense(Supplier<XPackLicenseState> licenseStateSupplier) {
        this.licenseStateSupplier = licenseStateSupplier;
    }

    /** Returns the current license state. */
    public XPackLicenseState get() {
        return licenseStateSupplier.get();
    }

    /** Returns {@code true} if the current license permits data federation. */
    public boolean isAllowed() {
        return EsqlLicenseChecker.isFederationAllowed(licenseStateSupplier.get());
    }

    /**
     * Returns {@code true} if the current license permits data federation, without recording feature usage.
     * Use before the resolver confirms a query actually targets a dataset, to avoid spurious telemetry.
     */
    public boolean isAllowedWithoutTracking() {
        return EsqlLicenseChecker.isFederationAllowedWithoutTracking(licenseStateSupplier.get());
    }

    /** Throws if the current license does not permit data federation. */
    public void check() {
        EsqlLicenseChecker.checkFederation(licenseStateSupplier.get());
    }
}
