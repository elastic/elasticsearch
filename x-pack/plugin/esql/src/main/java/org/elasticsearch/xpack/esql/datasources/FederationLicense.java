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
 * Holds the license state supplier for data federation operations. Registered as a component by
 * {@code EsqlPlugin} so that federation transport actions can inject it and call
 * {@link #check()} rather than directly using {@code XPackPlugin.getSharedLicenseState()}, which
 * is not overridable in integration tests.
 */
public class FederationLicense {

    private final Supplier<XPackLicenseState> licenseStateSupplier;

    public FederationLicense(Supplier<XPackLicenseState> licenseStateSupplier) {
        this.licenseStateSupplier = licenseStateSupplier;
    }

    /** Throws if the current license does not permit data federation. */
    public void check() {
        EsqlLicenseChecker.checkFederation(licenseStateSupplier.get());
    }
}
