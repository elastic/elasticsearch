/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.support;

import org.elasticsearch.license.XPackLicenseState;

import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.SecurityField.FIELD_LEVEL_SECURITY_FEATURE;

/**
 * Allows tracking of Document and Field Level Security feature usage.
 */
public class DlsFlsFeatureUsageTracker {

    private final XPackLicenseState licenseState;

    public DlsFlsFeatureUsageTracker(XPackLicenseState licenseState) {
        this.licenseState = licenseState;
    }

    public void trackDocumentLevelSecurityUsage() {
        DOCUMENT_LEVEL_SECURITY_FEATURE.check(licenseState);
    }

    public void trackFieldLevelSecurityUsage() {
        FIELD_LEVEL_SECURITY_FEATURE.check(licenseState);
    }

}
