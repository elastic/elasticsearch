/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.crossclusteraccess;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.security.Security;

public class CrossClusterAccessLicenseChecker {

    private final XPackLicenseState licenseState;

    public CrossClusterAccessLicenseChecker(XPackLicenseState licenseState) {
        this.licenseState = licenseState;
    }

    public void checkLicense() throws ElasticsearchSecurityException {
        if (false == Security.CROSS_CLUSTER_ACCESS_FEATURE.check(licenseState)) {
            throw LicenseUtils.newComplianceException(Security.CROSS_CLUSTER_ACCESS_FEATURE.getName());
        }
    }
}
