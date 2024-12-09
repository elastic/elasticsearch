/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;

public class EsqlLicenseChecker {

    public static final LicensedFeature.Momentary CCS_FEATURE = LicensedFeature.momentary(
        null,
        "esql-ccs",
        License.OperationMode.ENTERPRISE
    );

    /**
     * Only call this method once you know the user is doing a cross-cluster query, as it will update
     * the license_usage timestamp for the esql-ccs feature if the license is Enterprise (or Trial).
     * @param licenseState
     * @return true if the user has a license that allows ESQL CCS.
     */
    public static boolean isCcsAllowed(XPackLicenseState licenseState) {
        if (licenseState == null) {
            return false;
        }
        return CCS_FEATURE.check(licenseState);
    }

    public static ElasticsearchStatusException invalidLicenseForCcsException(XPackLicenseState licenseState) {
        String message = "A valid Enterprise license is required to run ES|QL cross-cluster searches. License found: ";
        if (licenseState == null) {
            message += "none";
        } else {
            message += licenseState.statusDescription();
        }
        return new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST);
    }
}
