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

    private static final LicensedFeature.Momentary CCS_FEATURE = LicensedFeature.momentary(
        null,
        "esql-ccs",
        License.OperationMode.ENTERPRISE
    );

    private static final LicensedFeature.Momentary QUERY_APPROXIMATION_FEATURE = LicensedFeature.momentary(
        null,
        "esql-approximation",
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

    /**
     * @param licenseState existing license state. Need to extract info on the current installed license.
     * @return ElasticsearchStatusException with an error message informing the caller what license is needed
     * to run ES|QL cross-cluster searches and what license (if any) was found.
     */
    public static ElasticsearchStatusException invalidLicenseForCcsException(XPackLicenseState licenseState) {
        return getException("A valid Enterprise license is required to run ES|QL cross-cluster searches.", licenseState);
    }

    /**
     * @param licenseState existing license state. Need to extract info on the current installed license.
     * @throws ElasticsearchStatusException if query approximation is not supported.
     */
    public static void checkQueryApproximation(XPackLicenseState licenseState) throws ElasticsearchStatusException {
        if (licenseState == null || QUERY_APPROXIMATION_FEATURE.check(licenseState) == false) {
            throw getException("A valid Enterprise license is required to use ES|QL query approximation.", licenseState);
        }
    }

    private static ElasticsearchStatusException getException(String message, XPackLicenseState licenseState) {
        return new ElasticsearchStatusException(
            message + " License found: " + (licenseState == null ? "none" : licenseState.statusDescription()),
            RestStatus.BAD_REQUEST
        );
    }
}
