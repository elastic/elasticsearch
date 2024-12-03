/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;

// MP TODO: should this be limited to EsqlCcsLicenseChecker?
public class EsqlLicenseChecker {

    public static final LicensedFeature.Momentary ESQL_FEATURE = LicensedFeature.momentary(null, "ES|QL", License.OperationMode.ENTERPRISE);

    public static boolean isCcsAllowed(XPackLicenseState licenseState) {
        if (licenseState == null) {
            return false;
        }
        return ESQL_FEATURE.checkWithoutTracking(licenseState);
    }

}
