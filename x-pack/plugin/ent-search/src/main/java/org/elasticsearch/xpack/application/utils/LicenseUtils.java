/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.utils;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.XPackField;

public final class LicenseUtils {
    public enum Product {
        SEARCH_APPLICATION("search application"),
        BEHAVIORAL_ANALYTICS("behavioral analytics"),
        QUERY_RULES("query rules");

        private final String name;

        Product(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static final LicensedFeature.Momentary LICENSED_ENT_SEARCH_FEATURE = LicensedFeature.momentary(
        null,
        XPackField.ENTERPRISE_SEARCH,
        License.OperationMode.PLATINUM
    );

    public static boolean supportedLicense(XPackLicenseState licenseState) {
        return LICENSED_ENT_SEARCH_FEATURE.check(licenseState);
    }

    public static ElasticsearchSecurityException newComplianceException(XPackLicenseState licenseState, Product product) {
        String licenseStatus = licenseState.statusDescription();

        ElasticsearchSecurityException e = new ElasticsearchSecurityException(
            "Current license is non-compliant for "
                + product.getName()
                + ". Current license is {}. "
                + "This feature requires an active trial, platinum or enterprise license.",
            RestStatus.FORBIDDEN,
            licenseStatus
        );
        return e;
    }

}
