/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.utils;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.XPackField;

import java.util.Locale;

public final class LicenseUtils {
    public enum Product {
        SEARCH_APPLICATION("search application", License.OperationMode.PLATINUM),
        BEHAVIORAL_ANALYTICS("behavioral analytics", License.OperationMode.PLATINUM),
        QUERY_RULES("query rules", License.OperationMode.ENTERPRISE),;

        private final String name;
        private final License.OperationMode requiredLicense;

        Product(String name, License.OperationMode requiredLicense) {
            this.name = name;
            this.requiredLicense = requiredLicense;
        }

        public String getName() {
            return name;
        }

        public LicensedFeature.Momentary getLicensedFeature() {
            return switch (requiredLicense) {
                case PLATINUM -> PLATINUM_LICENSED_FEATURE;
                case ENTERPRISE -> ENTERPRISE_LICENSED_FEATURE;
                default -> throw new IllegalStateException("Unknown license operation mode: " + requiredLicense);
            };
        }
    }

    public static final LicensedFeature.Momentary PLATINUM_LICENSED_FEATURE = LicensedFeature.momentary(
        null,
        XPackField.ENTERPRISE_SEARCH,
        License.OperationMode.PLATINUM
    );

    public static final LicensedFeature.Momentary ENTERPRISE_LICENSED_FEATURE = LicensedFeature.momentary(
        null,
        XPackField.ENTERPRISE_SEARCH,
        License.OperationMode.ENTERPRISE
    );

    public static boolean supportedLicense(Product product, XPackLicenseState licenseState) {
        return product.getLicensedFeature().check(licenseState);
    }

    public static ElasticsearchSecurityException newComplianceException(XPackLicenseState licenseState, Product product) {
        String licenseStatus = licenseState.statusDescription();
        String requiredLicenseStatus = product.requiredLicense.toString().toLowerCase(Locale.ROOT);

        ElasticsearchSecurityException e = new ElasticsearchSecurityException(
            "Current license is non-compliant for "
                + product.getName()
                + ". Current license is {}. "
                + "This feature requires an active trial, {}, or higher license.",
            RestStatus.FORBIDDEN,
            licenseStatus,
            requiredLicenseStatus
        );
        return e;
    }

}
