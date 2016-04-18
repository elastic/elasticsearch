/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestStatus;

public class LicenseUtils {

    public final static String EXPIRED_FEATURE_HEADER = "es.license.expired.feature";

    /**
     * Exception to be thrown when a feature action requires a valid license, but license
     * has expired
     *
     * <code>feature</code> accessible through {@link #EXPIRED_FEATURE_HEADER} in the
     * exception's rest header
     */
    public static ElasticsearchSecurityException newComplianceException(String feature) {
        ElasticsearchSecurityException e = new ElasticsearchSecurityException("current license is non-compliant for [{}]",
                RestStatus.UNAUTHORIZED, feature);
        e.addHeader(EXPIRED_FEATURE_HEADER, feature);
        return e;
    }

    /**
     * Checks if a given {@link ElasticsearchSecurityException} refers to a feature that
     * requires a valid license, but the license has expired.
     */
    public static boolean isLicenseExpiredException(ElasticsearchSecurityException exception) {
        return (exception != null) && (exception.getHeader(EXPIRED_FEATURE_HEADER) != null);
    }
}
