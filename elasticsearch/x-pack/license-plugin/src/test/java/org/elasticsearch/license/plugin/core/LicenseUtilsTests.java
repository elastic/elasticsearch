/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class LicenseUtilsTests extends ESTestCase {

    public void testNewExpirationException() {
        for (String feature : Arrays.asList("feature", randomAsciiOfLength(5), null, "")) {
            ElasticsearchSecurityException exception = LicenseUtils.newComplianceException(feature);
            assertNotNull(exception);
            assertThat(exception.getHeaderKeys(), contains(LicenseUtils.EXPIRED_FEATURE_HEADER));
            assertThat(exception.getHeader(LicenseUtils.EXPIRED_FEATURE_HEADER), hasSize(1));
            assertThat(exception.getHeader(LicenseUtils.EXPIRED_FEATURE_HEADER).iterator().next(), equalTo(feature));
        }
    }

    public void testIsLicenseExpiredException() {
        ElasticsearchSecurityException exception = LicenseUtils.newComplianceException("feature");
        assertTrue(LicenseUtils.isLicenseExpiredException(exception));

        exception = new ElasticsearchSecurityException("msg");
        assertFalse(LicenseUtils.isLicenseExpiredException(exception));
    }
}
