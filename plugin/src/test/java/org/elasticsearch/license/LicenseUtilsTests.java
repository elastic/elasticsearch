/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class LicenseUtilsTests extends ESTestCase {

    public void testNewExpirationException() {
        for (String feature : Arrays.asList("feature", randomAlphaOfLength(5), null, "")) {
            ElasticsearchSecurityException exception = LicenseUtils.newComplianceException(feature);
            assertNotNull(exception);
            assertThat(exception.getMetadataKeys(), contains(LicenseUtils.EXPIRED_FEATURE_METADATA));
            assertThat(exception.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA), hasSize(1));
            assertThat(exception.getMetadata(LicenseUtils.EXPIRED_FEATURE_METADATA).iterator().next(), equalTo(feature));
        }
    }

    public void testIsLicenseExpiredException() {
        ElasticsearchSecurityException exception = LicenseUtils.newComplianceException("feature");
        assertTrue(LicenseUtils.isLicenseExpiredException(exception));

        exception = new ElasticsearchSecurityException("msg");
        assertFalse(LicenseUtils.isLicenseExpiredException(exception));
    }
}
