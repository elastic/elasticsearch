/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.test.ESTestCase;

import java.time.Clock;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.license.LicenseSettings.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

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

    public void testVersionsUpToDate() {
        assertThat(LicenseUtils.getMaxCompatibleLicenseVersion(), equalTo(License.VERSION_CURRENT));
    }

    public void testGetXPackLicenseStatus() {
        Clock clock = Clock.systemUTC();
        License.LicenseType type = randomFrom(License.LicenseType.values());
        License.OperationMode mode = License.OperationMode.resolve(type);
        long issueDate = clock.millis() - TimeUnit.DAYS.toMillis(100);
        long notExpired = clock.millis() + TimeUnit.DAYS.toMillis(10);
        long isExpired = clock.millis() - TimeUnit.DAYS.toMillis(10);
        XPackLicenseStatus status;
        // not expired
        status = LicenseUtils.getXPackLicenseStatus(getLicense(type, issueDate, notExpired), clock);
        assertThat(status.mode(), is(mode));
        assertTrue(status.active());
        assertNull(status.expiryWarning());
        // not expired - special case
        if (License.LicenseType.BASIC.equals(type)) {
            status = LicenseUtils.getXPackLicenseStatus(getLicense(type, issueDate, BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS), clock);
            assertThat(status.mode(), is(mode));
            assertTrue(status.active());
            assertNull(status.expiryWarning());
        }
        // expired
        status = LicenseUtils.getXPackLicenseStatus(getLicense(type, issueDate, isExpired), clock);
        assertThat(status.mode(), is(mode));
        assertFalse(status.active());
        assertThat(status.expiryWarning(), containsStringIgnoringCase("license expired"));
        // deleted
        status = LicenseUtils.getXPackLicenseStatus(LicensesMetadata.LICENSE_TOMBSTONE, clock);
        assertFalse(status.active());
        assertThat(status.expiryWarning(), containsStringIgnoringCase("license expired"));
    }

    private License getLicense(License.LicenseType type, long issueDate, long expiryDate) {
        License.Builder builder = License.builder()
            .uid(UUIDs.randomBase64UUID(random()))
            .type(type)
            .issueDate(issueDate)
            .expiryDate(expiryDate)
            .issuedTo("me")
            .issuer("you");
        if (type.equals(License.LicenseType.ENTERPRISE)) {
            builder.maxResourceUnits(1);
        } else {
            builder.maxNodes(1);
        }
        return builder.build();
    }
}
