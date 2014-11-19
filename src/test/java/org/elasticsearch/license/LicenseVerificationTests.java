/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.LicenseVerifier;
import org.elasticsearch.license.plugin.core.LicensesMetaData;
import org.junit.Test;

import java.util.*;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class LicenseVerificationTests extends AbstractLicensingTestBase {

    @Test
    public void testGeneratedLicenses() throws Exception {
        License shieldLicense = TestUtils.generateSignedLicense("shield", TimeValue.timeValueHours(2 * 24));
        assertThat(LicenseVerifier.verifyLicense(shieldLicense), equalTo(true));
    }

    @Test
    public void testMultipleFeatureLicenses() throws Exception {
        License shieldLicense = TestUtils.generateSignedLicense("shield", TimeValue.timeValueHours(2 * 24));
        License marvelLicense = TestUtils.generateSignedLicense("marvel", TimeValue.timeValueHours(2 * 24));

        assertThat(LicenseVerifier.verifyLicenses(Arrays.asList(shieldLicense, marvelLicense)), equalTo(true));
    }

    @Test
    public void testLicenseTampering() throws Exception {
        License license = TestUtils.generateSignedLicense("shield", TimeValue.timeValueHours(2));

        final License tamperedLicense = License.builder()
                .fromLicenseSpec(license, license.signature())
                .expiryDate(license.expiryDate() + 10 * 24 * 60 * 60 * 1000l)
                .validate()
                .build();

        assertThat(LicenseVerifier.verifyLicense(tamperedLicense), equalTo(false));
    }

    @Test
    public void testRandomLicenseVerification() throws Exception {
        int n = randomIntBetween(5, 15);
        List<LicenseSpec> licenseSpecs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            licenseSpecs.add(generateRandomLicenseSpec());
        }

        Set<License> generatedLicenses = generateSignedLicenses(licenseSpecs);
        assertThat(generatedLicenses.size(), equalTo(n));

        for (License generatedLicense: generatedLicenses) {
            assertThat(LicenseVerifier.verifyLicense(generatedLicense), equalTo(true));
        }
    }
}
