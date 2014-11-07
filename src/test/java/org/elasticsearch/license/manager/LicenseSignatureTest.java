/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.manager;

import org.elasticsearch.license.AbstractLicensingTestBase;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.core.ESLicense;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class LicenseSignatureTest extends AbstractLicensingTestBase {

    private static ESLicenseManager esLicenseManager;

    @BeforeClass
    public static void setupManager() {
        esLicenseManager = new ESLicenseManager();
    }

    @Test
    public void testLicenseGeneration() throws Exception {
        int n = randomIntBetween(5, 15);
        List<LicenseSpec> licenseSpecs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            licenseSpecs.add(generateRandomLicenseSpec());
        }

        Set<ESLicense> generatedLicenses = generateSignedLicenses(licenseSpecs);
        assertThat(generatedLicenses.size(), equalTo(n));

        Set<String> signatures = new HashSet<>();
        for (ESLicense license : generatedLicenses) {
            signatures.add(license.signature());
        }
        Set<ESLicense> licenseFromSignatures = esLicenseManager.fromSignatures(signatures);

        TestUtils.isSame(generatedLicenses, licenseFromSignatures);
    }
}
