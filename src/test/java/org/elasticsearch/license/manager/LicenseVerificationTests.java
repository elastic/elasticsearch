/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.manager;

import net.nicholaswilliams.java.licensing.exception.InvalidLicenseException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.AbstractLicensingTestBase;
import org.elasticsearch.license.core.ESLicense;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class LicenseVerificationTests extends AbstractLicensingTestBase {

    private static ESLicenseManager esLicenseManager;

    @BeforeClass
    public static void setupManager() {
        esLicenseManager = new ESLicenseManager();
    }

    @Test
    public void testGeneratedLicenses() throws Exception {
        ESLicense shieldLicense = generateSignedLicense("shield", TimeValue.timeValueHours(2 * 24));
        Map<String, ESLicense> shieldLicenseMap = new HashMap<>();
        shieldLicenseMap.put("shield", shieldLicense);
        esLicenseManager.verifyLicenses(shieldLicenseMap);
    }

    @Test
    public void testMultipleFeatureLicenses() throws Exception {
        ESLicense shieldLicense = generateSignedLicense("shield", TimeValue.timeValueHours(2 * 24));
        ESLicense marvelLicense = generateSignedLicense("marvel", TimeValue.timeValueHours(2 * 24));
        Map<String, ESLicense> licenseMap = new HashMap<>();
        licenseMap.put("shield", shieldLicense);
        licenseMap.put("marvel", marvelLicense);

        esLicenseManager.verifyLicenses(licenseMap);
    }

    @Test
    public void testLicenseExpiry() throws Exception {
        long now = System.currentTimeMillis();
        long marvelIssueDate = dateMath("now-10d/d", now);

        ESLicense shieldLicense = generateSignedLicense("shield", TimeValue.timeValueHours(2 * 24));
        ESLicense marvelLicense = generateSignedLicense("marvel", marvelIssueDate, TimeValue.timeValueHours(2 * 24));
        Map<String, ESLicense> licenseMap = new HashMap<>();
        licenseMap.put("shield", shieldLicense);
        licenseMap.put("marvel", marvelLicense);

        try {
            esLicenseManager.verifyLicenses(licenseMap);
            fail("verifyLicenses should throw InvalidLicenseException [expired license]");
        } catch (InvalidLicenseException e) {
            assertThat(e.getMessage(), containsString("Expired License"));
        }

        licenseMap.clear();
        licenseMap.put("shield", shieldLicense);
        esLicenseManager.verifyLicenses(licenseMap);
    }

    @Test
    public void testLicenseTampering() throws Exception {
        ESLicense esLicense = generateSignedLicense("shield", TimeValue.timeValueHours(2));

        final ESLicense tamperedLicense = ESLicense.builder()
                .fromLicenseSpec(esLicense, esLicense.signature())
                .expiryDate(esLicense.expiryDate() + 10 * 24 * 60 * 60 * 1000l)
                .verify()
                .build();

        Map<String, ESLicense> licenseMap = new HashMap<>();
        licenseMap.put("shield", tamperedLicense);

        try {
            esLicenseManager.verifyLicenses(licenseMap);
            fail("Tampered license should throw exception");
        } catch (InvalidLicenseException e) {
            assertThat(e.getMessage(), containsString("Invalid License"));
        }
    }
}
