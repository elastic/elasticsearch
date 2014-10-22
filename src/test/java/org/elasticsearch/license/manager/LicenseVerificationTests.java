/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.manager;

import net.nicholaswilliams.java.licensing.exception.InvalidLicenseException;
import org.elasticsearch.license.AbstractLicensingTestBase;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.core.DateUtils;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.licensor.tools.FileBasedESLicenseProvider;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.ParseException;
import java.util.*;

import static org.junit.Assert.*;

public class LicenseVerificationTests extends AbstractLicensingTestBase {

    private static ESLicenseManager esLicenseManager;

    private static FileBasedESLicenseProvider esLicenseProvider;

    private final static Set<ESLicense> EMPTY_LICENSES = new HashSet<>();

    @BeforeClass
    public static void setupManager() {
        esLicenseProvider = new FileBasedESLicenseProvider(EMPTY_LICENSES);
        esLicenseManager = new ESLicenseManager();

    }

    @After
    public void clearManager() {
        esLicenseProvider.setLicenses(EMPTY_LICENSES);
    }


    @Test
    public void testGeneratedLicenses() throws Exception {
        Date issueDate = new Date();
        String issueDateStr = DateUtils.dateStringFromLongDate(issueDate.getTime());
        String expiryDateStr = DateUtils.dateStringFromLongDate(DateUtils.longExpiryDateFromDate(issueDate.getTime() + 24 * 60 * 60l));
        Map<String, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 2, issueDateStr, expiryDateStr);
        map.put(TestUtils.SHIELD, featureAttributes);

        Set<ESLicense> esLicensesOutput = ESLicenses.fromSource(generateSignedLicenses(map));

        esLicenseProvider.setLicenses(esLicensesOutput);

        esLicenseManager.verifyLicenses(esLicenseProvider.getEffectiveLicenses());

        verifyLicenseManager(esLicenseManager, esLicenseProvider, map);

    }

    @Test
    public void testMultipleFeatureLicenses() throws Exception {
        Date issueDate = new Date();
        String issueDateStr = DateUtils.dateStringFromLongDate(issueDate.getTime());
        String expiryDateStr = DateUtils.dateStringFromLongDate(DateUtils.longExpiryDateFromDate(issueDate.getTime() + 24 * 60 * 60 * 1000l));

        Map<String, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes shildFeatureAttributes =
                new TestUtils.FeatureAttributes("shield", "trial", "none", "foo bar Inc.", "elasticsearch", 2, issueDateStr, expiryDateStr);
        TestUtils.FeatureAttributes marvelFeatureAttributes =
                new TestUtils.FeatureAttributes("marvel", "subscription", "silver", "foo1 bar Inc.", "elasticsearc3h", 10, issueDateStr, expiryDateStr);
        map.put(TestUtils.SHIELD, shildFeatureAttributes);
        map.put(TestUtils.MARVEL, marvelFeatureAttributes);

        Set<ESLicense> esLicensesOutput = ESLicenses.fromSource(generateSignedLicenses(map));

        esLicenseProvider.setLicenses(esLicensesOutput);

        esLicenseManager.verifyLicenses(esLicenseProvider.getEffectiveLicenses());

        verifyLicenseManager(esLicenseManager, esLicenseProvider, map);

    }

    private static Date getDateBeforeDays(Date originalDate, int days) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setTimeZone(DateUtils.TIME_ZONE);
        calendar.setTimeInMillis(originalDate.getTime());

        int originalDays = calendar.get(Calendar.DAY_OF_YEAR);
        calendar.set(Calendar.DAY_OF_YEAR, originalDays - days);

        return calendar.getTime();
    }

    private static Date getDateAfterDays(Date originalDate, int days) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setTimeZone(DateUtils.TIME_ZONE);
        calendar.setTimeInMillis(originalDate.getTime());

        calendar.add(Calendar.DAY_OF_YEAR, days);

        return calendar.getTime();
    }


    @Test
    public void testLicenseExpiry() throws Exception {

        Date issueDate = getDateBeforeDays(new Date(), 60);
        Date expiryDate = getDateAfterDays(new Date(), 30);
        Date expiredExpiryDate = getDateBeforeDays(new Date(), 10);
        String issueDateStr = DateUtils.dateStringFromLongDate(issueDate.getTime());
        String expiryDateStr = DateUtils.dateStringFromLongDate(DateUtils.longExpiryDateFromDate(expiryDate.getTime()));

        final long longExpiryDateFromDate = DateUtils.longExpiryDateFromDate(expiredExpiryDate.getTime());
        assert longExpiryDateFromDate < System.currentTimeMillis();
        String expiredExpiryDateStr = DateUtils.dateStringFromLongDate(longExpiryDateFromDate);

        Map<String, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes shildFeatureAttributes =
                new TestUtils.FeatureAttributes("shield", "trial", "none", "foo bar Inc.", "elasticsearch", 2, issueDateStr, expiryDateStr);
        TestUtils.FeatureAttributes marvelFeatureAttributes =
                new TestUtils.FeatureAttributes("marvel", "internal", "silver", "foo1 bar Inc.", "elasticsearc3h", 10, issueDateStr, expiredExpiryDateStr);
        map.put(TestUtils.SHIELD, shildFeatureAttributes);
        map.put(TestUtils.MARVEL, marvelFeatureAttributes);

        Set<ESLicense> esLicensesOutput = ESLicenses.fromSource(generateSignedLicenses(map));

        esLicenseProvider.setLicenses(esLicensesOutput);

        // All validation for shield license should be normal as expected

        verifyLicenseManager(esLicenseManager, esLicenseProvider, Collections.singletonMap(TestUtils.SHIELD, shildFeatureAttributes));

        assertFalse("license for marvel should not be valid due to expired expiry date", esLicenseManager.hasLicenseForFeature(TestUtils.MARVEL, esLicenseProvider.getEffectiveLicenses()));
    }

    @Test
    public void testLicenseTampering() throws Exception {

        Date issueDate = new Date();
        String issueDateStr = DateUtils.dateStringFromLongDate(issueDate.getTime());
        String expiryDateStr = DateUtils.dateStringFromLongDate(DateUtils.longExpiryDateFromDate(issueDate.getTime() + 24 * 60 * 60l));
        Map<String, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 2, issueDateStr, expiryDateStr);
        map.put(TestUtils.SHIELD, featureAttributes);

        Set<ESLicense> esLicensesOutput = ESLicenses.fromSource(generateSignedLicenses(map));

        ESLicense esLicense = Utils.reduceAndMap(esLicensesOutput).get(TestUtils.SHIELD);

        final ESLicense tamperedLicense = ESLicense.builder()
                .fromLicense(esLicense)
                .expiryDate(esLicense.expiryDate() + 10 * 24 * 60 * 60 * 1000l)
                .feature(TestUtils.SHIELD)
                .issuer("elasticsqearch")
                .build();

        try {
            esLicenseProvider.setLicenses(Collections.singleton(tamperedLicense));
            esLicenseManager.verifyLicenses(esLicenseProvider.getEffectiveLicenses());
            fail();
        } catch (InvalidLicenseException e) {
            assertTrue("Exception should contain 'Invalid License' but got: " + e.getMessage(), e.getMessage().contains("Invalid License"));
        }
    }

    public static void verifyLicenseManager(ESLicenseManager esLicenseManager, ESLicenseProvider licenseProvider, Map<String, TestUtils.FeatureAttributes> featureAttributeMap) throws ParseException {

        for (Map.Entry<String, TestUtils.FeatureAttributes> entry : featureAttributeMap.entrySet()) {
            TestUtils.FeatureAttributes featureAttributes = entry.getValue();
            String featureType = entry.getKey();
            ESLicense license = licenseProvider.getESLicense(featureType);
            assertTrue("License should have issuedTo of " + featureAttributes.issuedTo, license.issuedTo().equals(featureAttributes.issuedTo));
            assertTrue("License should have issuer of " + featureAttributes.issuer, license.issuer().equals(featureAttributes.issuer));
            assertTrue("License should have issue date of " + DateUtils.longFromDateString(featureAttributes.issueDate), license.issueDate() == DateUtils.longFromDateString(featureAttributes.issueDate));
            assertTrue("License should have expiry date of " + DateUtils.longExpiryDateFromString(featureAttributes.expiryDate) + " got: " + license.expiryDate(), license.expiryDate() == DateUtils.longExpiryDateFromString(featureAttributes.expiryDate));
            assertTrue("License should have type of " + featureAttributes.type + " got: " + license.type().string(), license.type() == ESLicense.Type.fromString(featureAttributes.type));
            assertTrue("License should have subscription type of " + featureAttributes.subscriptionType, license.subscriptionType() == ESLicense.SubscriptionType.fromString(featureAttributes.subscriptionType));


            assertTrue("License should be valid for " + featureType, esLicenseManager.hasLicenseForFeature(featureType, licenseProvider.getEffectiveLicenses()));
            assertTrue("License should be valid for maxNodes = " + (featureAttributes.maxNodes), license.maxNodes() == featureAttributes.maxNodes);
        }
    }
}
