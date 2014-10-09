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
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.ParseException;
import java.util.*;

import static org.elasticsearch.license.core.ESLicenses.FeatureType;
import static org.elasticsearch.license.core.LicenseUtils.readLicensesFromString;
import static org.junit.Assert.*;

public class LicenseVerificationTests extends AbstractLicensingTestBase {

    private static ESLicenseManager esLicenseManager;

    private final static ESLicenses EMPTY_LICENSES = LicenseBuilders.licensesBuilder().build();

    @BeforeClass
    public static void setupManager() {
        esLicenseManager = ESLicenseManager.createLocalBasedInstance(LicenseBuilders.licensesBuilder().build(), pubKeyPath);
    }

    @After
    public void clearManager() {
        esLicenseManager.clearAndAddLicenses(EMPTY_LICENSES);
    }


    @Test
    public void testGeneratedLicenses() throws Exception {
        Date issueDate = new Date();
        String issueDateStr = DateUtils.dateStringFromLongDate(issueDate.getTime());
        String expiryDateStr = DateUtils.dateStringFromLongDate(DateUtils.longExpiryDateFromDate(issueDate.getTime() + 24 * 60 * 60l));
        Map<FeatureType, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 2, issueDateStr, expiryDateStr);
        map.put(FeatureType.SHIELD, featureAttributes);

        ESLicenses esLicensesOutput = readLicensesFromString(generateSignedLicenses(map));

        esLicenseManager.clearAndAddLicenses(esLicensesOutput);

        esLicenseManager.verifyLicenses();

        verifyLicenseManager(esLicenseManager, map);

    }

    @Test
    public void testMultipleFeatureLicenses() throws Exception {
        Date issueDate = new Date();
        String issueDateStr = DateUtils.dateStringFromLongDate(issueDate.getTime());
        String expiryDateStr = DateUtils.dateStringFromLongDate(DateUtils.longExpiryDateFromDate(issueDate.getTime() + 24 * 60 * 60 * 1000l));

        Map<FeatureType, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes shildFeatureAttributes =
                new TestUtils.FeatureAttributes("shield", "trial", "none", "foo bar Inc.", "elasticsearch", 2, issueDateStr, expiryDateStr);
        TestUtils.FeatureAttributes marvelFeatureAttributes =
                new TestUtils.FeatureAttributes("marvel", "subscription", "silver", "foo1 bar Inc.", "elasticsearc3h", 10, issueDateStr, expiryDateStr);
        map.put(FeatureType.SHIELD, shildFeatureAttributes);
        map.put(FeatureType.MARVEL, marvelFeatureAttributes);

        ESLicenses esLicensesOutput = readLicensesFromString(generateSignedLicenses(map));

        esLicenseManager.clearAndAddLicenses(esLicensesOutput);

        //printLicense(esLicenseManager.getEffectiveLicenses());

        esLicenseManager.verifyLicenses();

        verifyLicenseManager(esLicenseManager, map);

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

        Map<FeatureType, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes shildFeatureAttributes =
                new TestUtils.FeatureAttributes("shield", "trial", "none", "foo bar Inc.", "elasticsearch", 2, issueDateStr, expiryDateStr);
        TestUtils.FeatureAttributes marvelFeatureAttributes =
                new TestUtils.FeatureAttributes("marvel", "internal", "silver", "foo1 bar Inc.", "elasticsearc3h", 10, issueDateStr, expiredExpiryDateStr);
        map.put(FeatureType.SHIELD, shildFeatureAttributes);
        map.put(FeatureType.MARVEL, marvelFeatureAttributes);

        ESLicenses esLicensesOutput = readLicensesFromString(generateSignedLicenses(map));

        esLicenseManager.clearAndAddLicenses(esLicensesOutput);

        // All validation for shield license should be normal as expected
        verifyLicenseManager(esLicenseManager, Collections.singletonMap(FeatureType.SHIELD, shildFeatureAttributes));

        assertFalse("license for marvel should not be valid due to expired expiry date", esLicenseManager.hasLicenseForFeature(FeatureType.MARVEL));
    }

    @Test
    public void testLicenseTampering() throws Exception {

        Date issueDate = new Date();
        String issueDateStr = DateUtils.dateStringFromLongDate(issueDate.getTime());
        String expiryDateStr = DateUtils.dateStringFromLongDate(DateUtils.longExpiryDateFromDate(issueDate.getTime() + 24 * 60 * 60l));
        Map<FeatureType, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 2, issueDateStr, expiryDateStr);
        map.put(FeatureType.SHIELD, featureAttributes);

        ESLicenses esLicensesOutput = readLicensesFromString(generateSignedLicenses(map));

        ESLicenses.ESLicense esLicense = esLicensesOutput.get(FeatureType.SHIELD);

        long originalExpiryDate = esLicense.expiryDate();
        final ESLicenses.ESLicense tamperedLicense = LicenseBuilders.licenseBuilder(true)
                .fromLicense(esLicense)
                .expiryDate(esLicense.expiryDate() + 10 * 24 * 60 * 60 * 1000l)
                .feature(FeatureType.SHIELD)
                .issuer("elasticsqearch")
                .build();

        ESLicenses tamperedLicenses = LicenseBuilders.licensesBuilder().license(tamperedLicense).build();

        try {
            esLicenseManager.clearAndAddLicenses(tamperedLicenses);
            assertTrue("License manager should always report the original (signed) expiry date of: " + originalExpiryDate + " but got: " + esLicenseManager.getExpiryDateForLicense(FeatureType.SHIELD), esLicenseManager.getExpiryDateForLicense(FeatureType.SHIELD) == originalExpiryDate);
            esLicenseManager.verifyLicenses();
            fail();
        } catch (InvalidLicenseException e) {
            assertTrue("Exception should contain 'Invalid License' but got: " + e.getMessage(), e.getMessage().contains("Invalid License"));
        }
    }

    public static void verifyLicenseManager(ESLicenseManager esLicenseManager, Map<ESLicenses.FeatureType, TestUtils.FeatureAttributes> featureAttributeMap) throws ParseException {

        for (Map.Entry<FeatureType, TestUtils.FeatureAttributes> entry : featureAttributeMap.entrySet()) {
            TestUtils.FeatureAttributes featureAttributes = entry.getValue();
            FeatureType featureType = entry.getKey();
            assertTrue("License should have issuedTo of " + featureAttributes.issuedTo, esLicenseManager.getIssuedToForLicense(featureType).equals(featureAttributes.issuedTo));
            assertTrue("License should have issuer of " + featureAttributes.issuer, esLicenseManager.getIssuerForLicense(featureType).equals(featureAttributes.issuer));
            assertTrue("License should have issue date of " + DateUtils.longFromDateString(featureAttributes.issueDate), esLicenseManager.getIssueDateForLicense(featureType) == DateUtils.longFromDateString(featureAttributes.issueDate));
            assertTrue("License should have expiry date of " + DateUtils.longExpiryDateFromString(featureAttributes.expiryDate) + " got: " + esLicenseManager.getExpiryDateForLicense(featureType), esLicenseManager.getExpiryDateForLicense(featureType) == DateUtils.longExpiryDateFromString(featureAttributes.expiryDate));
            assertTrue("License should have type of " + featureAttributes.type + " got: " + esLicenseManager.getTypeForLicense(featureType).string(), esLicenseManager.getTypeForLicense(featureType) == ESLicenses.Type.fromString(featureAttributes.type));
            assertTrue("License should have subscription type of " + featureAttributes.subscriptionType, esLicenseManager.getSubscriptionTypeForLicense(featureType) == ESLicenses.SubscriptionType.fromString(featureAttributes.subscriptionType));


            assertTrue("License should be valid for " + featureType.string(), esLicenseManager.hasLicenseForFeature(featureType));
            assertTrue("License should be valid for maxNodes = " + (featureAttributes.maxNodes - 1), esLicenseManager.hasLicenseForNodes(featureType, featureAttributes.maxNodes - 1));
            assertTrue("License should be valid for maxNodes = " + (featureAttributes.maxNodes), esLicenseManager.hasLicenseForNodes(featureType, featureAttributes.maxNodes));
            assertFalse("License should not be valid for maxNodes = " + (featureAttributes.maxNodes + 1), esLicenseManager.hasLicenseForNodes(featureType, featureAttributes.maxNodes + 1));
        }
    }
}
