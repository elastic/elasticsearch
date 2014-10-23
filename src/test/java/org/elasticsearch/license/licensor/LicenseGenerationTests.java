/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor;

import org.elasticsearch.license.AbstractLicensingTestBase;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ESLicenses;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LicenseGenerationTests extends AbstractLicensingTestBase {

    @Test
    public void testSimpleLicenseGeneration() throws ParseException, IOException {
        Map<String, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 2, "2014-12-13", "2015-12-13");
        map.put(TestUtils.SHIELD, featureAttributes);

        String licenseOutput = generateSignedLicenses(map);

        Set<ESLicense> esLicensesOutput = new HashSet<>(ESLicenses.fromSource(licenseOutput));

        TestUtils.verifyESLicenses(esLicensesOutput, map);
    }

    @Test
    public void testMultipleStrings() throws ParseException, IOException {

        Map<String, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes shildFeatureAttributes =
                new TestUtils.FeatureAttributes("shield", "trial", "none", "foo bar Inc.", "elasticsearch", 2, "2014-12-13", "2015-12-13");
        TestUtils.FeatureAttributes marvelFeatureAttributes =
                new TestUtils.FeatureAttributes("marvel", "subscription", "silver", "foo1 bar Inc.", "elasticsearc3h", 10, "2014-01-13", "2014-12-13");
        map.put(TestUtils.SHIELD, shildFeatureAttributes);
        map.put(TestUtils.MARVEL, marvelFeatureAttributes);

        String licenseOutput = generateSignedLicenses(map);

        Set<ESLicense> esLicensesOutput = new HashSet<>(ESLicenses.fromSource(licenseOutput));

        TestUtils.verifyESLicenses(esLicensesOutput, map);
    }

    @Test
    public void testMissingCLTArgs() throws ParseException, IOException {

        Map<String, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shiedgdsld", "internal", "none", "foo bar Inc.", "elasticsearch", 23, "2014-12-13", "2015-12-13");
        map.put(TestUtils.SHIELD, featureAttributes);

        String licenseString = TestUtils.generateESLicenses(map);

        String[] args = new String[6];
        args[0] = "--linse";
        args[1] = licenseString;
        args[2] = "--publicKeyPath";
        args[3] = pubKeyPath;
        args[4] = "--privateKeyPath";
        args[5] = priKeyPath;

        try {
            String licenseOutput = TestUtils.runLicenseGenerationTool(args);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue("Exception should indicate mandatory param --license, got: " + e.getMessage(), e.getMessage().contains("license"));
        }
    }

    @Test
    public void testInvalidSubscriptionType() throws ParseException, IOException {
        Map<String, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shield", "trial", "nodavne", "foo bar Inc.", "elasticsearch", 25, "2014-12-13", "2015-12-13");
        map.put(TestUtils.SHIELD, featureAttributes);

        String licenseString = TestUtils.generateESLicenses(map);

        String[] args = new String[6];
        args[0] = "--license";
        args[1] = licenseString;
        args[2] = "--publicKeyPath";
        args[3] = pubKeyPath;
        args[4] = "--privateKeyPath";
        args[5] = priKeyPath;

        try {
            String licenseOutput = TestUtils.runLicenseGenerationTool(args);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue("Exception should indicate invalid SubscriptionType, got: " + e.getMessage(), e.getMessage().contains("Invalid SubscriptionType"));
        }
    }

    @Test
    public void testInvalidType() throws ParseException, IOException {

        Map<String, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shield", "inininternal", "gold", "foo bar Inc.", "elasticsearch", 12, "2014-12-13", "2015-12-13");
        map.put(TestUtils.SHIELD, featureAttributes);

        String licenseString = TestUtils.generateESLicenses(map);

        String[] args = new String[6];
        args[0] = "--license";
        args[1] = licenseString;
        args[2] = "--publicKeyPath";
        args[3] = pubKeyPath;
        args[4] = "--privateKeyPath";
        args[5] = priKeyPath;

        try {
            String licenseOutput = TestUtils.runLicenseGenerationTool(args);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue("Exception should indicate invalid Type, got: " + e.getMessage(), e.getMessage().contains("Invalid Type"));
        }
    }

}
