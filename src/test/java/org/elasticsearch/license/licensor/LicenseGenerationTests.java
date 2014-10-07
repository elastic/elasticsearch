/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor;

import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseUtils;
import org.elasticsearch.license.licensor.tools.KeyPairGeneratorTool;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.license.core.ESLicenses.FeatureType;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LicenseGenerationTests {

    private static String pubKeyPath = null;
    private static String priKeyPath = null;

    @BeforeClass
    public static void setup() throws IOException {

        // Generate temp KeyPair spec
        File privateKeyFile = File.createTempFile("privateKey", ".key");
        File publicKeyFile = File.createTempFile("publicKey", ".key");
        LicenseGenerationTests.pubKeyPath = publicKeyFile.getAbsolutePath();
        LicenseGenerationTests.priKeyPath = privateKeyFile.getAbsolutePath();
        assert privateKeyFile.delete();
        assert publicKeyFile.delete();

        // Generate keyPair
        String[] args = new String[4];
        args[0] = "--publicKeyPath";
        args[1] = LicenseGenerationTests.pubKeyPath;
        args[2] = "--privateKeyPath";
        args[3] = LicenseGenerationTests.priKeyPath;
        KeyPairGeneratorTool.main(args);
    }

    @Test
    public void testSimpleLicenseGeneration() throws ParseException, IOException {
        Map<FeatureType, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 2, "2014-12-13", "2015-12-13");
        map.put(FeatureType.SHIELD, featureAttributes);

        String licenseString = TestUtils.generateESLicenses(map);

        String[] args = new String[6];
        args[0] = "--license";
        args[1] = licenseString;
        args[2] = "--publicKeyPath";
        args[3] = pubKeyPath;
        args[4] = "--privateKeyPath";
        args[5] = priKeyPath;

        String licenseOutput = TestUtils.runLicenseGenerationTool(args);

        ESLicenses esLicensesOutput = LicenseUtils.readLicensesFromString(licenseOutput);

        TestUtils.verifyESLicenses(esLicensesOutput, map);
    }

    @Test
    public void testMultipleFeatureTypes() throws ParseException, IOException {

        Map<FeatureType, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes shildFeatureAttributes =
                new TestUtils.FeatureAttributes("shield", "trial", "none", "foo bar Inc.", "elasticsearch", 2, "2014-12-13", "2015-12-13");
        TestUtils.FeatureAttributes marvelFeatureAttributes =
                new TestUtils.FeatureAttributes("marvel", "subscription", "silver", "foo1 bar Inc.", "elasticsearc3h", 10, "2014-01-13", "2014-12-13");
        map.put(FeatureType.SHIELD, shildFeatureAttributes);
        map.put(FeatureType.MARVEL, marvelFeatureAttributes);

        String licenseString = TestUtils.generateESLicenses(map);
        String[] args = new String[6];
        args[0] = "--license";
        args[1] = licenseString;
        args[2] = "--publicKeyPath";
        args[3] = pubKeyPath;
        args[4] = "--privateKeyPath";
        args[5] = priKeyPath;

        String licenseOutput = TestUtils.runLicenseGenerationTool(args);

        ESLicenses esLicensesOutput = LicenseUtils.readLicensesFromString(licenseOutput);

        TestUtils.verifyESLicenses(esLicensesOutput, map);
    }

    @Test
    public void testMissingCLTArgs() throws ParseException, IOException {

        Map<FeatureType, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shiedgdsld", "internal", "none", "foo bar Inc.", "elasticsearch", 23, "2014-12-13", "2015-12-13");
        map.put(FeatureType.SHIELD, featureAttributes);

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
    public void testInvalidFeatureType() throws ParseException, IOException {

        Map<FeatureType, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shiedgdsld", "internal", "none", "foo bar Inc.", "elasticsearch", 23, "2014-12-13", "2015-12-13");
        map.put(FeatureType.SHIELD, featureAttributes);

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
            assertTrue("Exception should indicate invalid FeatureType, got: " + e.getMessage(), e.getMessage().contains("Invalid FeatureType"));
        }
    }

    @Test
    public void testInvalidSubscriptionType() throws ParseException, IOException {
        Map<FeatureType, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shield", "trial", "nodavne", "foo bar Inc.", "elasticsearch", 25, "2014-12-13", "2015-12-13");
        map.put(FeatureType.SHIELD, featureAttributes);

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

        Map<FeatureType, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shield", "inininternal", "gold", "foo bar Inc.", "elasticsearch", 12, "2014-12-13", "2015-12-13");
        map.put(FeatureType.SHIELD, featureAttributes);

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
