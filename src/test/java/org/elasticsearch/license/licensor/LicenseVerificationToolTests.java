/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseUtils;
import org.elasticsearch.license.licensor.tools.KeyPairGeneratorTool;
import org.elasticsearch.license.licensor.tools.LicenseVerificationTool;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LicenseVerificationToolTests {

    private static String pubKeyPath = null;
    private static String priKeyPath = null;
    private static String keyPass = null;

    @BeforeClass
    public static void setup() throws IOException {

        // Generate temp KeyPair spec
        File privateKeyFile = File.createTempFile("privateKey", ".key");
        File publicKeyFile = File.createTempFile("publicKey", ".key");
        LicenseVerificationToolTests.pubKeyPath = publicKeyFile.getAbsolutePath();
        LicenseVerificationToolTests.priKeyPath = privateKeyFile.getAbsolutePath();
        assert privateKeyFile.delete();
        assert publicKeyFile.delete();
        LicenseVerificationToolTests.keyPass = "password";

        // Generate keyPair
        String[] args = new String[6];
        args[0] = "--publicKeyPath";
        args[1] = LicenseVerificationToolTests.pubKeyPath;
        args[2] = "--privateKeyPath";
        args[3] = LicenseVerificationToolTests.priKeyPath;
        args[4] = "--keyPass";
        args[5] = LicenseVerificationToolTests.keyPass;
        KeyPairGeneratorTool.main(args);
    }

    @Test
    public void testEffectiveLicenseGeneration() throws Exception {
        Map<ESLicenses.FeatureType, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureWithLongerExpiryDate =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 10, "2014-12-13", "2015-12-13");
        map.put(ESLicenses.FeatureType.SHIELD, featureWithLongerExpiryDate);

        String signedLicense = runLicenseGenerationTool(TestUtils.generateESLicenses(map));
        String firstLicenseFile = getAsFilePath(signedLicense);

        TestUtils.FeatureAttributes featureWithShorterExpiryDate =
                new TestUtils.FeatureAttributes("shield", "trial", "none", "foo bar Inc.", "elasticsearch", 2, "2014-12-13", "2015-01-13");
        map.put(ESLicenses.FeatureType.SHIELD, featureWithShorterExpiryDate);

        signedLicense = runLicenseGenerationTool(TestUtils.generateESLicenses(map));
        String secondLicenseFile = getAsFilePath(signedLicense);

        String[] args = new String[6];
        args[0] = "--licensesFiles";
        args[1] = firstLicenseFile + ":" + secondLicenseFile;
        args[2] = "--publicKeyPath";
        args[3] = pubKeyPath;
        args[4] = "--keyPass";
        args[5] = keyPass;

        String effectiveLicenseStr = runLicenseVerificationTool(args);
        ESLicenses effectiveLicense = LicenseUtils.readLicensesFromString(effectiveLicenseStr);

        map.put(ESLicenses.FeatureType.SHIELD, featureWithLongerExpiryDate);

        // verify that the effective license strips out license for the same feature with earlier expiry dates
        TestUtils.verifyESLicenses(effectiveLicense, map);
    }

    @Test
    public void testEffectiveLicenseForMultiFeatures() throws Exception {
        Map<ESLicenses.FeatureType, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes shieldFeatureWithLongerExpiryDate =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 10, "2014-12-13", "2015-12-13");
        map.put(ESLicenses.FeatureType.SHIELD, shieldFeatureWithLongerExpiryDate);

        String signedLicense = runLicenseGenerationTool(TestUtils.generateESLicenses(map));
        String firstLicenseFile = getAsFilePath(signedLicense);

        TestUtils.FeatureAttributes marvelFeatureWithShorterExpiryDate =
                new TestUtils.FeatureAttributes("marvel", "trial", "none", "foo bar Inc.", "elasticsearch", 2, "2014-12-13", "2015-01-13");
        map.put(ESLicenses.FeatureType.MARVEL, marvelFeatureWithShorterExpiryDate);

        signedLicense = runLicenseGenerationTool(TestUtils.generateESLicenses(map));
        String secondLicenseFile = getAsFilePath(signedLicense);

        String[] args = new String[6];
        args[0] = "--licensesFiles";
        args[1] = firstLicenseFile + ":" + secondLicenseFile;
        args[2] = "--publicKeyPath";
        args[3] = pubKeyPath;
        args[4] = "--keyPass";
        args[5] = keyPass;

        String effectiveLicenseStr = runLicenseVerificationTool(args);
        ESLicenses effectiveLicense = LicenseUtils.readLicensesFromString(effectiveLicenseStr);

        // verify that the effective license contains both feature licenses
        TestUtils.verifyESLicenses(effectiveLicense, map);
    }

    @Test
    public void testEffectiveLicenseForMultiFeatures2() throws Exception {
        Map<ESLicenses.FeatureType, TestUtils.FeatureAttributes> map = new HashMap<>();

        TestUtils.FeatureAttributes shieldFeatureWithLongerExpiryDate =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 10, "2014-12-13", "2015-12-13");
        TestUtils.FeatureAttributes marvelFeatureWithShorterExpiryDate =
                new TestUtils.FeatureAttributes("marvel", "trial", "none", "foo bar Inc.", "elasticsearch", 2, "2014-12-13", "2015-01-13");

        map.put(ESLicenses.FeatureType.SHIELD, shieldFeatureWithLongerExpiryDate);
        map.put(ESLicenses.FeatureType.MARVEL, marvelFeatureWithShorterExpiryDate);

        String signedLicense = runLicenseGenerationTool(TestUtils.generateESLicenses(map));
        String firstLicenseFile = getAsFilePath(signedLicense);

        TestUtils.FeatureAttributes shieldFeatureWithShorterExpiryDate =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 10, "2014-12-13", "2015-11-13");
        TestUtils.FeatureAttributes marvelFeatureWithLongerExpiryDate =
                new TestUtils.FeatureAttributes("marvel", "trial", "none", "foo bar Inc.", "elasticsearch", 2, "2014-12-13", "2015-11-13");

        map.put(ESLicenses.FeatureType.SHIELD, shieldFeatureWithShorterExpiryDate);
        map.put(ESLicenses.FeatureType.MARVEL, marvelFeatureWithLongerExpiryDate);

        signedLicense = runLicenseGenerationTool(TestUtils.generateESLicenses(map));
        String secondLicenseFile = getAsFilePath(signedLicense);

        String[] args = new String[6];
        args[0] = "--licensesFiles";
        args[1] = firstLicenseFile + ":" + secondLicenseFile;
        args[2] = "--publicKeyPath";
        args[3] = pubKeyPath;
        args[4] = "--keyPass";
        args[5] = keyPass;

        String effectiveLicenseStr = runLicenseVerificationTool(args);
        ESLicenses effectiveLicense = LicenseUtils.readLicensesFromString(effectiveLicenseStr);

        map.put(ESLicenses.FeatureType.SHIELD, shieldFeatureWithLongerExpiryDate);
        map.put(ESLicenses.FeatureType.MARVEL, marvelFeatureWithLongerExpiryDate);

        // verify that the generated effective license is generated from choosing individual licences from multiple files
        TestUtils.verifyESLicenses(effectiveLicense, map);
    }

    public static String runLicenseVerificationTool(String[] args) throws IOException {
        File temp = File.createTempFile("temp", ".out");
        temp.deleteOnExit();
        try (FileOutputStream outputStream = new FileOutputStream(temp)) {
            LicenseVerificationTool.run(args, outputStream);
        }
        return FileUtils.readFileToString(temp);
    }

    public static String runLicenseGenerationTool(String licenseInput) throws IOException {
        String args[] = new String[8];

        args[0] = "--license";
        args[1] = licenseInput;
        args[2] = "--publicKeyPath";
        args[3] = pubKeyPath;
        args[4] = "--privateKeyPath";
        args[5] = priKeyPath;
        args[6] = "--keyPass";
        args[7] = keyPass;

        return TestUtils.runLicenseGenerationTool(args);
    }

    private static String getAsFilePath(String content) throws IOException {
        File temp = File.createTempFile("license", ".out");
        temp.deleteOnExit();
        FileUtils.write(temp, content);
        String tempFilePath = temp.getAbsolutePath();
        while (tempFilePath.contains(":")) {
            assert temp.delete();
            tempFilePath = getAsFilePath(content);
        }
        return tempFilePath;
    }

}
