/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.licensor.tools.KeyPairGeneratorTool;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class AbstractLicensingTestBase {

    protected static String pubKeyPath = null;
    protected static String priKeyPath = null;

    @BeforeClass
    public static void setup() throws IOException {

        // Generate temp KeyPair spec
        File privateKeyFile = File.createTempFile("privateKey", ".key");
        File publicKeyFile = File.createTempFile("publicKey", ".key");
        AbstractLicensingTestBase.pubKeyPath = publicKeyFile.getAbsolutePath();
        AbstractLicensingTestBase.priKeyPath = privateKeyFile.getAbsolutePath();
        assert privateKeyFile.delete();
        assert publicKeyFile.delete();

        // Generate keyPair
        String[] args = new String[4];
        args[0] = "--publicKeyPath";
        args[1] = AbstractLicensingTestBase.pubKeyPath;
        args[2] = "--privateKeyPath";
        args[3] = AbstractLicensingTestBase.priKeyPath;
        KeyPairGeneratorTool.main(args);
    }

    public String generateSignedLicenses(Map<ESLicenses.FeatureType, TestUtils.FeatureAttributes> map) throws IOException {
        String licenseString = TestUtils.generateESLicenses(map);
        return TestUtils.runLicenseGenerationTool(licenseString, pubKeyPath, priKeyPath);
    }
}
