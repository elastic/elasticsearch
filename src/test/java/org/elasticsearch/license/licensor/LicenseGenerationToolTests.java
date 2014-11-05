/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.license.AbstractLicensingTestBase;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.licensor.tools.LicenseGeneratorTool;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiOfLength;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class LicenseGenerationToolTests extends AbstractLicensingTestBase {

    @Test
    public void testSimple() throws Exception {
        LicenseSpec inputLicenseSpec = generateRandomLicenseSpec();
        String[] args = new String[6];
        args[0] = "--license";
        args[1] = generateESLicenseSpecString(Arrays.asList(inputLicenseSpec));
        args[2] = "--publicKeyPath";
        args[3] = pubKeyPath;
        args[4] = "--privateKeyPath";
        args[5] = priKeyPath;

        String licenseOutput = runLicenseGenerationTool(args);
        List<ESLicense> outputLicenses = ESLicenses.fromSource(licenseOutput);
        assertThat(outputLicenses.size(), equalTo(1));
        assertThat(outputLicenses.get(0).signature(), notNullValue());

        Set<ESLicense> expectedLicenses = generateSignedLicenses(Arrays.asList(inputLicenseSpec));
        ESLicense expectedLicense = ESLicense.builder()
                .fromLicenseSpec(expectedLicenses.iterator().next(), outputLicenses.get(0).signature())
                .build();

        TestUtils.isSame(expectedLicense, outputLicenses.get(0));
    }

    @Test
    public void testWithLicenseFile() throws Exception {
        LicenseSpec inputLicenseSpec = generateRandomLicenseSpec();

        Path tempFilePath = Files.createTempFile("license_spec", "json");
        File tempFile = tempFilePath.toFile();
        FileUtils.write(tempFile, generateESLicenseSpecString(Arrays.asList(inputLicenseSpec)));
        tempFile.deleteOnExit();

        String[] args = new String[6];
        args[0] = "--licenseFile";
        args[1] = tempFile.getAbsolutePath();
        args[2] = "--publicKeyPath";
        args[3] = pubKeyPath;
        args[4] = "--privateKeyPath";
        args[5] = priKeyPath;

        String licenseOutput = runLicenseGenerationTool(args);
        List<ESLicense> outputLicenses = ESLicenses.fromSource(licenseOutput);
        assertThat(outputLicenses.size(), equalTo(1));
        assertThat(outputLicenses.get(0).signature(), notNullValue());

        Set<ESLicense> expectedLicenses = generateSignedLicenses(Arrays.asList(inputLicenseSpec));
        ESLicense expectedLicense = ESLicense.builder()
                .fromLicenseSpec(expectedLicenses.iterator().next(), outputLicenses.get(0).signature())
                .build();

        TestUtils.isSame(expectedLicense, outputLicenses.get(0));
    }

    @Test
    public void testBadKeyPath() throws Exception {
        boolean pubKey = randomBoolean();

        String[] args = new String[6];
        args[0] = "--license";
        args[1] = generateESLicenseSpecString(Arrays.asList(generateRandomLicenseSpec()));
        args[2] = "--publicKeyPath";
        args[3] = (pubKey) ? pubKeyPath + randomAsciiOfLength(3) : pubKeyPath;
        args[4] = "--privateKeyPath";
        args[5] = (!pubKey) ? priKeyPath + randomAsciiOfLength(3) : priKeyPath;

        try {
            runLicenseGenerationTool(args);
            fail("Should not accept non-existent key paths");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("does not exist"));
        }

    }

    @Test
    public void testMissingCLTArgs() throws Exception {
        String[] args = new String[6];
        args[0] = "--linse";
        args[1] = generateESLicenseSpecString(Arrays.asList(generateRandomLicenseSpec()));
        args[2] = "--publicKeyPath";
        args[3] = pubKeyPath;
        args[4] = "--privateKeyPath";
        args[5] = priKeyPath;

        try {
            runLicenseGenerationTool(args);
            fail("should not accept arguments without --license");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("'--license'"));
        }
    }

    private static String runLicenseGenerationTool(String[] args) throws Exception {
        File temp = File.createTempFile("temp", ".out");
        temp.deleteOnExit();
        try (FileOutputStream outputStream = new FileOutputStream(temp)) {
            LicenseGeneratorTool.run(args, outputStream);
        }
        return FileUtils.readFileToString(temp);
    }
}
