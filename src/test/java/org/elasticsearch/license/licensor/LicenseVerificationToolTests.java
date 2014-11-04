/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.AbstractLicensingTestBase;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.licensor.tools.LicenseVerificationTool;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomRealisticUnicodeOfCodepointLengthBetween;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class LicenseVerificationToolTests extends AbstractLicensingTestBase {

    @Test
    public void testMissingCLTArgs() throws Exception {
        ESLicense singedLicense = generateSignedLicense(randomRealisticUnicodeOfCodepointLengthBetween(5, 15),
                TimeValue.timeValueHours(1));

        String[] args = new String[2];
        args[0] = "--licenssFiles";
        args[1] = dumpLicense(singedLicense);

        try {
            runLicenseVerificationTool(args);
            fail("mandatory param '--licensesFiles' should throw an exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("--licensesFiles"));
        }
    }

    @Test
    public void testSimple() throws Exception {
        ESLicense singedLicense = generateSignedLicense(randomRealisticUnicodeOfCodepointLengthBetween(5, 15),
                TimeValue.timeValueHours(1));

        String[] args = new String[2];
        args[0] = "--licensesFiles";
        args[1] = dumpLicense(singedLicense);

        String licenseOutput = runLicenseVerificationTool(args);
        List<ESLicense> licensesOutput = ESLicenses.fromSource(licenseOutput);

        assertThat(licensesOutput.size(), equalTo(1));

        ESLicense expectedLicense = ESLicense.builder()
                .fromLicenseSpec(singedLicense, licensesOutput.get(0).signature())
                .build();

        TestUtils.isSame(expectedLicense, licensesOutput.get(0));
    }

    @Test
    public void testWithLicenseFiles() throws Exception {
        int n = randomIntBetween(3, 10);
        Set<ESLicense> signedLicenses = new HashSet<>();
        for (int i = 0; i < n; i++) {
            signedLicenses.add(generateSignedLicense(randomRealisticUnicodeOfCodepointLengthBetween(5, 15),
                    TimeValue.timeValueHours(1)));
        }

        StringBuilder licenseFilePathString = new StringBuilder();
        ESLicense[] esLicenses = signedLicenses.toArray(new ESLicense[n]);
        for (int i = 0; i < n; i++) {
            licenseFilePathString.append(dumpLicense(esLicenses[i]));
            if (i != esLicenses.length - 1) {
                licenseFilePathString.append(":");
            }
        }

        String[] args = new String[2];
        args[0] = "--licensesFiles";
        args[1] = licenseFilePathString.toString();

        String licenseOutput = runLicenseVerificationTool(args);
        List<ESLicense> output = ESLicenses.fromSource(licenseOutput);

        assertThat(output.size(), equalTo(n));

        Set<ESLicense> licensesOutput = new HashSet<>();
        Map<String, ESLicense> expectedLicenses = ESLicenses.reduceAndMap(signedLicenses);
        for (ESLicense license : output) {
            licensesOutput.add(
                    ESLicense.builder()
                    .fromLicenseSpec(license, expectedLicenses.get(license.feature()).signature())
                    .build()
            );
        }

        TestUtils.isSame(signedLicenses, licensesOutput);

    }

    private String dumpLicense(ESLicense license) throws Exception {
        Path tempFilePath = Files.createTempFile("license_spec", "json");
        File tempFile = tempFilePath.toFile();
        tempFile.deleteOnExit();
        try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, outputStream);
            ESLicenses.toXContent(Collections.singletonList(license), builder, ToXContent.EMPTY_PARAMS);
            builder.flush();
        }
        return tempFile.getAbsolutePath();
    }


    private static String runLicenseVerificationTool(String[] args) throws IOException {
        File temp = File.createTempFile("temp", ".out");
        temp.deleteOnExit();
        try (FileOutputStream outputStream = new FileOutputStream(temp)) {
            LicenseVerificationTool.run(args, outputStream);
        }
        return FileUtils.readFileToString(temp);
    }
}
