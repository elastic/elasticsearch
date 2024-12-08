/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license.licensor.tools;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.licensor.TestUtils;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class LicenseVerificationToolTests extends CommandTestCase {
    protected Path pubKeyPath = null;
    protected Path priKeyPath = null;

    @Before
    public void setup() throws Exception {
        logger.error("project.basedir [{}]", System.getProperty("project.basedir"));
        pubKeyPath = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        priKeyPath = getDataPath(TestUtils.PRIVATE_KEY_RESOURCE);
    }

    @Override
    protected Command newCommand() {
        return new LicenseVerificationTool();
    }

    public void testMissingKeyPath() throws Exception {
        Path pub = createTempDir().resolve("pub");
        UserException e = expectThrows(UserException.class, () -> execute("--publicKeyPath", pub.toString()));
        assertTrue(e.getMessage(), e.getMessage().contains("pub does not exist"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
    }

    public void testMissingLicenseSpec() throws Exception {
        UserException e = expectThrows(UserException.class, () -> { execute("--publicKeyPath", pubKeyPath.toString()); });
        assertTrue(e.getMessage(), e.getMessage().contains("Must specify either --license or --licenseFile"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
    }

    public void testBrokenLicense() throws Exception {
        final TimeValue oneHour = TimeValue.timeValueHours(1);
        License signedLicense = TestUtils.generateSignedLicense(oneHour, pubKeyPath, priKeyPath);
        License tamperedLicense = License.builder()
            .fromLicenseSpec(signedLicense, signedLicense.signature())
            .expiryDate(signedLicense.expiryDate() + randomIntBetween(1, 1000))
            .build();
        UserException e = expectThrows(
            UserException.class,
            () -> execute("--publicKeyPath", pubKeyPath.toString(), "--license", TestUtils.dumpLicense(tamperedLicense))
        );
        assertEquals("Invalid License!", e.getMessage());
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
    }

    public void testLicenseSpecString() throws Exception {
        final TimeValue oneHour = TimeValue.timeValueHours(1);
        License signedLicense = TestUtils.generateSignedLicense(oneHour, pubKeyPath, priKeyPath);
        String output = execute("--publicKeyPath", pubKeyPath.toString(), "--license", TestUtils.dumpLicense(signedLicense));
        assertFalse(output, output.isEmpty());
    }

    public void testLicenseSpecFile() throws Exception {
        final TimeValue oneHour = TimeValue.timeValueHours(1);
        License signedLicense = TestUtils.generateSignedLicense(oneHour, pubKeyPath, priKeyPath);
        Path licenseSpecFile = createTempFile();
        Files.write(licenseSpecFile, TestUtils.dumpLicense(signedLicense).getBytes(StandardCharsets.UTF_8));
        String output = execute("--publicKeyPath", pubKeyPath.toString(), "--licenseFile", licenseSpecFile.toString());
        assertFalse(output, output.isEmpty());
    }

}
