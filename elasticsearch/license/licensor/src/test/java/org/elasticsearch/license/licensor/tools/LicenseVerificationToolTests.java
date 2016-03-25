/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.licensor.TestUtils;
import org.junit.Before;

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
        UserError e = expectThrows(UserError.class, () -> {
            execute("--publicKeyPath", pub.toString());
        });
        assertTrue(e.getMessage(), e.getMessage().contains("pub does not exist"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
    }

    public void testMissingLicenseSpec() throws Exception {
        UserError e = expectThrows(UserError.class, () -> {
            execute("--publicKeyPath", pubKeyPath.toString());
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Must specify either --license or --licenseFile"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
    }

    public void testBrokenLicense() throws Exception {
        License signedLicense = TestUtils.generateSignedLicense(TimeValue.timeValueHours(1), pubKeyPath, priKeyPath);
        License tamperedLicense = License.builder()
            .fromLicenseSpec(signedLicense, signedLicense.signature())
            .expiryDate(signedLicense.expiryDate() + randomIntBetween(1, 1000)).build();
        UserError e = expectThrows(UserError.class, () -> {
            execute("--publicKeyPath", pubKeyPath.toString(),
                    "--license", TestUtils.dumpLicense(tamperedLicense));
        });
        assertEquals("Invalid License!", e.getMessage());
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
    }

    public void testLicenseSpecString() throws Exception {
        License signedLicense = TestUtils.generateSignedLicense(TimeValue.timeValueHours(1), pubKeyPath, priKeyPath);
        String output = execute("--publicKeyPath", pubKeyPath.toString(),
                                "--license", TestUtils.dumpLicense(signedLicense));
        assertFalse(output, output.isEmpty());
    }

    public void testLicenseSpecFile() throws Exception {
        License signedLicense = TestUtils.generateSignedLicense(TimeValue.timeValueHours(1), pubKeyPath, priKeyPath);
        Path licenseSpecFile = createTempFile();
        Files.write(licenseSpecFile, TestUtils.dumpLicense(signedLicense).getBytes(StandardCharsets.UTF_8));
        String output = execute("--publicKeyPath", pubKeyPath.toString(),
                                "--licenseFile", licenseSpecFile.toString());
        assertFalse(output, output.isEmpty());
    }
}
