/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.licensor.TestUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

public class LicenseGenerationToolTests extends ESTestCase {
    protected Path pubKeyPath = null;
    protected Path priKeyPath = null;

    @Before
    public void setup() throws Exception {
        pubKeyPath = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        priKeyPath = getDataPath(TestUtils.PRIVATE_KEY_RESOURCE);
    }

    public void testMissingKeyPaths() throws Exception {
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Path pub = createTempDir().resolve("pub");
        Path pri = createTempDir().resolve("pri");
        UserError e = expectThrows(UserError.class, () -> {
            licenseGeneratorTool.execute(Terminal.DEFAULT, pub, pri, null, null);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("pri does not exist"));
        assertEquals(ExitCodes.USAGE, e.exitCode);

        Files.createFile(pri);
        e = expectThrows(UserError.class, () -> {
            licenseGeneratorTool.execute(Terminal.DEFAULT, pub, pri, null, null);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("pub does not exist"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
    }

    public void testMissingLicenseSpec() throws Exception {
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        UserError e = expectThrows(UserError.class, () -> {
            licenseGeneratorTool.execute(Terminal.DEFAULT, pubKeyPath, priKeyPath, null, null);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Must specify either --license or --licenseFile"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
    }

    public void testLicenseSpecString() throws Exception {
        TestUtils.LicenseSpec inputLicenseSpec = TestUtils.generateRandomLicenseSpec(License.VERSION_CURRENT);
        String licenseSpecString = TestUtils.generateLicenseSpecString(inputLicenseSpec);
        String output = runTool(licenseSpecString, null);
        License outputLicense = License.fromSource(output.getBytes(StandardCharsets.UTF_8));
        TestUtils.assertLicenseSpec(inputLicenseSpec, outputLicense);
    }

    public void testLicenseSpecFile() throws Exception {
        TestUtils.LicenseSpec inputLicenseSpec = TestUtils.generateRandomLicenseSpec(License.VERSION_CURRENT);
        String licenseSpecString = TestUtils.generateLicenseSpecString(inputLicenseSpec);
        Path licenseSpecFile = createTempFile();
        Files.write(licenseSpecFile, licenseSpecString.getBytes(StandardCharsets.UTF_8));
        String output = runTool(null, licenseSpecFile);
        License outputLicense = License.fromSource(output.getBytes(StandardCharsets.UTF_8));
        TestUtils.assertLicenseSpec(inputLicenseSpec, outputLicense);
    }

    private String runTool(String licenseSpecString, Path licenseSpecPath) throws Exception {
        CliToolTestCase.CaptureOutputTerminal outputTerminal = new CliToolTestCase.CaptureOutputTerminal();
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        licenseGeneratorTool.execute(outputTerminal, pubKeyPath, priKeyPath, licenseSpecString, licenseSpecPath);
        assertEquals(1, outputTerminal.getTerminalOutput().size());
        return outputTerminal.getTerminalOutput().get(0);
    }

}
