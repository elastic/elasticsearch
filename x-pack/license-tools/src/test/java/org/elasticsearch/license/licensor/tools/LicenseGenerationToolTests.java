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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.license.License;
import org.elasticsearch.license.licensor.TestUtils;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class LicenseGenerationToolTests extends CommandTestCase {

    protected Path pubKeyPath = null;
    protected Path priKeyPath = null;

    @Before
    public void setup() throws Exception {
        pubKeyPath = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        priKeyPath = getDataPath(TestUtils.PRIVATE_KEY_RESOURCE);
    }

    @Override
    protected Command newCommand() {
        return new LicenseGeneratorTool();
    }

    public void testMissingKeyPaths() throws Exception {
        Path pub = createTempDir().resolve("pub");
        Path pri = createTempDir().resolve("pri");
        UserException e = expectThrows(
            UserException.class,
            () -> execute("--publicKeyPath", pub.toString(), "--privateKeyPath", pri.toString())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("pri does not exist"));
        assertEquals(ExitCodes.USAGE, e.exitCode);

        Files.createFile(pri);
        e = expectThrows(UserException.class, () -> execute("--publicKeyPath", pub.toString(), "--privateKeyPath", pri.toString()));
        assertTrue(e.getMessage(), e.getMessage().contains("pub does not exist"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
    }

    public void testMissingLicenseSpec() throws Exception {
        UserException e = expectThrows(
            UserException.class,
            () -> execute("--publicKeyPath", pubKeyPath.toString(), "--privateKeyPath", priKeyPath.toString())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("Must specify either --license or --licenseFile"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
    }

    public void testLicenseSpecString() throws Exception {
        TestUtils.LicenseSpec inputLicenseSpec = TestUtils.generateRandomLicenseSpec(License.VERSION_CURRENT);
        String licenseSpecString = TestUtils.generateLicenseSpecString(inputLicenseSpec);
        String output = execute(
            "--publicKeyPath",
            pubKeyPath.toString(),
            "--privateKeyPath",
            priKeyPath.toString(),
            "--license",
            licenseSpecString
        );
        final BytesArray bytes = new BytesArray(output.getBytes(StandardCharsets.UTF_8));
        License outputLicense = License.fromSource(bytes, XContentType.JSON);
        TestUtils.assertLicenseSpec(inputLicenseSpec, outputLicense);
    }

    public void testLicenseSpecFile() throws Exception {
        TestUtils.LicenseSpec inputLicenseSpec = TestUtils.generateRandomLicenseSpec(License.VERSION_CURRENT);
        String licenseSpecString = TestUtils.generateLicenseSpecString(inputLicenseSpec);
        Path licenseSpecFile = createTempFile();
        Files.write(licenseSpecFile, licenseSpecString.getBytes(StandardCharsets.UTF_8));
        String output = execute(
            "--publicKeyPath",
            pubKeyPath.toString(),
            "--privateKeyPath",
            priKeyPath.toString(),
            "--licenseFile",
            licenseSpecFile.toString()
        );
        final BytesArray bytes = new BytesArray(output.getBytes(StandardCharsets.UTF_8));
        License outputLicense = License.fromSource(bytes, XContentType.JSON);
        TestUtils.assertLicenseSpec(inputLicenseSpec, outputLicense);
    }

}
