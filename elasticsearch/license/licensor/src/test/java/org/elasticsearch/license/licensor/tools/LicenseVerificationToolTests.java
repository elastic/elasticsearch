/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.cli.CliTool.Command;
import org.elasticsearch.common.cli.CliTool.ExitStatus;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.UserError;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.licensor.TestUtils;
import org.elasticsearch.license.licensor.tools.LicenseVerificationTool.LicenseVerifier;
import org.hamcrest.CoreMatchers;
import org.junit.Before;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsEqual.equalTo;

public class LicenseVerificationToolTests extends CliToolTestCase {
    protected Path pubKeyPath = null;
    protected Path priKeyPath = null;

    @Before
    public void setup() throws Exception {
        logger.error("project.basedir [{}]", System.getProperty("project.basedir"));
        pubKeyPath = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        priKeyPath = getDataPath(TestUtils.PRIVATE_KEY_RESOURCE);
    }

    public void testParsingMissingLicense() throws Exception {
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Path path = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME, new String[] { "--publicKeyPath", path.toString() });

        assertThat(command, instanceOf(Command.Exit.class));
        Command.Exit exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));
    }

    public void testParsingMissingPublicKeyPath() throws Exception {
        License inputLicense = TestUtils.generateSignedLicense(TimeValue.timeValueHours(1), pubKeyPath, priKeyPath);
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        UserError e = expectThrows(UserError.class, () -> {
            licenseVerificationTool.parse(LicenseVerificationTool.NAME,
                    new String[] { "--license", TestUtils.dumpLicense(inputLicense) });
        });
        assertThat(e.getMessage(), containsString("pub"));
    }

    public void testParsingNonExistentPublicKeyPath() throws Exception {
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Path path = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME,
                new String[] { "--publicKeyPath", path.toString().concat(".invalid") });

        assertThat(command, instanceOf(Command.Exit.class));
        Command.Exit exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));
    }

    public void testParsingSimple() throws Exception {
        License inputLicense = TestUtils.generateSignedLicense(TimeValue.timeValueHours(1), pubKeyPath, priKeyPath);
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME,
                new String[] { "--license", TestUtils.dumpLicense(inputLicense),
                        "--publicKeyPath", getDataPath(TestUtils.PUBLIC_KEY_RESOURCE).toString() });
        assertThat(command, instanceOf(LicenseVerifier.class));
        LicenseVerifier licenseVerifier = (LicenseVerifier) command;
        assertThat(inputLicense, equalTo(licenseVerifier.license));
    }

    public void testParsingLicenseFile() throws Exception {
        License inputLicense = TestUtils.generateSignedLicense(TimeValue.timeValueHours(1), pubKeyPath, priKeyPath);

        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME,
                new String[]{"--licenseFile", dumpLicenseAsFile(inputLicense),
                        "--publicKeyPath", getDataPath(TestUtils.PUBLIC_KEY_RESOURCE).toString()});
        assertThat(command, instanceOf(LicenseVerifier.class));
        LicenseVerifier licenseVerifier = (LicenseVerifier) command;
        assertThat(inputLicense, equalTo(licenseVerifier.license));

    }

    public void testParsingMultipleLicense() throws Exception {
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(1), pubKeyPath, priKeyPath);
        List<String> arguments = new ArrayList<>();
        arguments.add("--license");
        arguments.add(TestUtils.dumpLicense(license));
        arguments.add("--publicKeyPath");
        arguments.add(getDataPath(TestUtils.PUBLIC_KEY_RESOURCE).toString());
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME, arguments.toArray(new String[arguments.size()]));

        assertThat(command, instanceOf(LicenseVerifier.class));
        LicenseVerifier licenseVerifier = (LicenseVerifier) command;
        assertThat(licenseVerifier.license, equalTo(license));
    }

    public void testToolSimple() throws Exception {
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(1), pubKeyPath, priKeyPath);
        String output = runLicenseVerificationTool(license, getDataPath(TestUtils.PUBLIC_KEY_RESOURCE), ExitStatus.OK);
        License outputLicense = License.fromSource(output.getBytes(StandardCharsets.UTF_8));
        assertThat(outputLicense, CoreMatchers.equalTo(license));
    }

    public void testToolInvalidLicense() throws Exception {
        License signedLicense = TestUtils.generateSignedLicense(TimeValue.timeValueHours(1), pubKeyPath, priKeyPath);

        License tamperedLicense = License.builder()
                .fromLicenseSpec(signedLicense, signedLicense.signature())
                .expiryDate(signedLicense.expiryDate() + randomIntBetween(1, 1000)).build();

        runLicenseVerificationTool(tamperedLicense, getDataPath(TestUtils.PUBLIC_KEY_RESOURCE), ExitStatus.DATA_ERROR);
    }

    private String dumpLicenseAsFile(License license) throws Exception {
        Path tempFile = createTempFile();
        Files.write(tempFile, TestUtils.dumpLicense(license).getBytes(StandardCharsets.UTF_8));
        return tempFile.toAbsolutePath().toString();
    }

    private String runLicenseVerificationTool(License license, Path publicKeyPath, ExitStatus expectedExitStatus) throws Exception {
        CaptureOutputTerminal outputTerminal = new CaptureOutputTerminal();
        Settings settings = Settings.builder().put("path.home", createTempDir("LicenseVerificationToolTests")).build();
        LicenseVerifier licenseVerifier = new LicenseVerifier(outputTerminal, license, publicKeyPath);
        assertThat(execute(licenseVerifier, settings), equalTo(expectedExitStatus));
        if (expectedExitStatus == ExitStatus.OK) {
            assertThat(outputTerminal.getTerminalOutput().size(), equalTo(1));

            return outputTerminal.getTerminalOutput().get(0);
        } else {
            return null;
        }
    }

    private ExitStatus execute(Command cmd, Settings settings) throws Exception {
        Environment env = new Environment(settings);
        return cmd.execute(settings, env);
    }
}
