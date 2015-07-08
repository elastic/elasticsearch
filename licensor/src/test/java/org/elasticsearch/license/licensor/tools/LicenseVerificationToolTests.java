/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.apache.commons.cli.MissingOptionException;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.Licenses;
import org.elasticsearch.license.licensor.AbstractLicensingTestBase;
import org.elasticsearch.license.licensor.TestUtils;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.elasticsearch.common.cli.CliTool.Command;
import static org.elasticsearch.common.cli.CliTool.ExitStatus;
import static org.elasticsearch.license.licensor.tools.LicenseVerificationTool.LicenseVerifier;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsEqual.equalTo;

public class LicenseVerificationToolTests extends CliToolTestCase {

    @Test
    public void testParsingMissingLicense() throws Exception {
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Path path = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME, args(" --publicKeyPath " + path));

        assertThat(command, instanceOf(Command.Exit.class));
        Command.Exit exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));
    }

    @Test
    public void testParsingMissingPublicKeyPath() throws Exception {
        Path pubKeyPath = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        Path priKeyPath = getDataPath(TestUtils.PRIVATE_KEY_RESOURCE);
        License inputLicense = AbstractLicensingTestBase.generateSignedLicense("feature__1",
                TimeValue.timeValueHours(1), pubKeyPath.toString(), priKeyPath.toString());
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        try {
            licenseVerificationTool.parse(LicenseVerificationTool.NAME,
                    args("--license " + TestUtils.dumpLicense(inputLicense)));
        } catch (MissingOptionException e) {
            assertThat(e.getMessage(), containsString("pub"));
        }
    }

    @Test
    public void testParsingNonExistentPublicKeyPath() throws Exception {
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Path path = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME, args(" --publicKeyPath "
                + path.toString().concat(".invalid")));

        assertThat(command, instanceOf(Command.Exit.class));
        Command.Exit exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));
    }

    @Test
    public void testParsingSimple() throws Exception {
        Path pubKeyPath = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        Path priKeyPath = getDataPath(TestUtils.PRIVATE_KEY_RESOURCE);
        License inputLicense = AbstractLicensingTestBase.generateSignedLicense("feature__1",
                TimeValue.timeValueHours(1), pubKeyPath.toString(), priKeyPath.toString());
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME,
                args("--license " + TestUtils.dumpLicense(inputLicense)
                        + " --publicKeyPath " + getDataPath(TestUtils.PUBLIC_KEY_RESOURCE)));
        assertThat(command, instanceOf(LicenseVerifier.class));
        LicenseVerifier licenseVerifier = (LicenseVerifier) command;
        assertThat(licenseVerifier.licenses.size(), equalTo(1));
        License outputLicense = licenseVerifier.licenses.iterator().next();
        TestUtils.isSame(inputLicense, outputLicense);
    }

    @Test
    public void testParsingLicenseFile() throws Exception {
        Path pubKeyPath = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        Path priKeyPath = getDataPath(TestUtils.PRIVATE_KEY_RESOURCE);
        License inputLicense = AbstractLicensingTestBase.generateSignedLicense("feature__1",
                TimeValue.timeValueHours(1), pubKeyPath.toString(), priKeyPath.toString());

        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME,
                new String[] { "--licenseFile", dumpLicenseAsFile(inputLicense),
                        "--publicKeyPath", getDataPath(TestUtils.PUBLIC_KEY_RESOURCE).toString() });
        assertThat(command, instanceOf(LicenseVerifier.class));
        LicenseVerifier licenseVerifier = (LicenseVerifier) command;
        assertThat(licenseVerifier.licenses.size(), equalTo(1));
        License outputLicense = licenseVerifier.licenses.iterator().next();
        TestUtils.isSame(inputLicense, outputLicense);

    }

    @Test
    public void testParsingMultipleLicense() throws Exception {
        Path pubKeyPath = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        Path priKeyPath = getDataPath(TestUtils.PRIVATE_KEY_RESOURCE);

        int n = randomIntBetween(2, 5);
        Map<String, License> inputLicenses = new HashMap<>();
        for (int i = 0; i < n; i++) {
            License license = AbstractLicensingTestBase.generateSignedLicense("feature__" + i,
                    TimeValue.timeValueHours(1), pubKeyPath.toString(), priKeyPath.toString());
            inputLicenses.put(license.feature(), license);
        }

        StringBuilder argsBuilder = new StringBuilder();
        for (License inputLicense : inputLicenses.values()) {
            argsBuilder.append(" --license ")
                    .append(TestUtils.dumpLicense(inputLicense));
        }
        argsBuilder.append(" --publicKeyPath ").append(getDataPath(TestUtils.PUBLIC_KEY_RESOURCE).toString());
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME, args(argsBuilder.toString()));

        assertThat(command, instanceOf(LicenseVerifier.class));
        LicenseVerifier licenseVerifier = (LicenseVerifier) command;
        assertThat(licenseVerifier.licenses.size(), equalTo(inputLicenses.size()));

        for (License outputLicense : licenseVerifier.licenses) {
            License inputLicense = inputLicenses.get(outputLicense.feature());
            assertThat(inputLicense, notNullValue());
            TestUtils.isSame(inputLicense, outputLicense);
        }
    }

    @Test
    public void testToolSimple() throws Exception {
        Path pubKeyPath = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        Path priKeyPath = getDataPath(TestUtils.PRIVATE_KEY_RESOURCE);

        int n = randomIntBetween(2, 5);
        Map<String, License> inputLicenses = new HashMap<>();
        for (int i = 0; i < n; i++) {
            License license = AbstractLicensingTestBase.generateSignedLicense("feature__" + i,
                    TimeValue.timeValueHours(1), pubKeyPath.toString(), priKeyPath.toString());
            inputLicenses.put(license.feature(), license);
        }

        String output = runLicenseVerificationTool(new HashSet<>(inputLicenses.values()), getDataPath(TestUtils.PUBLIC_KEY_RESOURCE), ExitStatus.OK);
        List<License> outputLicenses = Licenses.fromSource(output.getBytes(StandardCharsets.UTF_8), true);
        assertThat(outputLicenses.size(), equalTo(inputLicenses.size()));

        for (License outputLicense : outputLicenses) {
            License inputLicense = inputLicenses.get(outputLicense.feature());
            assertThat(inputLicense, notNullValue());
            TestUtils.isSame(inputLicense, outputLicense);
        }
    }

    @Test
    public void testToolInvalidLicense() throws Exception {
        Path pubKeyPath = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        Path priKeyPath = getDataPath(TestUtils.PRIVATE_KEY_RESOURCE);
        License signedLicense = AbstractLicensingTestBase.generateSignedLicense("feature__1"
                , TimeValue.timeValueHours(1), pubKeyPath.toString(), priKeyPath.toString());

        License tamperedLicense = License.builder()
                .fromLicenseSpec(signedLicense, signedLicense.signature())
                .expiryDate(signedLicense.expiryDate() + randomIntBetween(1, 1000)).build();

        runLicenseVerificationTool(Collections.singleton(tamperedLicense), getDataPath(TestUtils.PUBLIC_KEY_RESOURCE), ExitStatus.DATA_ERROR);
    }

    private String dumpLicenseAsFile(License license) throws Exception {
        Path tempFile = createTempFile();
        Files.write(tempFile, TestUtils.dumpLicense(license).getBytes(StandardCharsets.UTF_8));
        return tempFile.toAbsolutePath().toString();
    }

    private String runLicenseVerificationTool(Set<License> licenses, Path publicKeyPath, ExitStatus expectedExitStatus) throws Exception {
        CaptureOutputTerminal outputTerminal = new CaptureOutputTerminal();
        Settings settings = Settings.builder().put("path.home", createTempDir("LicenseVerificationToolTests")).build();
        LicenseVerifier licenseVerifier = new LicenseVerifier(outputTerminal, licenses, publicKeyPath);
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
