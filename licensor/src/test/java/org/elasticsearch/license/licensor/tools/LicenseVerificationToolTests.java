/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.commons.MissingOptionException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.licensor.AbstractLicensingTestBase;
import org.elasticsearch.license.licensor.TestUtils;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.Licenses;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.elasticsearch.common.cli.CliTool.Command;
import static org.elasticsearch.common.cli.CliTool.ExitStatus;
import static org.elasticsearch.license.licensor.tools.LicenseVerificationTool.LicenseVerifier;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsEqual.equalTo;

public class LicenseVerificationToolTests extends CliToolTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testParsingMissingLicense() throws Exception {
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME, args(" --publicKeyPath " + AbstractLicensingTestBase.getTestPubKeyPath()));

        assertThat(command, instanceOf(Command.Exit.class));
        Command.Exit exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));
    }

    @Test
    public void testParsingMissingPublicKeyPath() throws Exception {
        License inputLicense = AbstractLicensingTestBase.generateSignedLicense("feature__1",
                TimeValue.timeValueHours(1));
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
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME, args(" --publicKeyPath "
                + AbstractLicensingTestBase.getTestPubKeyPath().concat(".invalid")));

        assertThat(command, instanceOf(Command.Exit.class));
        Command.Exit exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));
    }

    @Test
    public void testParsingSimple() throws Exception {
        License inputLicense = AbstractLicensingTestBase.generateSignedLicense("feature__1",
                TimeValue.timeValueHours(1));
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME,
                args("--license " + TestUtils.dumpLicense(inputLicense)
                        + " --publicKeyPath " + AbstractLicensingTestBase.getTestPubKeyPath()));
        assertThat(command, instanceOf(LicenseVerifier.class));
        LicenseVerifier licenseVerifier = (LicenseVerifier) command;
        assertThat(licenseVerifier.licenses.size(), equalTo(1));
        License outputLicense = licenseVerifier.licenses.iterator().next();
        TestUtils.isSame(inputLicense, outputLicense);
    }

    @Test
    public void testParsingLicenseFile() throws Exception {
        License inputLicense = AbstractLicensingTestBase.generateSignedLicense("feature__1",
                TimeValue.timeValueHours(1));

        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME,
                args("--licenseFile " + dumpLicenseAsFile(inputLicense)
                        + " --publicKeyPath " + AbstractLicensingTestBase.getTestPubKeyPath()));
        assertThat(command, instanceOf(LicenseVerifier.class));
        LicenseVerifier licenseVerifier = (LicenseVerifier) command;
        assertThat(licenseVerifier.licenses.size(), equalTo(1));
        License outputLicense = licenseVerifier.licenses.iterator().next();
        TestUtils.isSame(inputLicense, outputLicense);

    }

    @Test
    public void testParsingMultipleLicense() throws Exception {
        int n = randomIntBetween(2, 5);
        Map<String, License> inputLicenses = new HashMap<>();
        for (int i = 0; i < n; i++) {
            License license = AbstractLicensingTestBase.generateSignedLicense("feature__" + i,
                    TimeValue.timeValueHours(1));
            inputLicenses.put(license.feature(), license);
        }

        StringBuilder argsBuilder = new StringBuilder();
        for (License inputLicense : inputLicenses.values()) {
            argsBuilder.append(" --license ")
                    .append(TestUtils.dumpLicense(inputLicense));
        }
        argsBuilder.append(" --publicKeyPath ").append(AbstractLicensingTestBase.getTestPubKeyPath());
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
        int n = randomIntBetween(2, 5);
        Map<String, License> inputLicenses = new HashMap<>();
        for (int i = 0; i < n; i++) {
            License license = AbstractLicensingTestBase.generateSignedLicense("feature__" + i,
                    TimeValue.timeValueHours(1));
            inputLicenses.put(license.feature(), license);
        }

        String output = runLicenseVerificationTool(new HashSet<>(inputLicenses.values()), Paths.get(AbstractLicensingTestBase.getTestPubKeyPath()), ExitStatus.OK);
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
        License signedLicense = AbstractLicensingTestBase.generateSignedLicense("feature__1"
                , TimeValue.timeValueHours(1));

        License tamperedLicense = License.builder()
                .fromLicenseSpec(signedLicense, signedLicense.signature())
                .expiryDate(signedLicense.expiryDate() + randomIntBetween(1, 1000)).build();

        runLicenseVerificationTool(Collections.singleton(tamperedLicense), Paths.get(AbstractLicensingTestBase.getTestPubKeyPath()), ExitStatus.DATA_ERROR);
    }

    private String dumpLicenseAsFile(License license) throws Exception {
        File tempFile = temporaryFolder.newFile();
        Files.write(Paths.get(tempFile.getAbsolutePath()), TestUtils.dumpLicense(license).getBytes(StandardCharsets.UTF_8));
        return tempFile.getAbsolutePath();
    }

    private String runLicenseVerificationTool(Set<License> licenses, Path publicKeyPath, ExitStatus expectedExitStatus) throws Exception {
        CaptureOutputTerminal outputTerminal = new CaptureOutputTerminal();
        LicenseVerifier licenseVerifier = new LicenseVerifier(outputTerminal, licenses, publicKeyPath);
        assertThat(execute(licenseVerifier, ImmutableSettings.EMPTY), equalTo(expectedExitStatus));
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
