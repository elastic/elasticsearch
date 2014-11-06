/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.licensor.tools.LicenseVerificationTool;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.cli.CliTool.Command;
import static org.elasticsearch.common.cli.CliTool.ExitStatus;
import static org.elasticsearch.license.AbstractLicensingTestBase.generateSignedLicense;
import static org.elasticsearch.license.licensor.tools.LicenseVerificationTool.LicenseVerifier;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;

public class LicenseVerificationToolTests extends CliToolTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testParsingMissingLicense() throws Exception {
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME, args(""));

        assertThat(command, instanceOf(Command.Exit.class));
        Command.Exit exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));
    }

    @Test
    public void testParsingSimple() throws Exception {
        ESLicense inputLicense = generateSignedLicense(randomRealisticUnicodeOfCodepointLengthBetween(5, 15),
                TimeValue.timeValueHours(1));
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME,
                args("--license " + dumpLicense(inputLicense)));
        assertThat(command, instanceOf(LicenseVerifier.class));
        LicenseVerifier licenseVerifier = (LicenseVerifier) command;
        assertThat(licenseVerifier.licenses.size(), equalTo(1));
        ESLicense outputLicense = licenseVerifier.licenses.iterator().next();
        TestUtils.isSame(inputLicense, outputLicense);
    }

    @Test
    public void testParsingLicenseFile() throws Exception {
        ESLicense inputLicense = generateSignedLicense(randomRealisticUnicodeOfCodepointLengthBetween(5, 15),
                TimeValue.timeValueHours(1));

        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME,
                args("--licenseFile " + dumpLicenseAsFile(inputLicense)));
        assertThat(command, instanceOf(LicenseVerifier.class));
        LicenseVerifier licenseVerifier = (LicenseVerifier) command;
        assertThat(licenseVerifier.licenses.size(), equalTo(1));
        ESLicense outputLicense = licenseVerifier.licenses.iterator().next();
        TestUtils.isSame(inputLicense, outputLicense);

    }

    @Test
    public void testParsingMultipleLicense() throws Exception {
        int n = randomIntBetween(2, 5);
        Set<ESLicense> inputLicenses = new HashSet<>();
        for (int i = 0; i < n; i++) {
            inputLicenses.add(generateSignedLicense(randomRealisticUnicodeOfCodepointLengthBetween(5, 15),
                    TimeValue.timeValueHours(1)));
        }

        StringBuilder argsBuilder = new StringBuilder();
        for (ESLicense inputLicense : inputLicenses) {
            argsBuilder.append(" --license ")
                    .append(dumpLicense(inputLicense));
        }
        LicenseVerificationTool licenseVerificationTool = new LicenseVerificationTool();
        Command command = licenseVerificationTool.parse(LicenseVerificationTool.NAME, args(argsBuilder.toString()));

        assertThat(command, instanceOf(LicenseVerifier.class));
        LicenseVerifier licenseVerifier = (LicenseVerifier) command;
        assertThat(licenseVerifier.licenses.size(), equalTo(n));

        for (ESLicense inputLicense : inputLicenses) {
            boolean found = false;
            for (ESLicense outputLicense : licenseVerifier.licenses) {
                if (inputLicense.uid().equals(outputLicense.uid())) {
                    TestUtils.isSame(inputLicense, outputLicense);
                    found = true;
                    break;
                }
            }
            assertThat(found, equalTo(true));
        }
    }

    @Test
    public void testToolSimple() throws Exception {
        int n = randomIntBetween(2, 5);
        Set<ESLicense> inputLicenses = new HashSet<>();
        for (int i = 0; i < n; i++) {
            inputLicenses.add(generateSignedLicense(randomRealisticUnicodeOfCodepointLengthBetween(5, 15),
                    TimeValue.timeValueHours(1)));
        }

        String output = runLicenseVerificationTool(inputLicenses, ExitStatus.OK);
        List<ESLicense> outputLicenses = ESLicenses.fromSource(output.getBytes(StandardCharsets.UTF_8), true);
        assertThat(outputLicenses.size(), equalTo(n));

        for (ESLicense inputLicense : inputLicenses) {
            boolean found = false;
            for (ESLicense outputLicense : outputLicenses) {
                if (inputLicense.uid().equals(outputLicense.uid())) {
                    TestUtils.isSame(inputLicense, outputLicense);
                    found = true;
                    break;
                }
            }
            assertThat(found, equalTo(true));
        }
    }

    @Test
    public void testToolInvalidLicense() throws Exception {
        ESLicense signedLicense = generateSignedLicense(randomRealisticUnicodeOfCodepointLengthBetween(5, 15)
                , TimeValue.timeValueHours(1));

        ESLicense tamperedLicense = ESLicense.builder()
                .fromLicenseSpec(signedLicense, signedLicense.signature())
                .expiryDate(signedLicense.expiryDate() + randomIntBetween(1, 1000)).build();

        runLicenseVerificationTool(Collections.singleton(tamperedLicense), ExitStatus.DATA_ERROR);
    }

    private String dumpLicenseAsFile(ESLicense license) throws Exception {
        File tempFile = temporaryFolder.newFile();
        FileUtils.write(tempFile, dumpLicense(license));
        return tempFile.getAbsolutePath();

    }

    private String dumpLicense(ESLicense license) throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        ESLicenses.toXContent(Collections.singletonList(license), builder, ToXContent.EMPTY_PARAMS);
        builder.flush();
        return builder.string();
    }


    private String runLicenseVerificationTool(Set<ESLicense> licenses, ExitStatus expectedExitStatus) throws Exception {
        CaptureOutputTerminal outputTerminal = new CaptureOutputTerminal();
        LicenseVerifier licenseVerifier = new LicenseVerifier(outputTerminal, licenses);
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
