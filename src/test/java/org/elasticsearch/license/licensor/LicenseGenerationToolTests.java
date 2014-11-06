/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.commons.MissingOptionException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.licensor.tools.LicenseGeneratorTool;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.elasticsearch.common.cli.CliTool.ExitStatus;
import static org.elasticsearch.license.AbstractLicensingTestBase.*;
import static org.elasticsearch.license.licensor.tools.LicenseGeneratorTool.Command;
import static org.elasticsearch.license.licensor.tools.LicenseGeneratorTool.LicenseGenerator;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;

public class LicenseGenerationToolTests extends CliToolTestCase {

    protected static String pubKeyPath = null;
    protected static String priKeyPath = null;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();


    @BeforeClass
    public static void setup() throws Exception {
        pubKeyPath = getResourcePath("/public.key");
        priKeyPath = getResourcePath("/private.key");
    }

    @Test
    public void testParsingNonExistentKeyFile() throws Exception {
        LicenseSpec inputLicenseSpec = generateRandomLicenseSpec();
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        boolean invalidPubKeyPath = randomBoolean();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                args("--license " + generateESLicenseSpecString(Arrays.asList(inputLicenseSpec))
                        + " --publicKeyPath " + ((invalidPubKeyPath) ? pubKeyPath.concat("invalid") : pubKeyPath)
                        + " --privateKeyPath " + ((!invalidPubKeyPath) ? priKeyPath.concat("invalid") : priKeyPath)));

        assertThat(command, instanceOf(Command.Exit.class));
        Command.Exit exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));
    }

    @Test
    public void testParsingMissingLicenseSpec() throws Exception {
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                args(" --publicKeyPath " + pubKeyPath
                        + " --privateKeyPath " + priKeyPath));

        assertThat(command, instanceOf(Command.Exit.class));
        Command.Exit exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));
    }

    @Test
    public void testParsingMissingArgs() throws Exception {
        LicenseSpec inputLicenseSpec = generateRandomLicenseSpec();
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        boolean pubKeyMissing = randomBoolean();
        try {
            licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                    args("--license " + generateESLicenseSpecString(Arrays.asList(inputLicenseSpec))
                            + ((!pubKeyMissing) ? " --publicKeyPath " + pubKeyPath : "")
                            + ((pubKeyMissing) ? " --privateKeyPath " + priKeyPath : "")));
            fail("missing argument: " + ((pubKeyMissing) ? "publicKeyPath" : "privateKeyPath") + " should throw an exception");
        } catch (MissingOptionException e) {
            assertThat(e.getMessage(), containsString((pubKeyMissing) ? "pub" : "pri"));
        }
    }

    @Test
    public void testParsingSimple() throws Exception {
        LicenseSpec inputLicenseSpec = generateRandomLicenseSpec();
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                args("--license " + generateESLicenseSpecString(Arrays.asList(inputLicenseSpec))
                        + " --publicKeyPath " + pubKeyPath
                        + " --privateKeyPath " + priKeyPath));

        assertThat(command, instanceOf(LicenseGenerator.class));
        LicenseGenerator licenseGenerator = (LicenseGenerator) command;
        assertThat(licenseGenerator.publicKeyFilePath, equalTo(pubKeyPath));
        assertThat(licenseGenerator.privateKeyFilePath, equalTo(priKeyPath));
        assertThat(licenseGenerator.licenseSpecs.size(), equalTo(1));
        ESLicense outputLicenseSpec = licenseGenerator.licenseSpecs.iterator().next();

        assertLicenseSpec(inputLicenseSpec, outputLicenseSpec);
    }

    @Test
    public void testParsingLicenseFile() throws Exception {
        LicenseSpec inputLicenseSpec = generateRandomLicenseSpec();
        File tempFile = temporaryFolder.newFile("license_spec.json");
        FileUtils.write(tempFile, generateESLicenseSpecString(Arrays.asList(inputLicenseSpec)));

        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                args("--licenseFile " + tempFile.getAbsolutePath()
                        + " --publicKeyPath " + pubKeyPath
                        + " --privateKeyPath " + priKeyPath));

        assertThat(command, instanceOf(LicenseGenerator.class));
        LicenseGenerator licenseGenerator = (LicenseGenerator) command;
        assertThat(licenseGenerator.publicKeyFilePath, equalTo(pubKeyPath));
        assertThat(licenseGenerator.privateKeyFilePath, equalTo(priKeyPath));
        assertThat(licenseGenerator.licenseSpecs.size(), equalTo(1));
        ESLicense outputLicenseSpec = licenseGenerator.licenseSpecs.iterator().next();

        assertLicenseSpec(inputLicenseSpec, outputLicenseSpec);
    }

    @Test
    public void testParsingMultipleLicense() throws Exception {
        int n = randomIntBetween(2, 5);
        List<LicenseSpec> inputLicenseSpecs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            inputLicenseSpecs.add(generateRandomLicenseSpec());
        }
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                args("--license " + generateESLicenseSpecString(inputLicenseSpecs)
                        + " --publicKeyPath " + pubKeyPath
                        + " --privateKeyPath " + priKeyPath));

        assertThat(command, instanceOf(LicenseGenerator.class));
        LicenseGenerator licenseGenerator = (LicenseGenerator) command;
        assertThat(licenseGenerator.publicKeyFilePath, equalTo(pubKeyPath));
        assertThat(licenseGenerator.privateKeyFilePath, equalTo(priKeyPath));
        assertThat(licenseGenerator.licenseSpecs.size(), equalTo(n));

        for (LicenseSpec inputSpec : inputLicenseSpecs) {
            boolean found = false;
            for (ESLicense outputSpec : licenseGenerator.licenseSpecs) {
                if (inputSpec.uid.equals(outputSpec.uid())) {
                    assertLicenseSpec(inputSpec, outputSpec);
                    found = true;
                    break;
                }
            }
            assertThat(found, equalTo(true));
        }
    }

    @Test
    public void testTool() throws Exception {
        int n = randomIntBetween(1, 5);
        List<LicenseSpec> inputLicenseSpecs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            inputLicenseSpecs.add(generateRandomLicenseSpec());
        }
        List<ESLicense> licenseSpecs = ESLicenses.fromSource(generateESLicenseSpecString(inputLicenseSpecs).getBytes(StandardCharsets.UTF_8), false);

        String output = runLicenseGenerationTool(pubKeyPath, priKeyPath, new HashSet<>(licenseSpecs), ExitStatus.OK);
        List<ESLicense> outputLicenses = ESLicenses.fromSource(output.getBytes(StandardCharsets.UTF_8), true);
        assertThat(outputLicenses.size(), equalTo(n));

        for (LicenseSpec inputSpec : inputLicenseSpecs) {
            boolean found = false;
            for (ESLicense license : outputLicenses) {
                if (inputSpec.uid.equals(license.uid())) {
                    assertLicenseSpec(inputSpec, license);
                    found = true;
                    break;
                }
            }
            assertThat(found, equalTo(true));
        }
    }

    private String runLicenseGenerationTool(String pubKeyPath, String priKeyPath, Set<ESLicense> licenseSpecs, ExitStatus expectedExitStatus) throws Exception {
        CaptureOutputTerminal outputTerminal = new CaptureOutputTerminal();
        LicenseGenerator licenseGenerator = new LicenseGenerator(outputTerminal, pubKeyPath, priKeyPath, licenseSpecs);
        assertThat(execute(licenseGenerator, ImmutableSettings.EMPTY), equalTo(expectedExitStatus));
        assertThat(outputTerminal.getTerminalOutput().size(), equalTo(1));
        return outputTerminal.getTerminalOutput().get(0);
    }


    private ExitStatus execute(CliTool.Command cmd, Settings settings) throws Exception {
        Environment env = new Environment(settings);
        return cmd.execute(settings, env);
    }
}
