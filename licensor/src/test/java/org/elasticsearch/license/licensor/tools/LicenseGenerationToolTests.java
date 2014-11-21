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
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.Licenses;
import org.elasticsearch.license.licensor.AbstractLicensingTestBase;
import org.elasticsearch.license.licensor.TestUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.elasticsearch.common.cli.CliTool.ExitStatus;
import static org.elasticsearch.common.cli.CliTool.Command;
import static org.elasticsearch.license.licensor.tools.LicenseGeneratorTool.LicenseGenerator;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;

public class LicenseGenerationToolTests extends CliToolTestCase {

    protected static String pubKeyPath = null;
    protected static String priKeyPath = null;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();


    @BeforeClass
    public static void setup() throws Exception {
        pubKeyPath = AbstractLicensingTestBase.getTestPubKeyPath();
        priKeyPath = AbstractLicensingTestBase.getTestPriKeyPath();
    }

    @Test
    public void testParsingNonExistentKeyFile() throws Exception {
        TestUtils.LicenseSpec inputLicenseSpec = TestUtils.generateRandomLicenseSpec();
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                args("--license " + TestUtils.generateLicenseSpecString(Arrays.asList(inputLicenseSpec))
                        + " --publicKeyPath " + pubKeyPath.concat("invalid")
                        + " --privateKeyPath " + priKeyPath));

        assertThat(command, instanceOf(Command.Exit.class));
        Command.Exit exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));

        command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                args("--license " + TestUtils.generateLicenseSpecString(Arrays.asList(inputLicenseSpec))
                        + " --privateKeyPath " + priKeyPath.concat("invalid")
                        + " --publicKeyPath " + pubKeyPath));

        assertThat(command, instanceOf(Command.Exit.class));
        exitCommand = (Command.Exit) command;
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
        TestUtils.LicenseSpec inputLicenseSpec = TestUtils.generateRandomLicenseSpec();
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        boolean pubKeyMissing = randomBoolean();
        try {
            licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                    args("--license " + TestUtils.generateLicenseSpecString(Arrays.asList(inputLicenseSpec))
                            + ((!pubKeyMissing) ? " --publicKeyPath " + pubKeyPath : "")
                            + ((pubKeyMissing) ? " --privateKeyPath " + priKeyPath : "")));
            fail("missing argument: " + ((pubKeyMissing) ? "publicKeyPath" : "privateKeyPath") + " should throw an exception");
        } catch (MissingOptionException e) {
            assertThat(e.getMessage(), containsString((pubKeyMissing) ? "pub" : "pri"));
        }
    }

    @Test
    public void testParsingSimple() throws Exception {
        TestUtils.LicenseSpec inputLicenseSpec = TestUtils.generateRandomLicenseSpec();
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                args("--license " + TestUtils.generateLicenseSpecString(Arrays.asList(inputLicenseSpec))
                        + " --publicKeyPath " + pubKeyPath
                        + " --privateKeyPath " + priKeyPath));

        assertThat(command, instanceOf(LicenseGenerator.class));
        LicenseGenerator licenseGenerator = (LicenseGenerator) command;
        assertThat(licenseGenerator.publicKeyFilePath, equalTo(pubKeyPath));
        assertThat(licenseGenerator.privateKeyFilePath, equalTo(priKeyPath));
        assertThat(licenseGenerator.licenseSpecs.size(), equalTo(1));
        License outputLicenseSpec = licenseGenerator.licenseSpecs.iterator().next();

        TestUtils.assertLicenseSpec(inputLicenseSpec, outputLicenseSpec);
    }

    @Test
    public void testParsingLicenseFile() throws Exception {
        TestUtils.LicenseSpec inputLicenseSpec = TestUtils.generateRandomLicenseSpec();
        File tempFile = temporaryFolder.newFile("license_spec.json");
        Files.write(Paths.get(tempFile.getAbsolutePath()), TestUtils.generateLicenseSpecString(Arrays.asList(inputLicenseSpec)).getBytes(StandardCharsets.UTF_8));

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
        License outputLicenseSpec = licenseGenerator.licenseSpecs.iterator().next();

        TestUtils.assertLicenseSpec(inputLicenseSpec, outputLicenseSpec);
    }

    @Test
    public void testParsingMultipleLicense() throws Exception {
        int n = randomIntBetween(2, 5);
        Map<String, TestUtils.LicenseSpec> inputLicenseSpecs = new HashMap<>();
        for (int i = 0; i < n; i++) {
            TestUtils.LicenseSpec licenseSpec = TestUtils.generateRandomLicenseSpec();
            inputLicenseSpecs.put(licenseSpec.feature, licenseSpec);
        }
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                args("--license " + TestUtils.generateLicenseSpecString(new ArrayList<>(inputLicenseSpecs.values()))
                        + " --publicKeyPath " + pubKeyPath
                        + " --privateKeyPath " + priKeyPath));

        assertThat(command, instanceOf(LicenseGenerator.class));
        LicenseGenerator licenseGenerator = (LicenseGenerator) command;
        assertThat(licenseGenerator.publicKeyFilePath, equalTo(pubKeyPath));
        assertThat(licenseGenerator.privateKeyFilePath, equalTo(priKeyPath));
        assertThat(licenseGenerator.licenseSpecs.size(), equalTo(inputLicenseSpecs.size()));

        for (License outputLicenseSpec : licenseGenerator.licenseSpecs) {
            TestUtils.LicenseSpec inputLicenseSpec = inputLicenseSpecs.get(outputLicenseSpec.feature());
            assertThat(inputLicenseSpec, notNullValue());
            TestUtils.assertLicenseSpec(inputLicenseSpec, outputLicenseSpec);
        }
    }

    @Test
    public void testTool() throws Exception {
        int n = randomIntBetween(1, 5);
        Map<String, TestUtils.LicenseSpec> inputLicenseSpecs = new HashMap<>();
        for (int i = 0; i < n; i++) {
            TestUtils.LicenseSpec licenseSpec = TestUtils.generateRandomLicenseSpec();
            inputLicenseSpecs.put(licenseSpec.feature, licenseSpec);
        }
        List<License> licenseSpecs = Licenses.fromSource(TestUtils.generateLicenseSpecString(new ArrayList<>(inputLicenseSpecs.values())).getBytes(StandardCharsets.UTF_8), false);

        String output = runLicenseGenerationTool(pubKeyPath, priKeyPath, new HashSet<>(licenseSpecs), ExitStatus.OK);
        List<License> outputLicenses = Licenses.fromSource(output.getBytes(StandardCharsets.UTF_8), true);
        assertThat(outputLicenses.size(), equalTo(inputLicenseSpecs.size()));

        for (License outputLicense : outputLicenses) {
            TestUtils.LicenseSpec inputLicenseSpec = inputLicenseSpecs.get(outputLicense.feature());
            assertThat(inputLicenseSpec, notNullValue());
            TestUtils.assertLicenseSpec(inputLicenseSpec, outputLicense);
        }
    }

    private String runLicenseGenerationTool(String pubKeyPath, String priKeyPath, Set<License> licenseSpecs, ExitStatus expectedExitStatus) throws Exception {
        CaptureOutputTerminal outputTerminal = new CaptureOutputTerminal();
        LicenseGenerator licenseGenerator = new LicenseGenerator(outputTerminal, pubKeyPath, priKeyPath, licenseSpecs);
        assertThat(execute(licenseGenerator, ImmutableSettings.EMPTY), equalTo(expectedExitStatus));
        assertThat(outputTerminal.getTerminalOutput().size(), equalTo(1));
        return outputTerminal.getTerminalOutput().get(0);
    }


    private ExitStatus execute(Command cmd, Settings settings) throws Exception {
        Environment env = new Environment(settings);
        return cmd.execute(settings, env);
    }
}
