/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.apache.commons.cli.MissingOptionException;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.Licenses;
import org.elasticsearch.license.licensor.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.elasticsearch.common.cli.CliTool.Command;
import static org.elasticsearch.common.cli.CliTool.ExitStatus;
import static org.elasticsearch.license.licensor.tools.LicenseGeneratorTool.LicenseGenerator;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;

public class LicenseGenerationToolTests extends CliToolTestCase {

    protected Path pubKeyPath = null;
    protected Path priKeyPath = null;
    protected Path homeDir = null;


    @Before
    public void setup() throws Exception {
        logger.error("project.basedir [{}]", System.getProperty("project.basedir"));
        pubKeyPath = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        priKeyPath = getDataPath(TestUtils.PRIVATE_KEY_RESOURCE);
        homeDir = createTempDir();
    }

    @Test
    public void testParsingNonExistentKeyFile() throws Exception {
        TestUtils.LicenseSpec inputLicenseSpec = TestUtils.generateRandomLicenseSpec();
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                new String[] {"--license",  TestUtils.generateLicenseSpecString(Arrays.asList(inputLicenseSpec)),
                        "--publicKeyPath", pubKeyPath.toString().concat("invalid"),
                        "--privateKeyPath", priKeyPath.toString() });

        assertThat(command, instanceOf(Command.Exit.class));
        Command.Exit exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));

        command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                new String[] {"--license", TestUtils.generateLicenseSpecString(Arrays.asList(inputLicenseSpec)),
                        "--privateKeyPath", priKeyPath.toString().concat("invalid"),
                        "--publicKeyPath", pubKeyPath.toString() });

        assertThat(command, instanceOf(Command.Exit.class));
        exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));
    }

    @Test
    public void testParsingMissingLicenseSpec() throws Exception {
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                new String[] { "--publicKeyPath", pubKeyPath.toString(),
                        "--privateKeyPath", priKeyPath.toString() });

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
                    new String[] { "--license", TestUtils.generateLicenseSpecString(Arrays.asList(inputLicenseSpec)),
                            ((pubKeyMissing) ? "--privateKeyPath" : "--publicKeyPath"),
                            ((pubKeyMissing) ? priKeyPath.toString() : pubKeyPath.toString()) });
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
                new String[]{"--license", TestUtils.generateLicenseSpecString(Arrays.asList(inputLicenseSpec)),
                        "--publicKeyPath", pubKeyPath.toString(),
                        "--privateKeyPath", priKeyPath.toString() });

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
        Path tempFile = createTempFile();
        Files.write(tempFile, TestUtils.generateLicenseSpecString(Arrays.asList(inputLicenseSpec)).getBytes(StandardCharsets.UTF_8));

        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                new String[] { "--licenseFile", tempFile.toAbsolutePath().toString(),
                        "--publicKeyPath", pubKeyPath.toString(),
                        "--privateKeyPath", priKeyPath.toString() });

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
                new String[] { "--license", TestUtils.generateLicenseSpecString(new ArrayList<>(inputLicenseSpecs.values())),
                        "--publicKeyPath", pubKeyPath.toString(),
                        "--privateKeyPath", priKeyPath.toString() });

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

    private String runLicenseGenerationTool(Path pubKeyPath, Path priKeyPath, Set<License> licenseSpecs, ExitStatus expectedExitStatus) throws Exception {
        CaptureOutputTerminal outputTerminal = new CaptureOutputTerminal();
        Settings settings = Settings.builder().put("path.home", homeDir).build();
        LicenseGenerator licenseGenerator = new LicenseGenerator(outputTerminal, pubKeyPath, priKeyPath, licenseSpecs);
        assertThat(execute(licenseGenerator, settings), equalTo(expectedExitStatus));
        assertThat(outputTerminal.getTerminalOutput().size(), equalTo(1));
        return outputTerminal.getTerminalOutput().get(0);
    }


    private ExitStatus execute(Command cmd, Settings settings) throws Exception {
        Environment env = new Environment(settings);
        return cmd.execute(settings, env);
    }
}
