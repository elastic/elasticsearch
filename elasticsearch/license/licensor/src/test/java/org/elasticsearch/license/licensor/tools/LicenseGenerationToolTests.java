/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.elasticsearch.common.cli.CliTool.Command;
import org.elasticsearch.common.cli.CliTool.ExitStatus;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.UserError;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.licensor.TestUtils;
import org.elasticsearch.license.licensor.tools.LicenseGeneratorTool.LicenseGenerator;
import org.junit.Before;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;

public class LicenseGenerationToolTests extends CliToolTestCase {
    protected Path pubKeyPath = null;
    protected Path priKeyPath = null;

    @Before
    public void setup() throws Exception {
        logger.error("project.basedir [{}]", System.getProperty("project.basedir"));
        pubKeyPath = getDataPath(TestUtils.PUBLIC_KEY_RESOURCE);
        priKeyPath = getDataPath(TestUtils.PRIVATE_KEY_RESOURCE);
    }

    public void testParsingNonExistentKeyFile() throws Exception {
        TestUtils.LicenseSpec inputLicenseSpec = TestUtils.generateRandomLicenseSpec(License.VERSION_CURRENT);
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                new String[] {"--license",  TestUtils.generateLicenseSpecString(inputLicenseSpec),
                        "--publicKeyPath", pubKeyPath.toString().concat("invalid"),
                        "--privateKeyPath", priKeyPath.toString() });

        assertThat(command, instanceOf(Command.Exit.class));
        Command.Exit exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));

        command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                new String[] {"--license", TestUtils.generateLicenseSpecString(inputLicenseSpec),
                        "--privateKeyPath", priKeyPath.toString().concat("invalid"),
                        "--publicKeyPath", pubKeyPath.toString() });

        assertThat(command, instanceOf(Command.Exit.class));
        exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));
    }

    public void testParsingMissingLicenseSpec() throws Exception {
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                new String[] { "--publicKeyPath", pubKeyPath.toString(),
                        "--privateKeyPath", priKeyPath.toString() });

        assertThat(command, instanceOf(Command.Exit.class));
        Command.Exit exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));
    }

    public void testParsingMissingArgs() throws Exception {
        TestUtils.LicenseSpec inputLicenseSpec = TestUtils.generateRandomLicenseSpec(License.VERSION_CURRENT);
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        boolean pubKeyMissing = randomBoolean();
        UserError e = expectThrows(UserError.class, () -> {
            licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                new String[]{"--license", TestUtils.generateLicenseSpecString(inputLicenseSpec),
                    ((pubKeyMissing) ? "--privateKeyPath" : "--publicKeyPath"),
                    ((pubKeyMissing) ? priKeyPath.toString() : pubKeyPath.toString())});
        });
        assertThat(e.getMessage(), containsString((pubKeyMissing) ? "pub" : "pri"));
    }

    public void testParsingSimple() throws Exception {
        TestUtils.LicenseSpec inputLicenseSpec = TestUtils.generateRandomLicenseSpec(License.VERSION_CURRENT);
        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                new String[]{"--license", TestUtils.generateLicenseSpecString(inputLicenseSpec),
                        "--publicKeyPath", pubKeyPath.toString(),
                        "--privateKeyPath", priKeyPath.toString() });

        assertThat(command, instanceOf(LicenseGenerator.class));
        LicenseGenerator licenseGenerator = (LicenseGenerator) command;
        assertThat(licenseGenerator.publicKeyFilePath, equalTo(pubKeyPath));
        assertThat(licenseGenerator.privateKeyFilePath, equalTo(priKeyPath));
        TestUtils.assertLicenseSpec(inputLicenseSpec, licenseGenerator.licenseSpec);
    }

    public void testParsingLicenseFile() throws Exception {
        TestUtils.LicenseSpec inputLicenseSpec = TestUtils.generateRandomLicenseSpec(License.VERSION_CURRENT);
        Path tempFile = createTempFile();
        Files.write(tempFile, TestUtils.generateLicenseSpecString(inputLicenseSpec).getBytes(StandardCharsets.UTF_8));

        LicenseGeneratorTool licenseGeneratorTool = new LicenseGeneratorTool();
        Command command = licenseGeneratorTool.parse(LicenseGeneratorTool.NAME,
                new String[] { "--licenseFile", tempFile.toAbsolutePath().toString(),
                        "--publicKeyPath", pubKeyPath.toString(),
                        "--privateKeyPath", priKeyPath.toString() });

        assertThat(command, instanceOf(LicenseGenerator.class));
        LicenseGenerator licenseGenerator = (LicenseGenerator) command;
        assertThat(licenseGenerator.publicKeyFilePath, equalTo(pubKeyPath));
        assertThat(licenseGenerator.privateKeyFilePath, equalTo(priKeyPath));
        TestUtils.assertLicenseSpec(inputLicenseSpec, licenseGenerator.licenseSpec);
    }

    public void testTool() throws Exception {
        TestUtils.LicenseSpec licenseSpec = TestUtils.generateRandomLicenseSpec(License.VERSION_CURRENT);
        License license = License.fromSource(TestUtils.generateLicenseSpecString(licenseSpec).getBytes(StandardCharsets.UTF_8));
        String output = runLicenseGenerationTool(pubKeyPath, priKeyPath, license, ExitStatus.OK);
        License outputLicense = License.fromSource(output.getBytes(StandardCharsets.UTF_8));
        TestUtils.assertLicenseSpec(licenseSpec, outputLicense);
    }

    private String runLicenseGenerationTool(Path pubKeyPath, Path priKeyPath, License licenseSpec,
                                            ExitStatus expectedExitStatus) throws Exception {
        CaptureOutputTerminal outputTerminal = new CaptureOutputTerminal();
        Settings settings = Settings.builder().put("path.home", createTempDir("LicenseGenerationToolTests")).build();
        LicenseGenerator licenseGenerator = new LicenseGenerator(outputTerminal, pubKeyPath, priKeyPath, licenseSpec);
        assertThat(execute(licenseGenerator, settings), equalTo(expectedExitStatus));
        assertThat(outputTerminal.getTerminalOutput().size(), equalTo(1));
        return outputTerminal.getTerminalOutput().get(0);
    }

    private ExitStatus execute(Command cmd, Settings settings) throws Exception {
        Environment env = new Environment(settings);
        return cmd.execute(settings, env);
    }
}
