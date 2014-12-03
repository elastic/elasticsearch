/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.commons.MissingOptionException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.licensor.tools.KeyPairGeneratorTool.KeyGenerator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.elasticsearch.common.cli.CliTool.Command;
import static org.elasticsearch.common.cli.CliTool.ExitStatus;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;

public class KeyPairGenerationToolTests extends CliToolTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testParsingMissingPath() throws Exception {
        KeyPairGeneratorTool keyPairGeneratorTool = new KeyPairGeneratorTool();
        File tempFile = temporaryFolder.newFile();
        try {
            keyPairGeneratorTool.parse(KeyPairGeneratorTool.NAME, args(
                     "--privateKeyPath " + tempFile.getAbsolutePath()));
            fail("no public key path provided");
        } catch (MissingOptionException e) {
            assertThat(e.getMessage(), containsString("pub"));
        }
        try {
            keyPairGeneratorTool.parse(KeyPairGeneratorTool.NAME, args(
                    "--publicKeyPath " + tempFile.getAbsolutePath()));
            fail("no private key path provided");
        } catch (MissingOptionException e) {
            assertThat(e.getMessage(), containsString("pri"));
        }
    }

    @Test
    public void testParsingNeverOverrideKey() throws Exception {
        KeyPairGeneratorTool keyPairGeneratorTool = new KeyPairGeneratorTool();
        File tempFile = temporaryFolder.newFile();
        File tempFile2 = temporaryFolder.newFile();
        String nonExistentFilePath = tempFile2.getAbsolutePath();
        assertThat(tempFile2.delete(), equalTo(true));

        Command command = keyPairGeneratorTool.parse(KeyPairGeneratorTool.NAME, args("--privateKeyPath " + tempFile.getAbsolutePath()
                    + " --publicKeyPath " + nonExistentFilePath));

        assertThat(command, instanceOf(Command.Exit.class));
        Command.Exit exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));

        command = keyPairGeneratorTool.parse(KeyPairGeneratorTool.NAME, args("--publicKeyPath " + tempFile.getAbsolutePath()
                + " --privateKeyPath " + nonExistentFilePath));

        assertThat(command, instanceOf(Command.Exit.class));
        exitCommand = (Command.Exit) command;
        assertThat(exitCommand.status(), equalTo(ExitStatus.USAGE));
    }

    @Test
    public void testToolSimple() throws Exception {
        KeyPairGeneratorTool keyPairGeneratorTool = new KeyPairGeneratorTool();
        File tempFile1 = temporaryFolder.newFile();
        File tempFile2 = temporaryFolder.newFile();
        String publicKeyPath = tempFile1.getAbsolutePath();
        String privateKeyPath = tempFile2.getAbsolutePath();
        Path publicKeyFilePath = Paths.get(publicKeyPath);
        Path privateKeyFilePath = Paths.get(privateKeyPath);

        assertThat(tempFile1.delete(), equalTo(true));
        assertThat(tempFile2.delete(), equalTo(true));

        Command command = keyPairGeneratorTool.parse(KeyPairGeneratorTool.NAME, args("--privateKeyPath " + privateKeyPath
                + " --publicKeyPath " + publicKeyPath));

        assertThat(command, instanceOf(KeyGenerator.class));
        KeyGenerator keyGenerator = (KeyGenerator) command;
        assertThat(keyGenerator.privateKeyPath, equalTo(privateKeyFilePath));
        assertThat(keyGenerator.publicKeyPath, equalTo(publicKeyFilePath));

        assertThat(Files.exists(publicKeyFilePath), equalTo(false));
        assertThat(Files.exists(privateKeyFilePath), equalTo(false));

        assertThat(keyGenerator.execute(ImmutableSettings.EMPTY, new Environment(ImmutableSettings.EMPTY)), equalTo(ExitStatus.OK));
        assertThat(Files.exists(publicKeyFilePath), equalTo(true));
        assertThat(Files.exists(privateKeyFilePath), equalTo(true));

        Files.delete(publicKeyFilePath);
        Files.delete(privateKeyFilePath);
    }
}
