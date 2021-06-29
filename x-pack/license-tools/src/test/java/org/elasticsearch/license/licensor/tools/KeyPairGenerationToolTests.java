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

import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.containsString;

public class KeyPairGenerationToolTests extends CommandTestCase {

    @Override
    protected Command newCommand() {
        return new KeyPairGeneratorTool();
    }

    public void testMissingKeyPaths() throws Exception {
        Path exists = createTempFile("", "existing");
        Path dne = createTempDir().resolve("dne");
        UserException e = expectThrows(
                UserException.class,
                () -> execute(
                        "--publicKeyPath",
                        exists.toString(),
                        "--privateKeyPath",
                        dne.toString()));
        assertThat(e.getMessage(), containsString("existing"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
        e = expectThrows(
                UserException.class,
                () -> execute(
                        "--publicKeyPath",
                        dne.toString(),
                        "--privateKeyPath",
                        exists.toString()));
        assertThat(e.getMessage(), containsString("existing"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
    }

    public void testTool() throws Exception {
        Path keysDir = createTempDir();
        Path publicKeyFilePath = keysDir.resolve("public");
        Path privateKeyFilePath = keysDir.resolve("private");

        execute(
                "--publicKeyPath",
                publicKeyFilePath.toString(),
                "--privateKeyPath",
                privateKeyFilePath.toString());
        assertTrue(publicKeyFilePath.toString(), Files.exists(publicKeyFilePath));
        assertTrue(privateKeyFilePath.toString(), Files.exists(privateKeyFilePath));
    }

}
