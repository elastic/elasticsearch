/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import java.nio.file.Files;
import java.nio.file.Path;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.containsString;

public class KeyPairGenerationToolTests extends ESTestCase {

    public void testMissingKeyPaths() throws Exception {
        KeyPairGeneratorTool keyPairGeneratorTool = new KeyPairGeneratorTool();
        Path exists = createTempFile();
        Path dne = createTempDir().resolve("dne");
        UserError e = expectThrows(UserError.class, () -> {
            keyPairGeneratorTool.execute(Terminal.DEFAULT, exists, dne);
        });
        assertThat(e.getMessage(), containsString("pub"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
        e = expectThrows(UserError.class, () -> {
            keyPairGeneratorTool.execute(Terminal.DEFAULT, dne, exists);
        });
        assertThat(e.getMessage(), containsString("pri"));
        assertEquals(ExitCodes.USAGE, e.exitCode);
    }

    public void testTool() throws Exception {
        KeyPairGeneratorTool keyPairGeneratorTool = new KeyPairGeneratorTool();
        Path keysDir = createTempDir();
        Path publicKeyFilePath = keysDir.resolve("public");
        Path privateKeyFilePath = keysDir.resolve("private");

        keyPairGeneratorTool.execute(Terminal.DEFAULT, publicKeyFilePath, privateKeyFilePath);
        assertTrue(publicKeyFilePath.toString(), Files.exists(publicKeyFilePath));
        assertTrue(privateKeyFilePath.toString(), Files.exists(privateKeyFilePath));
    }
}
