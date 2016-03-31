/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@LuceneTestCase.SuppressFileSystems("*")
public class ListXPackExtensionCommandTests extends ESTestCase {

    Environment createEnv() throws IOException {
        Path home = createTempDir();
        Settings settings = Settings.builder()
            .put("path.home", home)
            .build();
        return new Environment(settings);
    }

    Path createExtensionDir(Environment env) throws IOException {
        Path path = env.pluginsFile().resolve("x-pack").resolve("extensions");
        return Files.createDirectories(path);
    }

    static MockTerminal listExtensions(Environment env) throws Exception {
        MockTerminal terminal = new MockTerminal();
        String[] args = {};
        int status = new ListXPackExtensionCommand(env).main(args, terminal);
        assertEquals(ExitCodes.OK, status);
        return terminal;
    }

    public void testExtensionsDirMissing() throws Exception {
        Environment env = createEnv();
        Path extDir = createExtensionDir(env);
        Files.delete(extDir);
        IOException e = expectThrows(IOException.class, () -> {
           listExtensions(env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Extensions directory missing"));
    }

    public void testNoExtensions() throws Exception {
        Environment env = createEnv();
        createExtensionDir(env);
        MockTerminal terminal = listExtensions(env);
        assertTrue(terminal.getOutput(), terminal.getOutput().isEmpty());
    }

    public void testOneExtension() throws Exception {
        Environment env = createEnv();
        Path extDir = createExtensionDir(env);
        Files.createDirectory(extDir.resolve("fake"));
        MockTerminal terminal = listExtensions(env);
        assertTrue(terminal.getOutput(), terminal.getOutput().contains("fake"));
    }

    public void testTwoExtensions() throws Exception {
        Environment env = createEnv();
        Path extDir = createExtensionDir(env);
        Files.createDirectory(extDir.resolve("fake1"));
        Files.createDirectory(extDir.resolve("fake2"));
        MockTerminal terminal = listExtensions(env);
        String output = terminal.getOutput();
        assertTrue(output, output.contains("fake1"));
        assertTrue(output, output.contains("fake2"));
    }
}
