/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

@LuceneTestCase.SuppressFileSystems("*")
public class RemoveXPackExtensionCommandTests extends ESTestCase {

    /** Creates a test environment with bin, config and plugins directories. */
    static Environment createEnv() throws IOException {
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

    static MockTerminal removeExtension(String name, Environment env) throws Exception {
        MockTerminal terminal = new MockTerminal();
        new RemoveXPackExtensionCommand(env).execute(terminal, name);
        return terminal;
    }

    static void assertRemoveCleaned(Path extDir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(extDir)) {
            for (Path file : stream) {
                if (file.getFileName().toString().startsWith(".removing")) {
                    fail("Removal dir still exists, " + file);
                }
            }
        }
    }

    public void testMissing() throws Exception {
        Environment env = createEnv();
        Path extDir = createExtensionDir(env);
        UserError e = expectThrows(UserError.class, () -> {
           removeExtension("dne", env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Extension dne not found"));
        assertRemoveCleaned(extDir);
    }

    public void testBasic() throws Exception {
        Environment env = createEnv();
        Path extDir = createExtensionDir(env);
        Files.createDirectory(extDir.resolve("fake"));
        Files.createFile(extDir.resolve("fake").resolve("extension.jar"));
        Files.createDirectory(extDir.resolve("fake").resolve("subdir"));
        Files.createDirectory(extDir.resolve("other"));
        removeExtension("fake", env);
        assertFalse(Files.exists(extDir.resolve("fake")));
        assertTrue(Files.exists(extDir.resolve("other")));
        assertRemoveCleaned(extDir);
    }
}
