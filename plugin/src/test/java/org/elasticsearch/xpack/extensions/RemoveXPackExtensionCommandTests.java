/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

@LuceneTestCase.SuppressFileSystems("*")
public class RemoveXPackExtensionCommandTests extends ESTestCase {

    private Path home;
    private Environment env;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        home = createTempDir();
        env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", home.toString()).build());
    }

    Path createExtensionDir(Environment env) throws IOException {
        Path path = env.pluginsFile().resolve("x-pack").resolve("extensions");
        return Files.createDirectories(path);
    }

    static MockTerminal removeExtension(String name, Path home) throws Exception {
        Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", home).build());
        MockTerminal terminal = new MockTerminal();
        new RemoveXPackExtensionCommand().execute(terminal, name, env);
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
        Path extDir = createExtensionDir(env);
        UserException e = expectThrows(UserException.class, () -> removeExtension("dne", home));
        assertTrue(e.getMessage(), e.getMessage().contains("Extension dne not found"));
        assertRemoveCleaned(extDir);
    }

    public void testBasic() throws Exception {
        Path extDir = createExtensionDir(env);
        Files.createDirectory(extDir.resolve("fake"));
        Files.createFile(extDir.resolve("fake").resolve("extension.jar"));
        Files.createDirectory(extDir.resolve("fake").resolve("subdir"));
        Files.createDirectory(extDir.resolve("other"));
        removeExtension("fake", home);
        assertFalse(Files.exists(extDir.resolve("fake")));
        assertTrue(Files.exists(extDir.resolve("other")));
        assertRemoveCleaned(extDir);
    }

}
