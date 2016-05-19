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
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@LuceneTestCase.SuppressFileSystems("*")
public class ListXPackExtensionCommandTests extends ESTestCase {

    private Path home;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        home = createTempDir();
    }

    private static Path createExtensionDir(final Path home) throws IOException {
        final Environment env = new Environment(Settings.builder().put("path.home", home.toString()).build());
        final Path path = env.pluginsFile().resolve("x-pack").resolve("extensions");
        return Files.createDirectories(path);
    }

    static MockTerminal listExtensions(Path home) throws Exception {
        MockTerminal terminal = new MockTerminal();
        int status = new ListXPackExtensionCommand().main(new String[] { "-Epath.home=" + home }, terminal);
        assertEquals(ExitCodes.OK, status);
        return terminal;
    }

    public void testExtensionsDirMissing() throws Exception {
        Path extDir = createExtensionDir(home);
        Files.delete(extDir);
        IOException e = expectThrows(IOException.class, () -> listExtensions(home));
        assertTrue(e.getMessage(), e.getMessage().contains("Extensions directory missing"));
    }

    public void testNoExtensions() throws Exception {
        createExtensionDir(home);
        MockTerminal terminal = listExtensions(home);
        assertTrue(terminal.getOutput(), terminal.getOutput().isEmpty());
    }

    public void testOneExtension() throws Exception {
        Path extDir = createExtensionDir(home);
        Files.createDirectory(extDir.resolve("fake"));
        MockTerminal terminal = listExtensions(home);
        assertTrue(terminal.getOutput(), terminal.getOutput().contains("fake"));
    }

    public void testTwoExtensions() throws Exception {
        Path extDir = createExtensionDir(home);
        Files.createDirectory(extDir.resolve("fake1"));
        Files.createDirectory(extDir.resolve("fake2"));
        MockTerminal terminal = listExtensions(home);
        String output = terminal.getOutput();
        assertTrue(output, output.contains("fake1"));
        assertTrue(output, output.contains("fake2"));
    }

}
