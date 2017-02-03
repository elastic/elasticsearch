/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;

@LuceneTestCase.SuppressFileSystems("*")
public class RemovePluginCommandTests extends ESTestCase {

    private Path home;
    private Environment env;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        home = createTempDir();
        Files.createDirectories(home.resolve("bin"));
        Files.createFile(home.resolve("bin").resolve("elasticsearch"));
        Files.createDirectories(home.resolve("plugins"));
        Settings settings = Settings.builder()
                .put("path.home", home)
                .build();
        env = new Environment(settings);
    }

    static MockTerminal removePlugin(String name, Path home) throws Exception {
        Environment env = new Environment(Settings.builder().put("path.home", home).build());
        MockTerminal terminal = new MockTerminal();
        new RemovePluginCommand().execute(terminal, name, env);
        return terminal;
    }

    static void assertRemoveCleaned(Environment env) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(env.pluginsFile())) {
            for (Path file : stream) {
                if (file.getFileName().toString().startsWith(".removing")) {
                    fail("Removal dir still exists, " + file);
                }
            }
        }
    }

    public void testMissing() throws Exception {
        UserException e = expectThrows(UserException.class, () -> removePlugin("dne", home));
        assertTrue(e.getMessage(), e.getMessage().contains("plugin dne not found"));
        assertRemoveCleaned(env);
    }

    public void testBasic() throws Exception {
        Files.createDirectory(env.pluginsFile().resolve("fake"));
        Files.createFile(env.pluginsFile().resolve("fake").resolve("plugin.jar"));
        Files.createDirectory(env.pluginsFile().resolve("fake").resolve("subdir"));
        Files.createDirectory(env.pluginsFile().resolve("other"));
        removePlugin("fake", home);
        assertFalse(Files.exists(env.pluginsFile().resolve("fake")));
        assertTrue(Files.exists(env.pluginsFile().resolve("other")));
        assertRemoveCleaned(env);
    }

    public void testBin() throws Exception {
        Files.createDirectories(env.pluginsFile().resolve("fake"));
        Path binDir = env.binFile().resolve("fake");
        Files.createDirectories(binDir);
        Files.createFile(binDir.resolve("somescript"));
        removePlugin("fake", home);
        assertFalse(Files.exists(env.pluginsFile().resolve("fake")));
        assertTrue(Files.exists(env.binFile().resolve("elasticsearch")));
        assertFalse(Files.exists(binDir));
        assertRemoveCleaned(env);
    }

    public void testBinNotDir() throws Exception {
        Files.createDirectories(env.pluginsFile().resolve("elasticsearch"));
        UserException e = expectThrows(UserException.class, () -> removePlugin("elasticsearch", home));
        assertTrue(e.getMessage(), e.getMessage().contains("not a directory"));
        assertTrue(Files.exists(env.pluginsFile().resolve("elasticsearch"))); // did not remove
        assertTrue(Files.exists(env.binFile().resolve("elasticsearch")));
        assertRemoveCleaned(env);
    }

    public void testConfigDirPreserved() throws Exception {
        Files.createDirectories(env.pluginsFile().resolve("fake"));
        final Path configDir = env.configFile().resolve("fake");
        Files.createDirectories(configDir);
        Files.createFile(configDir.resolve("fake.yml"));
        final MockTerminal terminal = removePlugin("fake", home);
        assertTrue(Files.exists(env.configFile().resolve("fake")));
        assertThat(terminal.getOutput(), containsString(expectedConfigDirPreservedMessage(configDir)));
        assertRemoveCleaned(env);
    }

    public void testNoConfigDirPreserved() throws Exception {
        Files.createDirectories(env.pluginsFile().resolve("fake"));
        final Path configDir = env.configFile().resolve("fake");
        final MockTerminal terminal = removePlugin("fake", home);
        assertThat(terminal.getOutput(), not(containsString(expectedConfigDirPreservedMessage(configDir))));
    }

    public void testRemoveUninstalledPluginErrors() throws Exception {
        UserException e = expectThrows(UserException.class, () -> removePlugin("fake", home));
        assertEquals(ExitCodes.CONFIG, e.exitCode);
        assertEquals("plugin fake not found; run 'elasticsearch-plugin list' to get list of installed plugins", e.getMessage());

        MockTerminal terminal = new MockTerminal();
        new RemovePluginCommand() {
            @Override
            protected boolean addShutdownHook() {
                return false;
            }
        }.main(new String[] { "-Epath.home=" + home, "fake" }, terminal);
        try (BufferedReader reader = new BufferedReader(new StringReader(terminal.getOutput()))) {
            assertEquals("-> Removing fake...", reader.readLine());
            assertEquals("ERROR: plugin fake not found; run 'elasticsearch-plugin list' to get list of installed plugins",
                    reader.readLine());
            assertNull(reader.readLine());
        }
    }

    public void testMissingPluginName() throws Exception {
        UserException e = expectThrows(UserException.class, () -> removePlugin(null, home));
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertEquals("plugin name is required", e.getMessage());
    }

    private String expectedConfigDirPreservedMessage(final Path configDir) {
        return "-> Preserving plugin config files [" + configDir + "] in case of upgrade, delete manually if not needed";
    }

}

