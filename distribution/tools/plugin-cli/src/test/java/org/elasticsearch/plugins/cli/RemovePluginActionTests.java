/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.cli;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.plugins.PluginTestUtil;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@LuceneTestCase.SuppressFileSystems("*")
public class RemovePluginActionTests extends ESTestCase {

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
        Settings settings = Settings.builder().put("path.home", home).build();
        env = TestEnvironment.newEnvironment(settings);
    }

    void createPlugin(String name) throws IOException {
        createPlugin(env.pluginsDir(), name, Version.CURRENT);
    }

    void createPlugin(String name, Version version) throws IOException {
        createPlugin(env.pluginsDir(), name, version);
    }

    void createPlugin(Path path, String name, Version version) throws IOException {
        PluginTestUtil.writePluginProperties(
            path.resolve(name),
            "description",
            "dummy",
            "name",
            name,
            "version",
            "1.0",
            "elasticsearch.version",
            version.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "SomeClass"
        );
    }

    static MockTerminal removePlugin(String pluginId, Path home, boolean purge) throws Exception {
        return removePlugin(List.of(pluginId), home, purge);
    }

    static MockTerminal removePlugin(List<String> pluginIds, Path home, boolean purge) throws Exception {
        Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", home).build());
        MockTerminal terminal = MockTerminal.create();
        final List<InstallablePlugin> plugins = pluginIds == null
            ? null
            : pluginIds.stream().map(InstallablePlugin::new).collect(Collectors.toList());
        new RemovePluginAction(terminal, env, purge).execute(plugins);
        return terminal;
    }

    static void assertRemoveCleaned(Environment env) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(env.pluginsDir())) {
            for (Path file : stream) {
                if (file.getFileName().toString().startsWith(".removing")) {
                    fail("Removal dir still exists, " + file);
                }
            }
        }
    }

    public void testMissing() throws Exception {
        UserException e = expectThrows(UserException.class, () -> removePlugin("dne", home, randomBoolean()));
        assertThat(e.getMessage(), containsString("plugin [dne] not found"));
        assertRemoveCleaned(env);
    }

    public void testBasic() throws Exception {
        createPlugin("fake");
        Files.createFile(env.pluginsDir().resolve("fake").resolve("plugin.jar"));
        Files.createDirectory(env.pluginsDir().resolve("fake").resolve("subdir"));
        createPlugin("other");
        removePlugin("fake", home, randomBoolean());
        assertFalse(Files.exists(env.pluginsDir().resolve("fake")));
        assertTrue(Files.exists(env.pluginsDir().resolve("other")));
        assertRemoveCleaned(env);
    }

    /** Check that multiple plugins can be removed at the same time. */
    public void testRemoveMultiple() throws Exception {
        createPlugin("fake");
        Files.createFile(env.pluginsDir().resolve("fake").resolve("plugin.jar"));
        Files.createDirectory(env.pluginsDir().resolve("fake").resolve("subdir"));

        createPlugin("other");
        Files.createFile(env.pluginsDir().resolve("other").resolve("plugin.jar"));
        Files.createDirectory(env.pluginsDir().resolve("other").resolve("subdir"));

        removePlugin("fake", home, randomBoolean());
        removePlugin("other", home, randomBoolean());
        assertFalse(Files.exists(env.pluginsDir().resolve("fake")));
        assertFalse(Files.exists(env.pluginsDir().resolve("other")));
        assertRemoveCleaned(env);
    }

    public void testBin() throws Exception {
        createPlugin("fake");
        Path binDir = env.binDir().resolve("fake");
        Files.createDirectories(binDir);
        Files.createFile(binDir.resolve("somescript"));
        removePlugin("fake", home, randomBoolean());
        assertFalse(Files.exists(env.pluginsDir().resolve("fake")));
        assertTrue(Files.exists(env.binDir().resolve("elasticsearch")));
        assertFalse(Files.exists(binDir));
        assertRemoveCleaned(env);
    }

    public void testBinNotDir() throws Exception {
        createPlugin("fake");
        Files.createFile(env.binDir().resolve("fake"));
        UserException e = expectThrows(UserException.class, () -> removePlugin("fake", home, randomBoolean()));
        assertThat(e.getMessage(), containsString("not a directory"));
        assertTrue(Files.exists(env.pluginsDir().resolve("fake"))); // did not remove
        assertTrue(Files.exists(env.binDir().resolve("fake")));
        assertRemoveCleaned(env);
    }

    public void testConfigDirPreserved() throws Exception {
        createPlugin("fake");
        final Path configDir = env.configDir().resolve("fake");
        Files.createDirectories(configDir);
        Files.createFile(configDir.resolve("fake.yml"));
        final MockTerminal terminal = removePlugin("fake", home, false);
        assertTrue(Files.exists(env.configDir().resolve("fake")));
        assertThat(terminal.getOutput(), containsString(expectedConfigDirPreservedMessage(configDir)));
        assertRemoveCleaned(env);
    }

    public void testPurgePluginExists() throws Exception {
        createPlugin("fake");
        final Path configDir = env.configDir().resolve("fake");
        if (randomBoolean()) {
            Files.createDirectories(configDir);
            Files.createFile(configDir.resolve("fake.yml"));
        }
        final MockTerminal terminal = removePlugin("fake", home, true);
        assertFalse(Files.exists(env.configDir().resolve("fake")));
        assertThat(terminal.getOutput(), not(containsString(expectedConfigDirPreservedMessage(configDir))));
        assertRemoveCleaned(env);
    }

    public void testPurgePluginDoesNotExist() throws Exception {
        final Path configDir = env.configDir().resolve("fake");
        Files.createDirectories(configDir);
        Files.createFile(configDir.resolve("fake.yml"));
        final MockTerminal terminal = removePlugin("fake", home, true);
        assertFalse(Files.exists(env.configDir().resolve("fake")));
        assertThat(terminal.getOutput(), not(containsString(expectedConfigDirPreservedMessage(configDir))));
        assertRemoveCleaned(env);
    }

    public void testPurgeNothingExists() throws Exception {
        final UserException e = expectThrows(UserException.class, () -> removePlugin("fake", home, true));
        assertThat(e.getMessage(), containsString("plugin [fake] not found"));
    }

    public void testPurgeOnlyMarkerFileExists() throws Exception {
        final Path configDir = env.configDir().resolve("fake");
        final Path removing = env.pluginsDir().resolve(".removing-fake");
        Files.createFile(removing);
        final MockTerminal terminal = removePlugin("fake", home, randomBoolean());
        assertFalse(Files.exists(removing));
        assertThat(terminal.getOutput(), not(containsString(expectedConfigDirPreservedMessage(configDir))));
    }

    public void testNoConfigDirPreserved() throws Exception {
        createPlugin("fake");
        final Path configDir = env.configDir().resolve("fake");
        final MockTerminal terminal = removePlugin("fake", home, randomBoolean());
        assertThat(terminal.getOutput(), not(containsString(expectedConfigDirPreservedMessage(configDir))));
    }

    public void testRemoveUninstalledPluginErrors() throws Exception {
        UserException e = expectThrows(UserException.class, () -> removePlugin("fake", home, randomBoolean()));
        assertEquals(ExitCodes.CONFIG, e.exitCode);
        assertThat(e.getMessage(), containsString("plugin [fake] not found"));

        MockTerminal terminal = MockTerminal.create();

        new MockRemovePluginCommand(env) {
        }.main(new String[] { "-Epath.home=" + home, "fake" }, terminal, new ProcessInfo(Map.of(), Map.of(), createTempDir()));
        try (
            BufferedReader reader = new BufferedReader(new StringReader(terminal.getOutput()));
            BufferedReader errorReader = new BufferedReader(new StringReader(terminal.getErrorOutput()))
        ) {
            assertThat(errorReader.readLine(), equalTo(""));
            assertThat(errorReader.readLine(), containsString("plugin [fake] not found"));
            assertThat(reader.readLine(), nullValue());
            assertThat(errorReader.readLine(), nullValue());
        }
    }

    public void testMissingPluginName() {
        UserException e = expectThrows(UserException.class, () -> removePlugin((List<String>) null, home, randomBoolean()));
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertEquals("At least one plugin ID is required", e.getMessage());

        e = expectThrows(UserException.class, () -> removePlugin(emptyList(), home, randomBoolean()));
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), equalTo("At least one plugin ID is required"));
    }

    public void testRemoveWhenRemovingMarker() throws Exception {
        createPlugin("fake");
        Files.createFile(env.pluginsDir().resolve("fake").resolve("plugin.jar"));
        Files.createFile(env.pluginsDir().resolve(".removing-fake"));
        removePlugin("fake", home, randomBoolean());
    }

    /**
     * Check that if a plugin exists that has since been migrated to a module, then it is still possible
     * to remove that plugin.
     */
    public void testRemoveMigratedPluginsWhenInstalled() throws Exception {
        for (String id : List.of("repository-azure", "repository-gcs", "repository-s3")) {
            createPlugin(id);
            Files.createFile(env.pluginsDir().resolve(id).resolve("plugin.jar"));
            final MockTerminal terminal = removePlugin(id, home, randomBoolean());

            assertThat(Files.exists(env.pluginsDir().resolve(id)), is(false));
            // This message shouldn't be printed if plugin was actually installed.
            assertThat(terminal.getErrorOutput(), not(containsString("plugin [" + id + "] is no longer a plugin")));
        }
    }

    /**
     * Check that if we attempt to remove a plugin that has been migrated to a module, and that plugin is
     * not actually installed, then we print an appropriate message and exit with a success code.
     */
    public void testRemoveMigratedPluginsWhenNotInstalled() throws Exception {
        for (String id : List.of("repository-azure", "repository-gcs", "repository-s3")) {
            final MockTerminal terminal = removePlugin(id, home, randomBoolean());
            assertThat(terminal.getErrorOutput(), containsString("plugin [" + id + "] is no longer a plugin"));
        }
    }

    /**
     * Check that when removing (1) a regular, installed plugin and (2) an uninstalled plugin that has been migrated
     * to a module, then the overall removal succeeds, and a message is printed about the migrated pluging.
     */
    public void testRemoveRegularInstalledPluginAndMigratedUninstalledPlugin() throws Exception {
        createPlugin("fake");
        Files.createFile(env.pluginsDir().resolve("fake").resolve("plugin.jar"));

        final MockTerminal terminal = removePlugin(List.of("fake", "repository-s3"), home, randomBoolean());

        assertThat(Files.exists(env.pluginsDir().resolve("fake")), is(false));
        assertThat(terminal.getErrorOutput(), containsString("plugin [repository-s3] is no longer a plugin"));
    }

    private String expectedConfigDirPreservedMessage(final Path configDir) {
        return "-> preserving plugin config files [" + configDir + "] in case of upgrade; use --purge if not needed";
    }

}
