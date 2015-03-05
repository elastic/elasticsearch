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

import com.google.common.collect.Lists;
import org.elasticsearch.common.cli.CliTool.ExitStatus;
import org.elasticsearch.common.cli.CliToolTestCase.CaptureOutputTerminal;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.repository.PluginDescriptor;
import org.elasticsearch.plugins.repository.PluginRepositoryException;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.plugins.AbstractPluginsTests.ZippedFileBuilder.createZippedFile;
import static org.elasticsearch.plugins.repository.PluginDescriptor.Builder.pluginDescriptor;
import static org.hamcrest.Matchers.*;

public class PluginManagerTests extends AbstractPluginsTests {

    /**
     * Main method to test a plugin with the PluginManager.
     *
     * It basically installs the plugin, checks the files, lists
     * all installed plugins and search for the new one, then removes the plugin and finally
     * checks that files are deleted.
     *
     * @param fqn   the fully qualified name of the plugin
     * @param zip   the zip file of the plugin to test
     * @param files the expected files of the plugin
     * @param env the environment
     */
    protected void testPlugin(String fqn, Path zip, Collection<String> files, Environment env) throws Exception {
        logger.info("-> Testing plugin '{}'", fqn);
        assertNotNull(fqn);

        // Creates the plugin descriptor corresponding to the plugin to test
        final PluginDescriptor plugin = pluginDescriptor(fqn).build();
        assertNotNull(plugin);

        // Installs the plugin
        CaptureOutputTerminal terminal = new CaptureOutputTerminal();
        ExitStatus status = new PluginManager(terminal)
                .parse("install", new String[]{fqn, "--url", zip.toUri().toString()})
                .execute(ImmutableSettings.EMPTY, env);

        assertThat(status, equalTo(ExitStatus.OK));
        assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Installing plugin [" + plugin.name() + "]...")));
        assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Plugin installed successfully!")));

        // Print all installed files
        if (logger.isDebugEnabled()) {
            logger.debug("-> List of installed files:");
            Files.walkFileTree(env.homeFile(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    logger.debug("dir : {}", dir);
                    return super.preVisitDirectory(dir, attrs);
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    logger.debug("file: {}", file);
                    return super.visitFile(file, attrs);
                }
            });
        }

        // Checks that files have been copied
        if (files != null) {
            for (String file : files) {
                Path f = env.homeFile().resolve(file);
                assertTrue("File must exist: " + f, Files.exists(f));

                if (file.startsWith("bin/" + plugin.name())) {
                    // check that the file is marked executable, without actually checking that we can execute it.
                    PosixFileAttributeView view = Files.getFileAttributeView(f, PosixFileAttributeView.class);
                    // the view might be null, on e.g. windows, there is nothing to check there!
                    if (view != null) {
                        PosixFileAttributes attributes = view.readAttributes();
                        assertTrue("unexpected permissions: " + attributes.permissions(),
                                attributes.permissions().contains(PosixFilePermission.OWNER_EXECUTE));
                    }
                }
            }
        }

        // List all plugin and verify that the plugin is present
        terminal = new CaptureOutputTerminal();
        status = new PluginManager.List(terminal).execute(ImmutableSettings.EMPTY, env);

        assertThat(status, equalTo(ExitStatus.OK));
        assertFalse(terminal.getTerminalOutput().isEmpty());
        if (plugin.version() != null) {
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith(plugin.name() + " (" + plugin.version() + ")")));
        } else {
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith(plugin.name() + " (no version)")));
        }

        // Removes the plugin
        terminal = new CaptureOutputTerminal();
        status = new PluginManager(terminal)
                .parse("uninstall", new String[]{plugin.name(), "-v"})
                .execute(ImmutableSettings.EMPTY, env);

        assertThat(terminal.verbosity(), equalTo(Terminal.Verbosity.VERBOSE));
        assertThat(status, equalTo(ExitStatus.OK));

        assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Uninstalling plugin [" + plugin.name() + "]...")));
        assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Found " + plugin.name())));
        assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Removed " + plugin.name())));

        // List all plugin and checks if the plugin is not found
        terminal = new CaptureOutputTerminal();
        status = new PluginManager.List(terminal).execute(ImmutableSettings.EMPTY, env);

        assertThat(status, equalTo(ExitStatus.OK));
        assertTrue(terminal.getTerminalOutput().isEmpty());

        // Check that files are correctly deleted
        if (files != null) {
            for (String file : files) {
                // Config files are never removed
                if (file.startsWith("config")) {
                    assertTrue("Files must exist: " + file, Files.exists(env.homeFile().resolve(file)));
                } else {
                    assertFalse("File must not exist: " + file, Files.exists(env.homeFile().resolve(file)));
                }
            }
        }

        logger.info("-> Plugin '{}' tested with success", plugin.name());
    }

    @Test
    public void testPluginUnavailable() throws Exception {
        try (InMemoryEnvironment env = new InMemoryEnvironment()) {
            CaptureOutputTerminal terminal = new CaptureOutputTerminal();
            ExitStatus status = new PluginManager(terminal)
                    .parse("install", new String[]{"plugin-unavailable", "--url", newTempFilePath().toString()})
                    .execute(ImmutableSettings.EMPTY, env);

            assertThat(status, equalTo(ExitStatus.UNAVAILABLE));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Installing plugin [plugin-unavailable]...")));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("ERROR: Download failed")));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("ERROR: Failed to resolve plugin")));
        }
    }



    /**
     * Test a plugin with the following zipped content :
     * <p/>
     * |
     * +-- /plugin-with-sourcefiles/
     *       +-- src/main/java/org/elasticsearch/plugin/foo/bar/FooBar.java
     *       +-- bin/
     *            +-- cmd.sh
     */
    @Test
    public void testPluginWithSourceFilesAndConfig() throws Exception {
        Path zip = createZippedFile(newTempDirPath(), "plugin_with_sourcefiles_and_bin.zip")
                .addDir("plugin-with-sourcefiles-and-bin/bin/")
                .addFile("plugin-with-sourcefiles-and-bin/bin/cmd.sh", "Command file")
                .addDir("plugin-with-sourcefiles-and-bin/src/main/java/org/elasticsearch/plugin/foo/bar/")
                .addFile("plugin-with-sourcefiles-and-bin/src/main/java/org/elasticsearch/plugin/foo/bar/FooBar.java", "package foo.bar")
                .toPath();

        try (InMemoryEnvironment env = new InMemoryEnvironment()) {

            // Installs the plugin
            CaptureOutputTerminal terminal = new CaptureOutputTerminal();
            ExitStatus status = new PluginManager(terminal)
                    .parse("install", new String[]{"plugin-with-sourcefiles-and-bin", "--url", zip.toUri().toString()})
                    .execute(ImmutableSettings.EMPTY, env);

            assertThat(status, equalTo(ExitStatus.CANT_CREATE));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Installing plugin [plugin-with-sourcefiles-and-bin]...")));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("ERROR: [local] Error when installing the plugin 'plugin-with-sourcefiles-and-bin'")));

            // Checks that the plugin has been removed
            assertNotNull(env);
            assertFalse(Files.exists(env.pluginsFile().resolve("plugin-with-sourcefiles-and-bin")));
            assertFalse(Files.exists(env.homeFile().resolve("bin").resolve("plugin-with-sourcefiles-and-bin")));
        }
    }

    /**
     * Test a plugin with the following zipped content :
     * <p/>
     * |
     * +-- /plugin-with-sourcefiles/src/main/java/org/elasticsearch/plugin/foo/bar/FooBar.java
     */
    @Test
    public void testPluginWithSourceFiles() throws Exception {
        Path zip = createZippedFile(newTempDirPath(), "plugin_with_sourcefiles.zip")
                .addDir("plugin-with-sourcefiles/src/main/java/org/elasticsearch/plugin/foo/bar/")
                .addFile("plugin-with-sourcefiles/src/main/java/org/elasticsearch/plugin/foo/bar/FooBar.java", "package foo.bar")
                .toPath();

        try (InMemoryEnvironment env = new InMemoryEnvironment()) {

            // Installs the plugin
            CaptureOutputTerminal terminal = new CaptureOutputTerminal();
            ExitStatus status = new PluginManager(terminal)
                    .parse("install", new String[]{"plugin-with-sourcefiles", "--url", zip.toUri().toString()})
                    .execute(ImmutableSettings.EMPTY, env);

            assertThat(status, equalTo(ExitStatus.CANT_CREATE));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Installing plugin [plugin-with-sourcefiles]...")));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("ERROR: [local] Error when installing the plugin 'plugin-with-sourcefiles'")));

            // Checks that the plugin has been removed
            assertNotNull(env);
            assertFalse(Files.exists(env.pluginsFile().resolve("plugin-with-sourcefiles")));
            assertFalse(Files.exists(env.homeFile().resolve("bin").resolve("plugin-with-sourcefiles")));
        }
    }


    /**
     * Test that configuration files are not remove and not overridden (see issue #7890):
     */
    @Test
    public void testPluginWithExistingConfigurations() throws Exception {
        try (InMemoryEnvironment env = new InMemoryEnvironment()) {

            Path tmp = newTempDirPath();

            //
            // First install, the plugin contains:
            //
            // +-- config/
            //      +-- config.yml
            //
            Path zip = createZippedFile(tmp, "plugin_with_config_v1.zip")
                    .addDir("config/")
                    .addFile("config/config.yml", "First version of the config file")
                    .toPath();

            List<String> files = Lists.newArrayList("config/plugin-with-existing-config/config.yml");

            testPlugin("plugin-with-existing-config", zip, files, env);

            Path configDir = env.configFile().resolve("plugin-with-existing-config");
            assertFileExist(configDir, "config.yml");

            //
            // Second install, the plugin contains:
            //
            // +-- config/
            //      |-- config.yml (version 2)
            //      |-- users/
            //           |-- users.yml
            //           +-- roles/
            //                +-- roles.yml
            zip = createZippedFile(tmp, "plugin_with_config_v2.zip")
                    .addDir("config/")
                    .addFile("config/config.yml", "Second version of the config file")
                    .addDir("config/users/")
                    .addFile("config/users/users.yml", "Users config file")
                    .addDir("config/users/roles/")
                    .addFile("config/users/roles/roles.yml", "Roles config file")
                    .toPath();

            files = Lists.newArrayList("config/plugin-with-existing-config/config.yml",
                    "config/plugin-with-existing-config/users/users.yml",
                    "config/plugin-with-existing-config/users/roles/roles.yml");

            testPlugin("plugin-with-existing-config", zip, files, env);

            assertFileExist(configDir, "config.yml");
            assertFileExist(configDir, "config.yml.new");
            assertFileExist(configDir, "users/users.yml");
            assertFileExist(configDir, "users/roles/roles.yml");

            assertFileContains(configDir.resolve("config.yml"), "First version of the config file");
            assertFileContains(configDir.resolve("config.yml.new"), "Second version of the config file");
            assertFileContains(configDir.resolve("users/users.yml"), "Users config file");
            assertFileContains(configDir.resolve("users/roles/roles.yml"), "Roles config file");

            //
            // Third install, the plugin contains:
            //
            // +-- config/
            //      |-- config.yml (version 3)
            //      |-- other.yml (version 1)
            //      |-- users/
            //           |-- users.yml (version 2)
            //           |-- groups.yml (version 1)
            //           +-- roles/
            //                +-- roles.yml (version 2)
            //                +-- admin.yml (version 1)
            zip = createZippedFile(tmp, "plugin_with_config_v3.zip")
                    .addDir("config/")
                    .addFile("config/config.yml", "Third version of the config file")
                    .addFile("config/other.yml", "First version of the other file")
                    .addDir("config/users/")
                    .addFile("config/users/users.yml", "Users config file (version 2)")
                    .addFile("config/users/groups.yml", "Groups config file (version 1)")
                    .addDir("config/users/roles/")
                    .addFile("config/users/roles/roles.yml", "Roles config file (version 2)")
                    .addFile("config/users/roles/admin.yml", "Admin config file (version 1)")
                    .toPath();

            files = Lists.newArrayList("config/plugin-with-existing-config/config.yml",
                    "config/plugin-with-existing-config/other.yml",
                    "config/plugin-with-existing-config/users/users.yml",
                    "config/plugin-with-existing-config/users/groups.yml",
                    "config/plugin-with-existing-config/users/roles/roles.yml",
                    "config/plugin-with-existing-config/users/roles/admin.yml");

            testPlugin("plugin-with-existing-config", zip, files, env);

            assertFileExist(configDir, "config.yml");
            assertFileExist(configDir, "config.yml.new");
            assertFileExist(configDir, "other.yml");
            assertFileExist(configDir, "users/users.yml");
            assertFileExist(configDir, "users/users.yml.new");
            assertFileExist(configDir, "users/roles/roles.yml");
            assertFileExist(configDir, "users/roles/roles.yml.new");
            assertFileExist(configDir, "users/roles/admin.yml");

            assertFileContains(configDir.resolve("config.yml"), "First version of the config file");
            assertFileContains(configDir.resolve("config.yml.new"), "Third version of the config file");
            assertFileContains(configDir.resolve("other.yml"), "First version of the other file");
            assertFileContains(configDir.resolve("users/users.yml"), "Users config file");
            assertFileContains(configDir.resolve("users/users.yml.new"), "Users config file (version 2)");
            assertFileContains(configDir.resolve("users/roles/roles.yml"), "Roles config file");
            assertFileContains(configDir.resolve("users/roles/roles.yml.new"), "Roles config file (version 2)");
            assertFileContains(configDir.resolve("users/roles/admin.yml"), "Admin config file (version 1)");
        }
    }

}
