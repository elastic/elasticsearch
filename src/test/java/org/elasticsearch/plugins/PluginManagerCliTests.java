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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliTool.ExitStatus;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.AbstractPluginsTests.InMemoryEnvironment;
import org.elasticsearch.plugins.AbstractPluginsTests.ZippedFileBuilder;
import org.junit.Test;

import java.nio.file.Files;

import static org.elasticsearch.plugins.AbstractPluginsTests.PluginStructureBuilder.createPluginStructure;
import static org.elasticsearch.plugins.AbstractPluginsTests.ZippedFileBuilder.createZippedFile;
import static org.hamcrest.Matchers.*;

public class PluginManagerCliTests extends CliToolTestCase {

    private CaptureOutputTerminal terminal = new CaptureOutputTerminal();
    private CliTool tool = new PluginManager(terminal);

    @Test
    public void testListOptions() throws Exception {
        assertCommand("list with no options", PluginManager.List.class, ExitStatus.OK, "list");
        assertCommand("list with verbose", PluginManager.List.class, ExitStatus.OK, "list", "-v");
        assertCommand("list with help (shortcut option)", CliTool.Command.Help.class, ExitStatus.OK, "list", "-h");
        assertCommand("list with help (long option)", CliTool.Command.Help.class, ExitStatus.OK, "list", "--help");

        try {
            assertCommand("list with unknown option", null, null, "list", "--unknown");
            fail("should have thrown a UnrecognizedOptionException");
        } catch (UnrecognizedOptionException e) {
            // Ok
        }

        try {
            assertCommand("list with verbose and silent options", null, null, "list", randomFrom("-v", "--verbose"), randomFrom("-s", "--silent"));
            fail("should have thrown a AlreadySelectedException");
        } catch (AlreadySelectedException e) {
            // Ok
        }
    }

    @Test
    public void testListCommandWithNoInstalledPlugins() throws Exception {
        try (InMemoryEnvironment env = new InMemoryEnvironment()) {
            ExitStatus status = new PluginManager.List(terminal).execute(ImmutableSettings.EMPTY, env);
            assertThat(terminal.getTerminalOutput(), hasSize(0));
            assertThat(status, equalTo(ExitStatus.OK));
        }
    }

    @Test
    public void testListCommandWithInstalledPlugins() throws Exception {
        try (InMemoryEnvironment env = new InMemoryEnvironment()) {

            // Creates an in-memory plugin directory structure:
            //
            // +-- plugins
            //      +-- plugin-witfh-empty-dir
            //
            createPluginStructure("plugin-with-empty-dir", env);

            // Creates an in-memory plugin directory structure:
            //
            // +-- plugins
            //      +-- plugin-with-jar-file
            //           +-- elasticsearch-plugin-with-jar-file-4.0.3.jar
            //
            createPluginStructure("plugin-with-jar-file", env)
                    .createPluginJar("elasticsearch-plugin-with-jar-file-4.0.3.jar").toPath();

            // Creates an in-memory plugin directory structure:
            //
            // +-- plugins
            //      +-- plugin-with-site-dir
            //           +-- _site
            //                 +-- es-plugin.properties
            //
            createPluginStructure("plugin-with-site-dir", env)
                    .createSiteDir()
                        .addSiteFile("es-plugin.properties", "version=1.4-beta1" + System.lineSeparator());

            ExitStatus status = new PluginManager.List(terminal).execute(ImmutableSettings.EMPTY, env);
            assertThat(terminal.getTerminalOutput(), hasSize(3));
            assertThat(status, equalTo(ExitStatus.OK));

            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("plugin-with-empty-dir (no version)")));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("plugin-with-jar-file (4.0.3)")));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("plugin-with-site-dir (1.4-beta1)")));
        }
    }

    @Test
    public void testListCommandWithInstalledPluginAndVerboseOption() throws Exception {
        try (InMemoryEnvironment env = new InMemoryEnvironment()) {

            // Creates an in-memory plugin directory structure:
            //
            // +-- plugins
            //     +-- plugin-with-deps
            //           +-- elasticsearch-plugin-with-jar-file-4.0.3.jar
            //                +-- meta.yml
            //
            createPluginStructure("plugin-with-deps", env)
                    .createPluginJar("elasticsearch-plugin-with-deps-1.0.3.jar")
                        .addFile("meta.yml",
                                "organisation: elasticsearch\n" +
                                "name: plugin-with-deps\n" +
                                "version: 1.0.3\n" +
                                "description: A plugin with dependencies\n" +
                                "\n" +
                                "dependencies:\n" +
                                "    - organisation: elasticsearch\n" +
                                "      name: analysis-icu\n" +
                                "      version: 2.4.2\n" +
                                "\n" +
                                "    - organisation: elasticsearch\n" +
                                "      name: cloud-azure\n" +
                                "      version: 1.3.0\n")
                    .toPath();

            ExitStatus status = new PluginManager(terminal).parse("list", args("-v")).execute(ImmutableSettings.EMPTY, env);
            assertThat(terminal.verbosity(), equalTo(Terminal.Verbosity.VERBOSE));
            assertThat(status, equalTo(ExitStatus.OK));

            assertThat(terminal.getTerminalOutput(), hasItems(equalToIgnoringWhiteSpace("plugin-with-deps (1.0.3)"),
                                                                equalToIgnoringWhiteSpace("organisation: elasticsearch"),
                                                                equalToIgnoringWhiteSpace("name        : plugin-with-deps"),
                                                                equalToIgnoringWhiteSpace("version     : 1.0.3"),
                                                                equalToIgnoringWhiteSpace("description : A plugin with dependencies"),
                                                                equalToIgnoringWhiteSpace("depends on  :"),
                                                                equalToIgnoringWhiteSpace("- elasticsearch:analysis-icu:2.4.2"),
                                                                equalToIgnoringWhiteSpace("- elasticsearch:cloud-azure:1.3.0")));
        }
    }

    @Test
    public void testInstallOptions() throws Exception {
        assertCommand("install with no options", CliTool.Command.Exit.class, ExitStatus.USAGE, "install");
        assertCommand("install with help (shortcut option)", CliTool.Command.Help.class, ExitStatus.OK, "install", "-h");
        assertCommand("install with help (long option)", CliTool.Command.Help.class, ExitStatus.OK, "install", "--help");
        assertCommand("install with forbidden plugin name", PluginManager.Install.class, ExitStatus.USAGE, "install", "?./");
        assertCommand("install with forbidden plugin name", PluginManager.Install.class, ExitStatus.USAGE, "install", ".");
        assertCommand("install with forbidden plugin name", PluginManager.Install.class, ExitStatus.USAGE, "install", "/");
        assertCommand("install with forbidden plugin name", PluginManager.Install.class, ExitStatus.USAGE, "install", "elasticsearch");
        assertCommand("install with forbidden plugin name", PluginManager.Install.class, ExitStatus.USAGE, "install", "elasticsearch.bat");
        assertCommand("install with forbidden plugin name", PluginManager.Install.class, ExitStatus.USAGE, "install", "elasticsearch.in.sh");
        assertCommand("install with forbidden plugin name", PluginManager.Install.class, ExitStatus.USAGE, "install", "plugin");
        assertCommand("install with forbidden plugin name", PluginManager.Install.class, ExitStatus.USAGE, "install", "plugin.bat");
        assertCommand("install with forbidden plugin name", PluginManager.Install.class, ExitStatus.USAGE, "install", "service.bat");
        assertCommand("install with forbidden plugin name", PluginManager.Install.class, ExitStatus.USAGE, "install", "ELASTICSEARCH");
        assertCommand("install with forbidden plugin name", PluginManager.Install.class, ExitStatus.USAGE, "install", "ELASTICSEARCH.IN.SH");
        assertCommand("install with forbidden plugin name", PluginManager.Install.class, ExitStatus.USAGE, "install", "PLUGIN");

        try {
            assertCommand("install with empty url (shortcut option)", null, null, "install", "-u");
            fail("should have thrown a MissingArgumentException");
        } catch (MissingArgumentException e) {
            // Ok
        }

        try {
            assertCommand("install with empty url (long option)", null, null, "install", "--url");
            fail("should have thrown a MissingArgumentException");
        } catch (MissingArgumentException e) {
            // Ok
        }

        try {
            assertCommand("install with empty timeout (shortcut option)", null, null, "install", "-t");
            fail("should have thrown a MissingArgumentException");
        } catch (MissingArgumentException e) {
            // Ok
        }

        try {
            assertCommand("install with empty timeout (long option)", null, null, "install", "--timeout");
            fail("should have thrown a MissingArgumentException");
        } catch (MissingArgumentException e) {
            // Ok
        }

        try {
            assertCommand("install with unknown option", null, null, "install", "--unknown");
            fail("should have thrown a UnrecognizedOptionException");
        } catch (UnrecognizedOptionException e) {
            // Ok
        }

        try {
            assertCommand("install with verbose and silent options", null, null, "install", randomFrom("-v", "--verbose"), randomFrom("-s", "--silent"));
            fail("should have thrown a AlreadySelectedException");
        } catch (AlreadySelectedException e) {
            // Ok
        }
    }

    @Test
    public void testInstallCommand() throws Exception {
        try (InMemoryEnvironment env = new InMemoryEnvironment()) {

            // Creates a fake zipped archive plugin with the following content:
            //
            //  |-- bin
            //  |     +-- plugin-test.sh
            //  |-- config
            //  |     +-- plugin-test.yml
            //  |-- _site
            //  |     |-- css
            //  |     +-- js
            //  |     |    +-- plugin-test.js
            //  |     +-- es-plugin.properties
            //  |-- elasticsearch-plugin-test-1.0.2.jar
            //  |     +-- meta.yml
            //  +-- LICENSE.txt
            //

            ZippedFileBuilder zip = createZippedFile(newTempDirPath(), "plugin.zip")
                    .addDir("bin/")
                        .addFile("bin/plugin-test.sh", "plugin test shell script")
                    .addDir("config/")
                        .addFile("config/plugin-test.yml", "plugin test config file")
                    .addDir("_site/")
                        .addDir("_site/css/")
                        .addDir("_site/js/")
                            .addFile("_site/js/plugin-test.js", "plugin test javascript file")
                        .addFile("_site/es-plugin.properties", "version=1.9.2" + System.lineSeparator() + "description=This is a test plugin" + System.lineSeparator())
                    .addFile("elasticsearch-plugin-test.jar",
                            createZippedFile()
                                    .addFile("meta.yml", "organisation: elasticsearch\n" + "name: plugin-test\n")
                            .toByteArray()
                    )
                    .addFile("LICENSE.txt", "plugin test license file");

            ExitStatus status = new PluginManager(terminal).parse("install", args("plugin-test -v --url " + zip.toPath().toUri().toString())).execute(ImmutableSettings.EMPTY, env);
            assertThat(terminal.verbosity(), equalTo(Terminal.Verbosity.VERBOSE));
            assertThat(status, equalTo(ExitStatus.OK));

            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Installing plugin [plugin-test]...")));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Connecting")));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Downloading")));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Unpacking plugin-test (1.9.2)")));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Plugin installed successfully!")));

            // Command returns OK, let's check that files are correctly installed
            ImmutableList<String> files = ImmutableList.of("bin",
                    "bin/plugin-test",
                    "bin/plugin-test/plugin-test.sh",
                    "config",
                    "config/plugin-test",
                    "config/plugin-test/plugin-test.yml",
                    "plugins",
                    "plugins/plugin-test",
                    "plugins/plugin-test/LICENSE.txt",
                    "plugins/plugin-test/_site",
                    "plugins/plugin-test/_site/css",
                    "plugins/plugin-test/_site/es-plugin.properties",
                    "plugins/plugin-test/_site/js",
                    "plugins/plugin-test/_site/js/plugin-test.js",
                    "plugins/plugin-test/elasticsearch-plugin-test.jar");

            for (String file : files) {
                assertTrue("File must exist: " + file, Files.exists(env.homeFile().resolve(file)));
            }
        }
    }

    @Test
    public void testUninstallOptions() throws Exception {
        assertCommand("uninstall with no options", CliTool.Command.Exit.class, ExitStatus.USAGE, "uninstall");
        assertCommand("uninstall with help (shortcut option)", CliTool.Command.Help.class, ExitStatus.OK, "uninstall", "-h");
        assertCommand("uninstall with help (long option)", CliTool.Command.Help.class, ExitStatus.OK, "uninstall", "--help");
        assertCommand("uninstall with forbidden plugin name", PluginManager.Uninstall.class, ExitStatus.USAGE, "uninstall", "@à+)");
        assertCommand("uninstall with forbidden plugin name", PluginManager.Uninstall.class, ExitStatus.USAGE, "uninstall", "%*");
        assertCommand("uninstall with forbidden plugin name", PluginManager.Uninstall.class, ExitStatus.USAGE, "uninstall", "/");
        assertCommand("uninstall with forbidden plugin name", PluginManager.Uninstall.class, ExitStatus.USAGE, "uninstall", "plugin");
        assertCommand("uninstall with forbidden plugin name", PluginManager.Uninstall.class, ExitStatus.USAGE, "uninstall","elasticsearch");
        assertCommand("uninstall with forbidden plugin name", PluginManager.Uninstall.class, ExitStatus.USAGE, "uninstall","elasticsearch.bat");
        assertCommand("uninstall with forbidden plugin name", PluginManager.Uninstall.class, ExitStatus.USAGE, "uninstall","elasticsearch.in.sh");
        assertCommand("uninstall with forbidden plugin name", PluginManager.Uninstall.class, ExitStatus.USAGE, "uninstall","plugin");
        assertCommand("uninstall with forbidden plugin name", PluginManager.Uninstall.class, ExitStatus.USAGE, "uninstall","plugin.bat");
        assertCommand("uninstall with forbidden plugin name", PluginManager.Uninstall.class, ExitStatus.USAGE, "uninstall","service.bat");
        assertCommand("uninstall with forbidden plugin name", PluginManager.Uninstall.class, ExitStatus.USAGE, "uninstall","ELASTICSEARCH");
        assertCommand("uninstall with forbidden plugin name", PluginManager.Uninstall.class, ExitStatus.USAGE, "uninstall","ELASTICSEARCH.IN.SH");
        assertCommand("uninstall with forbidden plugin name", PluginManager.Uninstall.class, ExitStatus.USAGE, "uninstall","PLUGIN");

        try {
            assertCommand("uninstall with unknown option", null, null, "uninstall", "--unknown");
            fail("should have thrown a UnrecognizedOptionException");
        } catch (UnrecognizedOptionException e) {
            // Ok
        }

        try {
            assertCommand("uninstall with verbose and silent options", null, null, "uninstall", randomFrom("-v", "--verbose"), randomFrom("-s", "--silent"));
            fail("should have thrown a AlreadySelectedException");
        } catch (AlreadySelectedException e) {
            // Ok
        }
    }

    @Test
    public void testUninstall() throws Exception {
        try (InMemoryEnvironment env = new InMemoryEnvironment()) {

            // Creates an in-memory plugin directory structure:
            // |
            // +-- bin
            //      +-- awesome-plugin
            // |         +-- cmd.sh
            // +-- config
            //      +-- awesome-plugin
            // |         +-- awesome.yml
            // +-- plugins
            //      +-- awesome-plugin
            //           +-- awesome-plugin.jar
            //           +-- _site
            //                 +-- es-plugin.properties
            //
            createPluginStructure("awesome-plugin", env)
                    .createBinDir()
                    .addBinFile("cmd.sh", "awesome plugin command script")
                    .createConfigDir()
                    .addConfigFile("awesome.yml", "awesome plugin config file")
                    .createSiteDir()
                    .addSiteFile("es-plugin.properties", "version=1.4.0.GA" + System.lineSeparator())
                    .createPluginJar("awesome-plugin.jar").toPath();

            ImmutableList<String> files = ImmutableList.of("bin/awesome-plugin",
                                                            "bin/awesome-plugin/cmd.sh",
                                                            "config/awesome-plugin",
                                                            "config/awesome-plugin/awesome.yml",
                                                            "plugins/awesome-plugin",
                                                            "plugins/awesome-plugin/_site",
                                                            "plugins/awesome-plugin/_site/es-plugin.properties",
                                                            "plugins/awesome-plugin/awesome-plugin.jar");

            for (String file : files) {
                assertTrue(Files.exists(env.homeFile().resolve(file)));
            }

            ExitStatus status = new PluginManager(terminal).parse("uninstall", args("awesome-plugin -v")).execute(ImmutableSettings.EMPTY, env);
            assertThat(terminal.verbosity(), equalTo(Terminal.Verbosity.VERBOSE));
            assertThat(status, equalTo(ExitStatus.OK));

            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Uninstalling plugin [awesome-plugin]...")));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Found awesome-plugin (1.4.0.GA)")));
            assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Removed awesome-plugin")));

            // Command returns OK, let's check that files are correctly deleted
            for (String file : files) {
                if (file.startsWith("config")) {
                    assertTrue(Files.exists(env.homeFile().resolve(file)));
                } else {
                    assertFalse(Files.exists(env.homeFile().resolve(file)));
                }
            }
        }
    }

    @Test
    public void testUninstallNotFound() throws Exception {
        ExitStatus status = new PluginManager(terminal).parse("uninstall", args("missing-plugin")).execute(ImmutableSettings.EMPTY, new Environment());
        assertThat(status, equalTo(ExitStatus.IO_ERROR));
    }

    private void assertCommand(String description, Class<? extends CliTool.Command> expectedCommandClass, ExitStatus expectedStatus, String cmd, String... args) throws Exception {
        logger.info("--> testing command {}: {} {}", description, cmd, Strings.arrayToDelimitedString(args, " "));

        assertCommandParsing(expectedCommandClass, cmd, args);
        assertCommandExecution(expectedStatus, cmd, args);
    }

    private void assertCommandParsing(Class<? extends CliTool.Command> expectedCommandClass, String cmd, String... args) throws Exception {
        CliTool.Command command = tool.parse(cmd, args);
        assertThat(command, instanceOf(expectedCommandClass));
    }

    private void assertCommandExecution(ExitStatus expectedStatus, String cmd, String... args) {
        int status = tool.execute(Strings.toStringArray(Lists.asList(cmd, args)));
        assertThat(status, equalTo(expectedStatus.status()));
    }

}
