/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.PluginTestUtil;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@LuceneTestCase.SuppressFileSystems("*")
public class ListPluginsCommandTests extends ESTestCase {

    private Path home;
    private Environment env;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        home = createTempDir();
        Files.createDirectories(home.resolve("plugins"));
        Settings settings = Settings.builder().put("path.home", home).build();
        env = TestEnvironment.newEnvironment(settings);
    }

    static MockTerminal listPlugins(Path home) throws Exception {
        return listPlugins(home, new String[0]);
    }

    static MockTerminal listPlugins(Path home, String[] args) throws Exception {
        String[] argsAndHome = new String[args.length + 1];
        System.arraycopy(args, 0, argsAndHome, 0, args.length);
        argsAndHome[args.length] = "-Epath.home=" + home;
        MockTerminal terminal = new MockTerminal();
        int status = new ListPluginsCommand() {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                Settings.Builder builder = Settings.builder().put("path.home", home);
                settings.forEach((k, v) -> builder.put(k, v));
                final Settings realSettings = builder.build();
                return new Environment(realSettings, home.resolve("config"));
            }

            @Override
            protected boolean addShutdownHook() {
                return false;
            }
        }.main(argsAndHome, terminal);
        assertEquals(ExitCodes.OK, status);
        return terminal;
    }

    private static String buildMultiline(String... args) {
        return Arrays.stream(args).collect(Collectors.joining("\n", "", "\n"));
    }

    private static void buildFakePlugin(final Environment env, final String description, final String name, final String classname)
        throws IOException {
        buildFakePlugin(env, description, name, classname, false);
    }

    private static void buildFakePlugin(
        final Environment env,
        final String description,
        final String name,
        final String classname,
        final boolean hasNativeController
    ) throws IOException {
        PluginTestUtil.writePluginProperties(
            env.pluginsFile().resolve(name),
            "description",
            description,
            "name",
            name,
            "version",
            "1.0",
            "elasticsearch.version",
            Version.CURRENT.toString(),
            "java.version",
            "1.8",
            "classname",
            classname,
            "has.native.controller",
            Boolean.toString(hasNativeController)
        );
    }

    public void testPluginsDirMissing() throws Exception {
        Files.delete(env.pluginsFile());
        IOException e = expectThrows(IOException.class, () -> listPlugins(home));
        assertEquals("Plugins directory missing: " + env.pluginsFile(), e.getMessage());
    }

    public void testNoPlugins() throws Exception {
        MockTerminal terminal = listPlugins(home);
        assertTrue(terminal.getOutput(), terminal.getOutput().isEmpty());
    }

    public void testOnePlugin() throws Exception {
        buildFakePlugin(env, "fake desc", "fake", "org.fake");
        MockTerminal terminal = listPlugins(home);
        assertEquals(buildMultiline("fake"), terminal.getOutput());
    }

    public void testTwoPlugins() throws Exception {
        buildFakePlugin(env, "fake desc", "fake1", "org.fake");
        buildFakePlugin(env, "fake desc 2", "fake2", "org.fake");
        MockTerminal terminal = listPlugins(home);
        assertEquals(buildMultiline("fake1", "fake2"), terminal.getOutput());
    }

    public void testPluginWithVerbose() throws Exception {
        buildFakePlugin(env, "fake desc", "fake_plugin", "org.fake");
        String[] params = { "-v" };
        MockTerminal terminal = listPlugins(home, params);
        assertEquals(
            buildMultiline(
                "Plugins directory: " + env.pluginsFile(),
                "fake_plugin",
                "- Plugin information:",
                "Name: fake_plugin",
                "Description: fake desc",
                "Version: 1.0",
                "Elasticsearch Version: " + Version.CURRENT.toString(),
                "Java Version: 1.8",
                "Native Controller: false",
                "Licensed: false",
                "Type: isolated",
                "Extended Plugins: []",
                " * Classname: org.fake"
            ),
            terminal.getOutput()
        );
    }

    public void testPluginWithNativeController() throws Exception {
        buildFakePlugin(env, "fake desc 1", "fake_plugin1", "org.fake", true);
        String[] params = { "-v" };
        MockTerminal terminal = listPlugins(home, params);
        assertEquals(
            buildMultiline(
                "Plugins directory: " + env.pluginsFile(),
                "fake_plugin1",
                "- Plugin information:",
                "Name: fake_plugin1",
                "Description: fake desc 1",
                "Version: 1.0",
                "Elasticsearch Version: " + Version.CURRENT.toString(),
                "Java Version: 1.8",
                "Native Controller: true",
                "Licensed: false",
                "Type: isolated",
                "Extended Plugins: []",
                " * Classname: org.fake"
            ),
            terminal.getOutput()
        );
    }

    public void testPluginWithVerboseMultiplePlugins() throws Exception {
        buildFakePlugin(env, "fake desc 1", "fake_plugin1", "org.fake");
        buildFakePlugin(env, "fake desc 2", "fake_plugin2", "org.fake2");
        String[] params = { "-v" };
        MockTerminal terminal = listPlugins(home, params);
        assertEquals(
            buildMultiline(
                "Plugins directory: " + env.pluginsFile(),
                "fake_plugin1",
                "- Plugin information:",
                "Name: fake_plugin1",
                "Description: fake desc 1",
                "Version: 1.0",
                "Elasticsearch Version: " + Version.CURRENT.toString(),
                "Java Version: 1.8",
                "Native Controller: false",
                "Licensed: false",
                "Type: isolated",
                "Extended Plugins: []",
                " * Classname: org.fake",
                "fake_plugin2",
                "- Plugin information:",
                "Name: fake_plugin2",
                "Description: fake desc 2",
                "Version: 1.0",
                "Elasticsearch Version: " + Version.CURRENT.toString(),
                "Java Version: 1.8",
                "Native Controller: false",
                "Licensed: false",
                "Type: isolated",
                "Extended Plugins: []",
                " * Classname: org.fake2"
            ),
            terminal.getOutput()
        );
    }

    public void testPluginWithoutVerboseMultiplePlugins() throws Exception {
        buildFakePlugin(env, "fake desc 1", "fake_plugin1", "org.fake");
        buildFakePlugin(env, "fake desc 2", "fake_plugin2", "org.fake2");
        MockTerminal terminal = listPlugins(home, new String[0]);
        String output = terminal.getOutput();
        assertEquals(buildMultiline("fake_plugin1", "fake_plugin2"), output);
    }

    public void testPluginWithoutDescriptorFile() throws Exception {
        final Path pluginDir = env.pluginsFile().resolve("fake1");
        Files.createDirectories(pluginDir);
        NoSuchFileException e = expectThrows(NoSuchFileException.class, () -> listPlugins(home));
        assertEquals(pluginDir.resolve(PluginDescriptor.ES_PLUGIN_PROPERTIES).toString(), e.getFile());
    }

    public void testPluginWithWrongDescriptorFile() throws Exception {
        final Path pluginDir = env.pluginsFile().resolve("fake1");
        PluginTestUtil.writePluginProperties(pluginDir, "description", "fake desc");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> listPlugins(home));
        final Path descriptorPath = pluginDir.resolve(PluginDescriptor.ES_PLUGIN_PROPERTIES);
        assertEquals("property [name] is missing in [" + descriptorPath.toString() + "]", e.getMessage());
    }

    public void testExistingIncompatiblePlugin() throws Exception {
        PluginTestUtil.writePluginProperties(
            env.pluginsFile().resolve("fake_plugin1"),
            "description",
            "fake desc 1",
            "name",
            "fake_plugin1",
            "version",
            "1.0",
            "elasticsearch.version",
            Version.fromString("1.0.0").toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "org.fake1"
        );
        buildFakePlugin(env, "fake desc 2", "fake_plugin2", "org.fake2");

        MockTerminal terminal = listPlugins(home);
        String message = "plugin [fake_plugin1] was built for Elasticsearch version 1.0.0 but version " + Version.CURRENT + " is required";
        assertEquals("fake_plugin1\nfake_plugin2\n", terminal.getOutput());
        assertEquals("WARNING: " + message + "\n", terminal.getErrorOutput());

        String[] params = { "-s" };
        terminal = listPlugins(home, params);
        assertEquals("fake_plugin1\nfake_plugin2\n", terminal.getOutput());
    }
}
