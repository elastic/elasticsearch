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


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

@LuceneTestCase.SuppressFileSystems("*")
public class ListPluginsCommandTests extends ESTestCase {

    private Path home;
    private Environment env;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        home = createTempDir();
        Files.createDirectories(home.resolve("plugins"));
        Settings settings = Settings.builder()
                .put("path.home", home)
                .build();
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
            protected Environment createEnv(Terminal terminal, Map<String, String> settings) throws UserException {
                Settings.Builder builder = Settings.builder().put("path.home", home);
                settings.forEach((k,v) -> builder.put(k, v));
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

    private static String buildMultiline(String... args){
        return Arrays.stream(args).collect(Collectors.joining("\n", "", "\n"));
    }

    private static void buildFakePlugin(
            final Environment env,
            final String description,
            final String name,
            final String classname) throws IOException {
        buildFakePlugin(env, description, name, classname, false, false);
    }

    private static void buildFakePlugin(
            final Environment env,
            final String description,
            final String name,
            final String classname,
            final boolean hasNativeController,
            final boolean requiresKeystore) throws IOException {
        PluginTestUtil.writeProperties(
                env.pluginsFile().resolve(name),
                "description", description,
                "name", name,
                "version", "1.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", classname,
                "has.native.controller", Boolean.toString(hasNativeController),
                "requires.keystore", Boolean.toString(requiresKeystore));
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
                        "Native Controller: false",
                        "Requires Keystore: false",
                        " * Classname: org.fake"),
                terminal.getOutput());
    }

    public void testPluginWithNativeController() throws Exception {
        buildFakePlugin(env, "fake desc 1", "fake_plugin1", "org.fake", true, false);
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
                "Native Controller: true",
                "Requires Keystore: false",
                " * Classname: org.fake"),
            terminal.getOutput());
    }

    public void testPluginWithRequiresKeystore() throws Exception {
        buildFakePlugin(env, "fake desc 1", "fake_plugin1", "org.fake", false, true);
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
                "Native Controller: false",
                "Requires Keystore: true",
                " * Classname: org.fake"),
            terminal.getOutput());
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
                        "Native Controller: false",
                        "Requires Keystore: false",
                        " * Classname: org.fake",
                        "fake_plugin2",
                        "- Plugin information:",
                        "Name: fake_plugin2",
                        "Description: fake desc 2",
                        "Version: 1.0",
                        "Native Controller: false",
                        "Requires Keystore: false",
                        " * Classname: org.fake2"),
                terminal.getOutput());
    }

    public void testPluginWithoutVerboseMultiplePlugins() throws Exception {
        buildFakePlugin(env, "fake desc 1", "fake_plugin1", "org.fake");
        buildFakePlugin(env, "fake desc 2", "fake_plugin2", "org.fake2");
        MockTerminal terminal = listPlugins(home, new String[0]);
        String output = terminal.getOutput();
        assertEquals(buildMultiline("fake_plugin1", "fake_plugin2"), output);
    }

    public void testPluginWithoutDescriptorFile() throws Exception{
        final Path pluginDir = env.pluginsFile().resolve("fake1");
        Files.createDirectories(pluginDir);
        NoSuchFileException e = expectThrows(NoSuchFileException.class, () -> listPlugins(home));
        assertEquals(pluginDir.resolve(PluginInfo.ES_PLUGIN_PROPERTIES).toString(), e.getFile());
    }

    public void testPluginWithWrongDescriptorFile() throws Exception{
        final Path pluginDir = env.pluginsFile().resolve("fake1");
        PluginTestUtil.writeProperties(pluginDir, "description", "fake desc");
        IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> listPlugins(home));
        final Path descriptorPath = pluginDir.resolve(PluginInfo.ES_PLUGIN_PROPERTIES);
        assertEquals(
                "property [name] is missing in [" + descriptorPath.toString() + "]",
                e.getMessage());
    }

    public void testExistingIncompatiblePlugin() throws Exception {
        PluginTestUtil.writeProperties(env.pluginsFile().resolve("fake_plugin1"),
            "description", "fake desc 1",
            "name", "fake_plugin1",
            "version", "1.0",
            "elasticsearch.version", Version.fromString("1.0.0").toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "org.fake1");
        buildFakePlugin(env, "fake desc 2", "fake_plugin2", "org.fake2");

        MockTerminal terminal = listPlugins(home);
        final String message = String.format(Locale.ROOT,
                "plugin [%s] is incompatible with version [%s]; was designed for version [%s]",
                "fake_plugin1",
                Version.CURRENT.toString(),
                "1.0.0");
        assertEquals(
                "fake_plugin1\n" + "WARNING: " + message + "\n" + "fake_plugin2\n",
                terminal.getOutput());

        String[] params = {"-s"};
        terminal = listPlugins(home, params);
        assertEquals("fake_plugin1\nfake_plugin2\n", terminal.getOutput());
    }

}
