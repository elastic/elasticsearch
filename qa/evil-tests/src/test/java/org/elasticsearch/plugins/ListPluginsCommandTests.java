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
import java.util.stream.Collectors;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.Version;

@LuceneTestCase.SuppressFileSystems("*")
public class ListPluginsCommandTests extends ESTestCase {

    Environment createEnv() throws IOException {
        Path home = createTempDir();
        Files.createDirectories(home.resolve("plugins"));
        Settings settings = Settings.builder()
            .put("path.home", home)
            .build();
        return new Environment(settings);
    }

    static MockTerminal listPlugins(Environment env) throws Exception {
        return listPlugins(env, new String[0]);
    }
    
    static MockTerminal listPlugins(Environment env, String[] args) throws Exception {
        MockTerminal terminal = new MockTerminal();
        int status = new ListPluginsCommand(env).main(args, terminal);
        assertEquals(ExitCodes.OK, status);
        return terminal;
    }
    
    static String buildMultiline(String... args){
        return Arrays.asList(args).stream().collect(Collectors.joining("\n", "", "\n"));
    }
    
    static void buildFakePlugin(Environment env, String description, String name, String classname) throws IOException {
        PluginTestUtil.writeProperties(env.pluginsFile().resolve(name),
                "description", description,
                "name", name,
                "version", "1.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", classname);
    }


    public void testPluginsDirMissing() throws Exception {
        Environment env = createEnv();
        Files.delete(env.pluginsFile());
        IOException e = expectThrows(IOException.class, () -> {
           listPlugins(env);
        });
        assertEquals(e.getMessage(), "Plugins directory missing: " + env.pluginsFile());
    }

    public void testNoPlugins() throws Exception {
        MockTerminal terminal = listPlugins(createEnv());
        assertTrue(terminal.getOutput(), terminal.getOutput().isEmpty());
    }

    public void testOnePlugin() throws Exception {
        Environment env = createEnv();
        buildFakePlugin(env, "fake desc", "fake", "org.fake");
        MockTerminal terminal = listPlugins(env);
        assertEquals(terminal.getOutput(), buildMultiline("fake"));
    }

    public void testTwoPlugins() throws Exception {
        Environment env = createEnv();
        buildFakePlugin(env, "fake desc", "fake1", "org.fake");
        buildFakePlugin(env, "fake desc 2", "fake2", "org.fake");
        MockTerminal terminal = listPlugins(env);
        assertEquals(terminal.getOutput(), buildMultiline("fake1", "fake2"));
    }
    
    public void testPluginWithVerbose() throws Exception {
        Environment env = createEnv();
        buildFakePlugin(env, "fake desc", "fake_plugin", "org.fake");
        String[] params = { "-v" };
        MockTerminal terminal = listPlugins(env, params);
        assertEquals(terminal.getOutput(), buildMultiline("Plugins directory: " + env.pluginsFile(), "fake_plugin",
                "- Plugin information:", "Name: fake_plugin", "Description: fake desc", "Version: 1.0", " * Classname: org.fake"));
    }
    
    public void testPluginWithVerboseMultiplePlugins() throws Exception {
        Environment env = createEnv();
        buildFakePlugin(env, "fake desc 1", "fake_plugin1", "org.fake");
        buildFakePlugin(env, "fake desc 2", "fake_plugin2", "org.fake2");
        String[] params = { "-v" };
        MockTerminal terminal = listPlugins(env, params);
        assertEquals(terminal.getOutput(), buildMultiline("Plugins directory: " + env.pluginsFile(),
                "fake_plugin1", "- Plugin information:", "Name: fake_plugin1", "Description: fake desc 1", "Version: 1.0",
                " * Classname: org.fake", "fake_plugin2", "- Plugin information:", "Name: fake_plugin2",
                "Description: fake desc 2", "Version: 1.0", " * Classname: org.fake2"));
    }
        
    public void testPluginWithoutVerboseMultiplePlugins() throws Exception {
        Environment env = createEnv();
        buildFakePlugin(env, "fake desc 1", "fake_plugin1", "org.fake");
        buildFakePlugin(env, "fake desc 2", "fake_plugin2", "org.fake2");
        MockTerminal terminal = listPlugins(env, new String[0]);
        String output = terminal.getOutput();
        assertEquals(output, buildMultiline("fake_plugin1", "fake_plugin2"));
    }
    
    public void testPluginWithoutDescriptorFile() throws Exception{
        Environment env = createEnv();
        Files.createDirectories(env.pluginsFile().resolve("fake1"));
        NoSuchFileException e = expectThrows(NoSuchFileException.class, () -> listPlugins(env));
        assertEquals(e.getFile(), env.pluginsFile().resolve("fake1").resolve(PluginInfo.ES_PLUGIN_PROPERTIES).toString());
    }
    
    public void testPluginWithWrongDescriptorFile() throws Exception{
        Environment env = createEnv();        
        PluginTestUtil.writeProperties(env.pluginsFile().resolve("fake1"),
                "description", "fake desc");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> listPlugins(env));
        assertEquals(e.getMessage(), "Property [name] is missing in [" +
                env.pluginsFile().resolve("fake1").resolve(PluginInfo.ES_PLUGIN_PROPERTIES).toString() + "]");
    }
    
}
