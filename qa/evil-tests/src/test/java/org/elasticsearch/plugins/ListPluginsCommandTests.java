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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.UserError;
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


    public void testPluginsDirMissing() throws Exception {
        Environment env = createEnv();
        Files.delete(env.pluginsFile());
        IOException e = expectThrows(IOException.class, () -> {
           listPlugins(env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Plugins directory missing"));
    }

    public void testNoPlugins() throws Exception {
        MockTerminal terminal = listPlugins(createEnv());
        assertTrue(terminal.getOutput(), terminal.getOutput().isEmpty());
    }

    public void testOnePlugin() throws Exception {
        Environment env = createEnv();
        PluginTestUtil.writeProperties(env.pluginsFile().resolve("fake"),
                "description", "fake desc",
                "name", "fake_plugin",
                "version", "1.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", "org.fake");
        
        MockTerminal terminal = listPlugins(env);
        assertTrue(terminal.getOutput(), terminal.getOutput().contains("fake"));
    }

    public void testTwoPlugins() throws Exception {
        Environment env = createEnv();
        PluginTestUtil.writeProperties(env.pluginsFile().resolve("fake1"),
                "description", "fake desc",
                "name", "fake_plugin",
                "version", "1.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", "org.fake");
        PluginTestUtil.writeProperties(env.pluginsFile().resolve("fake2"),
                "description", "fake desc",
                "name", "fake2_plugin",
                "version", "1.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", "org.fake");
        MockTerminal terminal = listPlugins(env);
        String output = terminal.getOutput();
        assertTrue(output, output.contains("fake1"));
        assertTrue(output, output.contains("fake2"));
    }
    
    public void testPluginWithVerbose() throws Exception {
        Environment env = createEnv();
        PluginTestUtil.writeProperties(env.pluginsFile().resolve("fake1"),
                "description", "fake desc",
                "name", "fake_plugin",
                "version", "1.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", "org.fake");
        String[] params = { "-v" };
        MockTerminal terminal = listPlugins(env, params);
        String output = terminal.getOutput();
        assertTrue(output, output.contains("Plugin information"));
        assertTrue(output, output.contains("Classname: org.fake"));
        assertTrue(output, output.contains("fake_plugin"));
        assertTrue(output, output.contains("1.0"));
        assertTrue(output, output.contains("fake desc"));
    }
    
    public void testPluginWithVerboseMultiplePlugins() throws Exception {
        Environment env = createEnv();
        
        PluginTestUtil.writeProperties(env.pluginsFile().resolve("fake1"),
                "description", "fake desc",
                "name", "fake_plugin",
                "version", "1.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", "org.fake");

        PluginTestUtil.writeProperties(env.pluginsFile().resolve("fake2"),
                "description", "fake desc",
                "name", "fake2_plugin",
                "version", "1.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", "org.fake2");
        
        PluginTestUtil.writeProperties(env.pluginsFile().resolve("fake3"),
                "description", "fake desc",
                "name", "fake3_plugin",
                "version", "1.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", "org.fake3");

        String[] params = { "-v" };
        MockTerminal terminal = listPlugins(env, params);
        String output = terminal.getOutput();
        
        //Should be 3 plugins, so if i split by plugin informations i should get 4 strings
        assertTrue(output, output.split("Plugin information").length == 4);
        assertTrue(output, output.contains("Classname: org.fake"));
        assertTrue(output, output.contains("Classname: org.fake2"));
        assertTrue(output, output.contains("Classname: org.fake3"));
        assertTrue(output, output.contains("fake_plugin"));
        assertTrue(output, output.contains("fake2_plugin"));
        assertTrue(output, output.contains("fake3_plugin"));
        assertTrue(output, output.contains("1.0"));
        assertTrue(output, output.contains("fake desc"));
    }
        
    public void testPluginWithoutVerboseMultiplePlugins() throws Exception {
        Environment env = createEnv();
        
        PluginTestUtil.writeProperties(env.pluginsFile().resolve("fake1"),
                "description", "fake desc",
                "name", "fake_plugin",
                "version", "1.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", "org.fake");

        PluginTestUtil.writeProperties(env.pluginsFile().resolve("fake2"),
                "description", "fake desc",
                "name", "fake2_plugin",
                "version", "1.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", "org.fake2");
        
        PluginTestUtil.writeProperties(env.pluginsFile().resolve("fake3"),
                "description", "fake desc",
                "name", "fake3_plugin",
                "version", "1.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", "org.fake3");

        MockTerminal terminal = listPlugins(env, new String[0]);
        String output = terminal.getOutput();
        assertFalse(output, output.contains("Plugin information"));
    }
    
    public void testPluginWithoutDescriptionFile() throws Exception{
        Environment env = createEnv();
        Files.createDirectories(env.pluginsFile().resolve("fake1"));
        NoSuchFileException e = expectThrows(NoSuchFileException.class, () -> {
        	listPlugins(env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("fake1\\plugin-descriptor.properties"));
    }
    
    public void testPluginWithWrongDescriptionFile() throws Exception{
        Environment env = createEnv();        
        PluginTestUtil.writeProperties(env.pluginsFile().resolve("fake1"),
                "description", "fake desc");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
        	listPlugins(env);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Property [name] is missing in"));
    }
}
