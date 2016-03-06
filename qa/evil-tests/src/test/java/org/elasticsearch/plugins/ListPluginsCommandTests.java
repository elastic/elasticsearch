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
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.MockTerminal;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

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
        MockTerminal terminal = new MockTerminal();
        String[] args = {};
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
        Files.createDirectory(env.pluginsFile().resolve("fake"));
        MockTerminal terminal = listPlugins(env);
        assertTrue(terminal.getOutput(), terminal.getOutput().contains("fake"));
    }

    public void testTwoPlugins() throws Exception {
        Environment env = createEnv();
        Files.createDirectory(env.pluginsFile().resolve("fake1"));
        Files.createDirectory(env.pluginsFile().resolve("fake2"));
        MockTerminal terminal = listPlugins(env);
        String output = terminal.getOutput();
        assertTrue(output, output.contains("fake1"));
        assertTrue(output, output.contains("fake2"));
    }
}
