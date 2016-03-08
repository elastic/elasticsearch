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
import java.nio.file.Path;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.junit.Before;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class PluginCliTests extends CommandTestCase {

    // the home dir for each test to use
    Path homeDir;

    // settings used to create an Environment for tools
    Settings.Builder settingsBuilder;

    @Before
    public void setupHome() {
        homeDir = createTempDir();
        settingsBuilder = Settings.builder()
            .put("path.home", homeDir);
    }

    @Override
    protected Command newCommand() {
        return new PluginCli(new Environment(settingsBuilder.build()));
    }

    public void testHelpWorks() throws Exception {
        MockTerminal terminal = new MockTerminal();
        /* nocommit
        assertThat(new PluginCli(terminal).execute(args("--help")), is(OK_AND_EXIT));
        assertTerminalOutputContainsHelpFile(terminal, "/org/elasticsearch/plugins/plugin.help");

        terminal.resetOutput();
        assertThat(new PluginCli(terminal).execute(args("install -h")), is(OK_AND_EXIT));
        assertTerminalOutputContainsHelpFile(terminal, "/org/elasticsearch/plugins/plugin-install.help");
        for (String plugin : InstallPluginCommand.OFFICIAL_PLUGINS) {
            assertThat(terminal.getOutput(), containsString(plugin));
        }

        terminal.resetOutput();
        assertThat(new PluginCli(terminal).execute(args("remove --help")), is(OK_AND_EXIT));
        assertTerminalOutputContainsHelpFile(terminal, "/org/elasticsearch/plugins/plugin-remove.help");

        terminal.resetOutput();
        assertThat(new PluginCli(terminal).execute(args("list -h")), is(OK_AND_EXIT));
        assertTerminalOutputContainsHelpFile(terminal, "/org/elasticsearch/plugins/plugin-list.help");
        */
    }

}
