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

import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.MockTerminal;

import static org.elasticsearch.common.cli.CliTool.ExitStatus.OK_AND_EXIT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class PluginCliTests extends CliToolTestCase {
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
