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
package org.elasticsearch.cluster.coordination;

import joptsimple.OptionSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.containsString;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoveCustomsCommandIT extends ESIntegTestCase {

    public void testRemoveCustomsAbortedByUser() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build());
        expectThrows(() -> removeCustoms(environment, true, new String[]{ "index-graveyard" }),
            ElasticsearchNodeCommand.ABORTED_BY_USER_MSG);
    }

    public void testRemoveCustomsSuccessful() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        createIndex("test");
        client().admin().indices().prepareDelete("test").get();
        assertEquals(1, client().admin().cluster().prepareState().get().getState().metadata().indexGraveyard().getTombstones().size());
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build());
        MockTerminal terminal = removeCustoms(environment, false,
            randomBoolean() ?
                new String[]{ "index-graveyard" } :
                new String[]{ "index-*" }
            );
        assertThat(terminal.getOutput(), containsString(RemoveCustomsCommand.CUSTOMS_REMOVED_MSG));
        assertThat(terminal.getOutput(), containsString("The following customs will be removed:"));
        assertThat(terminal.getOutput(), containsString("index-graveyard"));

        internalCluster().startNode(dataPathSettings);
        assertEquals(0, client().admin().cluster().prepareState().get().getState().metadata().indexGraveyard().getTombstones().size());
    }

    public void testCustomDoesNotMatch() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        createIndex("test");
        client().admin().indices().prepareDelete("test").get();
        assertEquals(1, client().admin().cluster().prepareState().get().getState().metadata().indexGraveyard().getTombstones().size());
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build());
        UserException ex = expectThrows(UserException.class, () -> removeCustoms(environment, false,
            new String[]{ "index-greveyard-with-typos" }));
        assertThat(ex.getMessage(), containsString("No custom metadata matching [index-greveyard-with-typos] were " +
            "found on this node"));
    }

    private MockTerminal executeCommand(ElasticsearchNodeCommand command, Environment environment, boolean abort, String... args)
        throws Exception {
        final MockTerminal terminal = new MockTerminal();
        final OptionSet options = command.getParser().parse(args);
        final String input;

        if (abort) {
            input = randomValueOtherThanMany(c -> c.equalsIgnoreCase("y"), () -> randomAlphaOfLength(1));
        } else {
            input = randomBoolean() ? "y" : "Y";
        }

        terminal.addTextInput(input);

        try {
            command.execute(terminal, options, environment);
        } finally {
            assertThat(terminal.getOutput(), containsString(ElasticsearchNodeCommand.STOP_WARNING_MSG));
        }

        return terminal;
    }

    private MockTerminal removeCustoms(Environment environment, boolean abort, String... args) throws Exception {
        final MockTerminal terminal = executeCommand(new RemoveCustomsCommand(), environment, abort, args);
        assertThat(terminal.getOutput(), containsString(RemoveCustomsCommand.CONFIRMATION_MSG));
        assertThat(terminal.getOutput(), containsString(RemoveCustomsCommand.CUSTOMS_REMOVED_MSG));
        return terminal;
    }

    private void expectThrows(ThrowingRunnable runnable, String message) {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, runnable);
        assertThat(ex.getMessage(), containsString(message));
    }
}
