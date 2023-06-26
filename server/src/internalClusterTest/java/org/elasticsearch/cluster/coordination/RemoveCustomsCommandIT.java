/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import joptsimple.OptionSet;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Map;

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
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        expectThrows(
            () -> removeCustoms(environment, true, new String[] { "index-graveyard" }),
            ElasticsearchNodeCommand.ABORTED_BY_USER_MSG
        );
    }

    public void testRemoveCustomsSuccessful() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        createIndex("test");
        indicesAdmin().prepareDelete("test").get();
        assertEquals(1, clusterAdmin().prepareState().get().getState().metadata().indexGraveyard().getTombstones().size());
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        MockTerminal terminal = removeCustoms(
            environment,
            false,
            randomBoolean() ? new String[] { "index-graveyard" } : new String[] { "index-*" }
        );
        assertThat(terminal.getOutput(), containsString(RemoveCustomsCommand.CUSTOMS_REMOVED_MSG));
        assertThat(terminal.getOutput(), containsString("The following customs will be removed:"));
        assertThat(terminal.getOutput(), containsString("index-graveyard"));

        internalCluster().startNode(dataPathSettings);
        assertEquals(0, clusterAdmin().prepareState().get().getState().metadata().indexGraveyard().getTombstones().size());
    }

    public void testCustomDoesNotMatch() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        createIndex("test");
        indicesAdmin().prepareDelete("test").get();
        assertEquals(1, clusterAdmin().prepareState().get().getState().metadata().indexGraveyard().getTombstones().size());
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        UserException ex = expectThrows(
            UserException.class,
            () -> removeCustoms(environment, false, new String[] { "index-greveyard-with-typos" })
        );
        assertThat(ex.getMessage(), containsString("No custom metadata matching [index-greveyard-with-typos] were found on this node"));
    }

    private MockTerminal executeCommand(ElasticsearchNodeCommand command, Environment environment, boolean abort, String... args)
        throws Exception {
        final MockTerminal terminal = MockTerminal.create();
        final OptionSet options = command.getParser().parse(args);
        final ProcessInfo processInfo = new ProcessInfo(Map.of(), Map.of(), createTempDir());
        final String input;

        if (abort) {
            input = randomValueOtherThanMany(c -> c.equalsIgnoreCase("y"), () -> randomAlphaOfLength(1));
        } else {
            input = randomBoolean() ? "y" : "Y";
        }

        terminal.addTextInput(input);

        try {
            command.execute(terminal, options, environment, processInfo);
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
