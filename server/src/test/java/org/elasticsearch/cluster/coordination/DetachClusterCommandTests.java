/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import joptsimple.OptionSet;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.terminal.Terminal;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithoutEntitlements;
import org.junit.Before;

import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

@WithoutEntitlements // commands don't run with entitlements enforced
public class DetachClusterCommandTests extends ESTestCase {

    private Settings settings;
    private Path[] dataPaths;

    @Before
    public void createDataPaths() throws Exception {
        final Path dataPath = createTempDir();
        settings = Settings.builder()
            .put(Environment.PATH_DATA_SETTING.getKey(), dataPath.toString())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .build();

        try (var nodeEnvironment = newNodeEnvironment(settings)) {
            dataPaths = nodeEnvironment.nodeDataPaths();
            final String nodeId = randomAlphaOfLength(10);
            try (
                PersistedClusterStateService.Writer writer = new PersistedClusterStateService(
                    dataPaths,
                    nodeId,
                    xContentRegistry(),
                    new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    () -> 0L,
                    () -> false
                ).createWriter()
            ) {
                writer.writeFullStateAndCommit(1L, ClusterState.EMPTY_STATE);
            }
        }
    }

    public void testDoesNotPrintClusterStateWhenNotVerbose() throws Exception {
        final MockTerminal terminal = MockTerminal.create();
        terminal.addTextInput("y");

        executeCommand(terminal);

        assertThat(terminal.getOutput(), not(containsString("old cluster state")));
    }

    public void testPrintsClusterStateWhenVerbose() throws Exception {
        final MockTerminal terminal = MockTerminal.create();
        terminal.setVerbosity(Terminal.Verbosity.VERBOSE);
        terminal.addTextInput("y");

        executeCommand(terminal);

        assertThat(terminal.getOutput(), containsString("old cluster state"));
    }

    private void executeCommand(MockTerminal terminal) throws Exception {
        final DetachClusterCommand command = new DetachClusterCommand();
        final OptionSet options = command.getParser().parse();

        command.processDataPaths(terminal, dataPaths, options, TestEnvironment.newEnvironment(settings));
    }
}
