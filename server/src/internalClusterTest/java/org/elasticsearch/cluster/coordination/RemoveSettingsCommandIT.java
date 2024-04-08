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
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoveSettingsCommandIT extends ESIntegTestCase {

    public void testRemoveSettingsAbortedByUser() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        updateClusterSettings(
            Settings.builder().put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
        );
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        expectThrows(
            () -> removeSettings(
                environment,
                true,
                new String[] { DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey() }
            ),
            ElasticsearchNodeCommand.ABORTED_BY_USER_MSG
        );
    }

    public void testRemoveSettingsSuccessful() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        updateClusterSettings(
            Settings.builder().put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
        );
        assertThat(
            clusterAdmin().prepareState().get().getState().metadata().persistentSettings().keySet(),
            contains(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey())
        );
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        MockTerminal terminal = removeSettings(
            environment,
            false,
            randomBoolean()
                ? new String[] { DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey() }
                : new String[] { "cluster.routing.allocation.disk.*" }
        );
        assertThat(terminal.getOutput(), containsString(RemoveSettingsCommand.SETTINGS_REMOVED_MSG));
        assertThat(terminal.getOutput(), containsString("The following settings will be removed:"));
        assertThat(
            terminal.getOutput(),
            containsString(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey() + ": " + false)
        );

        internalCluster().startNode(dataPathSettings);
        assertThat(
            clusterAdmin().prepareState().get().getState().metadata().persistentSettings().keySet(),
            not(contains(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey()))
        );
    }

    public void testSettingDoesNotMatch() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        updateClusterSettings(
            Settings.builder().put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
        );
        assertThat(
            clusterAdmin().prepareState().get().getState().metadata().persistentSettings().keySet(),
            contains(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey())
        );
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        UserException ex = expectThrows(
            UserException.class,
            () -> removeSettings(environment, false, new String[] { "cluster.routing.allocation.disk.bla.*" })
        );
        assertThat(
            ex.getMessage(),
            containsString("No persistent cluster settings matching [cluster.routing.allocation.disk.bla.*] were found on this node")
        );
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

    private MockTerminal removeSettings(Environment environment, boolean abort, String... args) throws Exception {
        final MockTerminal terminal = executeCommand(new RemoveSettingsCommand(), environment, abort, args);
        assertThat(terminal.getOutput(), containsString(RemoveSettingsCommand.CONFIRMATION_MSG));
        assertThat(terminal.getOutput(), containsString(RemoveSettingsCommand.SETTINGS_REMOVED_MSG));
        return terminal;
    }

    private void expectThrows(ThrowingRunnable runnable, String message) {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, runnable);
        assertThat(ex.getMessage(), containsString(message));
    }
}
