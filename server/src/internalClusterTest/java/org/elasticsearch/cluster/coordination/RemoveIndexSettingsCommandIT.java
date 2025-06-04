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

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoveIndexSettingsCommandIT extends ESIntegTestCase {

    static final Setting<Integer> FOO = Setting.intSetting("index.foo", 1, Setting.Property.IndexScope, Setting.Property.Dynamic);
    static final Setting<Integer> BAR = Setting.intSetting("index.bar", 2, Setting.Property.IndexScope, Setting.Property.Final);

    public static class ExtraSettingsPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(FOO, BAR);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), ExtraSettingsPlugin.class);
    }

    public void testRemoveSettingsAbortedByUser() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        var node = internalCluster().startNode();
        createIndex("test-index", Settings.builder().put(FOO.getKey(), 101).put(BAR.getKey(), 102).build());
        ensureYellow("test-index");
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Settings nodeSettings = Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build();
        ElasticsearchException error = expectThrows(
            ElasticsearchException.class,
            () -> removeIndexSettings(TestEnvironment.newEnvironment(nodeSettings), true, "index.foo")
        );
        assertThat(error.getMessage(), equalTo(ElasticsearchNodeCommand.ABORTED_BY_USER_MSG));
        internalCluster().startNode(nodeSettings);
    }

    public void testRemoveSettingsSuccessful() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        var node = internalCluster().startNode();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);

        int numIndices = randomIntBetween(1, 10);
        int[] barValues = new int[numIndices];
        for (int i = 0; i < numIndices; i++) {
            String index = "test-index-" + i;
            barValues[i] = between(1, 1000);
            createIndex(index, Settings.builder().put(FOO.getKey(), between(1, 1000)).put(BAR.getKey(), barValues[i]).build());
        }
        int moreIndices = randomIntBetween(1, 10);
        for (int i = 0; i < moreIndices; i++) {
            createIndex("more-index-" + i, Settings.EMPTY);
        }
        internalCluster().stopNode(node);

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );

        MockTerminal terminal = removeIndexSettings(environment, false, "index.foo");
        assertThat(terminal.getOutput(), containsString(RemoveIndexSettingsCommand.SETTINGS_REMOVED_MSG));
        for (int i = 0; i < numIndices; i++) {
            assertThat(terminal.getOutput(), containsString("Index setting [index.foo] will be removed from index [[test-index-" + i));
        }
        for (int i = 0; i < moreIndices; i++) {
            assertThat(terminal.getOutput(), not(containsString("Index setting [index.foo] will be removed from index [[more-index-" + i)));
        }
        Settings nodeSettings = Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build();
        internalCluster().startNode(nodeSettings);

        Map<String, Settings> getIndexSettings = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, "test-index-*")
            .get()
            .getIndexToSettings();
        for (int i = 0; i < numIndices; i++) {
            String index = "test-index-" + i;
            Settings indexSettings = getIndexSettings.get(index);
            assertFalse(indexSettings.hasValue("index.foo"));
            assertThat(indexSettings.get("index.bar"), equalTo(Integer.toString(barValues[i])));
        }
        getIndexSettings = client().admin().indices().prepareGetSettings(TEST_REQUEST_TIMEOUT, "more-index-*").get().getIndexToSettings();
        for (int i = 0; i < moreIndices; i++) {
            assertNotNull(getIndexSettings.get("more-index-" + i));
        }
    }

    public void testSettingDoesNotMatch() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        var node = internalCluster().startNode();
        createIndex("test-index", Settings.builder().put(FOO.getKey(), 101).put(BAR.getKey(), 102).build());
        ensureYellow("test-index");
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Settings nodeSettings = Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build();
        UserException error = expectThrows(
            UserException.class,
            () -> removeIndexSettings(TestEnvironment.newEnvironment(nodeSettings), true, "index.not_foo")
        );
        assertThat(error.getMessage(), containsString("No index setting matching [index.not_foo] were found on this node"));
        internalCluster().startNode(nodeSettings);
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

    private MockTerminal removeIndexSettings(Environment environment, boolean abort, String... args) throws Exception {
        final MockTerminal terminal = executeCommand(new RemoveIndexSettingsCommand(), environment, abort, args);
        assertThat(terminal.getOutput(), containsString(RemoveIndexSettingsCommand.CONFIRMATION_MSG));
        assertThat(terminal.getOutput(), containsString(RemoveIndexSettingsCommand.SETTINGS_REMOVED_MSG));
        return terminal;
    }
}
