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

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetaData;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoMinMasterNodes = false)
@TestLogging("_root:DEBUG,org.elasticsearch.cluster.service:TRACE,org.elasticsearch.discovery.zen:TRACE")
public class UnsafeBootstrapMasterIT extends ESIntegTestCase {

    private int bootstrapNodeId;

    @Before
    public void resetBootstrapNodeId() {
        bootstrapNodeId = -1;
    }

    /**
     * Performs cluster bootstrap when node with id bootstrapNodeId is started.
     * Any node of the batch could be selected as bootstrap target.
     */
    @Override
    protected List<Settings> addExtraClusterBootstrapSettings(List<Settings> allNodesSettings) {
        if (internalCluster().size() + allNodesSettings.size() == bootstrapNodeId) {
            List<String> nodeNames = new ArrayList<>();
            Collections.addAll(nodeNames, internalCluster().getNodeNames());
            allNodesSettings.forEach(settings -> nodeNames.add(Node.NODE_NAME_SETTING.get(settings)));

            List<Settings> newSettings = new ArrayList<>();
            int bootstrapIndex = randomInt(allNodesSettings.size() - 1);
            for (int i = 0; i < allNodesSettings.size(); i++) {
                Settings nodeSettings = allNodesSettings.get(i);
                if (i == bootstrapIndex) {
                    newSettings.add(Settings.builder().put(nodeSettings)
                            .putList(ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.getKey(), nodeNames)
                            .build());
                } else {
                    newSettings.add(nodeSettings);
                }
            }

            return newSettings;
        }
        return allNodesSettings;
    }

    private MockTerminal executeCommand(Environment environment, boolean abort) throws Exception {
        final UnsafeBootstrapMasterCommand command = new UnsafeBootstrapMasterCommand();
        final MockTerminal terminal = new MockTerminal();
        final OptionParser parser = new OptionParser();
        final OptionSet options = parser.parse();
        final String input;

        if (abort) {
            input = randomValueOtherThanMany(c -> c.equalsIgnoreCase("y"), () -> randomAlphaOfLength(1));
        } else {
            input = randomBoolean() ? "y" : "Y";
        }

        terminal.addTextInput(input);

        try {
            command.execute(terminal, options, environment);
            assertThat(terminal.getOutput(), containsString(UnsafeBootstrapMasterCommand.MASTER_NODE_BOOTSTRAPPED_MSG));
        } finally {
            assertThat(terminal.getOutput(), containsString(UnsafeBootstrapMasterCommand.STOP_WARNING_MSG));
        }

        return terminal;
    }

    private MockTerminal executeCommand(Environment environment) throws Exception {
        return executeCommand(environment, false);
    }

    private void expectThrows(ThrowingRunnable runnable, String message) {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, runnable);
        assertThat(ex.getMessage(), containsString(message));
    }

    public void testNotMasterEligible() {
        final Environment environment = TestEnvironment.newEnvironment(Settings.builder()
                .put(internalCluster().getDefaultSettings())
                .put(Node.NODE_MASTER_SETTING.getKey(), false)
                .build());
        expectThrows(() -> executeCommand(environment), UnsafeBootstrapMasterCommand.NOT_MASTER_NODE_MSG);
    }

    public void testNoDataFolder() {
        final Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());
        expectThrows(() -> executeCommand(environment), UnsafeBootstrapMasterCommand.NO_NODE_FOLDER_FOUND_MSG);
    }

    public void testNodeLocked() throws IOException {
        Settings envSettings = buildEnvSettings(Settings.EMPTY);
        Environment environment = TestEnvironment.newEnvironment(envSettings);
        try (NodeEnvironment ignored = new NodeEnvironment(envSettings, environment)) {
            expectThrows(() -> executeCommand(environment), UnsafeBootstrapMasterCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);
        }
    }

    public void testNoNodeMetaData() throws IOException {
        Settings envSettings = buildEnvSettings(Settings.EMPTY);
        Environment environment = TestEnvironment.newEnvironment(envSettings);
        try (NodeEnvironment nodeEnvironment = new NodeEnvironment(envSettings, environment)) {
            NodeMetaData.FORMAT.cleanupOldFiles(-1, nodeEnvironment.nodeDataPaths());
        }

        expectThrows(() -> executeCommand(environment), UnsafeBootstrapMasterCommand.NO_NODE_METADATA_FOUND_MSG);
    }

    public void testNotBootstrappedCluster() throws Exception {
        internalCluster().startNode(
                Settings.builder()
                        .put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s") // to ensure quick node startup
                        .build());
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().setLocal(true)
                    .execute().actionGet().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(DiscoverySettings.NO_MASTER_BLOCK_ID));
        });

        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());
        expectThrows(() -> executeCommand(environment), UnsafeBootstrapMasterCommand.GLOBAL_GENERATION_MISSING_MSG);
    }

    public void testNoManifestFile() throws IOException {
        bootstrapNodeId = 1;
        internalCluster().startNode();
        ensureStableCluster(1);
        NodeEnvironment nodeEnvironment = internalCluster().getMasterNodeInstance(NodeEnvironment.class);
        internalCluster().stopRandomDataNode();
        Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());
        Manifest.FORMAT.cleanupOldFiles(-1, nodeEnvironment.nodeDataPaths());

        expectThrows(() -> executeCommand(environment), UnsafeBootstrapMasterCommand.NO_MANIFEST_FILE_FOUND_MSG);
    }

    public void testNoMetaData() throws IOException {
        bootstrapNodeId = 1;
        internalCluster().startNode();
        ensureStableCluster(1);
        NodeEnvironment nodeEnvironment = internalCluster().getMasterNodeInstance(NodeEnvironment.class);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());
        MetaData.FORMAT.cleanupOldFiles(-1, nodeEnvironment.nodeDataPaths());

        expectThrows(() -> executeCommand(environment), UnsafeBootstrapMasterCommand.NO_GLOBAL_METADATA_MSG);
    }

    public void testAbortedByUser() throws IOException {
        bootstrapNodeId = 1;
        internalCluster().startNode();
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());
        expectThrows(() -> executeCommand(environment, true), UnsafeBootstrapMasterCommand.ABORTED_BY_USER_MSG);
    }

    public void test3MasterNodes2Failed() throws Exception {
        bootstrapNodeId = 3;
        List<String> masterNodes = internalCluster().startMasterOnlyNodes(3, Settings.EMPTY);

        String dataNode = internalCluster().startDataOnlyNode(Settings.EMPTY);
        createIndex("test");

        Client dataNodeClient = internalCluster().client(dataNode);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodes.get(1)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodes.get(2)));

        assertBusy(() -> {
            ClusterState state = dataNodeClient.admin().cluster().prepareState().setLocal(true)
                    .execute().actionGet().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(DiscoverySettings.NO_MASTER_BLOCK_ID));
        });

        final Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());
        expectThrows(() -> executeCommand(environment), UnsafeBootstrapMasterCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);

        NodeEnvironment nodeEnvironment = internalCluster().getMasterNodeInstance(NodeEnvironment.class);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodes.get(0)));

        MockTerminal terminal = executeCommand(environment);

        MetaData metaData = MetaData.FORMAT.loadLatestState(logger, xContentRegistry(), nodeEnvironment.nodeDataPaths());
        assertThat(terminal.getOutput(), containsString(
                String.format(Locale.ROOT, UnsafeBootstrapMasterCommand.CLUSTER_STATE_TERM_VERSION_MSG_FORMAT,
                        metaData.coordinationMetaData().term(), metaData.version())));

        internalCluster().startMasterOnlyNode(Settings.EMPTY);

        assertBusy(() -> {
            ClusterState state = dataNodeClient.admin().cluster().prepareState().setLocal(true)
                    .execute().actionGet().getState();
            assertFalse(state.blocks().hasGlobalBlockWithId(DiscoverySettings.NO_MASTER_BLOCK_ID));
            assertTrue(state.metaData().persistentSettings().getAsBoolean(UnsafeBootstrapMasterCommand.UNSAFE_BOOTSTRAP.getKey(), false));
        });

        ensureGreen("test");
    }
}
