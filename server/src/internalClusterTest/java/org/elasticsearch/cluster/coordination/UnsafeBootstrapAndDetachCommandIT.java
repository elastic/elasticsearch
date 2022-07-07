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
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.gateway.DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class UnsafeBootstrapAndDetachCommandIT extends ESIntegTestCase {

    private MockTerminal executeCommand(ElasticsearchNodeCommand command, Environment environment, int nodeOrdinal, boolean abort)
        throws Exception {
        final MockTerminal terminal = new MockTerminal();
        final OptionSet options = command.getParser().parse("-ordinal", Integer.toString(nodeOrdinal));
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

    private MockTerminal unsafeBootstrap(Environment environment, boolean abort) throws Exception {
        final MockTerminal terminal = executeCommand(new UnsafeBootstrapMasterCommand(), environment, 0, abort);
        assertThat(terminal.getOutput(), containsString(UnsafeBootstrapMasterCommand.CONFIRMATION_MSG));
        assertThat(terminal.getOutput(), containsString(UnsafeBootstrapMasterCommand.MASTER_NODE_BOOTSTRAPPED_MSG));
        return terminal;
    }

    private MockTerminal detachCluster(Environment environment, boolean abort) throws Exception {
        final MockTerminal terminal = executeCommand(new DetachClusterCommand(), environment, 0, abort);
        assertThat(terminal.getOutput(), containsString(DetachClusterCommand.CONFIRMATION_MSG));
        assertThat(terminal.getOutput(), containsString(DetachClusterCommand.NODE_DETACHED_MSG));
        return terminal;
    }

    private MockTerminal unsafeBootstrap(Environment environment) throws Exception {
        return unsafeBootstrap(environment, false);
    }

    private MockTerminal detachCluster(Environment environment) throws Exception {
        return detachCluster(environment, false);
    }

    private void expectThrows(ThrowingRunnable runnable, String message) {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, runnable);
        assertThat(ex.getMessage(), containsString(message));
    }

    public void testBootstrapNotMasterEligible() {
        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(nonMasterNode(internalCluster().getDefaultSettings())).build()
        );
        expectThrows(() -> unsafeBootstrap(environment), UnsafeBootstrapMasterCommand.NOT_MASTER_NODE_MSG);
    }

    public void testBootstrapNoDataFolder() {
        final Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());
        expectThrows(() -> unsafeBootstrap(environment), ElasticsearchNodeCommand.NO_NODE_FOLDER_FOUND_MSG);
    }

    public void testDetachNoDataFolder() {
        final Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());
        expectThrows(() -> detachCluster(environment), ElasticsearchNodeCommand.NO_NODE_FOLDER_FOUND_MSG);
    }

    public void testBootstrapNodeLocked() throws IOException {
        Settings envSettings = buildEnvSettings(Settings.EMPTY);
        Environment environment = TestEnvironment.newEnvironment(envSettings);
        try (NodeEnvironment ignored = new NodeEnvironment(envSettings, environment)) {
            expectThrows(() -> unsafeBootstrap(environment), ElasticsearchNodeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);
        }
    }

    public void testDetachNodeLocked() throws IOException {
        Settings envSettings = buildEnvSettings(Settings.EMPTY);
        Environment environment = TestEnvironment.newEnvironment(envSettings);
        try (NodeEnvironment ignored = new NodeEnvironment(envSettings, environment)) {
            expectThrows(() -> detachCluster(environment), ElasticsearchNodeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);
        }
    }

    public void testBootstrapNoNodeMetadata() {
        Settings envSettings = buildEnvSettings(Settings.EMPTY);
        Environment environment = TestEnvironment.newEnvironment(envSettings);
        expectThrows(() -> unsafeBootstrap(environment), ElasticsearchNodeCommand.NO_NODE_FOLDER_FOUND_MSG);
    }

    public void testBootstrapNotBootstrappedCluster() throws Exception {
        String node = internalCluster().startNode(
            Settings.builder()
                .put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s") // to ensure quick node startup
                .build()
        );
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));
        });

        Settings dataPathSettings = internalCluster().dataPathSettings(node);

        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        expectThrows(() -> unsafeBootstrap(environment), UnsafeBootstrapMasterCommand.EMPTY_LAST_COMMITTED_VOTING_CONFIG_MSG);
    }

    public void testBootstrapNoClusterState() throws IOException {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        NodeEnvironment nodeEnvironment = internalCluster().getAnyMasterNodeInstance(NodeEnvironment.class);
        internalCluster().stopRandomDataNode();
        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        PersistedClusterStateService.deleteAll(nodeEnvironment.nodeDataPaths());

        expectThrows(() -> unsafeBootstrap(environment), ElasticsearchNodeCommand.NO_NODE_METADATA_FOUND_MSG);
    }

    public void testDetachNoClusterState() throws IOException {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        NodeEnvironment nodeEnvironment = internalCluster().getAnyMasterNodeInstance(NodeEnvironment.class);
        internalCluster().stopRandomDataNode();
        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        PersistedClusterStateService.deleteAll(nodeEnvironment.nodeDataPaths());

        expectThrows(() -> detachCluster(environment), ElasticsearchNodeCommand.NO_NODE_METADATA_FOUND_MSG);
    }

    public void testBootstrapAbortedByUser() throws IOException {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        expectThrows(() -> unsafeBootstrap(environment, true), ElasticsearchNodeCommand.ABORTED_BY_USER_MSG);
    }

    public void testDetachAbortedByUser() throws IOException {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        expectThrows(() -> detachCluster(environment, true), ElasticsearchNodeCommand.ABORTED_BY_USER_MSG);
    }

    public void test3MasterNodes2Failed() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(2);
        List<String> masterNodes = new ArrayList<>();

        logger.info("--> start 1st master-eligible node");
        masterNodes.add(
            internalCluster().startMasterOnlyNode(
                Settings.builder().put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
            )
        ); // node ordinal 0

        logger.info("--> start one data-only node");
        String dataNode = internalCluster().startDataOnlyNode(
            Settings.builder().put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
        ); // node ordinal 1

        logger.info("--> start 2nd and 3rd master-eligible nodes and bootstrap");
        masterNodes.addAll(internalCluster().startMasterOnlyNodes(2)); // node ordinals 2 and 3

        logger.info("--> wait for all nodes to join the cluster");
        ensureStableCluster(4);

        logger.info("--> create index test");
        createIndex("test");
        ensureGreen("test");

        Settings master1DataPathSettings = internalCluster().dataPathSettings(masterNodes.get(0));
        Settings master2DataPathSettings = internalCluster().dataPathSettings(masterNodes.get(1));
        Settings master3DataPathSettings = internalCluster().dataPathSettings(masterNodes.get(2));
        Settings dataNodeDataPathSettings = internalCluster().dataPathSettings(dataNode);

        logger.info("--> stop 2nd and 3d master eligible node");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodes.get(1)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodes.get(2)));

        logger.info("--> ensure NO_MASTER_BLOCK on data-only node");
        assertBusy(() -> {
            ClusterState state = internalCluster().client(dataNode)
                .admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));
        });

        logger.info("--> try to unsafely bootstrap 1st master-eligible node, while node lock is held");
        Environment environmentMaster1 = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(master1DataPathSettings).build()
        );
        expectThrows(() -> unsafeBootstrap(environmentMaster1), UnsafeBootstrapMasterCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);

        logger.info("--> stop 1st master-eligible node and data-only node");
        NodeEnvironment nodeEnvironment = internalCluster().getAnyMasterNodeInstance(NodeEnvironment.class);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodes.get(0)));
        assertBusy(() -> internalCluster().getInstance(GatewayMetaState.class, dataNode).allPendingAsyncStatesWritten());
        internalCluster().stopRandomDataNode();

        logger.info("--> unsafely-bootstrap 1st master-eligible node");
        MockTerminal terminal = unsafeBootstrap(environmentMaster1);
        Metadata metadata = ElasticsearchNodeCommand.createPersistedClusterStateService(Settings.EMPTY, nodeEnvironment.nodeDataPaths())
            .loadBestOnDiskState().metadata;
        assertThat(
            terminal.getOutput(),
            containsString(
                String.format(
                    Locale.ROOT,
                    UnsafeBootstrapMasterCommand.CLUSTER_STATE_TERM_VERSION_MSG_FORMAT,
                    metadata.coordinationMetadata().term(),
                    metadata.version()
                )
            )
        );

        logger.info("--> start 1st master-eligible node");
        internalCluster().startMasterOnlyNode(master1DataPathSettings);

        logger.info("--> detach-cluster on data-only node");
        Environment environmentData = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataNodeDataPathSettings).build()
        );
        detachCluster(environmentData, false);

        logger.info("--> start data-only node");
        String dataNode2 = internalCluster().startDataOnlyNode(dataNodeDataPathSettings);

        logger.info("--> ensure there is no NO_MASTER_BLOCK and unsafe-bootstrap is reflected in cluster state");
        assertBusy(() -> {
            ClusterState state = internalCluster().client(dataNode2)
                .admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState();
            assertFalse(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));
            assertTrue(state.metadata().persistentSettings().getAsBoolean(UnsafeBootstrapMasterCommand.UNSAFE_BOOTSTRAP.getKey(), false));
        });

        logger.info("--> ensure index test is green");
        ensureGreen("test");
        IndexMetadata indexMetadata = clusterService().state().metadata().index("test");
        assertThat(indexMetadata.getSettings().get(IndexMetadata.SETTING_HISTORY_UUID), notNullValue());

        logger.info("--> detach-cluster on 2nd and 3rd master-eligible nodes");
        Environment environmentMaster2 = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(master2DataPathSettings).build()
        );
        detachCluster(environmentMaster2, false);
        Environment environmentMaster3 = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(master3DataPathSettings).build()
        );
        detachCluster(environmentMaster3, false);

        logger.info("--> start 2nd and 3rd master-eligible nodes and ensure 4 nodes stable cluster");
        internalCluster().startMasterOnlyNode(master2DataPathSettings);
        internalCluster().startMasterOnlyNode(master3DataPathSettings);
        ensureStableCluster(4);
    }

    public void testAllMasterEligibleNodesFailedDanglingIndexImport() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);

        Settings settings = Settings.builder().put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true).build();

        logger.info("--> start mixed data and master-eligible node and bootstrap cluster");
        String masterNode = internalCluster().startNode(settings); // node ordinal 0

        logger.info("--> start data-only node and ensure 2 nodes stable cluster");
        String dataNode = internalCluster().startDataOnlyNode(settings); // node ordinal 1
        ensureStableCluster(2);

        logger.info("--> index 1 doc and ensure index is green");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        ensureGreen("test");
        assertBusy(
            () -> internalCluster().getInstances(IndicesService.class)
                .forEach(indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten()))
        );

        logger.info("--> verify 1 doc in the index");
        assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1L);
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));

        logger.info("--> stop data-only node and detach it from the old cluster");
        Settings dataNodeDataPathSettings = Settings.builder()
            .put(internalCluster().dataPathSettings(dataNode), true)
            .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true)
            .build();
        assertBusy(() -> internalCluster().getInstance(GatewayMetaState.class, dataNode).allPendingAsyncStatesWritten());
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataNode));
        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder()
                .put(internalCluster().getDefaultSettings())
                .put(dataNodeDataPathSettings)
                .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true)
                .build()
        );
        detachCluster(environment, false);

        logger.info("--> stop master-eligible node, clear its data and start it again - new cluster should form");
        internalCluster().restartNode(masterNode, new InternalTestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });

        logger.info("--> start data-only only node and ensure 2 nodes stable cluster");
        internalCluster().startDataOnlyNode(dataNodeDataPathSettings);
        ensureStableCluster(2);

        logger.info("--> verify that the dangling index exists and has green status");
        assertBusy(() -> { assertThat(client().admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(true)); });
        ensureGreen("test");

        logger.info("--> verify the doc is there");
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));
    }

    public void testNoInitialBootstrapAfterDetach() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String masterNode = internalCluster().startMasterOnlyNode();
        Settings masterNodeDataPathSettings = internalCluster().dataPathSettings(masterNode);
        internalCluster().stopCurrentMasterNode();

        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(masterNodeDataPathSettings).build()
        );
        detachCluster(environment);

        String node = internalCluster().startMasterOnlyNode(
            Settings.builder()
                // give the cluster 2 seconds to elect the master (it should not)
                .put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "2s")
                .put(masterNodeDataPathSettings)
                .build()
        );

        ClusterState state = internalCluster().client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertTrue(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node));
    }

    public void testCanRunUnsafeBootstrapAfterErroneousDetachWithoutLoosingMetadata() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String masterNode = internalCluster().startMasterOnlyNode();
        Settings masterNodeDataPathSettings = internalCluster().dataPathSettings(masterNode);
        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest().persistentSettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "1234kb")
        );
        internalCluster().client().admin().cluster().updateSettings(req).get();

        ClusterState state = internalCluster().client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()), equalTo("1234kb"));

        internalCluster().stopCurrentMasterNode();

        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(masterNodeDataPathSettings).build()
        );
        detachCluster(environment);
        unsafeBootstrap(environment);

        internalCluster().startMasterOnlyNode(masterNodeDataPathSettings);
        ensureGreen();

        state = internalCluster().client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metadata().settings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()), equalTo("1234kb"));
    }
}
