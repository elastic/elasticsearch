/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.readiness;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.node.Node.INITIAL_STATE_TIMEOUT_SETTING;
import static org.elasticsearch.readiness.MockReadinessService.tcpReadinessProbeFalse;
import static org.elasticsearch.readiness.MockReadinessService.tcpReadinessProbeTrue;
import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.elasticsearch.test.NodeRoles.masterNode;
import static org.elasticsearch.test.NodeRoles.nonDataNode;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class ReadinessClusterIT extends ESIntegTestCase {

    private static AtomicLong versionCounter = new AtomicLong(1);

    private static String testErrorJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "not_cluster_settings": {
                     "search.allow_expensive_queries": "false"
                 }
             }
        }""";

    private static String testJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "cluster_settings": {
                     "indices.recovery.max_bytes_per_sec": "50mb"
                 }
             }
        }""";

    Path configDir;

    @Before
    public void setupMasterConfigDir() throws IOException {
        configDir = createTempDir();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Settings.builder().put(ReadinessService.PORT.getKey(), 0).build());
        return settings.build();
    }

    @Override
    protected Path nodeConfigPath(int nodeOrdinal) {
        return configDir;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getMockPlugins());
        plugins.add(MockReadinessService.TestPlugin.class);
        return Collections.unmodifiableList(plugins);
    }

    private void assertMasterNode(Client client, String node) {
        assertThat(client.admin().cluster().prepareState().get().getState().nodes().getMasterNode().getName(), equalTo(node));
    }

    private void expectMasterNotFound() {
        expectThrows(
            MasterNotDiscoveredException.class,
            clusterAdmin().prepareState().setMasterNodeTimeout(TimeValue.timeValueMillis(100))
        );
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/108613")
    public void testReadinessDuringRestarts() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        writeFileSettings(testJSON);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));

        expectMasterNotFound();
        assertFalse(internalCluster().getInstance(ReadinessService.class, dataNode).ready());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();

        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        tcpReadinessProbeTrue(internalCluster().getInstance(ReadinessService.class, dataNode));
        tcpReadinessProbeTrue(internalCluster().getInstance(ReadinessService.class, masterNode));

        final var masterReadinessService = internalCluster().getInstance(ReadinessService.class, masterNode);
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);

        logger.info("--> stop master node");
        Settings masterDataPathSettings = internalCluster().dataPathSettings(internalCluster().getMasterName());
        internalCluster().stopCurrentMasterNode();
        expectMasterNotFound();

        tcpReadinessProbeFalse(masterReadinessService);

        logger.info("--> start previous master node again");
        final String nextMasterEligibleNodeName = internalCluster().startNode(
            Settings.builder().put(nonDataNode(masterNode())).put(masterDataPathSettings)
        );

        assertMasterNode(internalCluster().nonMasterClient(), nextMasterEligibleNodeName);
        assertMasterNode(internalCluster().masterClient(), nextMasterEligibleNodeName);
        tcpReadinessProbeTrue(internalCluster().getInstance(ReadinessService.class, nextMasterEligibleNodeName));
    }

    public void testReadinessDuringRestartsNormalOrder() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        writeFileSettings(testJSON);
        logger.info("--> start master node");
        String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().validateClusterFormed();

        assertMasterNode(internalCluster().masterClient(), masterNode);

        logger.info("--> start 2 data nodes");
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        internalCluster().validateClusterFormed();

        tcpReadinessProbeTrue(internalCluster().getInstance(ReadinessService.class, masterNode));

        for (String dataNode : dataNodes) {
            tcpReadinessProbeTrue(internalCluster().getInstance(ReadinessService.class, dataNode));
        }

        logger.info("--> restart data node 1");
        internalCluster().restartNode(dataNodes.get(0), new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                tcpReadinessProbeTrue(internalCluster().getInstance(ReadinessService.class, masterNode));
                tcpReadinessProbeTrue(internalCluster().getInstance(ReadinessService.class, dataNodes.get(1)));

                return super.onNodeStopped(nodeName);
            }
        });

        logger.info("--> restart master");

        internalCluster().restartNode(masterNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                expectMasterNotFound();

                logger.info("--> master node [{}] stopped", nodeName);

                for (String dataNode : dataNodes) {
                    logger.info("--> checking data node [{}] for readiness", dataNode);
                    ReadinessService s = internalCluster().getInstance(ReadinessService.class, dataNode);
                    boolean awaitSuccessful = s.listenerThreadLatch.await(10, TimeUnit.SECONDS);
                    assertTrue(awaitSuccessful);
                    tcpReadinessProbeFalse(s);
                }

                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen();
        for (String dataNode : dataNodes) {
            tcpReadinessProbeTrue(internalCluster().getInstance(ReadinessService.class, dataNode));
        }
    }

    private CountDownLatch setupClusterStateListenerForError(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null && reservedState.errorMetadata() != null) {
                    assertEquals(ReservedStateErrorMetadata.ErrorKind.PARSING, reservedState.errorMetadata().errorKind());
                    assertThat(reservedState.errorMetadata().errors(), allOf(notNullValue(), hasSize(1)));
                    assertThat(
                        reservedState.errorMetadata().errors().get(0),
                        containsString("Missing handler definition for content key [not_cluster_settings]")
                    );
                    clusterService.removeListener(this);
                    savedClusterState.countDown();
                }
            }
        });

        // we need this after we setup the listener above, in case the node started and processed
        // settings before we set our listener to cluster state changes.
        causeClusterStateUpdate();

        return savedClusterState;
    }

    private void writeFileSettings(String json) throws Exception {
        long version = versionCounter.incrementAndGet();
        Path tempFilePath = createTempFile();
        Path fileSettings = configDir.resolve("operator").resolve("settings.json");
        Files.createDirectories(fileSettings.getParent());

        Files.write(tempFilePath, Strings.format(json, version).getBytes(StandardCharsets.UTF_8));
        Files.move(tempFilePath, fileSettings, StandardCopyOption.ATOMIC_MOVE);
        logger.info("--> New file settings: [{}]", Strings.format(json, version));
    }

    public void testNotReadyOnBadFileSettings() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        FileSettingsService dataFileSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        logger.info("--> write bad file settings before we boot master node");
        writeFileSettings(testErrorJSON);

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode(
            Settings.builder().put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
        );
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var savedClusterState = setupClusterStateListenerForError(masterNode);

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertTrue(masterFileSettingsService.watching());
        assertFalse(dataFileSettingsService.watching());

        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        ReadinessService s = internalCluster().getInstance(ReadinessService.class, internalCluster().getMasterName());
        assertNull(s.boundAddress());
    }

    public void testReadyAfterRestartWithBadFileSettings() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        writeFileSettings(testJSON);

        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        String masterNode = internalCluster().startMasterOnlyNode();

        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        assertBusy(() -> assertTrue("master node ready", internalCluster().getInstance(ReadinessService.class, masterNode).ready()));
        assertBusy(() -> assertTrue("data node ready", internalCluster().getInstance(ReadinessService.class, dataNode).ready()));

        logger.info("--> stop master node");
        Settings masterDataPathSettings = internalCluster().dataPathSettings(internalCluster().getMasterName());
        internalCluster().stopCurrentMasterNode();
        expectMasterNotFound();

        logger.info("--> write bad file settings before restarting master node");
        writeFileSettings(testErrorJSON);

        logger.info("--> restart master node");
        String nextMasterNode = internalCluster().startNode(Settings.builder().put(nonDataNode(masterNode())).put(masterDataPathSettings));

        assertMasterNode(internalCluster().nonMasterClient(), nextMasterNode);

        var savedClusterState = setupClusterStateListenerForError(nextMasterNode);
        assertTrue(savedClusterState.await(20, TimeUnit.SECONDS));

        assertTrue("master node ready on restart", internalCluster().getInstance(ReadinessService.class, nextMasterNode).ready());
    }

    public void testReadyWhenMissingFileSettings() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));

        final String masterNode = internalCluster().startMasterOnlyNode(
            Settings.builder().put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
        );
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var savedClusterState = setupClusterStateListener(masterNode);

        // we need this after we setup the listener above, in case the node started and processed
        // settings before we set our listener to cluster state changes.
        causeClusterStateUpdate();

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertTrue(masterFileSettingsService.watching());

        boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        ReadinessService s = internalCluster().getInstance(ReadinessService.class, masterNode);
        assertNotNull(s.boundAddress());
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListener(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null) {
                    clusterService.removeListener(this);
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private CountDownLatch setupReadinessProbeListener(String masterNode) {
        ReadinessService s = internalCluster().getInstance(ReadinessService.class, masterNode);
        CountDownLatch readinessProbeListening = new CountDownLatch(1);
        s.addBoundAddressListener(x -> readinessProbeListening.countDown());
        return readinessProbeListening;
    }

    public void testReadyAfterCorrectFileSettings() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        FileSettingsService dataFileSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());
        var savedClusterState = setupClusterStateListener(dataNode);

        logger.info("--> write correct file settings before we boot master node");
        writeFileSettings(testJSON);

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var readinessProbeListening = setupReadinessProbeListener(masterNode);

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertTrue(masterFileSettingsService.watching());
        assertFalse(dataFileSettingsService.watching());

        boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        ReadinessService s = internalCluster().getInstance(ReadinessService.class, internalCluster().getMasterName());
        readinessProbeListening.await(20, TimeUnit.SECONDS);
        tcpReadinessProbeTrue(s);
    }

    private void causeClusterStateUpdate() {
        final var latch = new CountDownLatch(1);
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitUnbatchedStateUpdateTask("poke", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return ClusterState.builder(currentState).build();
                }

                @Override
                public void onFailure(Exception e) {
                    assert false : e;
                }

                @Override
                public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                    latch.countDown();
                }
            });
        safeAwait(latch);
    }
}
