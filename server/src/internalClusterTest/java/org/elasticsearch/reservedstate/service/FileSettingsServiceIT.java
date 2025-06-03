/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.node.FetchHealthInfoCacheAction;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.node.Node.INITIAL_STATE_TIMEOUT_SETTING;
import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.elasticsearch.test.NodeRoles.masterNode;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
@LuceneTestCase.SuppressFileSystems("*")
public class FileSettingsServiceIT extends ESIntegTestCase {

    private final AtomicLong versionCounter = new AtomicLong(1);

    @Before
    public void resetVersionCounter() {
        versionCounter.set(1);
    }

    private static final String testJSON = """
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

    private static final String testJSON43mb = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "cluster_settings": {
                     "indices.recovery.max_bytes_per_sec": "43mb"
                 }
             }
        }""";

    private static final String testCleanupJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "cluster_settings": {}
             }
        }""";

    private static final String testErrorJSON = """
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

    private static final String testOtherErrorJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "bad_cluster_settings": {
                     "search.allow_expensive_queries": "false"
                 }
             }
        }""";

    private void assertMasterNode(Client client, String node) {
        assertThat(
            client.admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState().nodes().getMasterNode().getName(),
            equalTo(node)
        );
    }

    public static void writeJSONFile(String node, String json, Logger logger, Long version) throws Exception {
        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);
        writeJSONFile(node, json, logger, version, fileSettingsService.watchedFile());
    }

    public static void writeJSONFile(String node, String json, Logger logger, Long version, Path targetPath) throws Exception {
        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);

        Files.createDirectories(fileSettingsService.watchedFileDir());
        Path tempFilePath = createTempFile();

        String jsonWithVersion = Strings.format(json, version);
        logger.info("--> before writing JSON config to node {} with path {}", node, tempFilePath);
        logger.info(jsonWithVersion);

        Files.writeString(tempFilePath, jsonWithVersion);
        int retryCount = 0;
        do {
            try {
                // this can fail on Windows because of timing
                Files.move(tempFilePath, targetPath, StandardCopyOption.ATOMIC_MOVE);
                logger.info("--> after writing JSON config to node {} with path {}", node, targetPath);
                return;
            } catch (IOException e) {
                logger.info("--> retrying writing a settings file [{}]", retryCount);
                if (retryCount == 4) { // retry 5 times
                    throw e;
                }
                Thread.sleep(retryDelay(retryCount));
                retryCount++;
            }
        } while (true);
    }

    private static long retryDelay(int retryCount) {
        return 100 * (1 << retryCount) + Randomness.get().nextInt(10);
    }

    private Tuple<CountDownLatch, AtomicLong> setupCleanupClusterStateListener(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null) {
                    ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedClusterSettingsAction.NAME);
                    if (handlerMetadata != null && handlerMetadata.keys().contains("indices.recovery.max_bytes_per_sec") == false) {
                        clusterService.removeListener(this);
                        metadataVersion.set(event.state().metadata().version());
                        savedClusterState.countDown();
                    }
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListener(String node, long version) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null && reservedState.version() == version) {
                    clusterService.removeListener(this);
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertClusterStateSaveOK(CountDownLatch savedClusterState, AtomicLong metadataVersion, String expectedBytesPerSec)
        throws Exception {
        assertTrue(savedClusterState.await(20, TimeUnit.SECONDS));
        assertExpectedRecoveryBytesSettingAndVersion(metadataVersion, expectedBytesPerSec);
    }

    private static void assertExpectedRecoveryBytesSettingAndVersion(AtomicLong metadataVersion, String expectedBytesPerSec) {
        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(metadataVersion.get())
        ).actionGet();

        assertThat(
            clusterStateResponse.getState().metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()),
            equalTo(expectedBytesPerSec)
        );

        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "1234kb")
        );
        assertThat(
            expectThrows(ExecutionException.class, () -> clusterAdmin().updateSettings(req).get()).getMessage(),
            is(
                "java.lang.IllegalArgumentException: Failed to process request "
                    + "[org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest/unset] "
                    + "with errors: [[indices.recovery.max_bytes_per_sec] set as read-only by [file_settings]]"
            )
        );
    }

    public void testSettingsApplied() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        FileSettingsService dataFileSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var savedClusterState = setupClusterStateListener(masterNode, versionCounter.incrementAndGet());

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));
        assertFalse(dataFileSettingsService.watching());

        writeJSONFile(masterNode, testJSON, logger, versionCounter.get());
        assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2(), "50mb");
    }

    public void testSettingsAppliedOnStart() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        FileSettingsService dataFileSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());
        var savedClusterState = setupClusterStateListener(dataNode, versionCounter.incrementAndGet());

        // In internal cluster tests, the nodes share the config directory, so when we write with the data node path
        // the master will pick it up on start
        writeJSONFile(dataNode, testJSON, logger, versionCounter.get());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));
        assertFalse(dataFileSettingsService.watching());

        assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2(), "50mb");
    }

    public void testReservedStatePersistsOnRestart() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode(
            Settings.builder().put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
        );
        assertMasterNode(internalCluster().masterClient(), masterNode);
        var savedClusterState = setupClusterStateListener(masterNode, versionCounter.incrementAndGet());

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));

        logger.info("--> write some settings");
        writeJSONFile(masterNode, testJSON, logger, versionCounter.get());
        assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2(), "50mb");

        logger.info("--> restart master");
        internalCluster().restartNode(masterNode);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)).actionGet();
        assertThat(
            clusterStateResponse.getState()
                .metadata()
                .reservedStateMetadata()
                .get(FileSettingsService.NAMESPACE)
                .handlers()
                .get(ReservedClusterSettingsAction.NAME)
                .keys(),
            hasSize(1)
        );
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListenerForError(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
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
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertClusterStateNotSaved(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(metadataVersion.get())
        ).actionGet();

        assertThat(clusterStateResponse.getState().metadata().persistentSettings().get("search.allow_expensive_queries"), nullValue());

        // This should succeed, nothing was reserved
        updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", "false"));
    }

    public void testErrorSaved() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        FileSettingsService dataFileSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode(
            Settings.builder().put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
        );
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var savedClusterState = setupClusterStateListenerForError(masterNode);

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));
        assertFalse(dataFileSettingsService.watching());

        writeJSONFile(masterNode, testErrorJSON, logger, versionCounter.incrementAndGet());
        assertClusterStateNotSaved(savedClusterState.v1(), savedClusterState.v2());
    }

    public void testErrorCanRecoverOnRestart() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        FileSettingsService dataFileSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode(
            Settings.builder().put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
        );
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var savedClusterState = setupClusterStateListenerForError(masterNode);

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));
        assertFalse(dataFileSettingsService.watching());

        writeJSONFile(masterNode, testErrorJSON, logger, versionCounter.incrementAndGet());
        AtomicLong metadataVersion = savedClusterState.v2();
        assertClusterStateNotSaved(savedClusterState.v1(), metadataVersion);
        assertHasErrors(metadataVersion, "not_cluster_settings");

        // write valid json without version increment to simulate ES being able to process settings after a restart (usually, this would be
        // due to a code change)
        writeJSONFile(masterNode, testJSON, logger, versionCounter.get());
        internalCluster().restartNode(masterNode);
        ensureGreen();

        // we don't know the exact metadata version to wait for so rely on an assertBusy instead
        assertBusy(() -> assertExpectedRecoveryBytesSettingAndVersion(metadataVersion, "50mb"));
        assertBusy(() -> assertNoErrors(metadataVersion));
    }

    public void testNewErrorOnRestartReprocessing() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        FileSettingsService dataFileSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode(
            Settings.builder().put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
        );
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var savedClusterState = setupClusterStateListenerForError(masterNode);

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));
        assertFalse(dataFileSettingsService.watching());

        writeJSONFile(masterNode, testErrorJSON, logger, versionCounter.incrementAndGet());
        AtomicLong metadataVersion = savedClusterState.v2();
        assertClusterStateNotSaved(savedClusterState.v1(), metadataVersion);
        assertHasErrors(metadataVersion, "not_cluster_settings");

        // write json with new error without version increment to simulate ES failing to process settings after a restart for a new reason
        // (usually, this would be due to a code change)
        writeJSONFile(masterNode, testOtherErrorJSON, logger, versionCounter.get());
        assertHasErrors(metadataVersion, "not_cluster_settings");
        internalCluster().restartNode(masterNode);
        ensureGreen();

        assertBusy(() -> assertHasErrors(metadataVersion, "bad_cluster_settings"));
    }

    public void testSettingsAppliedOnMasterReElection() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();

        logger.info("--> start master eligible nodes, 2 more for quorum");
        String masterNode1 = internalCluster().startNode(Settings.builder().put(masterNode()).put("discovery.initial_state_timeout", "1s"));
        String masterNode2 = internalCluster().startNode(Settings.builder().put(masterNode()).put("discovery.initial_state_timeout", "1s"));
        internalCluster().validateClusterFormed();
        FileSettingsService master1FS = internalCluster().getInstance(FileSettingsService.class, masterNode1);
        FileSettingsService master2FS = internalCluster().getInstance(FileSettingsService.class, masterNode2);

        assertFalse(master1FS.watching());
        assertFalse(master2FS.watching());

        var savedClusterState = setupClusterStateListener(masterNode, versionCounter.incrementAndGet());
        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));

        writeJSONFile(masterNode, testJSON, logger, versionCounter.get());
        assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2(), "50mb");

        internalCluster().stopCurrentMasterNode();
        internalCluster().validateClusterFormed();
        ensureStableCluster(2);

        FileSettingsService masterFS = internalCluster().getCurrentMasterNodeInstance(FileSettingsService.class);
        assertTrue(masterFS.watching());
        logger.info("--> start another master eligible node to form a quorum");
        internalCluster().startNode(Settings.builder().put(masterNode()).put("discovery.initial_state_timeout", "1s"));
        internalCluster().validateClusterFormed();
        ensureStableCluster(3);

        savedClusterState = setupCleanupClusterStateListener(internalCluster().getMasterName());
        writeJSONFile(internalCluster().getMasterName(), testCleanupJSON, logger, versionCounter.incrementAndGet());

        boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        savedClusterState = setupClusterStateListener(internalCluster().getMasterName(), versionCounter.incrementAndGet());
        writeJSONFile(internalCluster().getMasterName(), testJSON43mb, logger, versionCounter.get());

        assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2(), "43mb");
    }

    public void testSymlinkUpdateTriggerReload() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNode = internalCluster().startMasterOnlyNode();
        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);
        Path baseDir = masterFileSettingsService.watchedFileDir();
        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));

        {
            var savedClusterState = setupClusterStateListener(masterNode, versionCounter.incrementAndGet());
            // Create the settings.json as a symlink to simulate k8 setup
            // settings.json -> ..data/settings.json
            // ..data -> ..TIMESTAMP_TEMP_FOLDER_1
            var fileDir = Files.createDirectories(baseDir.resolve("..TIMESTAMP_TEMP_FOLDER_1"));
            writeJSONFile(masterNode, testJSON, logger, versionCounter.get(), fileDir.resolve("settings.json"));
            var dataDir = Files.createSymbolicLink(baseDir.resolve("..data"), fileDir.getFileName());
            Files.createSymbolicLink(baseDir.resolve("settings.json"), dataDir.getFileName().resolve("settings.json"));
            assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2(), "50mb");
        }
        {
            var savedClusterState = setupClusterStateListener(masterNode, versionCounter.incrementAndGet());
            // Update ..data symlink to ..data -> ..TIMESTAMP_TEMP_FOLDER_2 to simulate kubernetes secret update
            var fileDir = Files.createDirectories(baseDir.resolve("..TIMESTAMP_TEMP_FOLDER_2"));
            writeJSONFile(masterNode, testJSON43mb, logger, versionCounter.get(), fileDir.resolve("settings.json"));
            Files.deleteIfExists(baseDir.resolve("..data"));
            Files.createSymbolicLink(baseDir.resolve("..data"), fileDir.getFileName());
            assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2(), "43mb");
        }
    }

    public void testHealthIndicatorWithSingleNode() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start the node");
        String nodeName = internalCluster().startNode();
        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, nodeName);
        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));

        ensureStableCluster(1);

        testHealthIndicatorOnError(nodeName, nodeName);
    }

    public void testHealthIndicatorWithSeparateHealthNode() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start a data node to act as the health node");
        String healthNode = internalCluster().startNode(
            Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s")
        );

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode(
            Settings.builder().put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s").build()
        );
        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);
        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));

        ensureStableCluster(2);

        testHealthIndicatorOnError(masterNode, healthNode);
    }

    /**
     * {@code masterNode} and {@code healthNode} can be the same node.
     */
    private void testHealthIndicatorOnError(String masterNode, String healthNode) throws Exception {
        logger.info("--> ensure all is well before the error");
        assertBusy(() -> {
            FetchHealthInfoCacheAction.Response healthNodeResponse = client().execute(
                FetchHealthInfoCacheAction.INSTANCE,
                new FetchHealthInfoCacheAction.Request()
            ).get();
            assertEquals(0, healthNodeResponse.getHealthInfo().fileSettingsHealthInfo().failureStreak());
        });

        logger.info("--> induce an error and wait for it to be processed");
        var savedClusterState = setupClusterStateListenerForError(masterNode);
        writeJSONFile(masterNode, testErrorJSON, logger, versionCounter.incrementAndGet());
        boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        logger.info("--> ensure the health node also reports it");
        assertBusy(() -> {
            FetchHealthInfoCacheAction.Response healthNodeResponse = client().execute(
                FetchHealthInfoCacheAction.INSTANCE,
                new FetchHealthInfoCacheAction.Request()
            ).get();
            assertEquals(
                "Cached info on health node should report one failure",
                1,
                healthNodeResponse.getHealthInfo().fileSettingsHealthInfo().failureStreak()
            );

            for (var node : Stream.of(masterNode, healthNode).distinct().toList()) {
                GetHealthAction.Response getHealthResponse = client(node).execute(
                    GetHealthAction.INSTANCE,
                    new GetHealthAction.Request(false, 123)
                ).get();
                assertEquals(
                    "Health should be yellow on node " + node,
                    YELLOW,
                    getHealthResponse.findIndicator(FileSettingsService.FileSettingsHealthIndicatorService.NAME).status()
                );
            }
        });
    }

    private void assertHasErrors(AtomicLong waitForMetadataVersion, String expectedError) {
        var errorMetadata = getErrorMetadata(waitForMetadataVersion);
        assertThat(errorMetadata, is(notNullValue()));
        assertThat(errorMetadata.errors(), containsInAnyOrder(containsString(expectedError)));
    }

    private void assertNoErrors(AtomicLong waitForMetadataVersion) {
        var errorMetadata = getErrorMetadata(waitForMetadataVersion);
        assertThat(errorMetadata, is(nullValue()));
    }

    private ReservedStateErrorMetadata getErrorMetadata(AtomicLong waitForMetadataVersion) {
        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(waitForMetadataVersion.get())
        ).actionGet();
        return clusterStateResponse.getState().getMetadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE).errorMetadata();
    }
}
