/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class FileSettingsServiceIT extends ESIntegTestCase {

    private static AtomicLong versionCounter = new AtomicLong(1);

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

    private void assertMasterNode(Client client, String node) {
        assertThat(
            client.admin().cluster().prepareState().execute().actionGet().getState().nodes().getMasterNode().getName(),
            equalTo(node)
        );
    }

    private void writeJSONFile(String node, String json) throws Exception {
        long version = versionCounter.incrementAndGet();

        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);

        Files.createDirectories(fileSettingsService.operatorSettingsDir());
        Path tempFilePath = createTempFile();

        Files.write(tempFilePath, Strings.format(json, version).getBytes(StandardCharsets.UTF_8));
        Files.move(tempFilePath, fileSettingsService.operatorSettingsFile(), StandardCopyOption.ATOMIC_MOVE);
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
                    ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedClusterSettingsAction.NAME);
                    if (handlerMetadata != null && handlerMetadata.keys().contains("indices.recovery.max_bytes_per_sec")) {
                        clusterService.removeListener(this);
                        metadataVersion.set(event.state().metadata().version());
                        savedClusterState.countDown();
                    }
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertClusterStateSaveOK(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = client().admin()
            .cluster()
            .state(new ClusterStateRequest().waitForMetadataVersion(metadataVersion.get()))
            .actionGet();

        assertThat(
            clusterStateResponse.getState().metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()),
            equalTo("50mb")
        );

        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest().persistentSettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "1234kb")
        );
        assertEquals(
            "java.lang.IllegalArgumentException: Failed to process request "
                + "[org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest/unset] "
                + "with errors: [[indices.recovery.max_bytes_per_sec] set as read-only by [file_settings]]",
            expectThrows(ExecutionException.class, () -> client().admin().cluster().updateSettings(req).get()).getMessage()
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
        var savedClusterState = setupClusterStateListener(masterNode);

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertTrue(masterFileSettingsService.watching());
        assertFalse(dataFileSettingsService.watching());

        writeJSONFile(masterNode, testJSON);
        assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2());
    }

    public void testSettingsAppliedOnStart() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        FileSettingsService dataFileSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());
        var savedClusterState = setupClusterStateListener(dataNode);

        // In internal cluster tests, the nodes share the config directory, so when we write with the data node path
        // the master will pick it up on start
        writeJSONFile(dataNode, testJSON);

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertTrue(masterFileSettingsService.watching());
        assertFalse(dataFileSettingsService.watching());

        assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2());
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

        final ClusterStateResponse clusterStateResponse = client().admin()
            .cluster()
            .state(new ClusterStateRequest().waitForMetadataVersion(metadataVersion.get()))
            .actionGet();

        assertThat(clusterStateResponse.getState().metadata().persistentSettings().get("search.allow_expensive_queries"), nullValue());

        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest().persistentSettings(
            Settings.builder().put("search.allow_expensive_queries", "false")
        );
        // This should succeed, nothing was reserved
        client().admin().cluster().updateSettings(req).get();
    }

    public void testErrorSaved() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        FileSettingsService dataFileSettingsService = internalCluster().getInstance(FileSettingsService.class, dataNode);

        assertFalse(dataFileSettingsService.watching());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        var savedClusterState = setupClusterStateListenerForError(masterNode);

        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);

        assertTrue(masterFileSettingsService.watching());
        assertFalse(dataFileSettingsService.watching());

        writeJSONFile(masterNode, testErrorJSON);
        assertClusterStateNotSaved(savedClusterState.v1(), savedClusterState.v2());
    }
}
