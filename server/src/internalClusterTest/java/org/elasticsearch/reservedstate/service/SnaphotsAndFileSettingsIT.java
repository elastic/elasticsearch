/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotState;
import org.junit.After;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that snapshot restore behaves correctly when we have file based settings that reserve part of the
 * cluster state
 */
public class SnaphotsAndFileSettingsIT extends AbstractSnapshotIntegTestCase {
    private static AtomicLong versionCounter = new AtomicLong(1);

    private static String testFileSettingsJSON = """
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

    private static String emptyFileSettingsJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "cluster_settings": {}
             }
        }""";

    @After
    public void cleanUp() throws Exception {
        awaitNoMoreRunningOperations();
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
                    if (handlerMetadata == null) {
                        fail("Should've found cluster settings in this metadata");
                    }
                    if (handlerMetadata.keys().contains("indices.recovery.max_bytes_per_sec")) {
                        clusterService.removeListener(this);
                        metadataVersion.set(event.state().metadata().version());
                        savedClusterState.countDown();
                    }
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private ClusterStateResponse assertClusterStateSaveOK(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        return clusterAdmin().state(new ClusterStateRequest().waitForMetadataVersion(metadataVersion.get())).actionGet();
    }

    public void testRestoreWithRemovedFileSettings() throws Exception {
        try {
            createRepository("test-repo", "fs");

            logger.info("--> set some persistent cluster settings");
            assertAcked(
                clusterAdmin().prepareUpdateSettings()
                    .setPersistentSettings(
                        Settings.builder()
                            .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(25))
                            .build()
                    )
            );

            ensureGreen();

            String masterNode = internalCluster().getMasterName();

            var savedClusterState = setupClusterStateListener(masterNode);
            FileSettingsService fs = internalCluster().getInstance(FileSettingsService.class, masterNode);

            logger.info("--> write some file based settings, putting some reserved state");
            writeJSONFile(masterNode, testFileSettingsJSON);
            final ClusterStateResponse savedStateResponse = assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2());
            assertThat(
                savedStateResponse.getState().metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()),
                equalTo("50mb")
            );

            logger.info("--> create full snapshot");
            createFullSnapshot("test-repo", "test-snap");
            assertThat(getSnapshot("test-repo", "test-snap").state(), equalTo(SnapshotState.SUCCESS));

            assertAcked(
                clusterAdmin().prepareUpdateSettings()
                    .setPersistentSettings(
                        Settings.builder()
                            .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(55))
                            .build()
                    )
            );

            logger.info("--> deleting operator file, no file based settings");
            Files.delete(fs.operatorSettingsFile());

            logger.info("--> restore global state from the snapshot");
            clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap").setRestoreGlobalState(true).setWaitForCompletion(true).get();

            ensureGreen();

            final ClusterStateResponse clusterStateResponse = clusterAdmin().state(new ClusterStateRequest().metadata(true)).actionGet();

            // We expect no reserved metadata state for file based settings, the operator file was deleted.
            assertNull(clusterStateResponse.getState().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE));

            final ClusterGetSettingsAction.Response getSettingsResponse = clusterAdmin().execute(
                ClusterGetSettingsAction.INSTANCE,
                new ClusterGetSettingsAction.Request()
            ).actionGet();

            assertThat(
                getSettingsResponse.persistentSettings().get(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey()),
                equalTo("25s")
            );
            // We didn't remove the setting set by file settings, we simply removed the reserved (operator) section.
            assertThat(getSettingsResponse.persistentSettings().get("indices.recovery.max_bytes_per_sec"), equalTo("50mb"));
        } finally {
            // cleanup
            assertAcked(
                clusterAdmin().prepareUpdateSettings()
                    .setPersistentSettings(
                        Settings.builder()
                            .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey(), (String) null)
                            .put("indices.recovery.max_bytes_per_sec", (String) null)
                            .build()
                    )
            );
        }
    }

    private Tuple<CountDownLatch, AtomicLong> removedReservedClusterStateListener(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null && reservedState.version() == 0L) {
                    clusterService.removeListener(this);
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private Tuple<CountDownLatch, AtomicLong> cleanedClusterStateListener(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null) {
                    ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedClusterSettingsAction.NAME);
                    if (handlerMetadata == null) {
                        fail("Should've found cluster settings in this metadata");
                    }
                    if (handlerMetadata.keys().isEmpty()) {
                        clusterService.removeListener(this);
                        metadataVersion.set(event.state().metadata().version());
                        savedClusterState.countDown();
                    }
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    public void testRestoreWithPersistedFileSettings() throws Exception {
        try {
            createRepository("test-repo", "fs");

            logger.info("--> set some persistent cluster settings");
            assertAcked(
                clusterAdmin().prepareUpdateSettings()
                    .setPersistentSettings(
                        Settings.builder()
                            .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(25))
                            .build()
                    )
            );

            ensureGreen();

            String masterNode = internalCluster().getMasterName();

            var savedClusterState = setupClusterStateListener(masterNode);
            FileSettingsService fs = internalCluster().getInstance(FileSettingsService.class, masterNode);

            logger.info("--> write some file based settings, putting some reserved state");
            writeJSONFile(masterNode, testFileSettingsJSON);
            final ClusterStateResponse savedStateResponse = assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2());
            assertThat(
                savedStateResponse.getState().metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()),
                equalTo("50mb")
            );

            logger.info("--> create full snapshot");
            createFullSnapshot("test-repo", "test-snap");
            assertThat(getSnapshot("test-repo", "test-snap").state(), equalTo(SnapshotState.SUCCESS));

            assertAcked(
                clusterAdmin().prepareUpdateSettings()
                    .setPersistentSettings(
                        Settings.builder()
                            .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(55))
                            .build()
                    )
            );

            logger.info("--> restore global state from the snapshot");
            var removedReservedState = removedReservedClusterStateListener(masterNode);
            var restoredReservedState = setupClusterStateListener(masterNode);

            clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap").setRestoreGlobalState(true).setWaitForCompletion(true).get();

            ensureGreen();

            // When the target cluster of a restore has an existing operator file, we don't un-reserve the reserved
            // cluster state for file based settings, but instead we reset the version to 0 and 'touch' the operator file
            // so that it gets re-processed.
            logger.info("--> reserved state version will be reset to 0, because of snapshot restore");
            assertTrue(removedReservedState.v1().await(20, TimeUnit.SECONDS));

            logger.info("--> reserved state would be restored");
            assertTrue(restoredReservedState.v1().await(20, TimeUnit.SECONDS));

            final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
                new ClusterStateRequest().metadata(true).waitForMetadataVersion(restoredReservedState.v2().get())
            ).actionGet();

            assertNotNull(clusterStateResponse.getState().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE));

            final ClusterGetSettingsAction.Response getSettingsResponse = clusterAdmin().execute(
                ClusterGetSettingsAction.INSTANCE,
                new ClusterGetSettingsAction.Request()
            ).actionGet();

            assertThat(
                getSettingsResponse.persistentSettings().get(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey()),
                equalTo("25s")
            );

            // we need to remove the reserved state, so that clean-up can happen
            var cleanupReservedState = cleanedClusterStateListener(masterNode);

            logger.info("--> clear the file based settings");
            writeJSONFile(masterNode, emptyFileSettingsJSON);
            assertClusterStateSaveOK(cleanupReservedState.v1(), cleanupReservedState.v2());
        } finally {
            // cleanup
            assertAcked(
                clusterAdmin().prepareUpdateSettings()
                    .setPersistentSettings(
                        Settings.builder()
                            .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey(), (String) null)
                            .put("indices.recovery.max_bytes_per_sec", (String) null)
                            .build()
                    )
            );
        }
    }

}
