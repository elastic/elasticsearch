/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotState;
import org.junit.After;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that snapshot restore behaves correctly when we have file based settings that reserve part of the
 * cluster state
 */
@LuceneTestCase.SuppressFileSystems("*")
public class SnapshotsAndFileSettingsIT extends AbstractSnapshotIntegTestCase {
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

    private long retryDelay(int retryCount) {
        return 100 * (1 << retryCount) + Randomness.get().nextInt(10);
    }

    private void writeJSONFile(String node, String json) throws Exception {
        long version = versionCounter.incrementAndGet();

        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);

        Files.createDirectories(fileSettingsService.watchedFileDir());
        Path tempFilePath = createTempFile();

        Files.write(tempFilePath, Strings.format(json, version).getBytes(StandardCharsets.UTF_8));
        int retryCount = 0;
        do {
            try {
                // this can fail on Windows because of timing
                Files.move(tempFilePath, fileSettingsService.watchedFile(), StandardCopyOption.ATOMIC_MOVE);
                return;
            } catch (IOException e) {
                logger.info("--> retrying writing a settings file [" + retryCount + "]");
                if (retryCount == 4) { // retry 5 times
                    throw e;
                }
                Thread.sleep(retryDelay(retryCount));
                retryCount++;
            }
        } while (true);
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListener(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null && reservedState.version() != 0L) {
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

    private ClusterStateResponse assertClusterStateSaveOK(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        return clusterAdmin().state(new ClusterStateRequest().waitForMetadataVersion(metadataVersion.get())).get();
    }

    public void testRestoreWithRemovedFileSettings() throws Exception {
        createRepository("test-repo", "fs");

        logger.info("--> set some persistent cluster settings");
        updateClusterSettings(
            Settings.builder()
                .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(25))
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

        updateClusterSettings(
            Settings.builder()
                .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(55))
        );

        logger.info("--> deleting operator file, no file based settings");
        Files.delete(fs.watchedFile());

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
        // cleanup
        updateClusterSettings(
            Settings.builder()
                .putNull(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey())
                .putNull("indices.recovery.max_bytes_per_sec")
        );
    }

    private Tuple<CountDownLatch, AtomicLong> removedReservedClusterStateListener(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(2);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                // we first wait for reserved state version to become 0, then we expect to see it non-zero
                if (reservedState != null && reservedState.version() == 0L) {
                    // don't remove the state listener yet, we need it to see the version non-zero
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                } else if (reservedState != null && reservedState.version() != 0L && savedClusterState.getCount() < 2) {
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
        createRepository("test-repo", "fs");

        logger.info("--> set some persistent cluster settings");
        updateClusterSettings(
            Settings.builder()
                .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(25))
        );

        ensureGreen();

        String masterNode = internalCluster().getMasterName();

        var savedClusterState = setupClusterStateListener(masterNode);

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

        updateClusterSettings(
            Settings.builder()
                .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(55))
        );

        logger.info("--> restore global state from the snapshot");
        var removedReservedState = removedReservedClusterStateListener(masterNode);

        clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap").setRestoreGlobalState(true).setWaitForCompletion(true).get();

        ensureGreen();

        // When the target cluster of a restore has an existing operator file, we don't un-reserve the reserved
        // cluster state for file based settings, but instead we reset the version to 0 and 'touch' the operator file
        // so that it gets re-processed.
        logger.info("--> reserved state version will be reset to 0, because of snapshot restore");
        // double timeout, we restore snapshot then apply the file
        assertTrue(removedReservedState.v1().await(40, TimeUnit.SECONDS));

        logger.info("--> reserved state would be restored to non-zero version");

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest().metadata(true).waitForMetadataVersion(removedReservedState.v2().get())
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
        // cleanup
        updateClusterSettings(
            Settings.builder()
                .putNull(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey())
                .putNull("indices.recovery.max_bytes_per_sec")
        );
    }
}
