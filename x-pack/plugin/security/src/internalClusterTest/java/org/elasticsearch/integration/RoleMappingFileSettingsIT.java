/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
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
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsRequest;
import org.elasticsearch.xpack.security.action.rolemapping.ReservedRoleMappingAction;
import org.junit.After;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests that file settings service can properly add role mappings and detect REST clashes
 * with the reserved role mappings.
 */
public class RoleMappingFileSettingsIT extends NativeRealmIntegTestCase {

    private static AtomicLong versionCounter = new AtomicLong(1);

    private static String emptyJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                "cluster_settings": {},
                "role_mappings": {}
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
                 },
                 "role_mappings": {
                       "everyone_kibana": {
                          "enabled": true,
                          "roles": [ "kibana_user" ],
                          "rules": { "field": { "username": "*" } },
                          "metadata": {
                             "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7",
                             "_foo": "something"
                          }
                       },
                       "everyone_fleet": {
                          "enabled": true,
                          "roles": [ "fleet_user" ],
                          "rules": { "field": { "username": "*" } },
                          "metadata": {
                             "uuid" : "b9a59ba9-6b92-4be3-bb8d-02bb270cb3a7",
                             "_foo": "something_else"
                          }
                       }
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
                 "role_mappings": {
                       "everyone_fleet_ok": {
                          "enabled": true,
                          "roles": [ "fleet_user" ],
                          "rules": { "field": { "username": "*" } },
                          "metadata": {
                             "uuid" : "b9a59ba9-6b92-4be3-bb8d-02bb270cb3a7"
                          }
                       },
                       "everyone_kibana_bad": {
                          "enabled": true,
                          "roles": [ "kibana_user" ],
                          "metadata": {
                             "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7"
                          }
                       }
                 }
             }
        }""";

    @After
    public void cleanUp() {
        updateClusterSettings(Settings.builder().putNull("indices.recovery.max_bytes_per_sec"));
    }

    private void writeJSONFile(String node, String json) throws Exception {
        long version = versionCounter.incrementAndGet();

        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);
        assertTrue(fileSettingsService.watching());

        Files.deleteIfExists(fileSettingsService.watchedFile());

        Files.createDirectories(fileSettingsService.watchedFileDir());
        Path tempFilePath = createTempFile();

        logger.info("--> BEFORE writing JSON config to node {} with path {}", node, tempFilePath);
        logger.info(Strings.format(json, version));
        Files.write(tempFilePath, Strings.format(json, version).getBytes(StandardCharsets.UTF_8));
        Files.move(tempFilePath, fileSettingsService.watchedFile(), StandardCopyOption.ATOMIC_MOVE);
        logger.info("--> AFTER writing JSON config to node {} with path {}", node, tempFilePath);
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListener(String node, String expectedKey) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null) {
                    ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedRoleMappingAction.NAME);
                    if (handlerMetadata != null && handlerMetadata.keys().contains(expectedKey)) {
                        clusterService.removeListener(this);
                        metadataVersion.set(event.state().metadata().version());
                        savedClusterState.countDown();
                    }
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListenerForCleanup(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null) {
                    ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedClusterSettingsAction.NAME);
                    if (handlerMetadata == null || handlerMetadata.keys().isEmpty()) {
                        clusterService.removeListener(this);
                        metadataVersion.set(event.state().metadata().version());
                        savedClusterState.countDown();
                    }
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertRoleMappingsSaveOK(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest().waitForMetadataVersion(metadataVersion.get())
        ).get();

        ReservedStateMetadata reservedState = clusterStateResponse.getState()
            .metadata()
            .reservedStateMetadata()
            .get(FileSettingsService.NAMESPACE);

        ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedRoleMappingAction.NAME);
        assertThat(handlerMetadata.keys(), allOf(notNullValue(), containsInAnyOrder("everyone_kibana", "everyone_fleet")));

        ReservedStateHandlerMetadata clusterStateHandlerMetadata = reservedState.handlers().get(ReservedClusterSettingsAction.NAME);
        assertThat(
            clusterStateHandlerMetadata.keys(),
            allOf(notNullValue(), containsInAnyOrder(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()))
        );

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
            expectThrows(ExecutionException.class, () -> clusterAdmin().updateSettings(req).get()).getMessage()
        );

        var request = new GetRoleMappingsRequest();
        request.setNames("everyone_kibana", "everyone_fleet");
        var response = client().execute(GetRoleMappingsAction.INSTANCE, request).get();
        // the native role mappings are not influenced by the cluster-state ones
        assertThat(response.hasMappings(), is(false));
        assertThat(response.mappings(), emptyArray());

        // TODO
        // 1. assert role mapping is effective
        // 2. native role mappings can be created
    }

    public void testRoleMappingsApplied() throws Exception {
        ensureGreen();

        var savedClusterState = setupClusterStateListener(internalCluster().getMasterName(), "everyone_kibana");
        writeJSONFile(internalCluster().getMasterName(), testJSON);

        assertRoleMappingsSaveOK(savedClusterState.v1(), savedClusterState.v2());
        logger.info("---> cleanup cluster settings...");

        savedClusterState = setupClusterStateListenerForCleanup(internalCluster().getMasterName());

        writeJSONFile(internalCluster().getMasterName(), emptyJSON);
        boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest().waitForMetadataVersion(savedClusterState.v2().get())
        ).get();

        assertNull(
            clusterStateResponse.getState().metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey())
        );

        var request = new GetRoleMappingsRequest();
        request.setNames("everyone_kibana", "everyone_fleet");
        var response = client().execute(GetRoleMappingsAction.INSTANCE, request).get();
        assertFalse(response.hasMappings());
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListenerForError(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null
                    && reservedState.errorMetadata() != null
                    && reservedState.errorMetadata().errorKind() == ReservedStateErrorMetadata.ErrorKind.PARSING) {
                    clusterService.removeListener(this);
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                    assertEquals(ReservedStateErrorMetadata.ErrorKind.PARSING, reservedState.errorMetadata().errorKind());
                    assertThat(reservedState.errorMetadata().errors(), allOf(notNullValue(), hasSize(1)));
                    assertThat(
                        reservedState.errorMetadata().errors().get(0),
                        containsString("failed to parse role-mapping [everyone_kibana_bad]. missing field [rules]")
                    );
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertRoleMappingsNotSaved(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        // TODO
        // assert role mapping are not effective
    }

    public void testErrorSaved() throws Exception {
        ensureGreen();

        // save an empty file to clear any prior state, this ensures we don't get a stale file left over by another test
        var savedClusterState = setupClusterStateListenerForCleanup(internalCluster().getMasterName());

        writeJSONFile(internalCluster().getMasterName(), emptyJSON);
        boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest().waitForMetadataVersion(savedClusterState.v2().get())
        ).get();

        assertNull(
            clusterStateResponse.getState().metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey())
        );

        // save a bad file
        savedClusterState = setupClusterStateListenerForError(internalCluster().getMasterName());

        writeJSONFile(internalCluster().getMasterName(), testErrorJSON);
        assertRoleMappingsNotSaved(savedClusterState.v1(), savedClusterState.v2());
    }
}
