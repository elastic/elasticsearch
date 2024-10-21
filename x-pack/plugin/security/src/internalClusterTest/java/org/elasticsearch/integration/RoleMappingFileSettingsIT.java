/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
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
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.security.action.rolemapping.ReservedRoleMappingAction;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.xcontent.XContentType.JSON;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

/**
 * Tests that file settings service can properly add role mappings.
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

    public static void writeJSONFile(String node, String json, Logger logger, AtomicLong versionCounter) throws Exception {
        writeJSONFile(node, json, logger, versionCounter, true);
    }

    public static void writeJSONFileWithoutVersionIncrement(String node, String json, Logger logger, AtomicLong versionCounter)
        throws Exception {
        writeJSONFile(node, json, logger, versionCounter, false);
    }

    private static void writeJSONFile(String node, String json, Logger logger, AtomicLong versionCounter, boolean incrementVersion)
        throws Exception {
        long version = incrementVersion ? versionCounter.incrementAndGet() : versionCounter.get();

        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);
        assertTrue(fileSettingsService.watching());

        Files.deleteIfExists(fileSettingsService.watchedFile());

        Files.createDirectories(fileSettingsService.watchedFileDir());
        Path tempFilePath = createTempFile();

        logger.info("--> before writing JSON config to node {} with path {}", node, tempFilePath);
        logger.info(Strings.format(json, version));
        Files.write(tempFilePath, Strings.format(json, version).getBytes(StandardCharsets.UTF_8));
        Files.move(tempFilePath, fileSettingsService.watchedFile(), StandardCopyOption.ATOMIC_MOVE);
        logger.info("--> after writing JSON config to node {} with path {}", node, tempFilePath);
    }

    public static Tuple<CountDownLatch, AtomicLong> setupClusterStateListener(String node, String expectedKey) {
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

    public static Tuple<CountDownLatch, AtomicLong> setupClusterStateListenerForCleanup(String node) {
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
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(metadataVersion.get())
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

        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "1234kb")
        );
        assertEquals(
            "java.lang.IllegalArgumentException: Failed to process request "
                + "[org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest/unset] "
                + "with errors: [[indices.recovery.max_bytes_per_sec] set as read-only by [file_settings]]",
            expectThrows(ExecutionException.class, () -> clusterAdmin().updateSettings(req).get()).getMessage()
        );

        for (UserRoleMapper userRoleMapper : internalCluster().getInstances(UserRoleMapper.class)) {
            PlainActionFuture<Set<String>> resolveRolesFuture = new PlainActionFuture<>();
            userRoleMapper.resolveRoles(
                new UserRoleMapper.UserData("anyUsername", null, List.of(), Map.of(), mock(RealmConfig.class)),
                resolveRolesFuture
            );
            assertThat(resolveRolesFuture.get(), containsInAnyOrder("kibana_user", "fleet_user"));
        }

        // the role mappings are not retrievable by the role mapping action (which only accesses "native" i.e. index-based role mappings)
        var request = new GetRoleMappingsRequest();
        request.setNames("everyone_kibana", "everyone_fleet");
        var response = client().execute(GetRoleMappingsAction.INSTANCE, request).get();
        assertFalse(response.hasMappings());
        assertThat(response.mappings(), emptyArray());

        // role mappings (with the same names) can also be stored in the "native" store
        var putRoleMappingResponse = client().execute(PutRoleMappingAction.INSTANCE, sampleRestRequest("everyone_kibana")).actionGet();
        assertTrue(putRoleMappingResponse.isCreated());
        putRoleMappingResponse = client().execute(PutRoleMappingAction.INSTANCE, sampleRestRequest("everyone_fleet")).actionGet();
        assertTrue(putRoleMappingResponse.isCreated());
    }

    public void testRoleMappingsApplied() throws Exception {
        ensureGreen();

        var savedClusterState = setupClusterStateListener(internalCluster().getMasterName(), "everyone_kibana");
        writeJSONFile(internalCluster().getMasterName(), testJSON, logger, versionCounter);

        assertRoleMappingsSaveOK(savedClusterState.v1(), savedClusterState.v2());
        logger.info("---> cleanup cluster settings...");

        savedClusterState = setupClusterStateListenerForCleanup(internalCluster().getMasterName());

        writeJSONFile(internalCluster().getMasterName(), emptyJSON, logger, versionCounter);
        boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(savedClusterState.v2().get())
        ).get();

        assertNull(
            clusterStateResponse.getState().metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey())
        );

        // native role mappings are not affected by the removal of the cluster-state based ones
        {
            var request = new GetRoleMappingsRequest();
            request.setNames("everyone_kibana", "everyone_fleet");
            var response = client().execute(GetRoleMappingsAction.INSTANCE, request).get();
            assertTrue(response.hasMappings());
            assertThat(
                Arrays.stream(response.mappings()).map(ExpressionRoleMapping::getName).toList(),
                containsInAnyOrder("everyone_kibana", "everyone_fleet")
            );
        }

        // and roles are resolved based on the native role mappings
        for (UserRoleMapper userRoleMapper : internalCluster().getInstances(UserRoleMapper.class)) {
            PlainActionFuture<Set<String>> resolveRolesFuture = new PlainActionFuture<>();
            userRoleMapper.resolveRoles(
                new UserRoleMapper.UserData("anyUsername", null, List.of(), Map.of(), mock(RealmConfig.class)),
                resolveRolesFuture
            );
            assertThat(resolveRolesFuture.get(), contains("kibana_user_native"));
        }

        {
            var request = new DeleteRoleMappingRequest();
            request.setName("everyone_kibana");
            var response = client().execute(DeleteRoleMappingAction.INSTANCE, request).get();
            assertTrue(response.isFound());
            request = new DeleteRoleMappingRequest();
            request.setName("everyone_fleet");
            response = client().execute(DeleteRoleMappingAction.INSTANCE, request).get();
            assertTrue(response.isFound());
        }

        // no roles are resolved now, because both native and cluster-state based stores have been cleared
        for (UserRoleMapper userRoleMapper : internalCluster().getInstances(UserRoleMapper.class)) {
            PlainActionFuture<Set<String>> resolveRolesFuture = new PlainActionFuture<>();
            userRoleMapper.resolveRoles(
                new UserRoleMapper.UserData("anyUsername", null, List.of(), Map.of(), mock(RealmConfig.class)),
                resolveRolesFuture
            );
            assertThat(resolveRolesFuture.get(), empty());
        }
    }

    public static Tuple<CountDownLatch, AtomicLong> setupClusterStateListenerForError(
        ClusterService clusterService,
        Consumer<ReservedStateErrorMetadata> errorMetadataConsumer
    ) {
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null && reservedState.errorMetadata() != null) {
                    clusterService.removeListener(this);
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                    errorMetadataConsumer.accept(reservedState.errorMetadata());
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    public void testErrorSaved() throws Exception {
        ensureGreen();

        // save an empty file to clear any prior state, this ensures we don't get a stale file left over by another test
        var savedClusterState = setupClusterStateListenerForCleanup(internalCluster().getMasterName());

        writeJSONFile(internalCluster().getMasterName(), emptyJSON, logger, versionCounter);
        boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(savedClusterState.v2().get())
        ).get();

        assertNull(
            clusterStateResponse.getState().metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey())
        );

        // save a bad file
        savedClusterState = setupClusterStateListenerForError(
            internalCluster().getCurrentMasterNodeInstance(ClusterService.class),
            errorMetadata -> {
                assertEquals(ReservedStateErrorMetadata.ErrorKind.PARSING, errorMetadata.errorKind());
                assertThat(errorMetadata.errors(), allOf(notNullValue(), hasSize(1)));
                assertThat(
                    errorMetadata.errors().get(0),
                    containsString("failed to parse role-mapping [everyone_kibana_bad]. missing field [rules]")
                );
            }
        );

        writeJSONFile(internalCluster().getMasterName(), testErrorJSON, logger, versionCounter);
        awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        // no roles are resolved because both role mapping stores are empty
        for (UserRoleMapper userRoleMapper : internalCluster().getInstances(UserRoleMapper.class)) {
            PlainActionFuture<Set<String>> resolveRolesFuture = new PlainActionFuture<>();
            userRoleMapper.resolveRoles(
                new UserRoleMapper.UserData("anyUsername", null, List.of(), Map.of(), mock(RealmConfig.class)),
                resolveRolesFuture
            );
            assertThat(resolveRolesFuture.get(), empty());
        }
    }

    public void testRoleMappingApplyWithSecurityIndexClosed() throws Exception {
        ensureGreen();

        // expect the role mappings to apply even if the .security index is closed
        var savedClusterState = setupClusterStateListener(internalCluster().getMasterName(), "everyone_kibana");

        try {
            var closeIndexResponse = indicesAdmin().close(new CloseIndexRequest(INTERNAL_SECURITY_MAIN_INDEX_7)).get();
            assertTrue(closeIndexResponse.isAcknowledged());

            writeJSONFile(internalCluster().getMasterName(), testJSON, logger, versionCounter);
            boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
            assertTrue(awaitSuccessful);

            // no native role mappings exist
            var request = new GetRoleMappingsRequest();
            request.setNames("everyone_kibana", "everyone_fleet");
            var response = client().execute(GetRoleMappingsAction.INSTANCE, request).get();
            assertFalse(response.hasMappings());

            // cluster state settings are also applied
            var clusterStateResponse = clusterAdmin().state(
                new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(savedClusterState.v2().get())
            ).get();
            assertThat(
                clusterStateResponse.getState().metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()),
                equalTo("50mb")
            );

            ReservedStateMetadata reservedState = clusterStateResponse.getState()
                .metadata()
                .reservedStateMetadata()
                .get(FileSettingsService.NAMESPACE);

            ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedRoleMappingAction.NAME);
            assertThat(handlerMetadata.keys(), containsInAnyOrder("everyone_kibana", "everyone_fleet"));

            // and roles are resolved based on the cluster-state role mappings
            for (UserRoleMapper userRoleMapper : internalCluster().getInstances(UserRoleMapper.class)) {
                PlainActionFuture<Set<String>> resolveRolesFuture = new PlainActionFuture<>();
                userRoleMapper.resolveRoles(
                    new UserRoleMapper.UserData("anyUsername", null, List.of(), Map.of(), mock(RealmConfig.class)),
                    resolveRolesFuture
                );
                assertThat(resolveRolesFuture.get(), containsInAnyOrder("kibana_user", "fleet_user"));
            }
        } finally {
            savedClusterState = setupClusterStateListenerForCleanup(internalCluster().getMasterName());
            writeJSONFile(internalCluster().getMasterName(), emptyJSON, logger, versionCounter);
            boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
            assertTrue(awaitSuccessful);

            var openIndexResponse = indicesAdmin().open(new OpenIndexRequest(INTERNAL_SECURITY_MAIN_INDEX_7)).get();
            assertTrue(openIndexResponse.isAcknowledged());
        }
    }

    private PutRoleMappingRequest sampleRestRequest(String name) throws Exception {
        var json = """
            {
                "enabled": true,
                "roles": [ "kibana_user_native" ],
                "rules": { "field": { "username": "*" } },
                "metadata": {
                    "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7"
                }
            }""";

        try (
            var bis = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis)
        ) {
            return new PutRoleMappingRequestBuilder(null).source(name, parser).request();
        }
    }
}
