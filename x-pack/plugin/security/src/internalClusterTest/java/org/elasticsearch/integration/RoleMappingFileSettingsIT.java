/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
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
import org.elasticsearch.common.Randomness;
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
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingResponse;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.security.action.rolemapping.ReservedRoleMappingAction;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

/**
 * Tests that file settings service can properly add role mappings.
 */
@LuceneTestCase.SuppressFileSystems("*")
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

    public static void writeJSONFile(String node, String json, Logger logger, Long version) throws Exception {
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
                Files.move(tempFilePath, fileSettingsService.watchedFile(), StandardCopyOption.ATOMIC_MOVE);
                logger.info("--> after writing JSON config to node {} with path {}", node, tempFilePath);
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

    // Wait for any file metadata
    public static Tuple<CountDownLatch, AtomicLong> setupClusterStateListener(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null) {
                    ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedRoleMappingAction.NAME);
                    if (handlerMetadata != null) {
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

        // the role mappings are retrievable by the role mapping action for BWC
        assertGetResponseHasMappings(true, "everyone_kibana", "everyone_fleet");

        // role mappings (with the same names) can be stored in the "native" store
        {
            PutRoleMappingResponse response = client().execute(PutRoleMappingAction.INSTANCE, sampleRestRequest("everyone_kibana"))
                .actionGet();
            assertTrue(response.isCreated());
            response = client().execute(PutRoleMappingAction.INSTANCE, sampleRestRequest("everyone_fleet")).actionGet();
            assertTrue(response.isCreated());
        }
        {
            // deleting role mappings that exist in the native store and in cluster-state should result in success
            var response = client().execute(DeleteRoleMappingAction.INSTANCE, deleteRequest("everyone_kibana")).actionGet();
            assertTrue(response.isFound());
            response = client().execute(DeleteRoleMappingAction.INSTANCE, deleteRequest("everyone_fleet")).actionGet();
            assertTrue(response.isFound());
        }

    }

    public void testClusterStateRoleMappingsAddedThenDeleted() throws Exception {
        ensureGreen();

        var savedClusterState = setupClusterStateListener(internalCluster().getMasterName(), "everyone_kibana");
        writeJSONFile(internalCluster().getMasterName(), testJSON, logger, versionCounter.incrementAndGet());

        assertRoleMappingsSaveOK(savedClusterState.v1(), savedClusterState.v2());
        logger.info("---> cleanup cluster settings...");

        {
            // Deleting non-existent native role mappings returns not found even if they exist in config file
            var response = client().execute(DeleteRoleMappingAction.INSTANCE, deleteRequest("everyone_kibana")).get();
            assertFalse(response.isFound());
        }

        savedClusterState = setupClusterStateListenerForCleanup(internalCluster().getMasterName());

        writeJSONFile(internalCluster().getMasterName(), emptyJSON, logger, versionCounter.incrementAndGet());
        boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(savedClusterState.v2().get())
        ).get();

        assertNull(
            clusterStateResponse.getState().metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey())
        );

        // cluster-state role mapping was removed and is not returned in the API anymore
        {
            var request = new GetRoleMappingsRequest();
            request.setNames("everyone_kibana", "everyone_fleet");
            var response = client().execute(GetRoleMappingsAction.INSTANCE, request).get();
            assertFalse(response.hasMappings());
        }

        // no role mappings means no roles are resolved
        for (UserRoleMapper userRoleMapper : internalCluster().getInstances(UserRoleMapper.class)) {
            PlainActionFuture<Set<String>> resolveRolesFuture = new PlainActionFuture<>();
            userRoleMapper.resolveRoles(
                new UserRoleMapper.UserData("anyUsername", null, List.of(), Map.of(), mock(RealmConfig.class)),
                resolveRolesFuture
            );
            assertThat(resolveRolesFuture.get(), empty());
        }
    }

    public void testGetRoleMappings() throws Exception {
        ensureGreen();

        final List<String> nativeMappings = List.of("everyone_kibana", "_everyone_kibana", "zzz_mapping", "123_mapping");
        for (var mapping : nativeMappings) {
            client().execute(PutRoleMappingAction.INSTANCE, sampleRestRequest(mapping)).actionGet();
        }

        var savedClusterState = setupClusterStateListener(internalCluster().getMasterName(), "everyone_kibana");
        writeJSONFile(internalCluster().getMasterName(), testJSON, logger, versionCounter.incrementAndGet());
        boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        var request = new GetRoleMappingsRequest();
        var response = client().execute(GetRoleMappingsAction.INSTANCE, request).get();
        assertTrue(response.hasMappings());
        assertThat(
            Arrays.stream(response.mappings()).map(ExpressionRoleMapping::getName).toList(),
            containsInAnyOrder(
                "everyone_kibana",
                ExpressionRoleMapping.addReadOnlySuffix("everyone_kibana"),
                "_everyone_kibana",
                ExpressionRoleMapping.addReadOnlySuffix("everyone_fleet"),
                "zzz_mapping",
                "123_mapping"
            )
        );

        List<Boolean> readOnlyFlags = new ArrayList<>();
        for (ExpressionRoleMapping mapping : response.mappings()) {
            boolean isReadOnly = ExpressionRoleMapping.hasReadOnlySuffix(mapping.getName())
                && mapping.getMetadata().get("_read_only") != null;
            readOnlyFlags.add(isReadOnly);
        }
        // assert that cluster-state role mappings come last
        assertThat(readOnlyFlags, contains(false, false, false, false, true, true));

        // it's possible to delete overlapping native role mapping
        assertTrue(client().execute(DeleteRoleMappingAction.INSTANCE, deleteRequest("everyone_kibana")).actionGet().isFound());

        // Fetch a specific file based role
        request = new GetRoleMappingsRequest();
        request.setNames(ExpressionRoleMapping.addReadOnlySuffix("everyone_kibana"));
        response = client().execute(GetRoleMappingsAction.INSTANCE, request).get();
        assertTrue(response.hasMappings());
        assertThat(
            Arrays.stream(response.mappings()).map(ExpressionRoleMapping::getName).toList(),
            containsInAnyOrder(ExpressionRoleMapping.addReadOnlySuffix("everyone_kibana"))
        );

        savedClusterState = setupClusterStateListenerForCleanup(internalCluster().getMasterName());
        String node = internalCluster().getMasterName();
        writeJSONFile(node, emptyJSON, logger, versionCounter.incrementAndGet());
        awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(savedClusterState.v2().get())
        ).get();

        assertNull(
            clusterStateResponse.getState().metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey())
        );

        // Make sure remaining native mappings can still be fetched
        request = new GetRoleMappingsRequest();
        response = client().execute(GetRoleMappingsAction.INSTANCE, request).get();
        assertTrue(response.hasMappings());
        assertThat(
            Arrays.stream(response.mappings()).map(ExpressionRoleMapping::getName).toList(),
            containsInAnyOrder("_everyone_kibana", "zzz_mapping", "123_mapping")
        );
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

        writeJSONFile(internalCluster().getMasterName(), emptyJSON, logger, versionCounter.incrementAndGet());
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

        String node = internalCluster().getMasterName();
        writeJSONFile(node, testErrorJSON, logger, versionCounter.incrementAndGet());
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

            String node = internalCluster().getMasterName();
            writeJSONFile(node, testJSON, logger, versionCounter.incrementAndGet());
            boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
            assertTrue(awaitSuccessful);

            // even if index is closed, cluster-state role mappings are still returned
            assertGetResponseHasMappings(true, "everyone_kibana", "everyone_fleet");

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
            String node = internalCluster().getMasterName();
            writeJSONFile(node, emptyJSON, logger, versionCounter.incrementAndGet());
            boolean awaitSuccessful = savedClusterState.v1().await(20, TimeUnit.SECONDS);
            assertTrue(awaitSuccessful);

            var openIndexResponse = indicesAdmin().open(new OpenIndexRequest(INTERNAL_SECURITY_MAIN_INDEX_7)).get();
            assertTrue(openIndexResponse.isAcknowledged());
        }
    }

    private DeleteRoleMappingRequest deleteRequest(String name) {
        var request = new DeleteRoleMappingRequest();
        request.setName(name);
        return request;
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

    private static void assertGetResponseHasMappings(boolean readOnly, String... mappings) throws InterruptedException, ExecutionException {
        var request = new GetRoleMappingsRequest();
        request.setNames(mappings);
        var response = client().execute(GetRoleMappingsAction.INSTANCE, request).get();
        assertTrue(response.hasMappings());
        assertThat(
            Arrays.stream(response.mappings()).map(ExpressionRoleMapping::getName).toList(),
            containsInAnyOrder(
                Arrays.stream(mappings)
                    .map(mapping -> readOnly ? ExpressionRoleMapping.addReadOnlySuffix(mapping) : mapping)
                    .toArray(String[]::new)
            )
        );
    }
}
