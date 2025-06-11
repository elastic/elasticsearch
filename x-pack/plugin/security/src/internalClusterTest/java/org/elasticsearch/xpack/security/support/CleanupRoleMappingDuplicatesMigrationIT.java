/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.integration.RoleMappingFileSettingsIT;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingResponse;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsResponse;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingResponse;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.integration.RoleMappingFileSettingsIT.setupClusterStateListener;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_DATA_KEY;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_KEY;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class CleanupRoleMappingDuplicatesMigrationIT extends SecurityIntegTestCase {

    private final AtomicLong versionCounter = new AtomicLong(1);

    @Before
    public void resetVersion() {
        versionCounter.set(1);
    }

    private static final String TEST_JSON_WITH_ROLE_MAPPINGS = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "role_mappings": {
                       "everyone_kibana_alone": {
                          "enabled": true,
                          "roles": [ "kibana_user" ],
                          "rules": { "field": { "username": "*" } },
                          "metadata": {
                             "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7",
                             "_foo": "something"
                          }
                       },
                       "everyone_fleet_alone": {
                          "enabled": false,
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

    private static final String TEST_JSON_WITH_FALLBACK_NAME = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "role_mappings": {
                       "name_not_available_after_deserialization": {
                          "enabled": true,
                          "roles": [ "kibana_user", "kibana_admin" ],
                          "rules": { "field": { "username": "*" } },
                          "metadata": {
                             "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7",
                             "_foo": "something"
                          }
                       }
                 }
             }
        }""";

    private static final String TEST_JSON_WITH_EMPTY_ROLE_MAPPINGS = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "role_mappings": {}
             }
        }""";

    public void testMigrationSuccessful() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNode();
        ensureGreen();
        final String masterNode = internalCluster().getMasterName();

        // Create a native role mapping to create security index and trigger migration (skipped initially)
        createNativeRoleMapping("everyone_kibana_alone");
        createNativeRoleMapping("everyone_fleet_alone");
        createNativeRoleMapping("dont_clean_this_up");
        assertAllRoleMappings("everyone_kibana_alone", "everyone_fleet_alone", "dont_clean_this_up");

        // Wait for file watcher to start
        awaitFileSettingsWatcher();
        // Setup listener to wait for role mapping
        var fileBasedRoleMappingsWrittenListener = setupClusterStateListener(masterNode, "everyone_kibana_alone");
        // Write role mappings
        RoleMappingFileSettingsIT.writeJSONFile(masterNode, TEST_JSON_WITH_ROLE_MAPPINGS, logger, versionCounter.incrementAndGet());
        assertTrue(fileBasedRoleMappingsWrittenListener.v1().await(20, TimeUnit.SECONDS));
        waitForMigrationCompletion(SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION);

        // First migration is on a new index, so should skip all migrations. If we reset, it should re-trigger and run all migrations
        resetMigration();

        // Wait for the first migration to finish
        waitForMigrationCompletion(SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION);

        assertAllRoleMappings(
            "everyone_kibana_alone" + ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_SUFFIX,
            "everyone_fleet_alone" + ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_SUFFIX,
            "dont_clean_this_up"
        );
    }

    public void testMigrationSuccessfulNoOverlap() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNode();
        ensureGreen();
        final String masterNode = internalCluster().getMasterName();

        // Create a native role mapping to create security index and trigger migration (skipped initially)
        createNativeRoleMapping("some_native_mapping");
        createNativeRoleMapping("some_other_native_mapping");
        assertAllRoleMappings("some_native_mapping", "some_other_native_mapping");

        // Wait for file watcher to start
        awaitFileSettingsWatcher();
        // Setup listener to wait for role mapping
        var fileBasedRoleMappingsWrittenListener = setupClusterStateListener(masterNode, "everyone_kibana_alone");
        // Write role mappings with fallback name, this should block any security migration
        RoleMappingFileSettingsIT.writeJSONFile(masterNode, TEST_JSON_WITH_ROLE_MAPPINGS, logger, versionCounter.incrementAndGet());
        assertTrue(fileBasedRoleMappingsWrittenListener.v1().await(20, TimeUnit.SECONDS));
        waitForMigrationCompletion(SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION);

        // First migration is on a new index, so should skip all migrations. If we reset, it should re-trigger and run all migrations
        resetMigration();

        // Wait for the first migration to finish
        waitForMigrationCompletion(SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION);

        assertAllRoleMappings(
            "everyone_kibana_alone" + ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_SUFFIX,
            "everyone_fleet_alone" + ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_SUFFIX,
            "some_native_mapping",
            "some_other_native_mapping"
        );
    }

    public void testMigrationSuccessfulNoNative() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNode();
        ensureGreen();
        final String masterNode = internalCluster().getMasterName();

        // Create a native role mapping to create security index and trigger migration (skipped initially)
        // Then delete it to test an empty role mapping store
        createNativeRoleMapping("some_native_mapping");
        deleteNativeRoleMapping("some_native_mapping");
        // Wait for file watcher to start
        awaitFileSettingsWatcher();
        // Setup listener to wait for role mapping
        var fileBasedRoleMappingsWrittenListener = setupClusterStateListener(masterNode, "everyone_kibana_alone");
        // Write role mappings with fallback name, this should block any security migration
        RoleMappingFileSettingsIT.writeJSONFile(masterNode, TEST_JSON_WITH_ROLE_MAPPINGS, logger, versionCounter.incrementAndGet());
        assertTrue(fileBasedRoleMappingsWrittenListener.v1().await(20, TimeUnit.SECONDS));
        waitForMigrationCompletion(SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION);

        // First migration is on a new index, so should skip all migrations. If we reset, it should re-trigger and run all migrations
        resetMigration();

        // Wait for the first migration to finish
        waitForMigrationCompletion(SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION);

        assertAllRoleMappings(
            "everyone_kibana_alone" + ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_SUFFIX,
            "everyone_fleet_alone" + ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_SUFFIX
        );
    }

    public void testMigrationFallbackNamePreCondition() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNode();
        ensureGreen();
        final String masterNode = internalCluster().getMasterName();
        // Wait for file watcher to start
        awaitFileSettingsWatcher();

        // Setup listener to wait for role mapping
        var nameNotAvailableListener = setupClusterStateListener(masterNode, "name_not_available_after_deserialization");
        // Write role mappings with fallback name, this should block any security migration
        RoleMappingFileSettingsIT.writeJSONFile(masterNode, TEST_JSON_WITH_FALLBACK_NAME, logger, versionCounter.incrementAndGet());
        assertTrue(nameNotAvailableListener.v1().await(20, TimeUnit.SECONDS));

        // Create a native role mapping to create security index and trigger migration
        createNativeRoleMapping("everyone_fleet_alone");
        waitForMigrationCompletion(SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION);
        // First migration is on a new index, so should skip all migrations. If we reset, it should re-trigger and run all migrations
        resetMigration();
        // Wait for the first migration to finish
        waitForMigrationCompletion(SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION - 1);

        // Make sure migration didn't run yet (blocked by the fallback name)
        assertMigrationLessThan(SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        SecurityIndexManager.RoleMappingsCleanupMigrationStatus status = SecurityIndexManager.getRoleMappingsCleanupMigrationStatus(
            clusterService.state().projectState(Metadata.DEFAULT_PROJECT_ID),
            SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION - 1
        );
        assertThat(status, equalTo(SecurityIndexManager.RoleMappingsCleanupMigrationStatus.NOT_READY));

        // Write file without fallback name in it to unblock migration
        RoleMappingFileSettingsIT.writeJSONFile(masterNode, TEST_JSON_WITH_ROLE_MAPPINGS, logger, versionCounter.incrementAndGet());
        waitForMigrationCompletion(SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION);
    }

    public void testSkipMigrationNoFileBasedMappings() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNode();
        ensureGreen();
        // Create a native role mapping to create security index and trigger migration (skipped initially)
        createNativeRoleMapping("everyone_kibana_alone");
        createNativeRoleMapping("everyone_fleet_alone");
        assertAllRoleMappings("everyone_kibana_alone", "everyone_fleet_alone");

        waitForMigrationCompletion(SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION);

        // First migration is on a new index, so should skip all migrations. If we reset, it should re-trigger and run all migrations
        resetMigration();

        // Wait for the first migration to finish
        waitForMigrationCompletion(SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION);

        assertAllRoleMappings("everyone_kibana_alone", "everyone_fleet_alone");
    }

    public void testSkipMigrationEmptyFileBasedMappings() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNode();
        ensureGreen();
        final String masterNode = internalCluster().getMasterName();

        // Wait for file watcher to start
        awaitFileSettingsWatcher();
        // Setup listener to wait for any role mapping
        var fileBasedRoleMappingsWrittenListener = setupClusterStateListener(masterNode);
        // Write role mappings
        RoleMappingFileSettingsIT.writeJSONFile(masterNode, TEST_JSON_WITH_EMPTY_ROLE_MAPPINGS, logger, versionCounter.incrementAndGet());
        assertTrue(fileBasedRoleMappingsWrittenListener.v1().await(20, TimeUnit.SECONDS));

        // Create a native role mapping to create security index and trigger migration (skipped initially)
        createNativeRoleMapping("everyone_kibana_alone");
        createNativeRoleMapping("everyone_fleet_alone");
        assertAllRoleMappings("everyone_kibana_alone", "everyone_fleet_alone");

        waitForMigrationCompletion(SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION);

        // First migration is on a new index, so should skip all migrations. If we reset, it should re-trigger and run all migrations
        resetMigration();

        // Wait for the first migration to finish
        waitForMigrationCompletion(SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION);

        assertAllRoleMappings("everyone_kibana_alone", "everyone_fleet_alone");
    }

    public void testNewIndexSkipMigration() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNode();
        final String masterNode = internalCluster().getMasterName();
        ensureGreen();
        deleteSecurityIndex(); // hack to force a new security index to be created
        ensureGreen();
        CountDownLatch awaitMigrations = awaitMigrationVersionUpdates(
            masterNode,
            SecurityMigrations.CLEANUP_ROLE_MAPPING_DUPLICATES_MIGRATION_VERSION
        );
        // Create a native role mapping to create security index and trigger migration
        createNativeRoleMapping("everyone_kibana_alone");
        // Make sure no migration ran (set to current version without applying prior migrations)
        safeAwait(awaitMigrations);
    }

    /**
     * Make sure all versions are applied to cluster state sequentially
     */
    private CountDownLatch awaitMigrationVersionUpdates(String node, final int... versions) {
        final ClusterService clusterService = internalCluster().clusterService(node);
        final CountDownLatch allVersionsCountDown = new CountDownLatch(1);
        final AtomicInteger currentVersionIdx = new AtomicInteger(0);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                int currentMigrationVersion = getCurrentMigrationVersion(event.state());
                if (currentMigrationVersion > 0) {
                    assertThat(versions[currentVersionIdx.get()], lessThanOrEqualTo(currentMigrationVersion));
                    if (versions[currentVersionIdx.get()] == currentMigrationVersion) {
                        currentVersionIdx.incrementAndGet();
                    }

                    if (currentVersionIdx.get() >= versions.length) {
                        clusterService.removeListener(this);
                        allVersionsCountDown.countDown();
                    }
                }
            }
        });

        return allVersionsCountDown;
    }

    private void assertAllRoleMappings(String... roleMappingNames) {
        GetRoleMappingsResponse response = client().execute(GetRoleMappingsAction.INSTANCE, new GetRoleMappingsRequest()).actionGet();

        assertTrue(response.hasMappings());
        assertThat(response.mappings().length, equalTo(roleMappingNames.length));

        assertThat(
            Arrays.stream(response.mappings()).map(ExpressionRoleMapping::getName).toList(),
            containsInAnyOrder(
                roleMappingNames

            )
        );
    }

    private void awaitFileSettingsWatcher() throws Exception {
        final String masterNode = internalCluster().getMasterName();
        FileSettingsService masterFileSettingsService = internalCluster().getInstance(FileSettingsService.class, masterNode);
        assertBusy(() -> assertTrue(masterFileSettingsService.watching()));
    }

    private void resetMigration() {
        client().execute(
            UpdateIndexMigrationVersionAction.INSTANCE,
            // -1 is a hack, since running a migration on version 0 on a new cluster will cause all migrations to be skipped (not needed)
            new UpdateIndexMigrationVersionAction.Request(TimeValue.MAX_VALUE, -1, INTERNAL_SECURITY_MAIN_INDEX_7)
        ).actionGet();
    }

    private void createNativeRoleMapping(String name) {
        PutRoleMappingRequest request = new PutRoleMappingRequest();
        request.setName(name);
        request.setRules(new FieldExpression("username", Collections.singletonList(new FieldExpression.FieldValue("*"))));
        request.setRoles(List.of("superuser"));

        ActionFuture<PutRoleMappingResponse> response = client().execute(PutRoleMappingAction.INSTANCE, request);
        response.actionGet();
    }

    private void deleteNativeRoleMapping(String name) {
        DeleteRoleMappingRequest request = new DeleteRoleMappingRequest();
        request.setName(name);

        ActionFuture<DeleteRoleMappingResponse> response = client().execute(DeleteRoleMappingAction.INSTANCE, request);
        response.actionGet();
    }

    private void assertMigrationVersionAtLeast(int expectedVersion) {
        assertThat(getCurrentMigrationVersion(), greaterThanOrEqualTo(expectedVersion));
    }

    private void assertMigrationLessThan(int expectedVersion) {
        assertThat(getCurrentMigrationVersion(), lessThan(expectedVersion));
    }

    private int getCurrentMigrationVersion(ClusterState state) {
        IndexMetadata indexMetadata = state.metadata().getProject().index(INTERNAL_SECURITY_MAIN_INDEX_7);
        if (indexMetadata == null || indexMetadata.getCustomData(MIGRATION_VERSION_CUSTOM_KEY) == null) {
            return 0;
        }
        return Integer.parseInt(indexMetadata.getCustomData(MIGRATION_VERSION_CUSTOM_KEY).get(MIGRATION_VERSION_CUSTOM_DATA_KEY));
    }

    private int getCurrentMigrationVersion() {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        return getCurrentMigrationVersion(clusterService.state());
    }

    private void waitForMigrationCompletion(int version) throws Exception {
        assertBusy(() -> assertMigrationVersionAtLeast(version));
    }
}
