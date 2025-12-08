/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_DATA_KEY;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_KEY;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ROLE_TYPE;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.elasticsearch.xpack.security.support.SecurityMigrations.ROLE_METADATA_FLATTENED_MIGRATION_VERSION;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class MetadataFlattenedMigrationIntegTests extends SecurityIntegTestCase {

    private final AtomicLong versionCounter = new AtomicLong(1);

    @Before
    public void resetVersion() {
        versionCounter.set(1);
    }

    public void testMigrationWithConcurrentUpdates() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNode();
        ensureGreen();

        waitForMigrationCompletion();
        var roles = createRoles();
        final var nativeRoleStore = internalCluster().getInstance(NativeRolesStore.class);
        PlainActionFuture<Void> roleUpdatesCompleteListener = new PlainActionFuture<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (RefCountingListener refs = new RefCountingListener(roleUpdatesCompleteListener)) {
            final AtomicBoolean runUpdateRolesBackground = new AtomicBoolean(true);
            executor.submit(() -> {
                while (runUpdateRolesBackground.get()) {
                    // Only update half the list so the other half can be verified as migrated
                    RoleDescriptor roleToUpdate = randomFrom(roles.subList(0, roles.size() / 2));

                    RoleDescriptor updatedRole = new RoleDescriptor(
                        roleToUpdate.getName(),
                        new String[] { "monitor" },
                        null,
                        null,
                        null,
                        null,
                        Map.of("test", "value", "timestamp", System.currentTimeMillis(), "random", randomAlphaOfLength(10)),
                        null
                    );
                    nativeRoleStore.putRole(
                        WriteRequest.RefreshPolicy.IMMEDIATE,
                        updatedRole,
                        refs.acquire(resp -> logger.trace("Updated role [{}]", updatedRole))
                    );
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            resetMigration();
            waitForMigrationCompletion();
            runUpdateRolesBackground.set(false);
            assertAllRolesHaveMetadataFlattened();
        } finally {
            // Await all role updates before shutting down
            roleUpdatesCompleteListener.get();
            terminate(executor);
        }
    }

    private void resetMigration() {
        client().execute(
            UpdateIndexMigrationVersionAction.INSTANCE,
            new UpdateIndexMigrationVersionAction.Request(
                TimeValue.MAX_VALUE,
                ROLE_METADATA_FLATTENED_MIGRATION_VERSION - 1,
                INTERNAL_SECURITY_MAIN_INDEX_7
            )
        ).actionGet();
    }

    private List<RoleDescriptor> createRoles() throws IOException {
        var roles = randomList(
            25,
            50,
            () -> new RoleDescriptor(
                randomAlphaOfLength(20),
                null,
                null,
                null,
                null,
                null,
                Map.of("test", "value", "timestamp", System.currentTimeMillis(), "random", randomAlphaOfLength(10)),
                Map.of()
            )
        );
        for (RoleDescriptor role : roles) {
            indexRoleDirectly(role);
        }
        indicesAdmin().prepareRefresh(INTERNAL_SECURITY_MAIN_INDEX_7).get();
        return roles;
    }

    private void indexRoleDirectly(RoleDescriptor role) throws IOException {
        XContentBuilder builder = buildRoleDocument(role);
        prepareIndex(INTERNAL_SECURITY_MAIN_INDEX_7).setId(ROLE_TYPE + "-" + role.getName())
            .setSource(builder)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    private XContentBuilder buildRoleDocument(RoleDescriptor role) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        // metadata_flattened is populated by the native role store, so write directly to index to simulate pre-migration state
        role.innerToXContent(builder, ToXContent.EMPTY_PARAMS, true);
        builder.endObject();
        return builder;
    }

    private int getCurrentMigrationVersion() {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        IndexMetadata indexMetadata = clusterService.state().metadata().index(INTERNAL_SECURITY_MAIN_INDEX_7);
        if (indexMetadata == null || indexMetadata.getCustomData(MIGRATION_VERSION_CUSTOM_KEY) == null) {
            return 0;
        }
        return Integer.parseInt(indexMetadata.getCustomData(MIGRATION_VERSION_CUSTOM_KEY).get(MIGRATION_VERSION_CUSTOM_DATA_KEY));
    }

    private void waitForMigrationCompletion() throws Exception {
        assertBusy(() -> assertThat(getCurrentMigrationVersion(), greaterThanOrEqualTo(ROLE_METADATA_FLATTENED_MIGRATION_VERSION)));
    }

    private void assertAllRolesHaveMetadataFlattened() {
        SearchRequest searchRequest = new SearchRequest(INTERNAL_SECURITY_MAIN_INDEX_7);
        searchRequest.source().query(QueryBuilders.termQuery("type", "role")).size(1000);
        SearchResponse response = client().search(searchRequest).actionGet();
        for (SearchHit hit : response.getHits().getHits()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> metadata = (Map<String, Object>) hit.getSourceAsMap().get("metadata_flattened");
            // Only check non-reserved roles
            if (metadata.get("_reserved") == null) {
                assertEquals("value", metadata.get("test"));
            }
        }
        response.decRef();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(PainlessPlugin.class)).collect(Collectors.toList());
    }
}
