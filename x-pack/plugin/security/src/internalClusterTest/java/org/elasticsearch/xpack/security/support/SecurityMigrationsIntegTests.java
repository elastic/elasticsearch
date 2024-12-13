/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_DATA_KEY;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_KEY;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class SecurityMigrationsIntegTests extends SecurityIntegTestCase {

    public void testNewIndexSkipMigration() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNode = internalCluster().getMasterName();
        ensureGreen();
        CountDownLatch awaitMigrations = awaitMigrationVersionUpdates(masterNode, SecurityMigrations.MIGRATIONS_BY_VERSION.lastKey());
        createSecurityIndexWithMigrationAndIndexVersion(SecurityMigrations.MIGRATIONS_BY_VERSION.lastKey(), IndexVersion.current());
        safeAwait(awaitMigrations);
    }

    public void testIndexNotOnLatestMigrationVersionRunAllMigrations() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNode = internalCluster().getMasterName();
        ensureGreen();
        CountDownLatch awaitMigrations = awaitMigrationVersionUpdates(
            masterNode,
            SecurityMigrations.MIGRATIONS_BY_VERSION.keySet().stream().mapToInt(Integer::intValue).toArray()
        );

        // Migration version = 3 means it's not equal to current migration version and therefore all migrations will run
        createSecurityIndexWithMigrationAndIndexVersion(3, IndexVersion.current());
        safeAwait(awaitMigrations);
    }

    public void testIndexNotOnLatestIndexVersionFallbackRunAllMigrations() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNode = internalCluster().getMasterName();
        ensureGreen();
        CountDownLatch awaitMigrations = awaitMigrationVersionUpdates(
            masterNode,
            SecurityMigrations.MIGRATIONS_BY_VERSION.keySet().stream().mapToInt(Integer::intValue).toArray()
        );

        // Migration version = 1 was handled by index version, test fallback by providing old IndexVersion
        createSecurityIndexWithMigrationAndIndexVersion(1, IndexVersions.V_7_11_0);
        safeAwait(awaitMigrations);
    }

    public void testIndexOnLatestIndexVersionFallbackSkipAllMigrations() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNode = internalCluster().getMasterName();
        ensureGreen();
        CountDownLatch awaitMigrations = awaitMigrationVersionUpdates(masterNode, SecurityMigrations.MIGRATIONS_BY_VERSION.lastKey());

        // Migration version = 1 was handled by index version, test fallback by providing current IndexVersion
        createSecurityIndexWithMigrationAndIndexVersion(1, IndexVersion.current());
        safeAwait(awaitMigrations);
    }

    private void createSecurityIndexWithMigrationAndIndexVersion(int migrationVersion, IndexVersion indexVersionCreated) {
        SystemIndexDescriptor mainIndexDescriptor = new SecuritySystemIndices(Settings.EMPTY).getSystemIndexDescriptors()
            .stream()
            .toList()
            .get(0);
        CreateIndexRequest request = new CreateIndexRequest(INTERNAL_SECURITY_MAIN_INDEX_7).origin(SECURITY_ORIGIN)
            .settings(
                Settings.builder()
                    .put(mainIndexDescriptor.getSettings())
                    .put(SecuritySystemIndices.MAIN_INDEX_CREATED_ON_MIGRATION_VERSION.getKey(), migrationVersion)
                    .put(SETTING_INDEX_VERSION_CREATED.getKey(), indexVersionCreated)
            )
            .mapping(mainIndexDescriptor.getMappings())
            .alias(new Alias(".security"))
            .waitForActiveShards(ActiveShardCount.ALL);
        admin().indices().create(request).actionGet();
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        // This allows us to set SETTING_INDEX_VERSION_CREATED
        return false;
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

    private int getCurrentMigrationVersion(ClusterState state) {
        IndexMetadata indexMetadata = state.metadata().getIndices().get(INTERNAL_SECURITY_MAIN_INDEX_7);
        if (indexMetadata == null || indexMetadata.getCustomData(MIGRATION_VERSION_CUSTOM_KEY) == null) {
            return 0;
        }
        return Integer.parseInt(indexMetadata.getCustomData(MIGRATION_VERSION_CUSTOM_KEY).get(MIGRATION_VERSION_CUSTOM_DATA_KEY));
    }
}
