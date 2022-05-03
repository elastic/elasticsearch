/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlConfigMigrationEligibilityCheckTests extends ESTestCase {

    private ClusterService clusterService;

    @Before
    public void setUpTests() {
        clusterService = mock(ClusterService.class);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testCanStartMigration_givenMigrationIsDisabled() {
        Settings settings = newSettings(false);
        givenClusterSettings(settings);
        ClusterState clusterState = mock(ClusterState.class);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertFalse(check.canStartMigration(clusterState));
    }

    public void testCanStartMigration_givenMissingIndex() {
        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests")).build();

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);
        assertFalse(check.canStartMigration(clusterState));
    }

    public void testCanStartMigration_givenMlConfigIsAlias() {
        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        // index has been replaced by an alias
        String reindexedName = ".reindexed_ml_config";
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(reindexedName, MlConfigIndex.indexName(), metadata, routingTable);
        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
            .metadata(metadata)
            .routingTable(routingTable.build())
            .build();

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);
        assertTrue(check.canStartMigration(clusterState));
    }

    public void testCanStartMigration_givenInactiveShards() {
        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        // index is present but no routing
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(metadata, routingTable);
        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
            .metadata(metadata)
            // the difference here is that the routing table that's been created is
            // _not_ added to the cluster state, simulating no route to the index
            .build();

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);
        assertFalse(check.canStartMigration(clusterState));
    }

    private void addMlConfigIndex(Metadata.Builder metadata, RoutingTable.Builder routingTable) {
        addMlConfigIndex(MlConfigIndex.indexName(), null, metadata, routingTable);
    }

    private void addMlConfigIndex(String indexName, String aliasName, Metadata.Builder metadata, RoutingTable.Builder routingTable) {
        final String uuid = "_uuid";
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
        indexMetadata.settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        );
        if (aliasName != null) {
            indexMetadata.putAlias(AliasMetadata.builder(aliasName));
        }
        metadata.put(indexMetadata);
        Index index = new Index(indexName, uuid);
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "")
        );
        shardRouting = shardRouting.initialize("node_id", null, 0L);
        shardRouting = shardRouting.moveToStarted();
        routingTable.add(
            IndexRoutingTable.builder(index).addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build())
        );
    }

    private void givenClusterSettings(Settings settings) {
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            new HashSet<>(Collections.singletonList(MlConfigMigrationEligibilityCheck.ENABLE_CONFIG_MIGRATION))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    }

    private static Settings newSettings(boolean migrationEnabled) {
        return Settings.builder().put(MlConfigMigrationEligibilityCheck.ENABLE_CONFIG_MIGRATION.getKey(), migrationEnabled).build();
    }
}
