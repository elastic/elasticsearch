/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.indices.SystemIndexDescriptor.VERSION_META_KEY;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SystemIndexMigrationServiceTests extends ESTestCase {
    private SystemIndices systemIndices;
    private PersistentTasksService persistentTasksService;
    private static final ClusterName CLUSTER_NAME = new ClusterName("system-index-migration-tests");
    private static final ClusterState EMPTY_CLUSTER_STATE = new ClusterState.Builder(CLUSTER_NAME).build();

    @Before
    public void setUpMocks() {
        persistentTasksService = mock(PersistentTasksService.class);
        systemIndices = mock(SystemIndices.class);
    }

    public void testMigrationsSubmitted() throws IOException {
        String indexName = ".test-1";
        String aliasName = ".test";

        SystemIndexDescriptor systemIndexDescriptor = createDescriptorBuilder(indexName, aliasName).addSystemIndexMigration(
            generateMigration()
        ).addSystemIndexMigration(generateMigration()).addSystemIndexMigration(generateMigration()).build();

        when(systemIndices.getSystemIndexDescriptors()).thenReturn(List.of(systemIndexDescriptor));
        SystemIndexMigrationService systemIndexMigrationService = new SystemIndexMigrationService(
            systemIndices,
            mock(FeatureService.class),
            persistentTasksService
        );

        systemIndexMigrationService.clusterChanged(event(createClusterStateForMasterWithIndex(indexName, aliasName)));

        verify(persistentTasksService, times(1)).sendStartRequest(
            eq(SystemIndexMigrationTaskParams.TASK_NAME),
            eq(SystemIndexMigrationTaskParams.TASK_NAME),
            eq(new SystemIndexMigrationTaskParams(indexName, new int[] { 1, 2, 3 })),
            eq(null),
            any()
        );
    }

    public void testOnlyEligibleAndPreConditionMigrationsSubmitted() throws IOException {
        String indexName = ".test-1";
        String aliasName = ".test";

        boolean preConditionPasses = randomBoolean();
        boolean eligibilityPasses = preConditionPasses == false;

        SystemIndexDescriptor systemIndexDescriptor = createDescriptorBuilder(indexName, aliasName).addSystemIndexMigration(
            generateMigration()
        )
            .addSystemIndexMigration(generateMigration())
            .addSystemIndexMigration(generateMigration(eligibilityPasses, preConditionPasses))
            .addSystemIndexMigration(generateMigration())
            .build();

        when(systemIndices.getSystemIndexDescriptors()).thenReturn(List.of(systemIndexDescriptor));
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(false);

        SystemIndexMigrationService systemIndexMigrationService = new SystemIndexMigrationService(
            systemIndices,
            featureService,
            persistentTasksService
        );

        systemIndexMigrationService.clusterChanged(event(createClusterStateForMasterWithIndex(indexName, aliasName)));

        verify(persistentTasksService, times(1)).sendStartRequest(
            eq(SystemIndexMigrationTaskParams.TASK_NAME),
            eq(SystemIndexMigrationTaskParams.TASK_NAME),
            eq(new SystemIndexMigrationTaskParams(indexName, new int[] { 1, 2 })),
            eq(null),
            any()
        );
    }

    private SystemIndexMigrationTask generateMigration() {
        return generateMigration(true, true);
    }

    private SystemIndexMigrationTask generateMigration(boolean eligible, boolean preCondition) {
        boolean eligibleNodeFeatures = eligible || randomBoolean();
        boolean eligibleMappingVersion = eligible || eligibleNodeFeatures == false;
        return new SystemIndexMigrationTask() {
            @Override
            public void migrate(ActionListener<Void> listener) {}

            @Override
            public Set<NodeFeature> nodeFeaturesRequired() {
                if (eligibleNodeFeatures) {
                    return Set.of();
                }
                return Set.of(new NodeFeature("not a real feature"));
            }

            @Override
            public int minMappingVersion() {
                if (eligibleMappingVersion) {
                    return 0;
                }
                return Integer.MAX_VALUE;
            }

            @Override
            public boolean checkPreConditions() {
                return preCondition;
            }
        };
    }

    public SystemIndexDescriptor.Builder createDescriptorBuilder(String indexName, String aliasName) throws IOException {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(".test-[0-9]+*")
            .setSettings(Settings.EMPTY)
            .setVersionMetaKey(VERSION_META_KEY)
            .setOrigin("test")
            .setAliasName(aliasName)
            .setMappings(jsonBuilder().startObject().startObject("_meta").field(VERSION_META_KEY, 1).endObject().endObject())
            .setPrimaryIndex(indexName);
    }

    private ClusterChangedEvent event(ClusterState clusterState) {
        return new ClusterChangedEvent("test-event", clusterState, EMPTY_CLUSTER_STATE);
    }

    private static ClusterState createClusterStateForMasterWithIndex(String indexName, String aliasName) {
        IndexMetadata.Builder indexMeta = IndexMetadata.builder(indexName);
        indexMeta.settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 1));
        indexMeta.putAlias(AliasMetadata.builder(aliasName).build());
        indexMeta.state(IndexMetadata.State.OPEN);

        Metadata.Builder metadataBuilder = new Metadata.Builder();
        metadataBuilder.put(indexMeta);

        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("1").roles(new HashSet<>(DiscoveryNodeRole.roles())).build())
            .masterNodeId("1")
            .localNodeId("1")
            .build();

        return ClusterState.builder(CLUSTER_NAME)
            .nodes(nodes)
            .metadata(
                Metadata.builder()
                    .generateClusterUuidIfNeeded()
                    .indices(
                        Map.of(
                            indexName,
                            IndexMetadata.builder(indexName)
                                .settings(
                                    settings(IndexVersion.current()).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                                        .build()
                                )
                                .build()
                        )
                    )
            )
            .metadata(metadataBuilder.build())
            .build();
    }

}
