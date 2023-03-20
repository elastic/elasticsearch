/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.health.node.ShardLimitsHealthIndicatorService.RED_INDICATOR_IMPACTS;
import static org.elasticsearch.health.node.ShardLimitsHealthIndicatorService.SHARD_LIMITS_REACHED_FROZEN_NODES;
import static org.elasticsearch.health.node.ShardLimitsHealthIndicatorService.SHARD_LIMITS_REACHED_NORMAL_NODES;
import static org.elasticsearch.health.node.ShardLimitsHealthIndicatorService.YELLOW_INDICATOR_IMPACTS;
import static org.elasticsearch.indices.ShardLimitValidator.FROZEN_GROUP;
import static org.elasticsearch.indices.ShardLimitValidator.INDEX_SETTING_SHARD_LIMIT_GROUP;
import static org.elasticsearch.indices.ShardLimitValidator.NORMAL_GROUP;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardLimitsHealthIndicatorServiceTests extends ESTestCase {

    public static final HealthMetadata.Disk DISK_METADATA = HealthMetadata.Disk.newBuilder().build();
    private DiscoveryNode normalNode;
    private DiscoveryNode frozenNode;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        normalNode = new DiscoveryNode(
            "normal_node",
            "normal_node",
            ESTestCase.buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        frozenNode = new DiscoveryNode(
            "frozen_node",
            "frozen_node",
            ESTestCase.buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE),
            Version.CURRENT
        );
    }

    public void testNoShardLimitsMetadata() throws IOException {
        var clusterService = createClusterService(
            createClusterState(
                randomValidMaxShards(),
                randomValidMaxShards(),
                new HealthMetadata(DISK_METADATA, null),
                createIndexInNormalNode(100)
            )
        );
        var target = new ShardLimitsHealthIndicatorService(clusterService);
        var indicatorResult = target.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

        assertEquals(indicatorResult.status(), HealthStatus.UNKNOWN);
        assertTrue(indicatorResult.impacts().isEmpty());
        assertTrue(indicatorResult.diagnosisList().isEmpty());
        assertEquals(indicatorResult.symptom(), "No shard limits data.");
        assertEquals(xContentToMap(indicatorResult.details()), Map.of());
    }

    public void testIndicatorYieldsGreenInCaseThereIsRoom() throws IOException {
        int maxShardsPerNode = randomValidMaxShards();
        int maxShardsPerNodeFrozen = randomValidMaxShards();
        var clusterService = createClusterService(maxShardsPerNode, maxShardsPerNodeFrozen, createIndexInNormalNode(maxShardsPerNode / 4));
        var indicatorResult = new ShardLimitsHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

        assertEquals(indicatorResult.status(), HealthStatus.GREEN);
        assertTrue(indicatorResult.impacts().isEmpty());
        assertTrue(indicatorResult.diagnosisList().isEmpty());
        assertEquals(indicatorResult.symptom(), "The cluster has enough room to add new shards.");
        assertThat(
            xContentToMap(indicatorResult.details()),
            is(
                Map.of(
                    "normal_nodes",
                    Map.of("max_shards_in_cluster", maxShardsPerNode),
                    "frozen_nodes",
                    Map.of("max_shards_in_cluster", maxShardsPerNodeFrozen)
                )
            )
        );
    }

    public void testIndicatorYieldsYellowInCaseThereIsNotEnoughRoom() throws IOException {
        {
            // Only normal_nodes does not have enough space
            int maxShardsPerNodeFrozen = randomValidMaxShards();
            var clusterService = createClusterService(25, maxShardsPerNodeFrozen, createIndexInNormalNode(4));
            var indicatorResult = new ShardLimitsHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), HealthStatus.YELLOW);
            assertEquals(indicatorResult.symptom(), "Cluster is close to reaching the maximum number of shards for normal nodes.");
            assertThat(indicatorResult.impacts(), equalTo(YELLOW_INDICATOR_IMPACTS));
            assertThat(indicatorResult.diagnosisList(), hasSize(1));
            assertThat(indicatorResult.diagnosisList().get(0), equalTo(SHARD_LIMITS_REACHED_NORMAL_NODES));
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "normal_nodes",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 8),
                        "frozen_nodes",
                        Map.of("max_shards_in_cluster", maxShardsPerNodeFrozen)
                    )
                )
            );
        }
        {
            // Only frozen_nodes does not have enough space
            int maxShardsPerNode = randomValidMaxShards();
            var clusterService = createClusterService(maxShardsPerNode, 25, createIndexInFrozenNode(4));
            var indicatorResult = new ShardLimitsHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), HealthStatus.YELLOW);
            assertEquals(indicatorResult.symptom(), "Cluster is close to reaching the maximum number of shards for frozen nodes.");
            assertThat(indicatorResult.impacts(), equalTo(YELLOW_INDICATOR_IMPACTS));
            assertThat(indicatorResult.diagnosisList(), hasSize(1));
            assertThat(indicatorResult.diagnosisList().get(0), equalTo(SHARD_LIMITS_REACHED_FROZEN_NODES));
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "normal_nodes",
                        Map.of("max_shards_in_cluster", maxShardsPerNode),
                        "frozen_nodes",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 8)
                    )
                )
            );
        }
        {
            // Both normal and frozen nodes does not have enough space
            var clusterService = createClusterService(25, 25, createIndexInNormalNode(4), createIndexInFrozenNode(4));
            var indicatorResult = new ShardLimitsHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), HealthStatus.YELLOW);
            assertEquals(
                indicatorResult.symptom(),
                "Cluster is close to reaching the maximum number of shards for normal and frozen nodes."
            );
            assertThat(indicatorResult.impacts(), equalTo(YELLOW_INDICATOR_IMPACTS));
            assertThat(indicatorResult.diagnosisList(), is(List.of(SHARD_LIMITS_REACHED_NORMAL_NODES, SHARD_LIMITS_REACHED_FROZEN_NODES)));
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "normal_nodes",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 8),
                        "frozen_nodes",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 8)
                    )
                )
            );
        }
    }

    public void testIndicatorYieldsRedInCaseThereIsNotEnoughRoom() throws IOException {
        {
            // Only normal_nodes does not have enough space
            int maxShardsPerNodeFrozen = randomValidMaxShards();
            var clusterService = createClusterService(25, maxShardsPerNodeFrozen, createIndexInNormalNode(11));
            var indicatorResult = new ShardLimitsHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), HealthStatus.RED);
            assertEquals(indicatorResult.symptom(), "Cluster is close to reaching the maximum number of shards for normal nodes.");
            assertThat(indicatorResult.impacts(), equalTo(RED_INDICATOR_IMPACTS));
            assertThat(indicatorResult.diagnosisList(), hasSize(1));
            assertThat(indicatorResult.diagnosisList().get(0), equalTo(SHARD_LIMITS_REACHED_NORMAL_NODES));
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "normal_nodes",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 22),
                        "frozen_nodes",
                        Map.of("max_shards_in_cluster", maxShardsPerNodeFrozen)
                    )
                )
            );
        }
        {
            // Only frozen_nodes does not have enough space
            int maxShardsPerNode = randomValidMaxShards();
            var clusterService = createClusterService(maxShardsPerNode, 25, createIndexInFrozenNode(11));
            var indicatorResult = new ShardLimitsHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), HealthStatus.RED);
            assertEquals(indicatorResult.symptom(), "Cluster is close to reaching the maximum number of shards for frozen nodes.");
            assertThat(indicatorResult.impacts(), equalTo(RED_INDICATOR_IMPACTS));
            assertThat(indicatorResult.diagnosisList(), hasSize(1));
            assertThat(indicatorResult.diagnosisList().get(0), equalTo(SHARD_LIMITS_REACHED_FROZEN_NODES));
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "normal_nodes",
                        Map.of("max_shards_in_cluster", maxShardsPerNode),
                        "frozen_nodes",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 22)
                    )
                )
            );
        }
        {
            // Both normal and frozen nodes does not have enough space
            var clusterService = createClusterService(25, 25, createIndexInNormalNode(11), createIndexInFrozenNode(11));
            var indicatorResult = new ShardLimitsHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), HealthStatus.RED);
            assertEquals(
                indicatorResult.symptom(),
                "Cluster is close to reaching the maximum number of shards for normal and frozen nodes."
            );
            assertThat(indicatorResult.impacts(), equalTo(RED_INDICATOR_IMPACTS));
            assertThat(indicatorResult.diagnosisList(), is(List.of(SHARD_LIMITS_REACHED_NORMAL_NODES, SHARD_LIMITS_REACHED_FROZEN_NODES)));
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "normal_nodes",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 22),
                        "frozen_nodes",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 22)
                    )
                )
            );
        }
    }

    private static int randomValidMaxShards() {
        return randomIntBetween(50, 1000);
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        return parser.map();
    }

    private static ClusterService createClusterService(ClusterState clusterState) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return clusterService;
    }

    private ClusterService createClusterService(int maxShardsPerNode, int maxShardsPerNodeFrozen, IndexMetadata.Builder... indexMetadata) {
        return createClusterService(
            createClusterState(
                maxShardsPerNode,
                maxShardsPerNodeFrozen,
                new HealthMetadata(DISK_METADATA, new HealthMetadata.ShardLimits(maxShardsPerNode, maxShardsPerNodeFrozen)),
                indexMetadata
            )
        );
    }

    private ClusterState createClusterState(
        int maxShardsPerNode,
        int maxShardsPerNodeFrozen,
        HealthMetadata healthMetadata,
        IndexMetadata.Builder... indexMetadata
    ) {
        var clusterState = ClusterStateCreationUtils.state(
            normalNode,
            normalNode,
            normalNode,
            new DiscoveryNode[] { normalNode, frozenNode }
        ).copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));

        var metadata = Metadata.builder()
            .persistentSettings(
                Settings.builder()
                    .put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), maxShardsPerNode)
                    .put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.getKey(), maxShardsPerNodeFrozen)
                    .build()
            );

        for (var idxMetadata : indexMetadata) {
            metadata.put(idxMetadata);
        }

        return ClusterState.builder(clusterState).metadata(metadata).build();
    }

    private static IndexMetadata.Builder createIndexInNormalNode(int shards) {
        return createIndex(shards, NORMAL_GROUP);
    }

    private static IndexMetadata.Builder createIndexInFrozenNode(int shards) {
        return createIndex(shards, FROZEN_GROUP);
    }

    private static IndexMetadata.Builder createIndex(int shards, String group) {
        return IndexMetadata.builder("index-" + randomAlphaOfLength(20))
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, shards)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
                    .put(INDEX_SETTING_SHARD_LIMIT_GROUP.getKey(), group)
            );
    }
}
