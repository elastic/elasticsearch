/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.node.ShardsCapacityHealthIndicatorService.RED_INDICATOR_IMPACTS;
import static org.elasticsearch.health.node.ShardsCapacityHealthIndicatorService.SHARDS_MAX_CAPACITY_REACHED_DATA_NODES;
import static org.elasticsearch.health.node.ShardsCapacityHealthIndicatorService.SHARDS_MAX_CAPACITY_REACHED_FROZEN_NODES;
import static org.elasticsearch.health.node.ShardsCapacityHealthIndicatorService.YELLOW_INDICATOR_IMPACTS;
import static org.elasticsearch.health.node.ShardsCapacityHealthIndicatorService.calculateFrom;
import static org.elasticsearch.indices.ShardLimitValidator.FROZEN_GROUP;
import static org.elasticsearch.indices.ShardLimitValidator.INDEX_SETTING_SHARD_LIMIT_GROUP;
import static org.elasticsearch.indices.ShardLimitValidator.NORMAL_GROUP;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ShardsCapacityHealthIndicatorServiceTests extends ESTestCase {

    public static final HealthMetadata.Disk DISK_METADATA = HealthMetadata.Disk.newBuilder().build();

    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private DiscoveryNode dataNode;
    private DiscoveryNode frozenNode;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        dataNode = DiscoveryNodeUtils.builder("data_node")
            .name("data_node")
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
            .build();

        frozenNode = DiscoveryNodeUtils.builder("frozen_node")
            .name("frozen_node")
            .roles(Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE))
            .build();

        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    @BeforeClass
    public static void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestClass().getSimpleName());
    }

    @AfterClass
    public static void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testNoShardsCapacityMetadata() throws IOException {
        var clusterService = createClusterService(
            createClusterState(
                randomValidMaxShards(),
                randomValidMaxShards(),
                new HealthMetadata(DISK_METADATA, null),
                createIndexInDataNode(100)
            )
        );
        var target = new ShardsCapacityHealthIndicatorService(clusterService);
        var indicatorResult = target.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

        assertEquals(indicatorResult.status(), HealthStatus.UNKNOWN);
        assertTrue(indicatorResult.impacts().isEmpty());
        assertTrue(indicatorResult.diagnosisList().isEmpty());
        assertEquals(indicatorResult.symptom(), "Unable to determine shard capacity status.");
        assertEquals(xContentToMap(indicatorResult.details()), Map.of());
    }

    public void testIndicatorYieldsGreenInCaseThereIsRoom() throws IOException {
        int maxShardsPerNode = randomValidMaxShards();
        int maxShardsPerNodeFrozen = randomValidMaxShards();
        var clusterService = createClusterService(maxShardsPerNode, maxShardsPerNodeFrozen, createIndexInDataNode(maxShardsPerNode / 4));
        var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

        assertEquals(indicatorResult.status(), HealthStatus.GREEN);
        assertTrue(indicatorResult.impacts().isEmpty());
        assertTrue(indicatorResult.diagnosisList().isEmpty());
        assertEquals(indicatorResult.symptom(), "The cluster has enough room to add new shards.");
        assertThat(
            xContentToMap(indicatorResult.details()),
            is(
                Map.of(
                    "data",
                    Map.of("max_shards_in_cluster", maxShardsPerNode),
                    "frozen",
                    Map.of("max_shards_in_cluster", maxShardsPerNodeFrozen)
                )
            )
        );
    }

    public void testDiagnoses() {
        assertEquals("shards_capacity", SHARDS_MAX_CAPACITY_REACHED_DATA_NODES.definition().indicatorName());
        assertEquals("decrease_shards_per_non_frozen_node", SHARDS_MAX_CAPACITY_REACHED_DATA_NODES.definition().id());
        assertThat(
            SHARDS_MAX_CAPACITY_REACHED_DATA_NODES.definition().cause(),
            allOf(containsString("maximum number of shards"), containsString(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey()))
        );
        assertThat(
            SHARDS_MAX_CAPACITY_REACHED_DATA_NODES.definition().action(),
            allOf(containsString("Increase the number of nodes in your cluster"), containsString("remove some non-frozen indices"))
        );

        assertEquals("shards_capacity", SHARDS_MAX_CAPACITY_REACHED_FROZEN_NODES.definition().indicatorName());
        assertEquals("decrease_shards_per_frozen_node", SHARDS_MAX_CAPACITY_REACHED_FROZEN_NODES.definition().id());
        assertThat(
            SHARDS_MAX_CAPACITY_REACHED_FROZEN_NODES.definition().cause(),
            allOf(containsString("maximum number of shards"), containsString(SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.getKey()))
        );
        assertThat(
            SHARDS_MAX_CAPACITY_REACHED_FROZEN_NODES.definition().action(),
            allOf(containsString("Increase the number of nodes in your cluster"), containsString("remove some frozen indices"))
        );
    }

    public void testIndicatorYieldsYellowInCaseThereIsNotEnoughRoom() throws IOException {
        {
            // Only data_nodes does not have enough space
            int maxShardsPerNodeFrozen = randomValidMaxShards();
            var clusterService = createClusterService(25, maxShardsPerNodeFrozen, createIndexInDataNode(4));
            var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), YELLOW);
            assertEquals(indicatorResult.symptom(), "Cluster is close to reaching the configured maximum number of shards for data nodes.");
            assertThat(indicatorResult.impacts(), equalTo(YELLOW_INDICATOR_IMPACTS));
            assertThat(indicatorResult.diagnosisList(), hasSize(1));
            assertThat(indicatorResult.diagnosisList().get(0), equalTo(SHARDS_MAX_CAPACITY_REACHED_DATA_NODES));
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "data",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 8),
                        "frozen",
                        Map.of("max_shards_in_cluster", maxShardsPerNodeFrozen)
                    )
                )
            );
        }
        {
            // Only frozen_nodes does not have enough space
            int maxShardsPerNode = randomValidMaxShards();
            var clusterService = createClusterService(maxShardsPerNode, 25, createIndexInFrozenNode(4));
            var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), YELLOW);
            assertEquals(
                indicatorResult.symptom(),
                "Cluster is close to reaching the configured maximum number of shards for frozen nodes."
            );
            assertThat(indicatorResult.impacts(), equalTo(YELLOW_INDICATOR_IMPACTS));
            assertThat(indicatorResult.diagnosisList(), hasSize(1));
            assertThat(indicatorResult.diagnosisList().get(0), equalTo(SHARDS_MAX_CAPACITY_REACHED_FROZEN_NODES));
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "data",
                        Map.of("max_shards_in_cluster", maxShardsPerNode),
                        "frozen",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 8)
                    )
                )
            );
        }
        {
            // Both data and frozen nodes does not have enough space
            var clusterService = createClusterService(25, 25, createIndexInDataNode(4), createIndexInFrozenNode(4));
            var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), YELLOW);
            assertEquals(
                indicatorResult.symptom(),
                "Cluster is close to reaching the configured maximum number of shards for data and frozen nodes."
            );
            assertThat(indicatorResult.impacts(), equalTo(YELLOW_INDICATOR_IMPACTS));
            assertThat(
                indicatorResult.diagnosisList(),
                is(List.of(SHARDS_MAX_CAPACITY_REACHED_DATA_NODES, SHARDS_MAX_CAPACITY_REACHED_FROZEN_NODES))
            );
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "data",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 8),
                        "frozen",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 8)
                    )
                )
            );
        }
    }

    public void testIndicatorYieldsRedInCaseThereIsNotEnoughRoom() throws IOException {
        {
            // Only data_nodes does not have enough space
            int maxShardsPerNodeFrozen = randomValidMaxShards();
            var clusterService = createClusterService(25, maxShardsPerNodeFrozen, createIndexInDataNode(11));
            var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), RED);
            assertEquals(indicatorResult.symptom(), "Cluster is close to reaching the configured maximum number of shards for data nodes.");
            assertThat(indicatorResult.impacts(), equalTo(RED_INDICATOR_IMPACTS));
            assertThat(indicatorResult.diagnosisList(), hasSize(1));
            assertThat(indicatorResult.diagnosisList().get(0), equalTo(SHARDS_MAX_CAPACITY_REACHED_DATA_NODES));
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "data",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 22),
                        "frozen",
                        Map.of("max_shards_in_cluster", maxShardsPerNodeFrozen)
                    )
                )
            );
        }
        {
            // Only frozen_nodes does not have enough space
            int maxShardsPerNode = randomValidMaxShards();
            var clusterService = createClusterService(maxShardsPerNode, 25, createIndexInFrozenNode(11));
            var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), RED);
            assertEquals(
                indicatorResult.symptom(),
                "Cluster is close to reaching the configured maximum number of shards for frozen nodes."
            );
            assertThat(indicatorResult.impacts(), equalTo(RED_INDICATOR_IMPACTS));
            assertThat(indicatorResult.diagnosisList(), hasSize(1));
            assertThat(indicatorResult.diagnosisList().get(0), equalTo(SHARDS_MAX_CAPACITY_REACHED_FROZEN_NODES));
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "data",
                        Map.of("max_shards_in_cluster", maxShardsPerNode),
                        "frozen",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 22)
                    )
                )
            );
        }
        {
            // Both data and frozen nodes does not have enough space
            var clusterService = createClusterService(25, 25, createIndexInDataNode(11), createIndexInFrozenNode(11));
            var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), RED);
            assertEquals(
                indicatorResult.symptom(),
                "Cluster is close to reaching the configured maximum number of shards for data and frozen nodes."
            );
            assertThat(indicatorResult.impacts(), equalTo(RED_INDICATOR_IMPACTS));
            assertThat(
                indicatorResult.diagnosisList(),
                is(List.of(SHARDS_MAX_CAPACITY_REACHED_DATA_NODES, SHARDS_MAX_CAPACITY_REACHED_FROZEN_NODES))
            );
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "data",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 22),
                        "frozen",
                        Map.of("max_shards_in_cluster", 25, "current_used_shards", 22)
                    )
                )
            );
        }
    }

    public void testCalculateMethods() {
        var mockedState = ClusterState.EMPTY_STATE;
        var randomMaxShardsPerNodeSetting = randomInt();
        Function<Integer, ShardsCapacityHealthIndicatorService.ShardsCapacityChecker> checkerWrapper = shardsToAdd -> (
            maxConfiguredShardsPerNode,
            numberOfNewShards,
            replicas,
            discoveryNodes,
            metadata) -> {
            assertEquals(mockedState.nodes(), discoveryNodes);
            assertEquals(mockedState.metadata(), metadata);
            assertEquals(randomMaxShardsPerNodeSetting, maxConfiguredShardsPerNode);
            return new ShardLimitValidator.Result(
                numberOfNewShards != shardsToAdd && replicas == 1,
                Optional.empty(),
                randomInt(),
                randomInt(),
                randomAlphaOfLength(5)
            );
        };

        assertEquals(
            calculateFrom(randomMaxShardsPerNodeSetting, mockedState.nodes(), mockedState.metadata(), checkerWrapper.apply(5)).status(),
            RED
        );
        assertEquals(
            calculateFrom(randomMaxShardsPerNodeSetting, mockedState.nodes(), mockedState.metadata(), checkerWrapper.apply(10)).status(),
            YELLOW
        );

        // Let's cover the holes :)
        Stream.of(randomIntBetween(1, 4), randomIntBetween(6, 9), randomIntBetween(11, Integer.MAX_VALUE))
            .map(checkerWrapper)
            .map(checker -> calculateFrom(randomMaxShardsPerNodeSetting, mockedState.nodes(), mockedState.metadata(), checker))
            .map(ShardsCapacityHealthIndicatorService.StatusResult::status)
            .forEach(status -> assertEquals(status, GREEN));
    }

    // We expose the indicator name and the diagnoses in the x-pack usage API. In order to index them properly in a telemetry index
    // they need to be declared in the health-api-indexer.edn in the telemetry repository.
    public void testMappedFieldsForTelemetry() {
        assertEquals(ShardsCapacityHealthIndicatorService.NAME, "shards_capacity");
        assertEquals(
            "elasticsearch:health:shards_capacity:diagnosis:decrease_shards_per_non_frozen_node",
            SHARDS_MAX_CAPACITY_REACHED_DATA_NODES.definition().getUniqueId()
        );
        assertEquals(
            "elasticsearch:health:shards_capacity:diagnosis:decrease_shards_per_frozen_node",
            SHARDS_MAX_CAPACITY_REACHED_FROZEN_NODES.definition().getUniqueId()
        );
    }

    public void testSkippingFieldsWhenVerboseIsFalse() {
        int maxShardsPerNodeFrozen = randomValidMaxShards();
        var clusterService = createClusterService(25, maxShardsPerNodeFrozen, createIndexInDataNode(11));
        var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(false, HealthInfo.EMPTY_HEALTH_INFO);

        assertEquals(indicatorResult.status(), RED);
        assertEquals(indicatorResult.symptom(), "Cluster is close to reaching the configured maximum number of shards for data nodes.");
        assertThat(indicatorResult.impacts(), equalTo(RED_INDICATOR_IMPACTS));
        assertThat(indicatorResult.diagnosisList(), hasSize(0));
        assertThat(indicatorResult.details(), is(HealthIndicatorDetails.EMPTY));
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

    private ClusterService createClusterService(ClusterState clusterState) {
        ClusterServiceUtils.setState(clusterService, clusterState);
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
        var clusterState = ClusterStateCreationUtils.state(dataNode, dataNode, dataNode, new DiscoveryNode[] { dataNode, frozenNode })
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));

        var metadata = Metadata.builder()
            .persistentSettings(
                Settings.builder()
                    .put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), maxShardsPerNode)
                    .put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.getKey(), maxShardsPerNodeFrozen)
                    .build()
            );

        for (var idxMetadata : indexMetadata) {
            metadata.put(idxMetadata);
        }

        return ClusterState.builder(clusterState).metadata(metadata).build();
    }

    private static IndexMetadata.Builder createIndexInDataNode(int shards) {
        return createIndex(shards, NORMAL_GROUP);
    }

    private static IndexMetadata.Builder createIndexInFrozenNode(int shards) {
        return createIndex(shards, FROZEN_GROUP);
    }

    private static IndexMetadata.Builder createIndex(int shards, String group) {
        return IndexMetadata.builder("index-" + randomAlphaOfLength(20))
            .settings(
                indexSettings(IndexVersion.current(), shards, 1).put(SETTING_CREATION_DATE, System.currentTimeMillis())
                    .put(INDEX_SETTING_SHARD_LIMIT_GROUP.getKey(), group)
            );
    }
}
