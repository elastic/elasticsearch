/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.index.IndexVersion;
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
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.hamcrest.Matchers.is;

public class ShardsCapacityHealthIndicatorServiceStatelessTests extends ESTestCase {

    public static final HealthMetadata.Disk DISK_METADATA = HealthMetadata.Disk.newBuilder().build();

    private static ThreadPool threadPool;

    private ClusterService clusterService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = ClusterServiceUtils.createClusterService(threadPool, Settings.builder().put("stateless.enabled", true).build());
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

    public void testIndicatorYieldsGreenInCaseThereIsRoom() throws IOException {
        int maxShardsPerNode = randomValidMaxShards();
        var clusterService = createClusterService(maxShardsPerNode, 1, 1, createIndex(maxShardsPerNode / 4));
        var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

        assertEquals(HealthStatus.GREEN, indicatorResult.status());
        assertTrue(indicatorResult.impacts().isEmpty());
        assertTrue(indicatorResult.diagnosisList().isEmpty());
        assertEquals("The cluster has enough room to add new shards.", indicatorResult.symptom());
        assertThat(
            xContentToMap(indicatorResult.details()),
            is(
                Map.of(
                    "index",
                    Map.of("max_shards_in_cluster", maxShardsPerNode),
                    "search",
                    Map.of("max_shards_in_cluster", maxShardsPerNode)
                )
            )
        );
    }

    public void testIndicatorYieldsColorInCaseThereIsNotEnoughRoom() {
        final int maxShardsPerNode = randomValidMaxShards();
        // Red health if there is no room for 5 more shards, so we take 4 out of the max
        doTestIndicatorYieldsYellowInCaseThereIsNotEnoughRoom(RED, maxShardsPerNode, maxShardsPerNode - 4);
        // Yellow health if there is no room for 10 more shards, so we take 9 out of the max
        doTestIndicatorYieldsYellowInCaseThereIsNotEnoughRoom(YELLOW, maxShardsPerNode, maxShardsPerNode - 9);
    }

    public void doTestIndicatorYieldsYellowInCaseThereIsNotEnoughRoom(HealthStatus status, int maxShardsPerNode, int indexNumShards) {
        {
            // Only index does not have enough space
            var clusterService = createClusterService(maxShardsPerNode, 1, 2, createIndex(indexNumShards));
            var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), status);
            assertEquals(
                "Cluster is close to reaching the configured maximum number of shards for index nodes.",
                indicatorResult.symptom()
            );
        }
        {
            // Only search does not have enough space
            var clusterService = createClusterService(maxShardsPerNode, 2, 1, createIndex(indexNumShards));
            var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), status);
            assertEquals(
                "Cluster is close to reaching the configured maximum number of shards for search nodes.",
                indicatorResult.symptom()
            );
        }
        {
            // Both data and frozen nodes does not have enough space
            var clusterService = createClusterService(maxShardsPerNode, 1, 1, createIndex(indexNumShards));
            var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(indicatorResult.status(), status);
            assertEquals(
                "Cluster is close to reaching the configured maximum number of shards for index and search nodes.",
                indicatorResult.symptom()
            );
        }
    }

    private static int randomValidMaxShards() {
        return randomIntBetween(15, 100);
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        return parser.map();
    }

    private ClusterService createClusterService(
        int maxShardsPerNode,
        int numIndexNodes,
        int numSearchNodes,
        IndexMetadata.Builder... indexMetadata
    ) {
        final ClusterState clusterState = createClusterState(
            nodesWithIndexAndSearch(numIndexNodes, numSearchNodes),
            maxShardsPerNode,
            new HealthMetadata(DISK_METADATA, new HealthMetadata.ShardLimits(maxShardsPerNode, 0, 10, 5)),
            indexMetadata
        );
        ClusterServiceUtils.setState(clusterService, clusterState);
        return clusterService;
    }

    private ClusterState createClusterState(
        DiscoveryNodes discoveryNodes,
        int maxShardsPerNode,
        HealthMetadata healthMetadata,
        IndexMetadata.Builder... indexMetadata
    ) {
        var clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(discoveryNodes)
            .build()
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));

        var metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), maxShardsPerNode).build());

        for (var idxMetadata : indexMetadata) {
            metadata.put(ProjectMetadata.builder(ProjectId.DEFAULT).put(idxMetadata));
        }

        return ClusterState.builder(clusterState).metadata(metadata).build();
    }

    private DiscoveryNodes nodesWithIndexAndSearch(int numIndexNodes, int numSearchNodes) {
        assert numIndexNodes > 0 : "there must be at least one index node";
        final String indexNodeId = "index";
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        builder.add(
            DiscoveryNodeUtils.builder(indexNodeId).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE)).build()
        );

        for (int i = 1; i < numIndexNodes; i++) {
            builder.add(DiscoveryNodeUtils.builder("index-" + i).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build());
        }
        for (int i = 0; i < numSearchNodes; i++) {
            builder.add(DiscoveryNodeUtils.builder("search-" + i).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build());
        }
        return builder.localNodeId(indexNodeId).masterNodeId(indexNodeId).build();
    }

    private static IndexMetadata.Builder createIndex(int shards) {
        return IndexMetadata.builder("index-" + randomAlphaOfLength(20))
            .settings(indexSettings(IndexVersion.current(), shards, 1).put(SETTING_CREATION_DATE, System.currentTimeMillis()));
    }

}
