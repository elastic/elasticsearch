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
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.node.ShardsCapacityHealthIndicatorService.RED_INDICATOR_IMPACTS;
import static org.elasticsearch.health.node.ShardsCapacityHealthIndicatorService.SHARDS_MAX_CAPACITY_REACHED_DATA_NODES;
import static org.elasticsearch.health.node.ShardsCapacityHealthIndicatorService.YELLOW_INDICATOR_IMPACTS;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ShardsCapacityHealthIndicatorServiceStatelessTests extends ESTestCase {

    public static final HealthMetadata.Disk DISK_METADATA = HealthMetadata.Disk.newBuilder().build();

    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private Set<ProjectId> projectIds;

    @Before
    public void startClusterService() {
        projectIds = randomBoolean()
            ? IntStream.range(0, randomIntBetween(1, 5)).mapToObj(i -> randomUniqueProjectId()).collect(toSet())
            : Set.of(randomProjectIdOrDefault());

        clusterService = ClusterServiceUtils.createClusterService(threadPool, Settings.builder().put("stateless.enabled", true).build());
    }

    @After
    public void stopClusterService() {
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
        var clusterService = createClusterService(maxShardsPerNode, 1, 1, () -> new IndexMetadata.Builder[] { createIndex(1) });
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

    public void testNoShardsCapacityMetadata() throws IOException {
        int maxShardsPerNode = randomValidMaxShards();
        var clusterService = createClusterService(
            maxShardsPerNode,
            1,
            1,
            new HealthMetadata(DISK_METADATA, null),
            () -> new IndexMetadata.Builder[] { createIndex(100) }
        );
        var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

        assertEquals(HealthStatus.UNKNOWN, indicatorResult.status());
        assertTrue(indicatorResult.impacts().isEmpty());
        assertTrue(indicatorResult.diagnosisList().isEmpty());
        assertEquals("Unable to determine shard capacity status.", indicatorResult.symptom());
        assertEquals(Map.of(), xContentToMap(indicatorResult.details()));
    }

    public void testIndicatorYieldsYellowInCaseThereIsNotEnoughRoom() throws IOException {
        int primariesPerProject = randomValidMaxShards();
        // Leave 9 shards of room since this is too few for the 10-shard yellow probe but enough for the 5-shard red probe.
        int maxShardsPerNode = primariesPerProject * projectIds.size() + 9;
        assertNotEnoughRoom(YELLOW, maxShardsPerNode, primariesPerProject);
    }

    public void testIndicatorYieldsRedInCaseThereIsNotEnoughRoom() throws IOException {
        int primariesPerProject = randomValidMaxShards();
        // Leave 4 shards of room since this is too few for the 5-shard red probe.
        int maxShardsPerNode = primariesPerProject * projectIds.size() + 4;
        assertNotEnoughRoom(RED, maxShardsPerNode, primariesPerProject);
    }

    public void testSkippingFieldsWhenVerboseIsFalse() {
        int primariesPerProject = randomValidMaxShards();
        int maxShardsPerNode = primariesPerProject * projectIds.size() + 4;
        var clusterService = createClusterService(
            maxShardsPerNode,
            1,
            1,
            () -> new IndexMetadata.Builder[] { createIndex(primariesPerProject) }
        );
        var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(false, HealthInfo.EMPTY_HEALTH_INFO);

        assertEquals(RED, indicatorResult.status());
        assertEquals(
            "Cluster is close to reaching the configured maximum number of shards for index and search nodes.",
            indicatorResult.symptom()
        );
        assertThat(indicatorResult.impacts(), equalTo(RED_INDICATOR_IMPACTS));
        assertThat(indicatorResult.diagnosisList(), hasSize(0));
        assertThat(indicatorResult.details(), is(HealthIndicatorDetails.EMPTY));
    }

    public void testDetailsIncludesProjectsOrderedByUsedShards() throws IOException {
        ProjectId mostUsed = ProjectId.fromId("proja");
        ProjectId middleUsed = ProjectId.fromId("projb");
        ProjectId leastUsed = ProjectId.fromId("projc");
        // The three projects will have 20+10+5 primaries (and the same number of replicas).
        // Setting the limit to 44 (only 9 shards of room) will make the indicator YELLOW
        int maxShardsPerNode = 44;
        var clusterService = createClusterService(
            maxShardsPerNode,
            1,
            1,
            Map.of(mostUsed, List.of(createIndex(20)), middleUsed, List.of(createIndex(10)), leastUsed, List.of(createIndex(5)))
        );
        var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

        assertEquals(YELLOW, indicatorResult.status());
        Map<String, Object> expectedProjects = Map.of(
            "proja",
            Map.of("current_used_shards", 20),
            "projb",
            Map.of("current_used_shards", 10),
            "projc",
            Map.of("current_used_shards", 5)
        );
        Map<String, Object> index = indexDetails(indicatorResult);
        assertThat(index.get("max_shards_in_cluster"), is(maxShardsPerNode));
        assertThat(index.get("current_used_shards"), is(35));
        assertThat(projectsDetails(index), is(expectedProjects));
        Map<String, Object> search = searchDetails(indicatorResult);
        assertThat(search.get("max_shards_in_cluster"), is(maxShardsPerNode));
        assertThat(search.get("current_used_shards"), is(35));
        assertThat(projectsDetails(search), is(expectedProjects));
        assertThat(
            Strings.toString(indicatorResult.details()),
            containsString(
                "\"projects\":{\"proja\":{\"current_used_shards\":20},\"projb\":{\"current_used_shards\":10},"
                    + "\"projc\":{\"current_used_shards\":5}}"
            )
        );
    }

    // If the `size` query parameter is less than the number of projects then we should only include
    // the top `size` projects
    public void testDetailsProjectsHonorsSize() throws IOException {
        ProjectId mostUsed = ProjectId.fromId("proja");
        ProjectId middleUsed = ProjectId.fromId("projb");
        ProjectId leastUsed = ProjectId.fromId("projc");
        int maxShardsPerNode = 44;
        var clusterService = createClusterService(
            maxShardsPerNode,
            1,
            1,
            Map.of(mostUsed, List.of(createIndex(20)), middleUsed, List.of(createIndex(10)), leastUsed, List.of(createIndex(5)))
        );
        var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, 2, HealthInfo.EMPTY_HEALTH_INFO);

        assertEquals(YELLOW, indicatorResult.status());
        Map<String, Object> expectedTopProjects = Map.of(
            "proja",
            Map.of("current_used_shards", 20),
            "projb",
            Map.of("current_used_shards", 10)
        );
        assertThat(projectsDetails(indexDetails(indicatorResult)), is(expectedTopProjects));
        assertThat(projectsDetails(searchDetails(indicatorResult)), is(expectedTopProjects));
        String detailsJson = Strings.toString(indicatorResult.details());
        assertThat(
            detailsJson,
            containsString("\"projects\":{\"proja\":{\"current_used_shards\":20},\"projb\":{\"current_used_shards\":10}}")
        );
        assertThat(detailsJson, not(containsString("projc")));
    }

    public void testSizeZeroOmitsProjects() throws IOException {
        int maxShardsPerNode = 44;
        var clusterService = createClusterService(
            maxShardsPerNode,
            1,
            1,
            Map.of(
                ProjectId.fromId("proja"),
                List.of(createIndex(20)),
                ProjectId.fromId("projb"),
                List.of(createIndex(10)),
                ProjectId.fromId("projc"),
                List.of(createIndex(5))
            )
        );
        var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, 0, HealthInfo.EMPTY_HEALTH_INFO);

        assertEquals(YELLOW, indicatorResult.status());
        assertThat(indexDetails(indicatorResult).get("current_used_shards"), is(35));
        assertThat(searchDetails(indicatorResult).get("current_used_shards"), is(35));
        assertThat(projectsDetails(indexDetails(indicatorResult)), is(Map.of()));
        assertThat(projectsDetails(searchDetails(indicatorResult)), is(Map.of()));
        assertThat(Strings.toString(indicatorResult.details()), containsString("\"projects\":{}"));
        assertThat(Strings.toString(indicatorResult.details()), not(containsString("proja")));
    }

    private void assertNotEnoughRoom(HealthStatus status, int maxShardsPerNode, int indexNumShards) throws IOException {
        var expectedImpacts = switch (status) {
            case RED -> RED_INDICATOR_IMPACTS;
            case YELLOW -> YELLOW_INDICATOR_IMPACTS;
            default -> throw new AssertionError("expected RED or YELLOW but was " + status);
        };

        {
            // Only index nodes do not have enough space
            var clusterService = createClusterService(
                maxShardsPerNode,
                1,
                2,
                () -> new IndexMetadata.Builder[] { createIndex(indexNumShards) }
            );
            var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(status, indicatorResult.status());
            assertEquals(
                "Cluster is close to reaching the configured maximum number of shards for index nodes.",
                indicatorResult.symptom()
            );
            assertThat(indicatorResult.impacts(), equalTo(expectedImpacts));
            assertThat(indicatorResult.diagnosisList(), hasSize(1));
            assertThat(indicatorResult.diagnosisList().getFirst(), equalTo(SHARDS_MAX_CAPACITY_REACHED_DATA_NODES));
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "index",
                        expectedNodeTypeDetails(maxShardsPerNode, indexNumShards * projectIds.size(), indexNumShards),
                        "search",
                        Map.of("max_shards_in_cluster", 2 * maxShardsPerNode)
                    )
                )
            );
        }
        {
            // Only search nodes do not have enough space
            var clusterService = createClusterService(
                maxShardsPerNode,
                2,
                1,
                () -> new IndexMetadata.Builder[] { createIndex(indexNumShards) }
            );
            var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(status, indicatorResult.status());
            assertEquals(
                "Cluster is close to reaching the configured maximum number of shards for search nodes.",
                indicatorResult.symptom()
            );
            assertThat(indicatorResult.impacts(), equalTo(expectedImpacts));
            assertThat(indicatorResult.diagnosisList(), hasSize(1));
            assertThat(indicatorResult.diagnosisList().getFirst(), equalTo(SHARDS_MAX_CAPACITY_REACHED_DATA_NODES));
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "index",
                        Map.of("max_shards_in_cluster", 2 * maxShardsPerNode),
                        "search",
                        expectedNodeTypeDetails(maxShardsPerNode, indexNumShards * projectIds.size(), indexNumShards)
                    )
                )
            );
        }
        {
            // Both index and search nodes do not have enough space
            var clusterService = createClusterService(
                maxShardsPerNode,
                1,
                1,
                () -> new IndexMetadata.Builder[] { createIndex(indexNumShards) }
            );
            var indicatorResult = new ShardsCapacityHealthIndicatorService(clusterService).calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

            assertEquals(status, indicatorResult.status());
            assertEquals(
                "Cluster is close to reaching the configured maximum number of shards for index and search nodes.",
                indicatorResult.symptom()
            );
            assertThat(indicatorResult.impacts(), equalTo(expectedImpacts));
            assertThat(indicatorResult.diagnosisList(), hasSize(1));
            assertThat(indicatorResult.diagnosisList().getFirst(), equalTo(SHARDS_MAX_CAPACITY_REACHED_DATA_NODES));
            assertThat(
                xContentToMap(indicatorResult.details()),
                is(
                    Map.of(
                        "index",
                        expectedNodeTypeDetails(maxShardsPerNode, indexNumShards * projectIds.size(), indexNumShards),
                        "search",
                        expectedNodeTypeDetails(maxShardsPerNode, indexNumShards * projectIds.size(), indexNumShards)
                    )
                )
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

    @SuppressWarnings("unchecked")
    private Map<String, Object> indexDetails(HealthIndicatorResult indicatorResult) throws IOException {
        return (Map<String, Object>) xContentToMap(indicatorResult.details()).get("index");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> searchDetails(HealthIndicatorResult indicatorResult) throws IOException {
        return (Map<String, Object>) xContentToMap(indicatorResult.details()).get("search");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> projectsDetails(Map<String, Object> index) {
        return (Map<String, Object>) index.get("projects");
    }

    private Map<String, Object> expectedNodeTypeDetails(int maxShardsInCluster, int currentUsedShards, int shardsUsedPerProject) {
        Map<String, Object> details = new HashMap<>();
        details.put("max_shards_in_cluster", maxShardsInCluster);
        details.put("current_used_shards", currentUsedShards);
        if (projectIds.size() > 1) {
            Map<String, Object> projects = new LinkedHashMap<>();
            projectIds.stream()
                .sorted(Comparator.comparing(ProjectId::id))
                .forEach(id -> projects.put(id.id(), Map.of("current_used_shards", shardsUsedPerProject)));
            details.put("projects", projects);
        }
        return details;
    }

    private ClusterService createClusterService(
        int maxShardsPerNode,
        int numIndexNodes,
        int numSearchNodes,
        Supplier<IndexMetadata.Builder[]> perProjectIndices
    ) {
        Map<ProjectId, List<IndexMetadata.Builder>> indicesByProject = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            indicesByProject.put(projectId, List.of(perProjectIndices.get()));
        }
        return createClusterService(
            maxShardsPerNode,
            numIndexNodes,
            numSearchNodes,
            new HealthMetadata(DISK_METADATA, new HealthMetadata.ShardLimits(maxShardsPerNode, 0, 10, 5)),
            indicesByProject
        );
    }

    private ClusterService createClusterService(
        int maxShardsPerNode,
        int numIndexNodes,
        int numSearchNodes,
        Map<ProjectId, List<IndexMetadata.Builder>> indicesByProject
    ) {
        return createClusterService(
            maxShardsPerNode,
            numIndexNodes,
            numSearchNodes,
            new HealthMetadata(DISK_METADATA, new HealthMetadata.ShardLimits(maxShardsPerNode, 0, 10, 5)),
            indicesByProject
        );
    }

    private ClusterService createClusterService(
        int maxShardsPerNode,
        int numIndexNodes,
        int numSearchNodes,
        HealthMetadata healthMetadata,
        Supplier<IndexMetadata.Builder[]> perProjectIndices
    ) {
        Map<ProjectId, List<IndexMetadata.Builder>> indicesByProject = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            indicesByProject.put(projectId, List.of(perProjectIndices.get()));
        }
        return createClusterService(maxShardsPerNode, numIndexNodes, numSearchNodes, healthMetadata, indicesByProject);
    }

    private ClusterService createClusterService(
        int maxShardsPerNode,
        int numIndexNodes,
        int numSearchNodes,
        HealthMetadata healthMetadata,
        Map<ProjectId, List<IndexMetadata.Builder>> indicesByProject
    ) {
        final ClusterState clusterState = createClusterState(
            nodesWithIndexAndSearch(numIndexNodes, numSearchNodes),
            maxShardsPerNode,
            healthMetadata,
            indicesByProject
        );
        ClusterServiceUtils.setState(clusterService, clusterState);
        return clusterService;
    }

    private ClusterState createClusterState(
        DiscoveryNodes discoveryNodes,
        int maxShardsPerNode,
        HealthMetadata healthMetadata,
        Map<ProjectId, List<IndexMetadata.Builder>> indicesByProject
    ) {
        var clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(discoveryNodes)
            .build()
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));

        var metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), maxShardsPerNode).build());
        var routingTable = GlobalRoutingTable.builder();

        for (var entry : indicesByProject.entrySet()) {
            var project = ProjectMetadata.builder(entry.getKey());
            for (var idxMetadata : entry.getValue()) {
                project.put(idxMetadata);
            }
            metadata.put(project);
            routingTable.put(entry.getKey(), RoutingTable.builder().build());
        }

        return ClusterState.builder(clusterState).metadata(metadata).routingTable(routingTable.build()).build();
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
