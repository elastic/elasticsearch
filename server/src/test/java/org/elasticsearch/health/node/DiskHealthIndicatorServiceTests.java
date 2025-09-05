/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.health.HealthIndicatorService.MAX_AFFECTED_RESOURCES_COUNT;
import static org.elasticsearch.health.ImpactArea.DEPLOYMENT_MANAGEMENT;
import static org.elasticsearch.health.node.DiskHealthIndicatorService.DiskHealthAnalyzer.INDICES_WITH_READONLY_BLOCK;
import static org.elasticsearch.health.node.DiskHealthIndicatorService.DiskHealthAnalyzer.NODES_OVER_FLOOD_STAGE_WATERMARK;
import static org.elasticsearch.health.node.DiskHealthIndicatorService.DiskHealthAnalyzer.NODES_OVER_HIGH_WATERMARK;
import static org.elasticsearch.health.node.DiskHealthIndicatorService.DiskHealthAnalyzer.NODES_WITH_ENOUGH_DISK_SPACE;
import static org.elasticsearch.health.node.DiskHealthIndicatorService.DiskHealthAnalyzer.NODES_WITH_UNKNOWN_DISK_STATUS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DiskHealthIndicatorServiceTests extends ESTestCase {

    public static final Set<DiscoveryNodeRole> DATA_ROLES = Set.of(
        DiscoveryNodeRole.DATA_ROLE,
        DiscoveryNodeRole.DATA_COLD_NODE_ROLE,
        DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE,
        DiscoveryNodeRole.DATA_HOT_NODE_ROLE,
        DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE,
        DiscoveryNodeRole.DATA_WARM_NODE_ROLE
    );

    public static final Set<DiscoveryNodeRole> NON_DATA_ROLES = Set.of(
        DiscoveryNodeRole.MASTER_ROLE,
        DiscoveryNodeRole.ML_ROLE,
        DiscoveryNodeRole.INGEST_ROLE,
        DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE,
        DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE,
        DiscoveryNodeRole.TRANSFORM_ROLE
    );

    // Other nodes refers to non-data, non-master roles in the scope of the disk health indicator
    public static final Set<DiscoveryNodeRole> OTHER_ROLES = Set.of(
        DiscoveryNodeRole.ML_ROLE,
        DiscoveryNodeRole.INGEST_ROLE,
        DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE,
        DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE,
        DiscoveryNodeRole.TRANSFORM_ROLE
    );

    private FeatureService featureService;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        featureService = Mockito.mock(FeatureService.class);
        Mockito.when(featureService.clusterHasFeature(any(), any())).thenReturn(true);
    }

    public void testServiceBasics() {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(discoveryNodes, false);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        {
            HealthStatus expectedStatus = HealthStatus.UNKNOWN;
            HealthInfo healthInfo = HealthInfo.EMPTY_HEALTH_INFO;
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
        }
        {
            HealthStatus expectedStatus = HealthStatus.YELLOW;
            HealthInfo healthInfo = createHealthInfoWithOneUnhealthyNode(expectedStatus, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
        }
        {
            HealthStatus expectedStatus = HealthStatus.RED;
            HealthInfo healthInfo = createHealthInfoWithOneUnhealthyNode(expectedStatus, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
        }
    }

    public void testIndicatorYieldsGreenWhenNodeHasUnknownStatus() {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(discoveryNodes, false);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);

        HealthStatus expectedStatus = HealthStatus.GREEN;
        HealthInfo healthInfo = createHealthInfoWithOneUnhealthyNode(HealthStatus.UNKNOWN, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
    }

    public void testGreen() throws IOException {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(discoveryNodes, false);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = HealthStatus.GREEN;
        HealthInfo healthInfo = createHealthInfoWithOneUnhealthyNode(expectedStatus, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(result.symptom(), equalTo("The cluster has enough available disk space."));
        assertThat(result.impacts().size(), equalTo(0));
        assertThat(result.diagnosisList().size(), equalTo(0));
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get(NODES_WITH_ENOUGH_DISK_SPACE), equalTo(discoveryNodes.size()));
        assertThat(details.get(NODES_WITH_UNKNOWN_DISK_STATUS), equalTo(0));
        assertThat(details.get(NODES_OVER_HIGH_WATERMARK), equalTo(0));
        assertThat(details.get(NODES_OVER_FLOOD_STAGE_WATERMARK), equalTo(0));
        assertThat(details.get(INDICES_WITH_READONLY_BLOCK), equalTo(0));
    }

    /*
     * Simulates a cluster with data nodes, dedicated master nodes and other nodes that are YELLOW.
     * We expect 3 impacts and 3 diagnosis.
     */
    public void testYellowMixedNodes() throws IOException {
        Set<DiscoveryNode> dataNodes = createNodes(DATA_ROLES);
        Set<DiscoveryNode> masterNodes = createNodes(NON_DATA_ROLES);
        Set<DiscoveryNode> otherNodes = createNodes(OTHER_ROLES);
        Map<String, Set<String>> indexNameToNodeIdsMap = new HashMap<>();
        Set<String> nodeIds = dataNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        for (int i = 0; i < randomIntBetween(1, 20); i++) {
            String indexName = randomAlphaOfLength(20);
            indexNameToNodeIdsMap.put(indexName, new HashSet<>(randomNonEmptySubsetOf(nodeIds)));
        }
        Set<DiscoveryNode> allNodes = Sets.union(Sets.union(dataNodes, masterNodes), otherNodes);

        final var clusterService = createClusterService(Set.of(), allNodes, indexNameToNodeIdsMap);
        HealthStatus expectedStatus = HealthStatus.YELLOW;
        HealthInfo healthInfo = createHealthInfo(new HealthInfoConfig(expectedStatus, allNodes.size(), allNodes));
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(result.symptom(), containsString("with roles: [data"));
        assertThat(result.symptom(), containsString("out of disk or running low on disk space."));
        assertThat(result.impacts().size(), equalTo(3));
        HealthIndicatorImpact impact = result.impacts().get(0);
        assertNotNull(impact);
        List<ImpactArea> impactAreas = impact.impactAreas();
        assertThat(impactAreas.size(), equalTo(1));
        assertThat(impactAreas.get(0), equalTo(ImpactArea.INGEST));
        assertThat(impact.severity(), equalTo(1));
        assertThat(
            impact.impactDescription(),
            startsWith("The cluster is at risk of not being able to insert or update documents in the affected indices [")
        );
        assertThat(result.diagnosisList().size(), equalTo(3));
        {
            Diagnosis diagnosis = result.diagnosisList().get(0);
            List<Diagnosis.Resource> affectedResources = diagnosis.affectedResources();
            assertThat(affectedResources.size(), equalTo(2));
            assertThat(affectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
            assertThat(affectedResources.get(0).getNodes().size(), is(dataNodes.size()));
            List<DiscoveryNode> affectedNodes = dataNodes.stream()
                .sorted(DiscoveryNode.DISCOVERY_NODE_COMPARATOR)
                .collect(Collectors.toList());
            assertThat(affectedResources.get(0).getNodes(), equalTo(affectedNodes));
            assertThat(affectedResources.get(1).getType(), is(Diagnosis.Resource.Type.INDEX));
            assertThat(affectedResources.get(1).getValues(), containsInAnyOrder(indexNameToNodeIdsMap.keySet().toArray(new String[0])));
        }
        {
            Diagnosis diagnosis = result.diagnosisList().get(1);
            List<Diagnosis.Resource> affectedResources = diagnosis.affectedResources();
            assertThat(affectedResources.size(), equalTo(1));
            assertThat(affectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
            assertThat(affectedResources.get(0).getNodes().size(), is(masterNodes.size()));
            List<DiscoveryNode> affectedNodes = masterNodes.stream()
                .sorted(DiscoveryNode.DISCOVERY_NODE_COMPARATOR)
                .collect(Collectors.toList());
            assertThat(affectedResources.get(0).getNodes(), equalTo(affectedNodes));
        }
        {
            Diagnosis diagnosis = result.diagnosisList().get(2);
            List<Diagnosis.Resource> affectedResources = diagnosis.affectedResources();
            assertThat(affectedResources.size(), equalTo(1));
            assertThat(affectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
            assertThat(affectedResources.get(0).getNodes().size(), is(otherNodes.size()));
            List<DiscoveryNode> affectedNodes = otherNodes.stream()
                .sorted(DiscoveryNode.DISCOVERY_NODE_COMPARATOR)
                .collect(Collectors.toList());
            assertThat(affectedResources.get(0).getNodes(), equalTo(affectedNodes));
        }
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get(NODES_WITH_ENOUGH_DISK_SPACE), equalTo(0));
        assertThat(details.get(NODES_WITH_UNKNOWN_DISK_STATUS), equalTo(0));
        assertThat(details.get(NODES_OVER_HIGH_WATERMARK), equalTo(allNodes.size()));
        assertThat(details.get(NODES_WITH_ENOUGH_DISK_SPACE), equalTo(0));
        assertThat(details.get(INDICES_WITH_READONLY_BLOCK), equalTo(0));
    }

    /*
     * Simulates a cluster with nodes with all roles that report RED status, the nodes have indices but and there are
     * no blocks in the cluster state. This situation can occur when the health monitor has identified that nodes are running out
     * of space but the DiskThresholdMonitor hasn't yet.
     * We expect 3 impacts and 1 diagnosis.
     */
    public void testRedNoBlockedIndicesAndRedAllRoleNodes() throws IOException {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        List<DiscoveryNode> affectedNodes = randomNonEmptySubsetOf(discoveryNodes).stream()
            .sorted(DiscoveryNode.DISCOVERY_NODE_COMPARATOR)
            .collect(Collectors.toList());
        Map<String, Set<String>> indexNameToNodeIdsMap = new HashMap<>();
        Set<String> affectedNodeIds = affectedNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        for (int i = 0; i < randomIntBetween(1, 20); i++) {
            String indexName = randomAlphaOfLength(20);
            indexNameToNodeIdsMap.put(indexName, new HashSet<>(randomNonEmptySubsetOf(affectedNodeIds)));
        }
        ClusterService clusterService = createClusterService(Set.of(), discoveryNodes, indexNameToNodeIdsMap);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        Map<String, DiskHealthInfo> diskInfoByNode = new HashMap<>();
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            if (affectedNodeIds.contains(discoveryNode.getId())) {
                diskInfoByNode.put(discoveryNode.getId(), new DiskHealthInfo(HealthStatus.RED, randomFrom(DiskHealthInfo.Cause.values())));
            } else {
                diskInfoByNode.put(discoveryNode.getId(), new DiskHealthInfo(HealthStatus.GREEN));
            }
        }
        HealthInfo healthInfo = new HealthInfo(
            diskInfoByNode,
            DataStreamLifecycleHealthInfo.NO_DSL_ERRORS,
            Map.of(),
            FileSettingsService.FileSettingsHealthInfo.INDETERMINATE
        );

        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(HealthStatus.RED));
        assertThat(
            result.symptom(),
            containsString(affectedNodes.size() + " node" + (affectedNodes.size() == 1 ? "" : "s") + " with roles: " + "[data")
        );
        assertThat(result.symptom(), containsString(" out of disk or running low on disk space."));
        assertThat(result.impacts().size(), equalTo(3));
        HealthIndicatorImpact impact = result.impacts().get(0);
        assertNotNull(impact);
        List<ImpactArea> impactAreas = impact.impactAreas();
        assertThat(impactAreas.size(), equalTo(1));
        assertThat(impactAreas.get(0), equalTo(ImpactArea.INGEST));
        assertThat(impact.severity(), equalTo(1));
        assertThat(
            impact.impactDescription(),
            startsWith("The cluster is at risk of not being able to insert or update documents in the affected indices [")
        );
        assertThat(result.diagnosisList().size(), equalTo(1));
        Diagnosis diagnosis = result.diagnosisList().get(0);
        List<Diagnosis.Resource> affectedResources = diagnosis.affectedResources();
        assertThat(affectedResources, iterableWithSize(2));
        assertThat(affectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
        assertThat(affectedResources.get(0).getNodes(), is(affectedNodes));

        assertThat(affectedResources.get(1).getType(), is(Diagnosis.Resource.Type.INDEX));
        assertThat(
            affectedResources.get(1).getValues(),
            is(
                indexNameToNodeIdsMap.keySet()
                    .stream()
                    .sorted(HealthIndicatorDisplayValues.indicesComparatorByPriorityAndName(clusterService.state().metadata()))
                    .collect(Collectors.toList())
            )
        );

        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get(NODES_WITH_ENOUGH_DISK_SPACE), equalTo(discoveryNodes.size() - affectedNodes.size()));
        assertThat(details.get(NODES_WITH_UNKNOWN_DISK_STATUS), equalTo(0));
        assertThat(details.get(NODES_OVER_HIGH_WATERMARK), equalTo(0));
        assertThat(details.get(NODES_OVER_FLOOD_STAGE_WATERMARK), equalTo(affectedNodes.size()));
        assertThat(details.get(INDICES_WITH_READONLY_BLOCK), equalTo(0));
    }

    /*
     * Simulates a cluster with healthy all-role nodes and a blocked index. This situation can occur when the cluster is recovering from
     * being out of disk space. Either the nodes that were out of space are currently above the high threshold but actively relocating
     * their shards to other nodes or all the nodes are healthy but the DiskThresholdMonitor hasn't updated yet the index status.
     * We expect 1 impact and 1 diagnosis.
     */
    public void testRedWithBlockedIndicesAndGreenNodes() throws IOException {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(discoveryNodes, true);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);

        HealthStatus expectedStatus = HealthStatus.RED;
        HealthInfo healthInfo = createHealthInfoWithOneUnhealthyNode(HealthStatus.GREEN, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(
            result.symptom(),
            equalTo(
                "1 index is not allowed to be updated. The cluster is recovering and ingest capabilities should be restored within a "
                    + "few minutes."
            )
        );
        assertThat(result.impacts().size(), equalTo(1));
        HealthIndicatorImpact impact = result.impacts().get(0);
        assertNotNull(impact);
        List<ImpactArea> impactAreas = impact.impactAreas();
        assertThat(impactAreas.size(), equalTo(1));
        assertThat(impactAreas.get(0), equalTo(ImpactArea.INGEST));
        assertThat(impact.severity(), equalTo(1));
        assertThat(impact.impactDescription(), startsWith("Cannot insert or update documents in the affected indices ["));
        assertThat(result.diagnosisList().size(), equalTo(1));
        Diagnosis diagnosis = result.diagnosisList().get(0);
        List<Diagnosis.Resource> affectedResources = diagnosis.affectedResources();
        assertThat(affectedResources.size(), is(1));
        assertThat(affectedResources.get(0).getType(), is(Diagnosis.Resource.Type.INDEX));
        assertThat(affectedResources.get(0).getValues(), iterableWithSize(1));

        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get(NODES_WITH_ENOUGH_DISK_SPACE), equalTo(discoveryNodes.size()));
        assertThat(details.get(NODES_WITH_UNKNOWN_DISK_STATUS), equalTo(0));
        assertThat(details.get(NODES_OVER_HIGH_WATERMARK), equalTo(0));
        assertThat(details.get(NODES_OVER_FLOOD_STAGE_WATERMARK), equalTo(0));
        assertThat(details.get(INDICES_WITH_READONLY_BLOCK), equalTo(1));
    }

    /*
     * Simulates a cluster with healthy all-role nodes and a blocked index. This situation can occur when the cluster was out of space,
     * the capacity has increased but not enough for all nodes to recover, there are nodes that are currently above the high threshold
     * and they are not able to relocate their shards to another node.
     * We expect 3 impact and 1 diagnosis.
     */
    public void testRedWithBlockedIndicesAndYellowNodes() throws IOException {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(discoveryNodes, true);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = HealthStatus.RED;
        int numberOfYellowNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(new HealthInfoConfig(HealthStatus.YELLOW, numberOfYellowNodes, discoveryNodes));
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(
            result.symptom(),
            equalTo(
                "1 index is not allowed to be updated. "
                    + (numberOfYellowNodes == 1 ? "1 node is" : numberOfYellowNodes + " nodes are")
                    + " out of disk or running low on disk space."
            )
        );
        assertThat(result.impacts().size(), equalTo(3));
        HealthIndicatorImpact impact = result.impacts().get(0);
        assertNotNull(impact);
        List<ImpactArea> impactAreas = impact.impactAreas();
        assertThat(impactAreas.size(), equalTo(1));
        assertThat(impactAreas.get(0), equalTo(ImpactArea.INGEST));
        assertThat(impact.severity(), equalTo(1));
        assertThat(impact.impactDescription(), startsWith("Cannot insert or update documents in the affected indices ["));
        assertThat(result.diagnosisList().size(), equalTo(1));
        Diagnosis diagnosis = result.diagnosisList().get(0);
        List<Diagnosis.Resource> affectedResources = diagnosis.affectedResources();
        assertThat(affectedResources.size(), equalTo(2));
        assertThat(affectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
        assertThat(affectedResources.get(0).getNodes().size(), is(numberOfYellowNodes));
        assertThat(affectedResources.get(1).getType(), is(Diagnosis.Resource.Type.INDEX));
        assertThat(affectedResources.get(1).getValues(), iterableWithSize(1));
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get(NODES_WITH_ENOUGH_DISK_SPACE), equalTo(discoveryNodes.size() - numberOfYellowNodes));
        assertThat(details.get(NODES_WITH_UNKNOWN_DISK_STATUS), equalTo(0));
        assertThat(details.get(NODES_OVER_HIGH_WATERMARK), equalTo(numberOfYellowNodes));
        assertThat(details.get(NODES_OVER_FLOOD_STAGE_WATERMARK), equalTo(0));
        assertThat(details.get(INDICES_WITH_READONLY_BLOCK), equalTo(1));
    }

    /*
     * Simulates a cluster with RED all-role nodes and a blocked index.
     * We expect 3 impact and 1 diagnosis.
     */
    public void testRedBlockedIndicesAndRedAllRolesNodes() throws IOException {
        HealthStatus expectedStatus = HealthStatus.RED;
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        int numberOfRedNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(new HealthInfoConfig(HealthStatus.RED, numberOfRedNodes, discoveryNodes));
        Set<String> redNodeIds = healthInfo.diskInfoByNode()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().healthStatus().equals(expectedStatus))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        Set<String> nonRedNodeIds = healthInfo.diskInfoByNode()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().healthStatus().equals(expectedStatus) == false)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        Map<String, Set<String>> indexNameToNodeIdsMap = new HashMap<>();
        int numberOfIndices = randomIntBetween(1, 1000);
        Set<String> blockedIndices = new HashSet<>();
        int numberOfBlockedIndices = randomIntBetween(1, numberOfIndices);
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = randomAlphaOfLength(20);
            /*
             * The following ensures the test has blocked indices in nodes that are recovering and
             * nodes that are still out of space.
             */
            if (randomBoolean()) {
                indexNameToNodeIdsMap.put(indexName, redNodeIds);
            } else {
                indexNameToNodeIdsMap.put(indexName, nonRedNodeIds);
            }
            if (i < numberOfBlockedIndices) {
                blockedIndices.add(indexName);
            }
        }
        ClusterService clusterService = createClusterService(blockedIndices, discoveryNodes, indexNameToNodeIdsMap);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(
            result.symptom(),
            equalTo(
                (numberOfBlockedIndices == 1 ? "1 index is" : numberOfBlockedIndices + " indices are")
                    + " not allowed to be updated. "
                    + (numberOfRedNodes == 1 ? "1 node is" : numberOfRedNodes + " nodes are")
                    + " out of disk or running low on disk space."
            )
        );
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get(NODES_WITH_ENOUGH_DISK_SPACE), equalTo(discoveryNodes.size() - numberOfRedNodes));
        assertThat(details.get(NODES_WITH_UNKNOWN_DISK_STATUS), equalTo(0));
        assertThat(details.get(NODES_OVER_HIGH_WATERMARK), equalTo(0));
        assertThat(details.get(NODES_OVER_FLOOD_STAGE_WATERMARK), equalTo(numberOfRedNodes));
        assertThat(details.get(INDICES_WITH_READONLY_BLOCK), equalTo(blockedIndices.size()));
    }

    public void testRedNodesWithoutAnyBlockedIndices() throws IOException {
        HealthStatus expectedStatus = HealthStatus.RED;
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        int numberOfRedNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(new HealthInfoConfig(HealthStatus.RED, numberOfRedNodes, discoveryNodes));
        Set<String> nonRedNodeIds = healthInfo.diskInfoByNode()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().healthStatus().equals(expectedStatus) == false)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        Map<String, Set<String>> indexNameToNodeIdsMap = new HashMap<>();
        int numberOfIndices = randomIntBetween(1, 1000);
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = randomAlphaOfLength(20);
            // all blocked indices must be hosted by non-red indices
            indexNameToNodeIdsMap.put(indexName, nonRedNodeIds);
        }
        ClusterService clusterService = createClusterService(Set.of(), discoveryNodes, indexNameToNodeIdsMap);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(result.impacts().size(), equalTo(3));
        assertThat(
            result.symptom(),
            equalTo(
                (numberOfRedNodes == 1 ? "1 node " : numberOfRedNodes + " nodes ")
                    + "with roles: [data, data_cold, data_content, data_frozen, data_hot, data_warm, index, ingest, master, ml, "
                    + "remote_cluster_client, search, transform, voting_only] "
                    + (numberOfRedNodes == 1 ? "is" : "are")
                    + " out of disk or running low on disk space."
            )
        );
        assertThat(
            result.impacts().stream().flatMap(impact -> impact.impactAreas().stream()).collect(Collectors.toSet()),
            is(Set.of(DEPLOYMENT_MANAGEMENT))
        );
        assertThat(result.diagnosisList().size(), is(1));
        Diagnosis diagnosis = result.diagnosisList().get(0);
        assertThat(diagnosis.definition().cause(), is("Disk is almost full."));
        assertThat(diagnosis.affectedResources().size(), is(1));
        assertThat(diagnosis.affectedResources().get(0).getNodes().size(), is(numberOfRedNodes));
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get(NODES_WITH_ENOUGH_DISK_SPACE), equalTo(discoveryNodes.size() - numberOfRedNodes));
        assertThat(details.get(NODES_WITH_UNKNOWN_DISK_STATUS), equalTo(0));
        assertThat(details.get(NODES_OVER_HIGH_WATERMARK), equalTo(0));
        assertThat(details.get(NODES_OVER_FLOOD_STAGE_WATERMARK), equalTo(numberOfRedNodes));
        assertThat(details.get(INDICES_WITH_READONLY_BLOCK), equalTo(0));
    }

    public void testMissingHealthInfo() {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        Set<DiscoveryNode> discoveryNodesInClusterState = new HashSet<>(discoveryNodes);
        discoveryNodesInClusterState.add(DiscoveryNodeUtils.create(randomAlphaOfLength(30), UUID.randomUUID().toString()));
        ClusterService clusterService = createClusterService(discoveryNodesInClusterState, false);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        {
            HealthInfo healthInfo = HealthInfo.EMPTY_HEALTH_INFO;
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.UNKNOWN));
        }
        {
            HealthInfo healthInfo = createHealthInfoWithOneUnhealthyNode(HealthStatus.GREEN, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.GREEN));
        }
        {
            HealthInfo healthInfo = createHealthInfoWithOneUnhealthyNode(HealthStatus.YELLOW, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        }
        {
            HealthInfo healthInfo = createHealthInfoWithOneUnhealthyNode(HealthStatus.RED, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.RED));
        }
    }

    /*
     * Simulates a cluster with no blocked indices and healthy data nodes. Only non data nodes that report RED or YELLOW.
     * We expect unhealthy status 2 impacts and 1 diagnosis.
     */
    public void testUnhealthyMasterNodes() {
        DiscoveryNodeRole otherRole = randomFrom(OTHER_ROLES);
        Set<DiscoveryNodeRole> roles = Set.of(DiscoveryNodeRole.MASTER_ROLE, otherRole);
        Set<DiscoveryNode> discoveryNodes = createNodes(roles);
        ClusterService clusterService = createClusterService(discoveryNodes, false);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = randomFrom(HealthStatus.RED, HealthStatus.YELLOW);
        int numberOfProblemNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(new HealthInfoConfig(expectedStatus, numberOfProblemNodes, discoveryNodes));
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(
            result.symptom(),
            equalTo(
                numberOfProblemNodes
                    + " node"
                    + (numberOfProblemNodes == 1 ? "" : "s")
                    + " with roles: ["
                    + roles.stream().map(DiscoveryNodeRole::roleName).sorted().collect(Collectors.joining(", "))
                    + "] "
                    + (numberOfProblemNodes == 1 ? "is" : "are")
                    + " out of disk or running low on disk space."
            )
        );
        List<HealthIndicatorImpact> impacts = result.impacts();
        assertThat(impacts.size(), equalTo(2));
        assertThat(impacts.get(0).impactDescription(), equalTo("Cluster stability might be impaired."));
        assertThat(impacts.get(0).severity(), equalTo(1));
        assertThat(impacts.get(0).impactAreas(), equalTo(List.of(DEPLOYMENT_MANAGEMENT)));
        assertThat(impacts.get(1).impactDescription(), equalTo("The [" + otherRole.roleName() + "] functionality might be impaired."));
        assertThat(impacts.get(1).severity(), equalTo(3));
        assertThat(impacts.get(1).impactAreas(), equalTo(List.of(DEPLOYMENT_MANAGEMENT)));

        List<Diagnosis> diagnosisList = result.diagnosisList();
        assertThat(diagnosisList.size(), equalTo(1));
        Diagnosis diagnosis = diagnosisList.get(0);
        List<Diagnosis.Resource> affectedResources = diagnosis.affectedResources();
        assertThat(affectedResources.size(), equalTo(1));
        assertThat(affectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
        assertThat(affectedResources.get(0).getNodes().size(), is(numberOfProblemNodes));
        Diagnosis.Definition diagnosisDefinition = diagnosis.definition();
        if (expectedStatus == HealthStatus.RED) {
            assertThat(diagnosisDefinition.cause(), equalTo("Disk is full."));
        } else {
            assertThat(diagnosisDefinition.cause(), equalTo("The cluster is running low on disk space."));
        }
        assertThat(
            diagnosisDefinition.action(),
            equalTo("Please add capacity to the current nodes, or replace them with ones with higher capacity.")
        );
    }

    /*
     * Simulates a cluster with no blocked indices and healthy data & master nodes. Only other nodes that report RED or YELLOW.
     *  We expect unhealthy status, with 1 impact and 1 diagnosis.
     */
    public void testUnhealthyNonDataNonMasterNodes() {
        Set<DiscoveryNodeRole> roles = new HashSet<>(randomNonEmptySubsetOf(OTHER_ROLES));
        Set<DiscoveryNode> nodes = createNodes(roles);
        ClusterService clusterService = createClusterService(nodes, false);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = randomFrom(HealthStatus.RED, HealthStatus.YELLOW);
        int numberOfProblemNodes = randomIntBetween(1, nodes.size());
        HealthInfo healthInfo = createHealthInfo(new HealthInfoConfig(expectedStatus, numberOfProblemNodes, nodes));
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        String displayRoles = roles.stream().map(DiscoveryNodeRole::roleName).sorted().collect(Collectors.joining(", "));
        assertThat(
            result.symptom(),
            equalTo(
                numberOfProblemNodes
                    + " node"
                    + (numberOfProblemNodes == 1 ? "" : "s")
                    + " with roles: ["
                    + displayRoles
                    + "] "
                    + (numberOfProblemNodes == 1 ? "is" : "are")
                    + " out of disk or running low on disk space."
            )
        );
        List<HealthIndicatorImpact> impacts = result.impacts();
        assertThat(impacts.size(), equalTo(1));
        assertThat(impacts.get(0).impactDescription(), equalTo("The [" + displayRoles + "] functionality might be impaired."));
        assertThat(impacts.get(0).severity(), equalTo(3));
        assertThat(impacts.get(0).impactAreas(), equalTo(List.of(DEPLOYMENT_MANAGEMENT)));
        List<Diagnosis> diagnosisList = result.diagnosisList();
        assertThat(diagnosisList.size(), equalTo(1));
        Diagnosis diagnosis = diagnosisList.get(0);
        List<Diagnosis.Resource> affectedResources = diagnosis.affectedResources();
        assertThat(affectedResources.size(), equalTo(1));
        assertThat(affectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
        assertThat(affectedResources.get(0).getNodes().size(), is(numberOfProblemNodes));
        Diagnosis.Definition diagnosisDefinition = diagnosis.definition();
        if (expectedStatus == HealthStatus.RED) {
            assertThat(diagnosisDefinition.cause(), equalTo("Disk is full."));
        } else {
            assertThat(diagnosisDefinition.cause(), equalTo("The cluster is running low on disk space."));
        }
        assertThat(
            diagnosisDefinition.action(),
            equalTo("Please add capacity to the current nodes, or replace them with ones with higher capacity.")
        );
    }

    /*
     * Simulates a cluster with blocked indices, yellow data nodes, master nodes that report RED, other nodes that also report RED.
     * We expect RED status, 3 impacts and 3 diagnoses.
     */
    public void testBlockedIndexWithRedNonDataNodesAndYellowDataNodes() {
        Set<DiscoveryNodeRole> otherRoles = new HashSet<>(randomNonEmptySubsetOf(OTHER_ROLES));
        Set<DiscoveryNodeRole> dataRoles = new HashSet<>(randomNonEmptySubsetOf(DATA_ROLES));
        Set<DiscoveryNodeRole> masterRole = Set.of(DiscoveryNodeRole.MASTER_ROLE);
        Set<DiscoveryNode> dataNodes = createNodes(dataRoles);
        Set<DiscoveryNode> masterNodes = createNodes(masterRole);
        Set<DiscoveryNode> otherNodes = createNodes(otherRoles);
        ClusterService clusterService = createClusterService(Sets.union(Sets.union(dataNodes, masterNodes), otherNodes), true);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        int numberOfRedMasterNodes = randomIntBetween(1, masterNodes.size());
        int numberOfRedOtherNodes = randomIntBetween(1, otherNodes.size());
        int numberOfYellowDataNodes = randomIntBetween(1, dataNodes.size());
        HealthInfo healthInfo = createHealthInfo(
            List.of(
                new HealthInfoConfig(HealthStatus.YELLOW, numberOfYellowDataNodes, dataNodes),
                new HealthInfoConfig(HealthStatus.RED, numberOfRedMasterNodes, masterNodes),
                new HealthInfoConfig(HealthStatus.RED, numberOfRedOtherNodes, otherNodes)
            )
        );
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(HealthStatus.RED));
        assertThat(
            result.symptom(),
            equalTo(
                "1 index is not allowed to be updated. "
                    + (numberOfYellowDataNodes + (numberOfYellowDataNodes == 1 ? " node is" : " nodes are"))
                    + " out of disk or running low on disk space. "
                    + (numberOfRedMasterNodes + numberOfRedOtherNodes)
                    + " nodes with roles: "
                    + Stream.concat(masterRole.stream(), otherRoles.stream()).map(DiscoveryNodeRole::roleName).sorted().toList()
                    + " are out of disk or running low on disk space."
            )
        );
        List<HealthIndicatorImpact> impacts = result.impacts();
        assertThat(impacts.size(), equalTo(3));
        assertThat(impacts.get(0).impactDescription(), containsString("Cannot insert or update documents in the affected indices ["));
        assertThat(impacts.get(0).severity(), equalTo(1));
        assertThat(impacts.get(0).impactAreas(), equalTo(List.of(ImpactArea.INGEST)));
        assertThat(impacts.get(1).impactDescription(), equalTo("Cluster stability might be impaired."));
        assertThat(impacts.get(1).severity(), equalTo(1));
        assertThat(impacts.get(1).impactAreas(), equalTo(List.of(DEPLOYMENT_MANAGEMENT)));
        assertThat(
            impacts.get(2).impactDescription(),
            equalTo("The " + otherRoles.stream().map(DiscoveryNodeRole::roleName).sorted().toList() + " functionality might be impaired.")
        );
        assertThat(impacts.get(2).severity(), equalTo(3));
        assertThat(impacts.get(2).impactAreas(), equalTo(List.of(DEPLOYMENT_MANAGEMENT)));

        List<Diagnosis> diagnosisList = result.diagnosisList();
        assertThat(diagnosisList.size(), equalTo(3));
        {
            Diagnosis diagnosis = diagnosisList.get(0);
            List<Diagnosis.Resource> dataAffectedResources = diagnosis.affectedResources();
            assertThat(dataAffectedResources.size(), equalTo(2));
            assertThat(dataAffectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
            assertThat(dataAffectedResources.get(0).getNodes().size(), is(numberOfYellowDataNodes));
            assertThat(dataAffectedResources.get(1).getType(), is(Diagnosis.Resource.Type.INDEX));
            assertThat(dataAffectedResources.get(1).getValues().size(), is(1));
            Diagnosis.Definition dataDiagnosisDefinition = diagnosis.definition();
            assertThat(
                dataDiagnosisDefinition.cause(),
                equalTo(
                    "1 index resides on nodes that have run or are likely to run out of disk space, "
                        + "this can temporarily disable writing on this index."
                )
            );
            assertThat(
                dataDiagnosisDefinition.action(),
                equalTo(
                    "Enable autoscaling (if applicable), add disk capacity or free up disk space to resolve this. If you have already "
                        + "taken action please wait for the rebalancing to complete."
                )
            );
        }
        {
            Diagnosis diagnosis = diagnosisList.get(1);
            List<Diagnosis.Resource> masterAffectedResources = diagnosis.affectedResources();
            assertThat(masterAffectedResources.size(), equalTo(1));
            assertThat(masterAffectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
            assertThat(masterAffectedResources.get(0).getNodes().size(), is(numberOfRedMasterNodes));
            Diagnosis.Definition masterDiagnosisDefinition = diagnosis.definition();
            assertThat(masterDiagnosisDefinition.cause(), equalTo("Disk is full."));
            assertThat(
                masterDiagnosisDefinition.action(),
                equalTo("Please add capacity to the current nodes, or replace them with ones with higher capacity.")
            );
        }

        {
            Diagnosis diagnosis = diagnosisList.get(2);
            List<Diagnosis.Resource> nonDataNonMasterAffectedResources = diagnosis.affectedResources();
            assertThat(nonDataNonMasterAffectedResources.size(), equalTo(1));
            assertThat(nonDataNonMasterAffectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
            assertThat(nonDataNonMasterAffectedResources.get(0).getNodes().size(), is(numberOfRedOtherNodes));

            Diagnosis.Definition nonDataNonMasterDiagnosisDefinition = diagnosis.definition();
            assertThat(nonDataNonMasterDiagnosisDefinition.cause(), equalTo("Disk is full."));
            assertThat(
                nonDataNonMasterDiagnosisDefinition.action(),
                equalTo("Please add capacity to the current nodes, or replace them with ones with higher capacity.")
            );
        }
    }

    public void testGetIndicesForNodes() {
        Map<String, DiscoveryNode> discoveryNodes = createNodesWithAllRoles().stream()
            .collect(Collectors.toMap(DiscoveryNode::getId, Function.identity()));
        HealthStatus expectedStatus = HealthStatus.RED;
        int numberOfRedNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(new HealthInfoConfig(expectedStatus, numberOfRedNodes, discoveryNodes.values()));
        List<DiscoveryNode> redNodes = healthInfo.diskInfoByNode()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().healthStatus().equals(expectedStatus))
            .map(entry -> discoveryNodes.get(entry.getKey()))
            .collect(Collectors.toList());
        List<DiscoveryNode> nonRedNodes = healthInfo.diskInfoByNode()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().healthStatus().equals(expectedStatus) == false)
            .map(entry -> discoveryNodes.get(entry.getKey()))
            .collect(Collectors.toList());
        Map<String, Set<String>> indexNameToNodeIdsMap = new HashMap<>();
        int numberOfIndices = randomIntBetween(1, 1000);
        Set<String> redNodeIndices = new HashSet<>();
        Set<String> nonRedNodeIndices = new HashSet<>();
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = randomAlphaOfLength(20);
            if (nonRedNodes.isEmpty() || randomBoolean()) {
                indexNameToNodeIdsMap.put(indexName, Set.of(randomFrom(redNodes).getId()));
                redNodeIndices.add(indexName);
            } else {
                indexNameToNodeIdsMap.put(indexName, Set.of(randomFrom(nonRedNodes).getId()));
                nonRedNodeIndices.add(indexName);
            }
        }
        final var clusterService = createClusterService(Set.of(), discoveryNodes.values(), indexNameToNodeIdsMap);
        assertThat(
            DiskHealthIndicatorService.DiskHealthAnalyzer.getIndicesForNodes(redNodes, clusterService.state()),
            equalTo(redNodeIndices)
        );
        assertThat(
            DiskHealthIndicatorService.DiskHealthAnalyzer.getIndicesForNodes(nonRedNodes, clusterService.state()),
            equalTo(nonRedNodeIndices)
        );
    }

    public void testNodeHealthStatusCounts() {
        {
            // A bit of everything
            int unknownCount = randomIntBetween(1, 10);
            int greenCount = randomIntBetween(1, 10);
            int yellowCount = randomIntBetween(1, 10);
            int redCount = randomIntBetween(1, 10);
            Set<DiscoveryNode> nodes = createNodes(unknownCount + greenCount + yellowCount + redCount, DiscoveryNodeRole.roles());
            int i = 0;
            Map<String, DiskHealthInfo> nodesDiskHealth = new HashMap<>();
            for (DiscoveryNode node : nodes) {
                HealthStatus status;
                if (i < unknownCount) {
                    status = HealthStatus.UNKNOWN;
                } else if (i < unknownCount + greenCount) {
                    status = HealthStatus.GREEN;
                } else if (i < unknownCount + greenCount + yellowCount) {
                    status = HealthStatus.YELLOW;
                } else {
                    status = HealthStatus.RED;
                }
                nodesDiskHealth.put(node.getId(), new DiskHealthInfo(status, randomFrom(DiskHealthInfo.Cause.values())));
                i++;
            }
            ClusterState clusterState = createClusterState(Set.of(), nodes, Map.of());
            Map<HealthStatus, Integer> counts = DiskHealthIndicatorService.DiskHealthAnalyzer.countNodesByHealthStatus(
                nodesDiskHealth,
                clusterState
            );
            assertThat(counts.get(HealthStatus.UNKNOWN), equalTo(unknownCount));
            assertThat(counts.get(HealthStatus.GREEN), equalTo(greenCount));
            assertThat(counts.get(HealthStatus.YELLOW), equalTo(yellowCount));
            assertThat(counts.get(HealthStatus.RED), equalTo(redCount));
        }
        {
            // No nodes
            Map<HealthStatus, Integer> counts = DiskHealthIndicatorService.DiskHealthAnalyzer.countNodesByHealthStatus(
                Map.of(),
                ClusterState.EMPTY_STATE
            );
            assertThat(counts.get(HealthStatus.UNKNOWN), equalTo(0));
            assertThat(counts.get(HealthStatus.GREEN), equalTo(0));
            assertThat(counts.get(HealthStatus.YELLOW), equalTo(0));
            assertThat(counts.get(HealthStatus.RED), equalTo(0));
        }
        {
            // No disk health info, cluster state with nodes
            Set<DiscoveryNode> nodes = createNodes(DiscoveryNodeRole.roles());
            ClusterState clusterState = createClusterState(Set.of(), nodes, Map.of());
            Map<HealthStatus, Integer> counts = DiskHealthIndicatorService.DiskHealthAnalyzer.countNodesByHealthStatus(
                Map.of(),
                clusterState
            );
            assertThat(counts.get(HealthStatus.UNKNOWN), equalTo(nodes.size()));
            assertThat(counts.get(HealthStatus.GREEN), equalTo(0));
            assertThat(counts.get(HealthStatus.YELLOW), equalTo(0));
            assertThat(counts.get(HealthStatus.RED), equalTo(0));
        }
        {
            // Disk health info for one node, no nodes in cluster state
            Map<String, DiskHealthInfo> nodesDiskHealth = Map.of(
                randomAlphaOfLength(10),
                new DiskHealthInfo(randomFrom(HealthStatus.values()))
            );
            Map<HealthStatus, Integer> counts = DiskHealthIndicatorService.DiskHealthAnalyzer.countNodesByHealthStatus(
                nodesDiskHealth,
                ClusterState.EMPTY_STATE
            );
            assertThat(counts.get(HealthStatus.UNKNOWN), equalTo(0));
            assertThat(counts.get(HealthStatus.GREEN), equalTo(0));
            assertThat(counts.get(HealthStatus.YELLOW), equalTo(0));
            assertThat(counts.get(HealthStatus.RED), equalTo(0));
        }
    }

    public void testLimitNumberOfAffectedResources() {
        Set<DiscoveryNodeRole> otherRoles = new HashSet<>(randomNonEmptySubsetOf(OTHER_ROLES));
        Set<DiscoveryNodeRole> dataRoles = new HashSet<>(randomNonEmptySubsetOf(DATA_ROLES));
        Set<DiscoveryNodeRole> masterRole = Set.of(DiscoveryNodeRole.MASTER_ROLE);
        Set<DiscoveryNode> dataNodes = createNodes(30, dataRoles);
        Set<DiscoveryNode> masterNodes = createNodes(20, masterRole);
        Set<DiscoveryNode> otherNodes = createNodes(10, otherRoles);
        ClusterService clusterService = createClusterService(Sets.union(Sets.union(dataNodes, masterNodes), otherNodes), true);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        int numberOfRedMasterNodes = masterNodes.size();
        int numberOfRedOtherNodes = otherNodes.size();
        int numberOfYellowDataNodes = dataNodes.size();
        HealthInfo healthInfo = createHealthInfo(
            List.of(
                new HealthInfoConfig(HealthStatus.YELLOW, numberOfYellowDataNodes, dataNodes),
                new HealthInfoConfig(HealthStatus.RED, numberOfRedMasterNodes, masterNodes),
                new HealthInfoConfig(HealthStatus.RED, numberOfRedOtherNodes, otherNodes)
            )
        );
        {
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, 0, healthInfo);
            List<Diagnosis> diagnosisList = result.diagnosisList();
            assertThat(diagnosisList.size(), equalTo(3));
            {
                Diagnosis diagnosis = diagnosisList.get(0);
                List<Diagnosis.Resource> dataAffectedResources = diagnosis.affectedResources();
                assertThat(dataAffectedResources.size(), equalTo(2));
                assertThat(dataAffectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
                assertThat(dataAffectedResources.get(0).getNodes().size(), is(0));
                assertThat(dataAffectedResources.get(1).getType(), is(Diagnosis.Resource.Type.INDEX));
                assertThat(dataAffectedResources.get(1).getValues().size(), is(0));
            }
            {
                Diagnosis diagnosis = diagnosisList.get(1);
                List<Diagnosis.Resource> masterAffectedResources = diagnosis.affectedResources();
                assertThat(masterAffectedResources.size(), equalTo(1));
                assertThat(masterAffectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
                assertThat(masterAffectedResources.get(0).getNodes().size(), is(0));
            }

            {
                Diagnosis diagnosis = diagnosisList.get(2);
                List<Diagnosis.Resource> nonDataNonMasterAffectedResources = diagnosis.affectedResources();
                assertThat(nonDataNonMasterAffectedResources.size(), equalTo(1));
                assertThat(nonDataNonMasterAffectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
                assertThat(nonDataNonMasterAffectedResources.get(0).getNodes().size(), is(0));
            }
        }

        {
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, 10, healthInfo);
            List<Diagnosis> diagnosisList = result.diagnosisList();
            assertThat(diagnosisList.size(), equalTo(3));
            {
                Diagnosis diagnosis = diagnosisList.get(0);
                List<Diagnosis.Resource> dataAffectedResources = diagnosis.affectedResources();
                assertThat(dataAffectedResources.size(), equalTo(2));
                assertThat(dataAffectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
                assertThat(dataAffectedResources.get(0).getNodes().size(), is(10));
                assertThat(dataAffectedResources.get(1).getType(), is(Diagnosis.Resource.Type.INDEX));
                assertThat(dataAffectedResources.get(1).getValues().size(), is(1));
            }
            {
                Diagnosis diagnosis = diagnosisList.get(1);
                List<Diagnosis.Resource> masterAffectedResources = diagnosis.affectedResources();
                assertThat(masterAffectedResources.size(), equalTo(1));
                assertThat(masterAffectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
                assertThat(masterAffectedResources.get(0).getNodes().size(), is(10));
            }

            {
                Diagnosis diagnosis = diagnosisList.get(2);
                List<Diagnosis.Resource> nonDataNonMasterAffectedResources = diagnosis.affectedResources();
                assertThat(nonDataNonMasterAffectedResources.size(), equalTo(1));
                assertThat(nonDataNonMasterAffectedResources.get(0).getType(), is(Diagnosis.Resource.Type.NODE));
                assertThat(nonDataNonMasterAffectedResources.get(0).getNodes().size(), is(10));
            }
        }
    }

    public void testSkippingFieldsWhenVerboseIsFalse() {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(discoveryNodes, false);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = HealthStatus.RED;
        HealthInfo healthInfo = createHealthInfoWithOneUnhealthyNode(expectedStatus, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(false, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(result.details(), equalTo(HealthIndicatorDetails.EMPTY));
        assertThat(result.diagnosisList(), equalTo(List.of()));
        assertThat(result.impacts().isEmpty(), equalTo(false));
        assertThat(result.symptom().isEmpty(), equalTo(false));
    }

    // We expose the indicator name and the diagnoses in the x-pack usage API. In order to index them properly in a telemetry index
    // they need to be declared in the health-api-indexer.edn in the telemetry repository.
    public void testMappedFieldsForTelemetry() {
        assertThat(DiskHealthIndicatorService.NAME, equalTo("disk"));
        assertThat(
            DiskHealthIndicatorService.DiskHealthAnalyzer.createDataNodeDiagnosis(0, List.of()).definition().getUniqueId(),
            equalTo("elasticsearch:health:disk:diagnosis:add_disk_capacity_data_nodes")
        );
        assertThat(
            DiskHealthIndicatorService.DiskHealthAnalyzer.createNonDataNodeDiagnosis(
                HealthStatus.RED,
                List.of(),
                MAX_AFFECTED_RESOURCES_COUNT,
                true
            ).definition().getUniqueId(),
            equalTo("elasticsearch:health:disk:diagnosis:add_disk_capacity_master_nodes")
        );
        assertThat(
            DiskHealthIndicatorService.DiskHealthAnalyzer.createNonDataNodeDiagnosis(
                HealthStatus.RED,
                List.of(),
                MAX_AFFECTED_RESOURCES_COUNT,
                false
            ).definition().getUniqueId(),
            equalTo("elasticsearch:health:disk:diagnosis:add_disk_capacity")
        );
    }

    private Set<DiscoveryNode> createNodesWithAllRoles() {
        return createNodes(DiscoveryNodeRole.roles());
    }

    private Set<DiscoveryNode> createNodes(Set<DiscoveryNodeRole> roles) {
        return createNodes(randomIntBetween(1, 200), roles);
    }

    private Set<DiscoveryNode> createNodes(int numberOfNodes, Set<DiscoveryNodeRole> roles) {
        Set<DiscoveryNode> discoveryNodes = new HashSet<>();
        for (int i = 0; i < numberOfNodes; i++) {
            discoveryNodes.add(DiscoveryNodeUtils.builder(UUID.randomUUID().toString()).name(randomAlphaOfLength(30)).roles(roles).build());
        }
        return discoveryNodes;
    }

    private HealthInfo createHealthInfoWithOneUnhealthyNode(HealthStatus expectedStatus, Set<DiscoveryNode> nodes) {
        return createHealthInfo(new HealthInfoConfig(expectedStatus, 1, nodes));
    }

    private HealthInfo createHealthInfo(HealthInfoConfig healthInfoConfig) {
        return createHealthInfo(List.of(healthInfoConfig));
    }

    private HealthInfo createHealthInfo(List<HealthInfoConfig> healthInfoConfigs) {
        Map<String, DiskHealthInfo> diskInfoByNode = new HashMap<>();
        for (HealthInfoConfig config : healthInfoConfigs) {
            int nodesWithExpectedStatusCount = 0;
            for (DiscoveryNode node : config.nodes) {
                final DiskHealthInfo diskHealthInfo;
                if (nodesWithExpectedStatusCount < config.numberOfUnhealthyNodes) {
                    diskHealthInfo = randomBoolean()
                        ? new DiskHealthInfo(config.unhealthyStatus)
                        : new DiskHealthInfo(config.unhealthyStatus, randomFrom(DiskHealthInfo.Cause.values()));
                    nodesWithExpectedStatusCount++;
                } else {
                    diskHealthInfo = randomBoolean()
                        ? new DiskHealthInfo(HealthStatus.GREEN)
                        : new DiskHealthInfo(HealthStatus.GREEN, randomFrom(DiskHealthInfo.Cause.values()));
                }
                diskInfoByNode.put(node.getId(), diskHealthInfo);
            }
        }
        return new HealthInfo(
            diskInfoByNode,
            DataStreamLifecycleHealthInfo.NO_DSL_ERRORS,
            Map.of(),
            FileSettingsService.FileSettingsHealthInfo.INDETERMINATE
        );
    }

    private static ClusterService createClusterService(Collection<DiscoveryNode> nodes, boolean withBlockedIndex) {
        int numberOfIndices = 1;
        int numberOfBlockedIndices = withBlockedIndex ? 1 : 0;
        Map<String, Set<String>> indexNameToNodeIdsMap = new HashMap<>();
        Set<String> blockedIndices = new HashSet<>(numberOfBlockedIndices);
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = randomAlphaOfLength(20);
            indexNameToNodeIdsMap.put(indexName, Set.of(randomFrom(nodes).getId()));
            if (i < numberOfBlockedIndices) {
                blockedIndices.add(indexName);
            }
        }
        return createClusterService(blockedIndices, nodes, indexNameToNodeIdsMap);
    }

    private static ClusterService createClusterService(
        Set<String> blockedIndices,
        Collection<DiscoveryNode> nodes,
        Map<String, Set<String>> indexNameToNodeIdsMap
    ) {
        ClusterState clusterState = createClusterState(blockedIndices, nodes, indexNameToNodeIdsMap);
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return clusterService;
    }

    static ClusterState createClusterState(
        Set<String> blockedIndices,
        Collection<DiscoveryNode> nodes,
        Map<String, Set<String>> indexNameToNodeIdsMap
    ) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodes.forEach(nodesBuilder::add);
        nodesBuilder.localNodeId(randomFrom(nodes).getId());
        nodesBuilder.masterNodeId(randomFrom(nodes).getId());
        ClusterBlocks.Builder clusterBlocksBuilder = new ClusterBlocks.Builder();
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        for (String index : indexNameToNodeIdsMap.keySet()) {
            int numberOfShards = indexNameToNodeIdsMap.get(index).size() == 0 ? 1 : indexNameToNodeIdsMap.get(index).size();
            IndexMetadata indexMetadata = IndexMetadata.builder(index)
                .settings(
                    indexSettings(IndexVersion.current(), numberOfShards, 0).put(SETTING_CREATION_DATE, System.currentTimeMillis())
                        .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), blockedIndices.contains(index))
                )
                .build();
            if (blockedIndices.contains(index)) {
                clusterBlocksBuilder.addBlocks(indexMetadata);
            }

            IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex());
            int shardAutoincrementNumber = 0;
            for (String nodeId : indexNameToNodeIdsMap.get(index)) {
                ShardId shardId = new ShardId(indexMetadata.getIndex(), shardAutoincrementNumber);
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = IndexShardRoutingTable.builder(shardId);
                indexShardRoutingBuilder.addShard(TestShardRouting.newShardRouting(shardId, nodeId, true, ShardRoutingState.STARTED));
                indexRoutingTable.addIndexShard(indexShardRoutingBuilder);
            }

            metadata.put(indexMetadata, false);
            routingTable.add(indexRoutingTable);
        }
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.nodes(nodesBuilder);
        state.metadata(metadata.generateClusterUuidIfNeeded().build());
        state.routingTable(routingTable.build());
        state.blocks(clusterBlocksBuilder);
        return state.build();
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        return parser.map();
    }

    record HealthInfoConfig(HealthStatus unhealthyStatus, int numberOfUnhealthyNodes, Collection<DiscoveryNode> nodes) {
        HealthInfoConfig {
            assert numberOfUnhealthyNodes <= nodes.size();
        }
    }
}
