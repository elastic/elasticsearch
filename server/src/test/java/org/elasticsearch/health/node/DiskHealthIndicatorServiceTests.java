/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
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

    public void testServiceBasics() {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(false, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        {
            HealthStatus expectedStatus = HealthStatus.GREEN;
            HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
        }
        {
            HealthStatus expectedStatus = HealthStatus.YELLOW;
            HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
        }
        {
            HealthStatus expectedStatus = HealthStatus.RED;
            HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
        }
    }

    public void testGreen() throws IOException {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(false, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = HealthStatus.GREEN;
        HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(result.symptom(), equalTo("The cluster has enough available disk space."));
        assertThat(result.impacts().size(), equalTo(0));
        assertThat(result.diagnosisList().size(), equalTo(0));
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get("green_nodes"), equalTo(discoveryNodes.size()));
        assertThat(details.get("unknown_nodes"), equalTo(0));
        assertThat(details.get("yellow_nodes"), equalTo(0));
        assertThat(details.get("red_nodes"), equalTo(0));
        assertThat(details.get("blocked_indices"), equalTo(0));
    }

    /*
     * This method tests that we get the expected behavior when there are all role nodes with indices that report RED status and there are
     * no blocks in the cluster state. We expect 3 impacts and 1 diagnosis.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/90305")
    public void testNoBlockedIndicesAndRedAllRoleNodes() throws IOException {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        Map<String, Set<String>> indexNameToNodeIdsMap = new HashMap<>();
        Set<String> nodeIds = discoveryNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        for (int i = 0; i < randomIntBetween(1, 20); i++) {
            String indexName = randomAlphaOfLength(20);
            indexNameToNodeIdsMap.put(indexName, new HashSet<>(randomNonEmptySubsetOf(nodeIds)));
        }
        ClusterService clusterService = createClusterService(Set.of(), discoveryNodes, indexNameToNodeIdsMap);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = HealthStatus.RED;
        HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(result.symptom(), containsString("1 node with roles: [data"));
        assertThat(result.symptom(), containsString("] is out of disk or running low on disk space."));
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
        List<String> affectedResources = diagnosis.affectedResources();
        assertThat(affectedResources.size(), equalTo(1));
        String expectedRedNodeId = healthInfo.diskInfoByNode()
            .entrySet()
            .stream()
            .filter(entry -> expectedStatus.equals(entry.getValue().healthStatus()))
            .map(Map.Entry::getKey)
            .findAny()
            .orElseThrow();
        assertThat(affectedResources.get(0), equalTo(expectedRedNodeId));
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get("green_nodes"), equalTo(discoveryNodes.size() - 1));
        assertThat(details.get("unknown_nodes"), equalTo(0));
        assertThat(details.get("yellow_nodes"), equalTo(0));
        assertThat(details.get("red_nodes"), equalTo(1));
        assertThat(details.get("blocked_indices"), equalTo(0));
    }

    /*
     * This method tests that we get the expected behavior when there are nodes with indices that report RED status and there are no
     * blocks in the cluster state. We expect the status to be RED, to have 3 impacts and 1 diagnosis.
     */
    public void testRedNodesAndNoBlockedIndices() throws IOException {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        HealthStatus expectedStatus = HealthStatus.RED;
        int numberOfRedNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(expectedStatus, numberOfRedNodes, discoveryNodes);
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
        int numberOfRedIndices = randomIntBetween(1, numberOfIndices);
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = randomAlphaOfLength(20);
            /*
             * The following is artificial but useful for making sure the test has the right counts. The first numberOfRedIndices indices
             *  are always placed on all the red nodes. All other indices are placed on all the non-red nodes.
             */
            if (i < numberOfRedIndices) {
                indexNameToNodeIdsMap.put(indexName, redNodeIds);
            } else {
                indexNameToNodeIdsMap.put(indexName, nonRedNodeIds);
            }
        }
        ClusterService clusterService = createClusterService(Set.of(), discoveryNodes, indexNameToNodeIdsMap);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(
            result.symptom(),
            containsString(numberOfRedNodes + " node" + (numberOfRedNodes == 1 ? "" : "s") + " with roles: [data")
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
        List<String> affectedResources = diagnosis.affectedResources();
        assertThat(affectedResources.size(), equalTo(numberOfRedNodes));
        assertTrue(affectedResources.containsAll(redNodeIds));
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get("green_nodes"), equalTo(discoveryNodes.size() - numberOfRedNodes));
        assertThat(details.get("unknown_nodes"), equalTo(0));
        assertThat(details.get("yellow_nodes"), equalTo(0));
        assertThat(details.get("red_nodes"), equalTo(numberOfRedNodes));
        assertThat(details.get("blocked_indices"), equalTo(0));
    }

    /*
     * Tests when there is an index that has a block on it but the nodes report green (so the lock is probably about to be released or
     * shards are being rebalanced).
     */
    public void testHasBlockButOtherwiseGreen() {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(true, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        {
            HealthStatus expectedStatus = HealthStatus.RED;
            HealthInfo healthInfo = createHealthInfo(HealthStatus.GREEN, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
            assertThat(
                result.symptom(),
                equalTo(
                    "1 index is not allowed to be updated because the cluster was running out of "
                        + "disk space. The cluster is recovering and ingest capabilities should be restored within a few minutes."
                )
            );
        }
    }

    /*
     * Tests when there is an index that has a block on it but the nodes report yellow.
     */
    public void testHasBlockButOtherwiseYellow() {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(1, 1, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = HealthStatus.RED;
        int numberOfYellowNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(HealthStatus.YELLOW, numberOfYellowNodes, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(
            result.symptom(),
            equalTo(
                "1 index is not allowed to be updated because "
                    + (numberOfYellowNodes == 1 ? "1 node is" : numberOfYellowNodes + " nodes are")
                    + " out of disk or running low on disk space."
            )
        );
    }

    /*
     * In this test, there are blocked indices, all-role nodes that report RED. We expect the overall status to be RED and 3 impacts and
     * 1 diagnosis.
     */
    public void testHasBlockedIndicesAndUnhealthyAllRolesNodes() throws IOException {
        HealthStatus expectedStatus = HealthStatus.RED;
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        int numberOfRedNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(HealthStatus.RED, numberOfRedNodes, discoveryNodes);
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
             * The following is artificial but useful for making sure the test represents blocked indices in nodes that are recovering and
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
                    + " not allowed to be updated because "
                    + (numberOfRedNodes == 1 ? "1 node is" : numberOfRedNodes + " nodes are")
                    + " out of disk or running low on disk space."
            )
        );
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get("green_nodes"), equalTo(discoveryNodes.size() - numberOfRedNodes));
        assertThat(details.get("unknown_nodes"), equalTo(0));
        assertThat(details.get("yellow_nodes"), equalTo(0));
        assertThat(details.get("red_nodes"), equalTo(numberOfRedNodes));
        assertThat(details.get("blocked_indices"), equalTo(blockedIndices.size()));
    }

    public void testMissingHealthInfo() {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        Set<DiscoveryNode> discoveryNodesInClusterState = new HashSet<>(discoveryNodes);
        discoveryNodesInClusterState.add(
            new DiscoveryNode(
                randomAlphaOfLength(30),
                UUID.randomUUID().toString(),
                buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                DiscoveryNodeRole.roles(),
                Version.CURRENT
            )
        );
        ClusterService clusterService = createClusterService(false, discoveryNodesInClusterState);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        {
            HealthInfo healthInfo = HealthInfo.EMPTY_HEALTH_INFO;
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.UNKNOWN));
        }
        {
            HealthInfo healthInfo = createHealthInfo(HealthStatus.GREEN, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.GREEN));
        }
        {
            HealthInfo healthInfo = createHealthInfo(HealthStatus.YELLOW, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        }
        {
            HealthInfo healthInfo = createHealthInfo(HealthStatus.RED, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.RED));
        }
    }

    /*
     * In this test, there are no blocked indices, only master nodes that report RED or YELLOW. We expect the overall status
     * to indicate an issue, with 2 impacts and 1 diagnosis.
     */
    public void testMasterNodeProblems() {
        DiscoveryNodeRole otherRole = randomFrom(OTHER_ROLES);
        Set<DiscoveryNodeRole> roles = Set.of(DiscoveryNodeRole.MASTER_ROLE, otherRole);
        Set<DiscoveryNode> discoveryNodes = createNodes(roles);
        ClusterService clusterService = createClusterService(false, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = randomFrom(HealthStatus.RED, HealthStatus.YELLOW);
        int numberOfProblemNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(expectedStatus, numberOfProblemNodes, discoveryNodes);
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
        assertThat(impacts.get(0).severity(), equalTo(2));
        assertThat(impacts.get(0).impactAreas(), equalTo(List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)));
        assertThat(impacts.get(1).impactDescription(), equalTo("The [" + otherRole.roleName() + "] functionality might be impaired."));
        assertThat(impacts.get(1).severity(), equalTo(2));
        assertThat(impacts.get(1).impactAreas(), equalTo(List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)));

        List<Diagnosis> diagnosisList = result.diagnosisList();
        assertThat(diagnosisList.size(), equalTo(1));
        Diagnosis diagnosis = diagnosisList.get(0);
        List<String> affectedResources = diagnosis.affectedResources();
        assertThat(affectedResources.size(), equalTo(numberOfProblemNodes));
        Diagnosis.Definition diagnosisDefinition = diagnosis.definition();
        assertThat(diagnosisDefinition.cause(), equalTo("Disk is almost full."));
        assertThat(
            diagnosisDefinition.action(),
            equalTo("Please add capacity to the current nodes, or replace them with ones with higher capacity.")
        );
    }

    /*
     * In this test, there are no blocked indices, only non-master, non-data nodes that report RED or YELLOW. We expect the overall status
     * to indicate an issue, with 1 impact and 1 diagnosis.
     */
    public void testNonDataNonMasterNodeProblems() {
        Set<DiscoveryNodeRole> roles = new HashSet<>(randomNonEmptySubsetOf(OTHER_ROLES));
        Set<DiscoveryNode> nodes = createNodes(roles);
        ClusterService clusterService = createClusterService(false, nodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = randomFrom(HealthStatus.RED, HealthStatus.YELLOW);
        int numberOfProblemNodes = randomIntBetween(1, nodes.size());
        HealthInfo healthInfo = createHealthInfo(expectedStatus, numberOfProblemNodes, nodes);
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
        assertThat(impacts.get(0).severity(), equalTo(2));
        assertThat(impacts.get(0).impactAreas(), equalTo(List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)));
        List<Diagnosis> diagnosisList = result.diagnosisList();
        assertThat(diagnosisList.size(), equalTo(1));
        Diagnosis diagnosis = diagnosisList.get(0);
        List<String> affectedResources = diagnosis.affectedResources();
        assertThat(affectedResources.size(), equalTo(numberOfProblemNodes));
        Diagnosis.Definition diagnosisDefinition = diagnosis.definition();
        assertThat(diagnosisDefinition.cause(), equalTo("Disk is almost full."));
        assertThat(
            diagnosisDefinition.action(),
            equalTo("Please add capacity to the current nodes, or replace them with ones with higher capacity.")
        );
    }

    /*
     * In this test, there are indices with blocks on them, master nodes that report RED, non-data nodes that report RED, and data
     * nodes that report YELLOW. We expect the overall status will be RED, with 3 impacts and 3 diagnoses (for the 3 different node
     * types experiencing problems).
     */
    public void testBlockedIndexWithRedNonDataNodesAndYellowDataNodes() {
        Set<DiscoveryNodeRole> nonDataNonMasterRoles = new HashSet<>(randomNonEmptySubsetOf(OTHER_ROLES));
        Set<DiscoveryNodeRole> dataRoles = new HashSet<>(randomNonEmptySubsetOf(DATA_ROLES));
        Set<DiscoveryNodeRole> masterRole = Set.of(DiscoveryNodeRole.MASTER_ROLE);
        Set<DiscoveryNode> masterDiscoveryNodes = createNodes(masterRole);
        Set<DiscoveryNode> nonDataNonMasterDiscoveryNodes = createNodes(nonDataNonMasterRoles);
        Set<DiscoveryNode> dataDiscoveryNodes = createNodes(dataRoles);
        ClusterService clusterService = createClusterService(
            1,
            1,
            Stream.concat(
                masterDiscoveryNodes.stream(),
                (Stream.concat(nonDataNonMasterDiscoveryNodes.stream(), dataDiscoveryNodes.stream()))
            ).collect(Collectors.toSet())
        );
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        int numberOfRedMasterNodes = randomIntBetween(1, masterDiscoveryNodes.size());
        int numberOfRedNonDataNonMasterNodes = randomIntBetween(1, nonDataNonMasterDiscoveryNodes.size());
        int numberOfYellowDataNodes = randomIntBetween(1, dataDiscoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(
            HealthStatus.RED,
            numberOfRedMasterNodes,
            masterDiscoveryNodes,
            HealthStatus.RED,
            numberOfRedNonDataNonMasterNodes,
            nonDataNonMasterDiscoveryNodes,
            HealthStatus.YELLOW,
            numberOfYellowDataNodes,
            dataDiscoveryNodes
        );
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(HealthStatus.RED));
        assertThat(
            result.symptom(),
            equalTo(
                "1 index is not allowed to be updated because "
                    + (numberOfYellowDataNodes + (numberOfYellowDataNodes == 1 ? " node is" : " nodes are"))
                    + " out of disk or running low on disk space. Furthermore "
                    + (numberOfRedMasterNodes + numberOfRedNonDataNonMasterNodes)
                    + " nodes with roles: "
                    + Stream.concat(masterRole.stream(), nonDataNonMasterRoles.stream()).map(DiscoveryNodeRole::roleName).sorted().toList()
                    + " are out of disk or running low on disk space."
            )
        );
        List<HealthIndicatorImpact> impacts = result.impacts();
        assertThat(impacts.size(), equalTo(3));
        assertThat(impacts.get(0).impactDescription(), containsString("Cannot insert or update documents in the affected indices ["));
        assertThat(impacts.get(0).severity(), equalTo(1));
        assertThat(impacts.get(0).impactAreas(), equalTo(List.of(ImpactArea.INGEST)));
        assertThat(impacts.get(1).impactDescription(), equalTo("Cluster stability might be impaired."));
        assertThat(impacts.get(1).severity(), equalTo(2));
        assertThat(impacts.get(1).impactAreas(), equalTo(List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)));
        assertThat(
            impacts.get(2).impactDescription(),
            equalTo(
                "The "
                    + nonDataNonMasterRoles.stream().map(DiscoveryNodeRole::roleName).sorted().toList()
                    + " functionality might be impaired."
            )
        );
        assertThat(impacts.get(2).severity(), equalTo(2));
        assertThat(impacts.get(2).impactAreas(), equalTo(List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)));

        List<Diagnosis> diagnosisList = result.diagnosisList();
        assertThat(diagnosisList.size(), equalTo(3));
        Diagnosis dataDiagnosis = diagnosisList.get(0);
        List<String> dataAffectedResources = dataDiagnosis.affectedResources();
        assertThat(dataAffectedResources.size(), equalTo(numberOfYellowDataNodes));
        Diagnosis.Definition dataDiagnosisDefinition = dataDiagnosis.definition();
        assertThat(
            dataDiagnosisDefinition.cause(),
            equalTo("1 index resides on nodes that have run out of space and writing has been blocked by the system.")
        );
        assertThat(
            dataDiagnosisDefinition.action(),
            equalTo(
                "Enable autoscaling (if applicable), add disk capacity or free up disk space to resolve this. If you have already "
                    + "taken action please wait for the rebalancing to complete."
            )
        );

        Diagnosis masterDiagnosis = diagnosisList.get(1);
        List<String> masterAffectedResources = masterDiagnosis.affectedResources();
        assertThat(masterAffectedResources.size(), equalTo(numberOfRedMasterNodes));
        Diagnosis.Definition masterDiagnosisDefinition = masterDiagnosis.definition();
        assertThat(masterDiagnosisDefinition.cause(), equalTo("Disk is almost full."));
        assertThat(
            masterDiagnosisDefinition.action(),
            equalTo("Please add capacity to the current nodes, or replace them with ones with higher capacity.")
        );

        Diagnosis nonDataNonMasterDiagnosis = diagnosisList.get(2);
        List<String> nonDataNonMasterAffectedResources = nonDataNonMasterDiagnosis.affectedResources();
        assertThat(nonDataNonMasterAffectedResources.size(), equalTo(numberOfRedNonDataNonMasterNodes));
        Diagnosis.Definition nonDataNonMasterDiagnosisDefinition = nonDataNonMasterDiagnosis.definition();
        assertThat(nonDataNonMasterDiagnosisDefinition.cause(), equalTo("Disk is almost full."));
        assertThat(
            nonDataNonMasterDiagnosisDefinition.action(),
            equalTo("Please add capacity to the current nodes, or replace them with ones with higher capacity.")
        );
    }

    public void testGetNodeIdsReportingStatus() {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        Map<String, DiskHealthInfo> diskInfoByNode = new HashMap<>(discoveryNodes.size());
        Map<HealthStatus, Set<String>> statusToNodeIdMap = new HashMap<>(HealthStatus.values().length);
        for (DiscoveryNode node : discoveryNodes) {
            HealthStatus status = randomFrom(HealthStatus.values());
            final DiskHealthInfo diskHealthInfo = randomBoolean()
                ? new DiskHealthInfo(status)
                : new DiskHealthInfo(status, randomFrom(DiskHealthInfo.Cause.values()));
            Set<String> nodeIdsForStatus = statusToNodeIdMap.computeIfAbsent(status, k -> new HashSet<>());
            nodeIdsForStatus.add(node.getId());
            diskInfoByNode.put(node.getId(), diskHealthInfo);
        }
        for (HealthStatus status : HealthStatus.values()) {
            assertThat(
                DiskHealthIndicatorService.getNodeIdsReportingStatus(diskInfoByNode, status),
                equalTo(statusToNodeIdMap.get(status) == null ? Set.of() : statusToNodeIdMap.get(status))
            );
        }
    }

    public void testGetRolesOnNodes() {
        Set<DiscoveryNodeRole> roles = new HashSet<>(
            randomSubsetOf(randomIntBetween(1, DiscoveryNodeRole.roles().size()), DiscoveryNodeRole.roles())
        );
        Set<DiscoveryNode> discoveryNodes = createNodes(roles);
        ClusterService clusterService = createClusterService(false, discoveryNodes);
        Set<DiscoveryNodeRole> result = DiskHealthIndicatorService.getRolesOnNodes(
            discoveryNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet()),
            clusterService.state()
        );
        assertThat(result, equalTo(roles));
    }

    public void testGetNodesWithDataRole() {
        Set<DiscoveryNode> nonDataNodes = createNodes(new HashSet<>(randomNonEmptySubsetOf(NON_DATA_ROLES)));
        Set<DiscoveryNode> pureDataNodes = createNodes(new HashSet<>(randomNonEmptySubsetOf(DATA_ROLES)));
        Set<DiscoveryNode> mixedNodes = createNodes(
            Stream.concat(randomNonEmptySubsetOf(NON_DATA_ROLES).stream(), randomNonEmptySubsetOf(DATA_ROLES).stream())
                .collect(Collectors.toSet())
        );
        Set<DiscoveryNode> allNodes = Stream.concat(Stream.concat(nonDataNodes.stream(), pureDataNodes.stream()), mixedNodes.stream())
            .collect(Collectors.toSet());
        ClusterService clusterService = createClusterService(false, allNodes);
        assertThat(
            DiskHealthIndicatorService.getNodesWithDataRole(
                allNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet()),
                clusterService.state()
            ),
            equalTo(Stream.concat(pureDataNodes.stream(), mixedNodes.stream()).map(DiscoveryNode::getId).collect(Collectors.toSet()))
        );
    }

    public void testGetNodesWithMasterRole() {
        Set<DiscoveryNodeRole> nonMasterRoles = Sets.union(DATA_ROLES, OTHER_ROLES);
        Set<DiscoveryNode> nonMasterNodes = createNodes(new HashSet<>(randomNonEmptySubsetOf(nonMasterRoles)));
        Set<DiscoveryNode> pureMasterNodes = createNodes(Set.of(DiscoveryNodeRole.MASTER_ROLE));
        Set<DiscoveryNode> mixedNodes = createNodes(DiscoveryNodeRole.roles());
        Set<DiscoveryNode> allNodes = Stream.concat(Stream.concat(nonMasterNodes.stream(), pureMasterNodes.stream()), mixedNodes.stream())
            .collect(Collectors.toSet());
        ClusterService clusterService = createClusterService(false, allNodes);
        assertThat(
            DiskHealthIndicatorService.getNodesWithMasterRole(
                allNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet()),
                clusterService.state()
            ),
            equalTo(pureMasterNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet()))
        );
    }

    public void testGetNodesWithNonDataNonMasterRoles() {
        Set<DiscoveryNodeRole> dataAndMasterRoles = Sets.union(Set.of(DiscoveryNodeRole.MASTER_ROLE), DATA_ROLES);
        Set<DiscoveryNode> dataAndMasterNodes = createNodes(new HashSet<>(randomNonEmptySubsetOf(dataAndMasterRoles)));
        Set<DiscoveryNode> pureNonDataNonMasterNodes = createNodes(new HashSet<>(randomNonEmptySubsetOf(OTHER_ROLES)));
        Set<DiscoveryNode> mixedNodes = createNodes(
            Stream.concat(randomNonEmptySubsetOf(dataAndMasterRoles).stream(), randomNonEmptySubsetOf(OTHER_ROLES).stream())
                .collect(Collectors.toSet())
        );
        Set<DiscoveryNode> allNodes = Stream.concat(
            Stream.concat(dataAndMasterNodes.stream(), pureNonDataNonMasterNodes.stream()),
            mixedNodes.stream()
        ).collect(Collectors.toSet());
        ClusterService clusterService = createClusterService(false, allNodes);
        assertThat(
            DiskHealthIndicatorService.getNodesWithNonDataNonMasterRoles(
                allNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet()),
                clusterService.state()
            ),
            equalTo(pureNonDataNonMasterNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet()))
        );
    }

    public void testGetIndicesForNodes() {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        HealthStatus expectedStatus = HealthStatus.RED;
        int numberOfRedNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(expectedStatus, numberOfRedNodes, discoveryNodes);
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
        Set<String> redNodeIndices = new HashSet<>();
        Set<String> nonRedNodeIndices = new HashSet<>();
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = randomAlphaOfLength(20);
            if (nonRedNodeIds.isEmpty() || randomBoolean()) {
                indexNameToNodeIdsMap.put(indexName, redNodeIds);
                redNodeIndices.add(indexName);
            } else {
                indexNameToNodeIdsMap.put(indexName, nonRedNodeIds);
                nonRedNodeIndices.add(indexName);
            }
        }
        ClusterService clusterService = createClusterService(Set.of(), discoveryNodes, indexNameToNodeIdsMap);
        assertThat(DiskHealthIndicatorService.getIndicesForNodes(redNodeIds, clusterService.state()), equalTo(redNodeIndices));
        assertThat(DiskHealthIndicatorService.getIndicesForNodes(nonRedNodeIds, clusterService.state()), equalTo(nonRedNodeIndices));
    }

    public void testGetNodeIdsForIndices() {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        HealthStatus expectedStatus = HealthStatus.RED;
        int numberOfRedNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(expectedStatus, numberOfRedNodes, discoveryNodes);
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
        Set<String> redNodeIndices = new HashSet<>();
        Set<String> nonRedNodeIndices = new HashSet<>();
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = randomAlphaOfLength(20);
            if (nonRedNodeIds.isEmpty() || randomBoolean()) {
                indexNameToNodeIdsMap.put(indexName, redNodeIds);
                redNodeIndices.add(indexName);
            } else {
                indexNameToNodeIdsMap.put(indexName, nonRedNodeIds);
                nonRedNodeIndices.add(indexName);
            }
        }
        ClusterService clusterService = createClusterService(Set.of(), discoveryNodes, indexNameToNodeIdsMap);
        assertThat(DiskHealthIndicatorService.getNodeIdsForIndices(redNodeIndices, clusterService.state()), equalTo(redNodeIds));
        assertThat(DiskHealthIndicatorService.getNodeIdsForIndices(nonRedNodeIndices, clusterService.state()), equalTo(nonRedNodeIds));
    }

    private Set<DiscoveryNode> createNodesWithAllRoles() {
        return createNodes(DiscoveryNodeRole.roles());
    }

    private Set<DiscoveryNode> createNodes(Set<DiscoveryNodeRole> roles) {
        int numberOfNodes = randomIntBetween(1, 200);
        Set<DiscoveryNode> discoveryNodes = new HashSet<>();
        for (int i = 0; i < numberOfNodes; i++) {
            discoveryNodes.add(
                new DiscoveryNode(
                    randomAlphaOfLength(30),
                    UUID.randomUUID().toString(),
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    roles,
                    Version.CURRENT
                )
            );
        }
        return discoveryNodes;
    }

    private HealthInfo createHealthInfo(HealthStatus expectedStatus, Set<DiscoveryNode> nodes) {
        return createHealthInfo(expectedStatus, 1, nodes);
    }

    private HealthInfo createHealthInfo(HealthStatus expectedStatus, int numberOfNodesWithExpectedStatus, Set<DiscoveryNode> nodes) {
        assert numberOfNodesWithExpectedStatus <= nodes.size();
        Map<String, DiskHealthInfo> diskInfoByNode = new HashMap<>(nodes.size());
        createHealthInfoForNodes(diskInfoByNode, expectedStatus, numberOfNodesWithExpectedStatus, nodes);
        return new HealthInfo(diskInfoByNode);
    }

    /*
     * This version of the method is similar to the one above, except it applies three different statuses to three different sets of nodes.
     */
    private HealthInfo createHealthInfo(
        HealthStatus expectedStatus1,
        int numberOfNodesWithExpectedStatus1,
        Set<DiscoveryNode> nodes1,
        HealthStatus expectedStatus2,
        int numberOfNodesWithExpectedStatus2,
        Set<DiscoveryNode> nodes2,
        HealthStatus expectedStatus3,
        int numberOfNodesWithExpectedStatus3,
        Set<DiscoveryNode> nodes3
    ) {
        assert numberOfNodesWithExpectedStatus1 <= nodes1.size();
        assert numberOfNodesWithExpectedStatus2 <= nodes2.size();
        assert numberOfNodesWithExpectedStatus3 <= nodes3.size();
        Map<String, DiskHealthInfo> diskInfoByNode = new HashMap<>();
        createHealthInfoForNodes(diskInfoByNode, expectedStatus1, numberOfNodesWithExpectedStatus1, nodes1);
        createHealthInfoForNodes(diskInfoByNode, expectedStatus2, numberOfNodesWithExpectedStatus2, nodes2);
        createHealthInfoForNodes(diskInfoByNode, expectedStatus3, numberOfNodesWithExpectedStatus3, nodes3);
        return new HealthInfo(diskInfoByNode);
    }

    private void createHealthInfoForNodes(
        Map<String, DiskHealthInfo> diskInfoByNode,
        HealthStatus expectedStatus,
        int numberOfNodesWithExpectedStatus,
        Set<DiscoveryNode> nodes
    ) {
        int numberWithNonGreenStatus3 = 0;
        for (DiscoveryNode node : nodes) {
            final DiskHealthInfo diskHealthInfo;
            if (numberWithNonGreenStatus3 < numberOfNodesWithExpectedStatus) {
                diskHealthInfo = randomBoolean()
                    ? new DiskHealthInfo(expectedStatus)
                    : new DiskHealthInfo(expectedStatus, randomFrom(DiskHealthInfo.Cause.values()));
                numberWithNonGreenStatus3++;
            } else {
                diskHealthInfo = randomBoolean()
                    ? new DiskHealthInfo(HealthStatus.GREEN)
                    : new DiskHealthInfo(HealthStatus.GREEN, randomFrom(DiskHealthInfo.Cause.values()));
            }
            diskInfoByNode.put(node.getId(), diskHealthInfo);
        }
    }

    private static ClusterService createClusterService(boolean blockIndex, Set<DiscoveryNode> nodes) {
        return createClusterService(1, blockIndex ? 1 : 0, nodes);
    }

    private static ClusterService createClusterService(int numberOfIndices, int numberOfIndicesToBlock, Set<DiscoveryNode> nodes) {
        Map<String, Set<String>> indexNameToNodeIdsMap = new HashMap<>();
        Set<String> blockedIndices = new HashSet<>(numberOfIndicesToBlock);
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = randomAlphaOfLength(20);
            /*
             * The following effectively makes it so that the index does not exist on any node. That's not realistic, but works out for
             * tests where we want for there to be no indices on red/yellow nodes
             */
            indexNameToNodeIdsMap.put(indexName, Set.of());
            if (i < numberOfIndicesToBlock) {
                blockedIndices.add(indexName);
            }
        }
        return createClusterService(blockedIndices, nodes, indexNameToNodeIdsMap);
    }

    private static ClusterService createClusterService(
        Set<String> blockedIndices,
        Set<DiscoveryNode> nodes,
        Map<String, Set<String>> indexNameToNodeIdsMap
    ) {
        RoutingTable routingTable = mock(RoutingTable.class);
        List<ShardRouting> shardRoutings = new ArrayList<>();
        when(routingTable.allShards()).thenReturn(shardRoutings);

        ClusterBlocks.Builder clusterBlocksBuilder = new ClusterBlocks.Builder();
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        List<ClusterBlocks> clusterBlocksList = new ArrayList<>();
        for (String indexName : indexNameToNodeIdsMap.keySet()) {
            boolean blockIndex = blockedIndices.contains(indexName);
            IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), blockIndex)
                    .build()
            ).build();
            indexMetadataMap.put(indexMetadata.getIndex().getName(), indexMetadata);
            if (blockIndex) {
                ClusterBlocks clusterBlocks = clusterBlocksBuilder.addBlocks(indexMetadata).build();
                clusterBlocksList.add(clusterBlocks);
            }
            for (String nodeId : indexNameToNodeIdsMap.get(indexName)) {
                ShardRouting shardRouting = TestShardRouting.newShardRouting(
                    indexMetadata.getIndex().getName(),
                    randomIntBetween(1, 5),
                    nodeId,
                    randomBoolean(),
                    ShardRoutingState.STARTED
                );
                shardRoutings.add(shardRouting);
            }
        }
        Metadata.Builder metadataBuilder = Metadata.builder();
        metadataBuilder.indices(indexMetadataMap);
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        for (DiscoveryNode node : nodes) {
            nodesBuilder.add(node);
        }
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(routingTable)
            .metadata(metadataBuilder.build())
            .nodes(nodesBuilder);
        for (ClusterBlocks clusterBlocks : clusterBlocksList) {
            clusterStateBuilder.blocks(clusterBlocks);
        }
        clusterStateBuilder.nodes(nodesBuilder);
        ClusterState clusterState = clusterStateBuilder.build();
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return clusterService;
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        return parser.map();
    }
}
