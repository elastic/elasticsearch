/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService.CoordinationDiagnosticsDetails;
import static org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService.CoordinationDiagnosticsResult;
import static org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService.CoordinationDiagnosticsStatus;

public class CoordinationDiagnosticsActionTests extends ESTestCase {

    public void testSerialization() {
        DiscoveryNode node1 = new DiscoveryNode(
            "node1",
            UUID.randomUUID().toString(),
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
        DiscoveryNode node2 = new DiscoveryNode(
            "node2",
            UUID.randomUUID().toString(),
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
        CoordinationDiagnosticsDetails details = new CoordinationDiagnosticsDetails(
            node1,
            List.of(node1, node2),
            randomAlphaOfLengthBetween(0, 30),
            randomAlphaOfLengthBetween(0, 30),
            randomAlphaOfLengthBetween(0, 30)
        );
        CoordinationDiagnosticsResult result =
            new CoordinationDiagnosticsResult(randomFrom(CoordinationDiagnosticsStatus.values()),
                randomAlphaOfLength(100), details);
        CoordinationDiagnosticsAction.Response response = new CoordinationDiagnosticsAction.Response(result);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            response,
            history -> copyWriteable(history, writableRegistry(), CoordinationDiagnosticsAction.Response::new),
            this::mutateResponse
        );

        CoordinationDiagnosticsAction.Request request = new CoordinationDiagnosticsAction.Request(randomBoolean());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            request,
            history -> copyWriteable(history, writableRegistry(), CoordinationDiagnosticsAction.Request::new),
            this::mutateRequest
        );
    }

    private CoordinationDiagnosticsAction.Request mutateRequest(CoordinationDiagnosticsAction.Request originalRequest) {
        return new CoordinationDiagnosticsAction.Request(originalRequest.explain == false);
    }

    private CoordinationDiagnosticsAction.Response mutateResponse(CoordinationDiagnosticsAction.Response originalResponse) {
        CoordinationDiagnosticsResult
            originalResult = originalResponse.getCoordinationDiagnosticsResult();
        return new CoordinationDiagnosticsAction.Response(
            new CoordinationDiagnosticsResult(originalResult.status(),
                randomAlphaOfLength(100), originalResult.details()));
    }

    /**
     * Returns a map of the master eligible nodes for a test, with node id as the key and DiscoveryNode as the value. The number of master
     * eligible nodes will be between 1 and 7.
     * @return A map of the master eligible nodes for a test
     */
    private Map<String, DiscoveryNode> getMasterEligibleNodes() {
        Map<String, DiscoveryNode> masterEligibleNodes = new HashMap<>();
        int numberOfMasterEligibleNodes = randomIntBetween(1, 7);
        for (int i = 0; i < numberOfMasterEligibleNodes; i++) {
            String id = "_id" + i;
            DiscoveryNode masterNode = new DiscoveryNode(id, buildNewFakeTransportAddress(), Version.CURRENT);
            masterEligibleNodes.put(id, masterNode);
        }
        return masterEligibleNodes;
    }

    private ClusterFormationFailureHelper.ClusterFormationState getClusterFormationState() {
        Map<String, DiscoveryNode> masterEligibleNodesMap = getMasterEligibleNodes();
        List<String> initialMasterNodesSetting = Arrays.stream(generateRandomStringArray(7, 30, false, false)).toList();
        DiscoveryNode localNode = masterEligibleNodesMap.values().stream().findAny().get();
        ImmutableOpenMap.Builder<String, DiscoveryNode> masterEligibleNodesBuilder = new ImmutableOpenMap.Builder<>();
        masterEligibleNodesMap.forEach(masterEligibleNodesBuilder::put);
        ImmutableOpenMap<String, DiscoveryNode> masterEligibleNodes = masterEligibleNodesBuilder.build();
        return new ClusterFormationFailureHelper.ClusterFormationState(
            initialMasterNodesSetting,
            localNode,
            masterEligibleNodes,
            randomLong(),
            randomLong(),
            new CoordinationMetadata.VotingConfiguration(Collections.emptySet()),
            new CoordinationMetadata.VotingConfiguration(Collections.emptySet()),
            Collections.emptyList(),
            Collections.emptyList(),
            randomLong(),
            randomBoolean(),
            new StatusInfo(randomFrom(StatusInfo.Status.HEALTHY, StatusInfo.Status.UNHEALTHY), randomAlphaOfLength(20)),
            Collections.emptyList()
        );
    }
}
