/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterFormationInfoActionTests extends ESTestCase {

    public void testSerialization() {
        ClusterFormationFailureHelper.ClusterFormationState clusterFormationState = getClusterFormationState();
        ClusterFormationInfoAction.Response response = new ClusterFormationInfoAction.Response(clusterFormationState);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            response,
            history -> copyWriteable(history, writableRegistry(), ClusterFormationInfoAction.Response::new),
            this::mutateResponse
        );

        ClusterFormationInfoAction.Request request = new ClusterFormationInfoAction.Request();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            request,
            history -> copyWriteable(history, writableRegistry(), ClusterFormationInfoAction.Request::new)
        );
    }

    private ClusterFormationInfoAction.Response mutateResponse(ClusterFormationInfoAction.Response originalResponse) {
        ClusterFormationFailureHelper.ClusterFormationState clusterFormationState = originalResponse.getClusterFormationState();
        switch (randomIntBetween(1, 3)) {
            case 1 -> {
                ClusterFormationFailureHelper.ClusterFormationState newClusterFormationState =
                    new ClusterFormationFailureHelper.ClusterFormationState(
                        clusterFormationState.initialMasterNodesSetting(),
                        clusterFormationState.localNode(),
                        clusterFormationState.masterEligibleNodes(),
                        clusterFormationState.clusterStateVersion() + 1,
                        clusterFormationState.acceptedTerm(),
                        clusterFormationState.lastAcceptedConfiguration(),
                        clusterFormationState.lastCommittedConfiguration(),
                        clusterFormationState.resolvedAddresses(),
                        clusterFormationState.foundPeers(),
                        clusterFormationState.currentTerm(),
                        clusterFormationState.hasDiscoveredQuorum(),
                        clusterFormationState.statusInfo(),
                        clusterFormationState.inFlightJoinStatuses()
                    );
                return new ClusterFormationInfoAction.Response(newClusterFormationState);
            }
            case 2 -> {
                ClusterFormationFailureHelper.ClusterFormationState newClusterFormationState =
                    new ClusterFormationFailureHelper.ClusterFormationState(
                        clusterFormationState.initialMasterNodesSetting(),
                        clusterFormationState.localNode(),
                        clusterFormationState.masterEligibleNodes(),
                        clusterFormationState.clusterStateVersion(),
                        clusterFormationState.acceptedTerm() + 1,
                        clusterFormationState.lastAcceptedConfiguration(),
                        clusterFormationState.lastCommittedConfiguration(),
                        clusterFormationState.resolvedAddresses(),
                        clusterFormationState.foundPeers(),
                        clusterFormationState.currentTerm(),
                        clusterFormationState.hasDiscoveredQuorum(),
                        clusterFormationState.statusInfo(),
                        clusterFormationState.inFlightJoinStatuses()
                    );
                return new ClusterFormationInfoAction.Response(newClusterFormationState);
            }
            case 3 -> {
                ClusterFormationFailureHelper.ClusterFormationState newClusterFormationState =
                    new ClusterFormationFailureHelper.ClusterFormationState(
                        clusterFormationState.initialMasterNodesSetting(),
                        clusterFormationState.localNode(),
                        clusterFormationState.masterEligibleNodes(),
                        clusterFormationState.clusterStateVersion(),
                        clusterFormationState.acceptedTerm(),
                        clusterFormationState.lastAcceptedConfiguration(),
                        clusterFormationState.lastCommittedConfiguration(),
                        clusterFormationState.resolvedAddresses(),
                        clusterFormationState.foundPeers(),
                        clusterFormationState.currentTerm(),
                        clusterFormationState.hasDiscoveredQuorum() == false,
                        clusterFormationState.statusInfo(),
                        clusterFormationState.inFlightJoinStatuses()
                    );
                return new ClusterFormationInfoAction.Response(newClusterFormationState);
            }
            default -> throw new IllegalStateException();
        }
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
            DiscoveryNode masterNode = DiscoveryNodeUtils.create(id);
            masterEligibleNodes.put(id, masterNode);
        }
        return masterEligibleNodes;
    }

    private ClusterFormationFailureHelper.ClusterFormationState getClusterFormationState() {
        Map<String, DiscoveryNode> masterEligibleNodesMap = getMasterEligibleNodes();
        List<String> initialMasterNodesSetting = Arrays.asList(generateRandomStringArray(7, 30, false, false));
        DiscoveryNode localNode = masterEligibleNodesMap.values().stream().findAny().get();
        return new ClusterFormationFailureHelper.ClusterFormationState(
            initialMasterNodesSetting,
            localNode,
            Map.copyOf(masterEligibleNodesMap),
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

    public void testTransportDoExecute() {
        TransportService transportService = mock(TransportService.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.relativeTimeInMillis()).thenReturn(System.currentTimeMillis());
        Coordinator coordinator = mock(Coordinator.class);
        ClusterFormationFailureHelper.ClusterFormationState clusterFormationState = getClusterFormationState();
        when(coordinator.getClusterFormationState()).thenReturn(clusterFormationState);
        ClusterFormationInfoAction.TransportAction action = new ClusterFormationInfoAction.TransportAction(
            transportService,
            actionFilters,
            coordinator
        );

        final List<ClusterFormationFailureHelper.ClusterFormationState> result = new ArrayList<>();
        ActionListener<ClusterFormationInfoAction.Response> listener = new ActionListener<>() {
            @Override
            public void onResponse(ClusterFormationInfoAction.Response response) {
                result.add(response.getClusterFormationState());
            }

            @Override
            public void onFailure(Exception e) {
                fail("Not expecting failure");
            }
        };
        action.doExecute(null, new ClusterFormationInfoAction.Request(), listener);
        assertEquals(clusterFormationState, result.get(0));
    }
}
