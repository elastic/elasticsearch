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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * This indicator reports the health of peer-to-peer transport connections across the cluster.
 *
 * <ul>
 *   <li>{@code GREEN} when all nodes have full connectivity to all other nodes in the cluster.</li>
 *   <li>{@code YELLOW} when at least one node reports a disconnected peer, indicating partial connectivity loss.</li>
 * </ul>
 *
 * The indicator surfaces an aggregate view, since there are O(n^2) peer links in a cluster.
 */
public class PeerConnectionsHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "peer_connections";

    private static final String IMPACT_SEARCH_PARTIAL_RESULTS_ID = "search_partial_results";
    private static final String IMPACT_SHARD_RECOVERY_BLOCKED_ID = "shard_recovery_blocked";

    static final List<HealthIndicatorImpact> YELLOW_IMPACTS = List.of(
        new HealthIndicatorImpact(
            NAME,
            IMPACT_SEARCH_PARTIAL_RESULTS_ID,
            2,
            "Searches might return partial results if shards on disconnected nodes cannot be reached.",
            List.of(ImpactArea.SEARCH)
        ),
        new HealthIndicatorImpact(
            NAME,
            IMPACT_SHARD_RECOVERY_BLOCKED_ID,
            2,
            "Shard recoveries and relocations between disconnected nodes will be blocked.",
            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
        )
    );

    static final Diagnosis TROUBLESHOOT_NETWORK = new Diagnosis(
        new Diagnosis.Definition(
            NAME,
            "troubleshoot_peer_connections",
            "One or more nodes have lost their transport connection to peer nodes in the cluster.",
            "Check network connectivity between the affected nodes. See network disconnect troubleshooting guidance at "
                + ReferenceDocs.NETWORK_DISCONNECT_TROUBLESHOOTING,
            ReferenceDocs.NETWORK_DISCONNECT_TROUBLESHOOTING.toString()
        ),
        null
    );

    private final ClusterService clusterService;

    public PeerConnectionsHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        Map<String, PeerConnectionsHealthInfo> peerInfoByNode = healthInfo.peerConnectionsInfoByNode();
        if (peerInfoByNode == null || peerInfoByNode.isEmpty()) {
            return createIndicator(
                HealthStatus.GREEN,
                "No peer connection issues detected.",
                HealthIndicatorDetails.EMPTY,
                Collections.emptyList(),
                Collections.emptyList()
            );
        }

        ClusterState clusterState = clusterService.state();
        Set<String> nodesWithDisconnectedPeers = new HashSet<>();
        Set<String> allDisconnectedPeerNodeIds = new HashSet<>();

        for (Map.Entry<String, PeerConnectionsHealthInfo> entry : peerInfoByNode.entrySet()) {
            PeerConnectionsHealthInfo info = entry.getValue();
            if (info.hasDisconnectedPeers()) {
                nodesWithDisconnectedPeers.add(entry.getKey());
                allDisconnectedPeerNodeIds.addAll(info.disconnectedPeers());
            }
        }

        if (nodesWithDisconnectedPeers.isEmpty()) {
            return createIndicator(
                HealthStatus.GREEN,
                "No peer connection issues detected.",
                HealthIndicatorDetails.EMPTY,
                Collections.emptyList(),
                Collections.emptyList()
            );
        }

        int totalNodes = clusterState.nodes().getSize();
        String symptom = String.format(
            Locale.ROOT,
            "%d %s %s disconnected peer transport connections.",
            nodesWithDisconnectedPeers.size(),
            nodesWithDisconnectedPeers.size() == 1 ? "node" : "nodes",
            nodesWithDisconnectedPeers.size() == 1 ? "has" : "have"
        );

        HealthIndicatorDetails details = verbose
            ? new SimpleHealthIndicatorDetails(
                Map.of(
                    "nodes_with_disconnected_peers",
                    nodesWithDisconnectedPeers.size(),
                    "total_disconnected_peer_links",
                    countTotalDisconnectedLinks(peerInfoByNode),
                    "total_nodes",
                    totalNodes,
                    "affected_nodes",
                    resolveNodeNames(nodesWithDisconnectedPeers, clusterState)
                )
            )
            : HealthIndicatorDetails.EMPTY;

        List<Diagnosis> diagnoses = verbose ? List.of(TROUBLESHOOT_NETWORK) : List.of();

        return createIndicator(HealthStatus.YELLOW, symptom, details, YELLOW_IMPACTS, diagnoses);
    }

    private static int countTotalDisconnectedLinks(Map<String, PeerConnectionsHealthInfo> peerInfoByNode) {
        int count = 0;
        for (PeerConnectionsHealthInfo info : peerInfoByNode.values()) {
            count += info.disconnectedPeers().size();
        }
        return count;
    }

    private static List<String> resolveNodeNames(Set<String> nodeIds, ClusterState clusterState) {
        return nodeIds.stream().sorted().map(nodeId -> {
            DiscoveryNode node = clusterState.nodes().get(nodeId);
            if (node != null) {
                return node.getName() + " (" + nodeId + ")";
            }
            return nodeId;
        }).toList();
    }
}
