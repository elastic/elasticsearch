/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.health.ServerHealthComponents.CLUSTER_COORDINATION;

public class InstanceHasMasterHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "instance_has_master";

    private static final String INSTANCE_HAS_MASTER_GREEN_SUMMARY = "Health coordinating instance has an elected master node.";
    private static final String INSTANCE_HAS_MASTER_RED_SUMMARY = "Health coordinating instance does not have an elected master node.";

    private static final String HELP_URL = "https://ela.st/fix-master";

    private static final String NO_MASTER_INGEST_IMPACT = "The cluster cannot create, delete, or rebalance indices, and cannot insert or "
        + "update documents.";
    private static final String NO_MASTER_DEPLOYMENT_MANAGEMENT_IMPACT = "Scheduled tasks such as Watcher, ILM, and SLM will not work. "
        + "The _cat APIs will not work.";
    private static final String NO_MASTER_BACKUP_IMPACT = "Snapshot and restore will not work. Searchable snapshots cannot be mounted.";

    private final ClusterService clusterService;

    public InstanceHasMasterHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String component() {
        return CLUSTER_COORDINATION;
    }

    @Override
    public String helpURL() {
        return HELP_URL;
    }

    @Override
    public HealthIndicatorResult calculate(boolean explain) {

        DiscoveryNode coordinatingNode = clusterService.localNode();
        ClusterState clusterState = clusterService.state();
        DiscoveryNodes nodes = clusterState.nodes();
        DiscoveryNode masterNode = nodes.getMasterNode();

        HealthStatus instanceHasMasterStatus = masterNode == null ? HealthStatus.RED : HealthStatus.GREEN;
        String instanceHasMasterSummary = masterNode == null ? INSTANCE_HAS_MASTER_RED_SUMMARY : INSTANCE_HAS_MASTER_GREEN_SUMMARY;
        List<HealthIndicatorImpact> impacts = new ArrayList<>();
        if (masterNode == null) {
            impacts.add(new HealthIndicatorImpact(1, NO_MASTER_INGEST_IMPACT, List.of(ImpactArea.INGEST)));
            impacts.add(new HealthIndicatorImpact(1, NO_MASTER_DEPLOYMENT_MANAGEMENT_IMPACT, List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)));
            impacts.add(new HealthIndicatorImpact(3, NO_MASTER_BACKUP_IMPACT, List.of(ImpactArea.BACKUP)));
        }

        return createIndicator(
            instanceHasMasterStatus,
            instanceHasMasterSummary,
            getDetails(explain, masterNode, coordinatingNode),
            impacts,
            Collections.emptyList()
        );
    }

    private static HealthIndicatorDetails getDetails(boolean explain, DiscoveryNode coordinatingNode, @Nullable DiscoveryNode masterNode) {
        if (explain == false) {
            return HealthIndicatorDetails.EMPTY;
        }
        Map<String, String> masterNodeMap = new HashMap<>();
        masterNodeMap.put("node_id", masterNode == null ? null : masterNode.getId());
        masterNodeMap.put("name", masterNode == null ? null : masterNode.getName());
        Map<String, String> coordinatingNodeMap = Map.of("node_id", coordinatingNode.getId(), "name", coordinatingNode.getName());
        Map<String, Object> details = Map.of("master_node", masterNodeMap, "coordinating_node", coordinatingNodeMap);
        return new HealthIndicatorDetails(details);
    }
}
