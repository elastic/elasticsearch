/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

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
import org.elasticsearch.health.node.HealthInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * This indicator reports the health of cluster resilience.
 * <p>
 * The indicator will report:
 * * YELLOW status:
 *    1. There's gte 3 nodes in the cluster and lt 3 of them are master-eligible.
 *    2. A two-node cluster has two master-eligible nodes.
 * * GREEN otherwise
 * </p>
 */
public class ClusterResilienceIndicatorService implements HealthIndicatorService {

    public static final String NAME = "cluster_resilience";
    public static final String MASTER_LESS_THEN_3_IMPACT_ID = "master_less_then_3";
    public static final String TWO_NODES_MASTER_IMPACT_ID = "two_nodes_master";
    private final ClusterService clusterService;

    public static final Diagnosis DESIGNING_FOR_RESILIENCE = new Diagnosis(
        new Diagnosis.Definition(
            NAME,
            "designing_for_resilience",
            "The Elasticsearch cluster is not designed for high availability.",
            "See designing for resilience guidance at " + ReferenceDocs.DESIGNING_FOR_RESILIENCE,
            ReferenceDocs.DESIGNING_FOR_RESILIENCE.toString()
        ),
        null
    );

    public ClusterResilienceIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        var state = clusterService.state();
        HealthStatus status;
        String summary = null;
        final List<HealthIndicatorImpact> impacts = new ArrayList<>();
        int masterAndDataNodeNum = state.nodes().getMasterAndDataNodes().size();
        int masterNodeNum = state.nodes().getMasterNodes().size();
        if (masterAndDataNodeNum > 3 && masterNodeNum < 3) {
            // There's ≥3 nodes in the cluster and <3 of them are master-eligible
            status = HealthStatus.YELLOW;
            summary = "This cluster does not have enough master-eligible nodes to tolerate master failure.";
            String impactDesc = String.format(Locale.ROOT,"The cluster total master and data nodes is %d which is more than 3, " +
                "but only has %d master-eligible nodes which is less than three, " +
                "this would not resilient to master failure.", masterAndDataNodeNum, masterNodeNum);
            impacts.add(new HealthIndicatorImpact(NAME, MASTER_LESS_THEN_3_IMPACT_ID,
                2, impactDesc, List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)));
        } else if (masterAndDataNodeNum == 2 && masterNodeNum == 2) {
            // A two-node cluster has two master-eligible nodes
            status = HealthStatus.YELLOW;
            summary = "This cluster cannot reliably tolerate the loss of either node due to both two nodes are master-eligible.";
            String impactDesc = "The cluster has 2 nodes and both of them are master-eligible nodes, " +
                "the election will fail if either node is unavailable, your cluster cannot reliably tolerate the loss of either node.";
            impacts.add(new HealthIndicatorImpact(NAME, TWO_NODES_MASTER_IMPACT_ID,
                2, impactDesc, List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)));
        } else {
            status = HealthStatus.GREEN;
        }

        return createIndicator(status, summary, getDetails(verbose), impacts,
            status == HealthStatus.GREEN ? List.of() : getDiagnosis(verbose));
    }

    private HealthIndicatorDetails getDetails(boolean verbose) {
        if (verbose) {
            var state = clusterService.state();
            return new SimpleHealthIndicatorDetails(
                Map.of(
                    "number_of_nodes",
                    state.nodes().size(),
                    "number_of_master_and_data_nodes",
                    state.nodes().getMasterAndDataNodes().size(),
                    "number_of_master_nodes",
                    state.nodes().getMasterNodes().size(),
                    "number_of_data_nodes",
                    state.nodes().getDataNodes().size()
                )
            );
        } else {
            return HealthIndicatorDetails.EMPTY;
        }
    }

    private List<Diagnosis> getDiagnosis(boolean verbose) {
        if (verbose) {
            return List.of(DESIGNING_FOR_RESILIENCE);
        } else {
            return List.of();
        }
    }
}
