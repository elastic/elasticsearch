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
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.coordination.ClusterResilienceIndicatorService.NAME;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterResilienceIndicatorServiceTests extends ESTestCase {

    public void testLessThen3MasterNodes() {
        ClusterState clusterState = createClusterState(2, 3);
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        ClusterResilienceIndicatorService service = new ClusterResilienceIndicatorService(clusterService);
        HealthIndicatorResult result = service.calculate(true, 0, null);

        assertThat(
            result,
            equalTo(
                createExpectedResult(
                    YELLOW,
                    "This cluster does not have enough master-eligible nodes to tolerate master failure.",
                    Map.of("number_of_nodes", 5, "number_of_master_and_data_nodes", 5,
                        "number_of_master_nodes", 2, "number_of_data_nodes", 5),
                    List.of(
                        new HealthIndicatorImpact(
                            NAME,
                            ClusterResilienceIndicatorService.MASTER_LESS_THEN_3_IMPACT_ID,
                            2,
                            "The cluster total master and data nodes is 5 which is more than 3, " +
                                "but only has 2 master-eligible nodes which is less than three, " +
                                "this would not resilient to master failure.",
                            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
                        )
                    ),
                    List.of(ClusterResilienceIndicatorService.DESIGNING_FOR_RESILIENCE)
                )
            )
        );
    }

    public void test2NodesBothAreMasterNodes() {
        ClusterState clusterState = createClusterState(2, 0);
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        ClusterResilienceIndicatorService service = new ClusterResilienceIndicatorService(clusterService);
        HealthIndicatorResult result = service.calculate(true, 0, null);

        assertThat(
            result,
            equalTo(
                createExpectedResult(
                    YELLOW,
                    "This cluster cannot reliably tolerate the loss of either node due to both two nodes are master-eligible.",
                    Map.of("number_of_nodes", 2, "number_of_master_and_data_nodes", 2,
                        "number_of_master_nodes", 2, "number_of_data_nodes", 2),
                    List.of(
                        new HealthIndicatorImpact(
                            NAME,
                            ClusterResilienceIndicatorService.TWO_NODES_MASTER_IMPACT_ID,
                            2,
                            "The cluster has 2 nodes and both of them are master-eligible nodes, " +
                                "the election will fail if either node is unavailable, " +
                                "your cluster cannot reliably tolerate the loss of either node.",
                            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
                        )
                    ),
                    List.of(ClusterResilienceIndicatorService.DESIGNING_FOR_RESILIENCE)
                )
            )
        );
    }

    private ClusterState createClusterState(int masterAndDataNodeNum, int dataNodeNum) {
        DiscoveryNodes.Builder nodesBulder = DiscoveryNodes.builder();
        for (int i=0; i<masterAndDataNodeNum; i++) {
            String nodeName = "node" + i + "_md";
            DiscoveryNode node = DiscoveryNodeUtils.create(nodeName);
            nodesBulder.localNodeId(nodeName).masterNodeId(nodeName).add(node).build();
        }
        for (int i=0; i<dataNodeNum; i++) {
            String nodeName = "node" + i + "_d";
            DiscoveryNode node = DiscoveryNodeUtils.create(nodeName, buildNewFakeTransportAddress(), Map.of(),
                Set.of(DiscoveryNodeRole.DATA_ROLE));
            nodesBulder.add(node);
        }

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.nodes()).thenReturn(nodesBulder.build());
        return clusterState;
    }

    private HealthIndicatorResult createExpectedResult(
        HealthStatus status,
        String symptom,
        Map<String, Object> details,
        List<HealthIndicatorImpact> impacts,
        List<Diagnosis> diagnosisList
    ) {
        return new HealthIndicatorResult(
            NAME,
            status,
            symptom,
            new SimpleHealthIndicatorDetails(details),
            impacts,
            diagnosisList
        );
    }
}
