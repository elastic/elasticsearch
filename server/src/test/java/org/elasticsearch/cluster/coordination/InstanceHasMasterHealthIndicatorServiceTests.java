/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.coordination.InstanceHasMasterHealthIndicatorService.INSTANCE_HAS_MASTER_RED_SUMMARY;
import static org.elasticsearch.cluster.coordination.InstanceHasMasterHealthIndicatorService.NAME;
import static org.elasticsearch.cluster.coordination.InstanceHasMasterHealthIndicatorService.NO_MASTER;
import static org.elasticsearch.cluster.coordination.InstanceHasMasterHealthIndicatorService.NO_MASTER_BACKUP_IMPACT;
import static org.elasticsearch.cluster.coordination.InstanceHasMasterHealthIndicatorService.NO_MASTER_DEPLOYMENT_MANAGEMENT_IMPACT;
import static org.elasticsearch.cluster.coordination.InstanceHasMasterHealthIndicatorService.NO_MASTER_INGEST_IMPACT;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.ServerHealthComponents.CLUSTER_COORDINATION;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InstanceHasMasterHealthIndicatorServiceTests extends ESTestCase {

    public void testIsRedWhenInstanceHasNoMaster() {
        var clusterState = ClusterState.builder(new ClusterName("test-cluster")).build();
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        DiscoveryNode localNode = new DiscoveryNode(
            "thisNode",
            "withThisId",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        when(clusterService.localNode()).thenReturn(localNode);
        var service = new InstanceHasMasterHealthIndicatorService(clusterService);

        List<HealthIndicatorImpact> expectedImpacts = new ArrayList<>();
        expectedImpacts.add(new HealthIndicatorImpact(1, NO_MASTER_INGEST_IMPACT, List.of(ImpactArea.INGEST)));
        expectedImpacts.add(
            new HealthIndicatorImpact(1, NO_MASTER_DEPLOYMENT_MANAGEMENT_IMPACT, List.of(ImpactArea.DEPLOYMENT_MANAGEMENT))
        );
        expectedImpacts.add(new HealthIndicatorImpact(3, NO_MASTER_BACKUP_IMPACT, List.of(ImpactArea.BACKUP)));

        HealthIndicatorResult actual = service.calculate(true);
        assertThat(
            actual,
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    CLUSTER_COORDINATION,
                    RED,
                    INSTANCE_HAS_MASTER_RED_SUMMARY,
                    InstanceHasMasterHealthIndicatorService.HELP_URL,
                    // lambda identity is tricky (especially as the InstanceHasMasterHealthIndicatorService#details is a capturing one)
                    // so we test the details contents separately
                    actual.details(),
                    expectedImpacts,
                    List.of(NO_MASTER)
                )
            )
        );

        assertThat(
            Strings.toString(actual.details()),
            equalTo(
                "{\"coordinating_node\":{\"node_id\":\"withThisId\",\"name\":\"thisNode\"},\"master_node\":{\"node_id\":null,"
                    + "\"name\":null}}"
            )
        );
    }

}
