/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class RoutingAllocationTests extends ESTestCase {

    public void testMaxShardWriteLoadProportionForNode_SingleShard() {
        final var indexName = randomIdentifier();
        final var clusterState = ClusterStateCreationUtils.state(indexName, 1, 1);
        final var shard0 = clusterState.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).shardId();

        // only shard means 1.0
        expectMaxShardWriteLoadProportion(clusterState, Map.of(shard0, randomDoubleBetween(0.001, 20.0, false)), 1.0);

        // inexplicably not in map means 0.0
        expectMaxShardWriteLoadProportion(clusterState, Map.of(), 0.0);

        // only shard in map, with zero load
        expectMaxShardWriteLoadProportion(clusterState, Map.of(shard0, 0.0), 0.0);

        // not-only shard in map, with 0 load
        expectMaxShardWriteLoadProportion(
            clusterState,
            Map.of(shard0, 0.0, new ShardId(randomIdentifier(), randomUUID(), 0), randomDoubleBetween(0.0001, 20.0, true)),
            0.0
        );
    }

    public void testMaxShardWriteLoadProportionForNode_MultipleShards() {
        final var indexName = randomIdentifier();
        final var clusterState = ClusterStateCreationUtils.state(indexName, 1, 2);
        final var shard0 = clusterState.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).shardId();
        final var shard1 = clusterState.routingTable(ProjectId.DEFAULT).index(indexName).shard(1).shardId();

        double shard0Load = randomDoubleBetween(0.0001, 10.0, true);
        double shard1Load = randomDoubleBetween(shard0Load, 20.0, false);

        // picks the biggest one
        expectMaxShardWriteLoadProportion(
            clusterState,
            Map.of(shard0, shard0Load, shard1, shard1Load),
            shard1Load / (shard0Load + shard1Load)
        );

        // both zero
        expectMaxShardWriteLoadProportion(clusterState, Map.of(shard0, 0.0, shard1, 0.0), 0.0);

        // not in map
        expectMaxShardWriteLoadProportion(clusterState, Map.of(), 0.0);

        // one in map
        expectMaxShardWriteLoadProportion(clusterState, Map.of(shard0, 0.0), 0.0);

        // one shard has 100% of the load
        expectMaxShardWriteLoadProportion(
            clusterState,
            randomBoolean()
                ? Map.of(shard0, randomDoubleBetween(0.0001, 10.0, true), shard1, 0.0)
                : Map.of(shard0, randomDoubleBetween(0.0001, 10.0, true)),
            1.0
        );

        // totally random map entry
        expectMaxShardWriteLoadProportion(
            clusterState,
            Map.of(new ShardId(randomIndexName(), randomUUID(), 0), randomDoubleBetween(0.0001, 20.0, true)),
            0.0
        );
    }

    public void testDecisionPopulatesLabelForInterestingDecisionsWhenNotDebugging() {
        final var routingAllocation = TestRoutingAllocationFactory.forClusterState(
            ClusterStateCreationUtils.state(randomIdentifier(), 1, 1)
        ).build();
        final var label = randomIdentifier();
        for (Decision decision : new Decision[] { Decision.NO, Decision.NOT_PREFERRED }) {
            final var result = (Decision.Single) routingAllocation.decision(decision, label, "reason [%s]", "param");
            assertThat(result.type(), equalTo(decision.type()));
            assertThat(result.label(), equalTo(label));
            assertNull(result.getExplanation());
            assertSame(result, routingAllocation.decision(decision, label, "reason [%s]", "param"));
        }
    }

    public void testDecisionDoesNotLabelUninterestingDecisionsWhenNotDebugging() {
        final var routingAllocation = TestRoutingAllocationFactory.forClusterState(
            ClusterStateCreationUtils.state(randomIdentifier(), 1, 1)
        ).build();
        assertSame(Decision.YES, routingAllocation.decision(Decision.YES, randomIdentifier(), "reason"));
        assertSame(Decision.THROTTLE, routingAllocation.decision(Decision.THROTTLE, randomIdentifier(), "reason"));
    }

    public void testDecisionBypassesCacheWhenDebugging() {
        final var routingAllocation = TestRoutingAllocationFactory.forClusterState(
            ClusterStateCreationUtils.state(randomIdentifier(), 1, 1)
        ).build();
        routingAllocation.debugDecision(true);
        final var label = randomIdentifier();
        final var result = (Decision.Single) routingAllocation.decision(Decision.NO, label, "reason [%s]", "param");
        assertThat(result.label(), equalTo(label));
        assertThat(result.getExplanation(), equalTo("reason [param]"));
        assertNotSame(result, routingAllocation.decision(Decision.NO, label, "reason [%s]", "param"));
    }

    private void expectMaxShardWriteLoadProportion(ClusterState clusterState, Map<ShardId, Double> clusterInfoWriteLoads, double expected) {
        final var node0 = clusterState.getRoutingNodes().node("node_0");
        final var routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState)
            .clusterInfo(ClusterInfo.builder().shardWriteLoads(clusterInfoWriteLoads).build())
            .build();
        assertThat(routingAllocation.maxShardWriteLoadProportionForNode(node0), equalTo(expected));
    }
}
