/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INDEX_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.MASTER_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.SEARCH_ROLE;

/**
 * Locks the external-read worker-selection contract: one eligibility predicate,
 * two cluster-state snapshots.
 * <p>
 * Floor and dispatch both apply {@link NodeEligibilityStrategy#EXTERNAL_WORKER_NODES}
 * ({@code canContainData && !INDEX_ROLE}). Floor is computed at coalesce time from the
 * live node set, joining workers included, because coalescing runs before
 * {@link ExternalWorkerReadiness#awaitEligibleWorker} and the group count is an input
 * to {@link AdaptiveStrategy}. Dispatch is computed later from
 * {@link ExternalWorkerReadiness#readyNodes}, so joining workers do not receive splits.
 * That overshoot (extra groups that then queue) is the accepted cost of not undershooting
 * a scan that waited for a joining worker.
 * <p>
 * An index-only cluster has zero remote workers: the floor stays at {@code task_concurrency}
 * (the coordinator still fills that many drivers) and every strategy returns {@code LOCAL}.
 *
 * @see NodeEligibilityStrategyTests
 * @see ExternalDistributionTests#testExternalCoalesceFloorUsesEligibleWorkers
 * @see ExternalWorkerReadinessTests
 * @see DataNodeComputeHandlerExternalErrorTests#testAllExternalWorkersJoining
 */
public class ExternalWorkerSelectionPolicyTests extends ESTestCase {

    private static final int TASK_CONCURRENCY = 14;

    public void testFloorCountsJoiningWorkersThatDispatchExcludes() {
        DiscoveryNode search1 = node("search-1", SEARCH_ROLE);
        DiscoveryNode search2 = node("search-2", SEARCH_ROLE);
        DiscoveryNode joining = node("search-joining", SEARCH_ROLE);
        DiscoveryNode index = node("index-1", INDEX_ROLE);
        DiscoveryNodes all = DiscoveryNodes.builder().add(search1).add(search2).add(joining).add(index).build();
        // Dispatch snapshot: joining workers stripped, as ExternalWorkerReadiness.readyNodes does.
        DiscoveryNodes ready = DiscoveryNodes.builder(all).remove(joining).build();

        assertEquals(Set.of("search-1", "search-2", "search-joining"), eligibleIds(all));
        assertEquals(Set.of("search-1", "search-2"), eligibleIds(ready));
        assertEquals(TASK_CONCURRENCY * 3, ComputeService.externalCoalesceFloor(TASK_CONCURRENCY, all));
        assertEquals(TASK_CONCURRENCY * 2, ComputeService.externalCoalesceFloor(TASK_CONCURRENCY, ready));

        assertDistributedTo(ready, Set.of("search-1", "search-2"));
    }

    public void testFloorAndDispatchShareExternalWorkerPredicate() {
        DiscoveryNodes mixed = DiscoveryNodes.builder()
            .add(node("index-1", INDEX_ROLE))
            .add(node("search-1", SEARCH_ROLE))
            .add(node("data-1", DATA_HOT_NODE_ROLE))
            .add(node("master-1", MASTER_ROLE))
            .build();

        Set<String> eligible = eligibleIds(mixed);
        assertEquals(Set.of("search-1", "data-1"), eligible);
        assertEquals(TASK_CONCURRENCY * eligible.size(), ComputeService.externalCoalesceFloor(TASK_CONCURRENCY, mixed));

        assertDistributedTo(mixed, eligible);
    }

    public void testIndexOnlyFloorIsCoordinatorWidthAndDispatchIsLocal() {
        DiscoveryNodes indexOnly = DiscoveryNodes.builder().add(node("index-1", INDEX_ROLE)).build();
        assertTrue(eligibleIds(indexOnly).isEmpty());
        assertEquals(TASK_CONCURRENCY, ComputeService.externalCoalesceFloor(TASK_CONCURRENCY, indexOnly));

        List<ExternalSplit> splits = splits(6);
        PhysicalPlan plan = reducingPlan();
        for (ExternalDistributionStrategy strategy : strategies()) {
            ExternalDistributionPlan local = strategy.planDistribution(context(plan, splits, indexOnly));
            assertFalse(strategy.getClass().getSimpleName(), local.distributed());
            assertTrue(local.nodeAssignments().isEmpty());
        }
    }

    private static List<ExternalDistributionStrategy> strategies() {
        return List.of(new AdaptiveStrategy(), new RoundRobinStrategy(), new WeightedRoundRobinStrategy());
    }

    private static void assertDistributedTo(DiscoveryNodes available, Set<String> expectedNodeIds) {
        List<ExternalSplit> splits = splits(6);
        PhysicalPlan plan = reducingPlan();
        for (ExternalDistributionStrategy strategy : strategies()) {
            ExternalDistributionPlan distributed = strategy.planDistribution(context(plan, splits, available));
            assertTrue(strategy.getClass().getSimpleName(), distributed.distributed());
            assertEquals(strategy.getClass().getSimpleName(), expectedNodeIds, distributed.nodeAssignments().keySet());
        }
    }

    private static DiscoveryNode node(String id, DiscoveryNodeRole role) {
        return DiscoveryNodeUtils.builder(id).roles(Set.of(role)).build();
    }

    private static Set<String> eligibleIds(DiscoveryNodes nodes) {
        return NodeEligibilityStrategy.EXTERNAL_WORKER_NODES.eligibleNodes(nodes)
            .stream()
            .map(DiscoveryNode::getId)
            .collect(Collectors.toSet());
    }

    private static ExternalDistributionContext context(PhysicalPlan plan, List<ExternalSplit> splits, DiscoveryNodes nodes) {
        return new ExternalDistributionContext(plan, splits, nodes, QueryPragmas.EMPTY);
    }

    private static PhysicalPlan reducingPlan() {
        return new AggregateExec(
            Source.EMPTY,
            new ExternalSourceExec(Source.EMPTY, "s3://bucket/data/*.parquet", "parquet", List.of(), Map.of(), Map.of(), null),
            List.of(),
            List.of(),
            AggregatorMode.SINGLE,
            List.of(),
            null
        );
    }

    private static List<ExternalSplit> splits(int count) {
        List<ExternalSplit> splits = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            splits.add(
                new FileSplit("parquet", StoragePath.of("s3://bucket/file" + i + ".parquet"), 0, 1024, ".parquet", Map.of(), Map.of())
            );
        }
        return splits;
    }
}
