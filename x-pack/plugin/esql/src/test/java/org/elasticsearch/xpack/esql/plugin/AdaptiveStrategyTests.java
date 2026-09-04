/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INDEX_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.SEARCH_ROLE;

public class AdaptiveStrategyTests extends ESTestCase {

    private final AdaptiveStrategy strategy = new AdaptiveStrategy();

    public void testSingleSplitReturnsCoordinator() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            createExternalSourceExec(),
            createSplits(1),
            createNodes(3),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse(plan.distributed());
    }

    public void testEmptySplitsReturnsCoordinator() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            createExternalSourceExec(),
            List.of(),
            createNodes(3),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse(plan.distributed());
    }

    public void testAggregationWithMultipleSplitsDistributes() {
        PhysicalPlan source = createExternalSourceExec();
        PhysicalPlan planWithAgg = new AggregateExec(Source.EMPTY, source, List.of(), List.of(), AggregatorMode.SINGLE, List.of(), null);

        ExternalDistributionContext context = new ExternalDistributionContext(
            planWithAgg,
            createSplits(4),
            createNodes(2),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        assertEquals(2, plan.nodeAssignments().size());
    }

    public void testLimitOnlyReturnsCoordinator() {
        PhysicalPlan source = createExternalSourceExec();
        Literal limitExpr = new Literal(Source.EMPTY, 10, DataType.INTEGER);
        PhysicalPlan planWithLimit = new LimitExec(Source.EMPTY, source, limitExpr, null);

        ExternalDistributionContext context = new ExternalDistributionContext(
            planWithLimit,
            createSplits(5),
            createNodes(3),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse(plan.distributed());
    }

    public void testTopNWithMultipleSplitsDistributes() {
        PhysicalPlan source = createExternalSourceExec();
        Literal limitExpr = new Literal(Source.EMPTY, 10, DataType.INTEGER);
        Order order = new Order(Source.EMPTY, limitExpr, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        PhysicalPlan planWithTopN = new TopNExec(Source.EMPTY, source, List.of(order), limitExpr, null);

        ExternalDistributionContext context = new ExternalDistributionContext(
            planWithTopN,
            createSplits(4),
            createNodes(2),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        assertEquals(2, plan.nodeAssignments().size());
    }

    public void testManySplitsNoAggregationDistributes() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            createExternalSourceExec(),
            createSplits(10),
            createNodes(3),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        assertEquals(3, plan.nodeAssignments().size());
    }

    public void testFewSplitsNoAggregationStaysLocal() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            createExternalSourceExec(),
            createSplits(2),
            createNodes(3),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse(plan.distributed());
    }

    public void testNoEligibleNodesReturnsLocal() {
        PhysicalPlan source = createExternalSourceExec();
        PhysicalPlan planWithAgg = new AggregateExec(Source.EMPTY, source, List.of(), List.of(), AggregatorMode.SINGLE, List.of(), null);

        ExternalDistributionContext context = new ExternalDistributionContext(
            planWithAgg,
            createSplits(5),
            DiscoveryNodes.builder().build(),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse(plan.distributed());
    }

    public void testDistributionIsEvenRoundRobin() {
        PhysicalPlan source = createExternalSourceExec();
        PhysicalPlan planWithAgg = new AggregateExec(Source.EMPTY, source, List.of(), List.of(), AggregatorMode.SINGLE, List.of(), null);

        ExternalDistributionContext context = new ExternalDistributionContext(
            planWithAgg,
            createSplits(6),
            createNodes(3),
            QueryPragmas.EMPTY
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        for (List<ExternalSplit> assigned : plan.nodeAssignments().values()) {
            assertEquals(2, assigned.size());
        }
    }

    /**
     * A lone split stays put only while it is the query's only read. The four cases below fix where that boundary is:
     * the sibling count flips the decision, the LIMIT-only and no-pipeline-breaker rules still override it, and the
     * end result is that a query of many one-split producers spreads instead of piling onto the coordinator.
     */
    public void testSingleSplitAsOnlyProducerStaysLocal() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            createPlanWithAggregation(),
            createSplits(1),
            createNodes(3),
            QueryPragmas.EMPTY,
            0,
            1
        );

        assertFalse(strategy.planDistribution(context).distributed());
    }

    public void testSingleSplitWithSiblingProducersDistributes() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            createPlanWithAggregation(),
            createSplits(1),
            createNodes(3),
            QueryPragmas.EMPTY,
            0,
            100
        );

        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue(plan.distributed());
        assertEquals(1, totalAssigned(plan));
    }

    /** A LIMIT-only plan reads little regardless of how many producers there are, so the hop still is not worth it. */
    public void testSingleSplitWithSiblingProducersStaysLocalForLimitOnly() {
        Literal limitExpr = new Literal(Source.EMPTY, 10, DataType.INTEGER);
        PhysicalPlan planWithLimit = new LimitExec(Source.EMPTY, createExternalSourceExec(), limitExpr, null);

        ExternalDistributionContext context = new ExternalDistributionContext(
            planWithLimit,
            createSplits(1),
            createNodes(3),
            QueryPragmas.EMPTY,
            0,
            100
        );

        assertFalse(strategy.planDistribution(context).distributed());
    }

    /**
     * A fan-in producer reaches the decision unresolved, carrying its pushed-down aggregation as a logical plan inside
     * a fragment. Judging reduction by the physical nodes alone would find none here, which is the shape every
     * many-dataset query is built from, so the fragment has to be looked into.
     */
    public void testSingleSplitWithSiblingProducersDistributesWhenAggregateIsInsideFragment() {
        LogicalPlan aggregateInFragment = new Aggregate(Source.EMPTY, EsqlTestUtils.emptySource(), List.of(), List.of());

        ExternalDistributionContext context = new ExternalDistributionContext(
            new FragmentExec(aggregateInFragment),
            createSplits(1),
            createNodes(3),
            QueryPragmas.EMPTY,
            0,
            6
        );

        assertTrue(strategy.planDistribution(context).distributed());
    }

    /** Looking into the fragment must find a reduction, not merely a fragment. */
    public void testSingleSplitWithSiblingProducersStaysLocalWhenFragmentHoldsNoReduction() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            new FragmentExec(EsqlTestUtils.emptySource()),
            createSplits(1),
            createNodes(3),
            QueryPragmas.EMPTY,
            0,
            6
        );

        assertFalse(strategy.planDistribution(context).distributed());
    }

    /** Having siblings is not on its own a reason to distribute: the existing placement rules still have to say yes. */
    public void testSingleSplitWithSiblingProducersStaysLocalWithoutPipelineBreaker() {
        ExternalDistributionContext context = new ExternalDistributionContext(
            createExternalSourceExec(),
            createSplits(1),
            createNodes(3),
            QueryPragmas.EMPTY,
            0,
            100
        );

        assertFalse(strategy.planDistribution(context).distributed());
    }

    /** The outcome the change is for: six one-split producers over three nodes read two splits per node, not six on one. */
    public void testOneSplitProducersSpreadOverNodesInsteadOfStackingOnCoordinator() {
        DiscoveryNodes nodes = createNodes(3);
        Map<String, Integer> perNode = new LinkedHashMap<>();

        for (int producer = 0; producer < 6; producer++) {
            ExternalDistributionContext context = new ExternalDistributionContext(
                createPlanWithAggregation(),
                createSplits(1),
                nodes,
                QueryPragmas.EMPTY,
                producer,
                6
            );
            ExternalDistributionPlan plan = strategy.planDistribution(context);
            assertTrue("producer " + producer, plan.distributed());
            for (var entry : plan.nodeAssignments().entrySet()) {
                perNode.merge(entry.getKey(), entry.getValue().size(), Integer::sum);
            }
        }

        assertEquals(List.of(2, 2, 2), List.copyOf(perNode.values()));
    }

    private static int totalAssigned(ExternalDistributionPlan plan) {
        int total = 0;
        for (List<ExternalSplit> assigned : plan.nodeAssignments().values()) {
            total += assigned.size();
        }
        return total;
    }

    private static PhysicalPlan createPlanWithAggregation() {
        return new AggregateExec(Source.EMPTY, createExternalSourceExec(), List.of(), List.of(), AggregatorMode.SINGLE, List.of(), null);
    }

    public void testIndexCoordinatorAssignsDistributableScanToSearchWorker() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("search-1").roles(Set.of(SEARCH_ROLE)).build())
            .build();
        PhysicalPlan planWithAgg = new AggregateExec(
            Source.EMPTY,
            createExternalSourceExec(),
            List.of(),
            List.of(),
            AggregatorMode.SINGLE,
            List.of(),
            null
        );

        ExternalDistributionPlan plan = strategy.planDistribution(
            new ExternalDistributionContext(planWithAgg, createSplits(4), nodes, QueryPragmas.EMPTY)
        );

        assertTrue(plan.distributed());
        assertEquals(Set.of("search-1"), plan.nodeAssignments().keySet());
        assertEquals(4, plan.nodeAssignments().get("search-1").size());
    }

    public void testIndexOnlyClusterReturnsLocal() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build())
            .build();
        PhysicalPlan planWithAgg = new AggregateExec(
            Source.EMPTY,
            createExternalSourceExec(),
            List.of(),
            List.of(),
            AggregatorMode.SINGLE,
            List.of(),
            null
        );

        ExternalDistributionPlan plan = strategy.planDistribution(
            new ExternalDistributionContext(planWithAgg, createSplits(5), nodes, QueryPragmas.EMPTY)
        );

        assertFalse(plan.distributed());
        assertTrue(plan.nodeAssignments().isEmpty());
    }

    public void testLocalShapesStayLocalEvenWhenSearchWorkerExists() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("search-1").roles(Set.of(SEARCH_ROLE)).build())
            .build();

        ExternalDistributionPlan singleSplit = strategy.planDistribution(
            new ExternalDistributionContext(createExternalSourceExec(), createSplits(1), nodes, QueryPragmas.EMPTY)
        );
        assertFalse(singleSplit.distributed());

        PhysicalPlan limitOnly = new LimitExec(
            Source.EMPTY,
            createExternalSourceExec(),
            new Literal(Source.EMPTY, 10, DataType.INTEGER),
            null
        );
        ExternalDistributionPlan limitPlan = strategy.planDistribution(
            new ExternalDistributionContext(limitOnly, createSplits(5), nodes, QueryPragmas.EMPTY)
        );
        assertFalse(limitPlan.distributed());

        // One search worker: few splits with no pipeline breaker stay local (2 <= 1 is false, so
        // manySplits is true only when splits > eligible workers). With 1 search worker, 2 splits
        // would distribute; use more search workers so the few-split path stays LOCAL.
        DiscoveryNodes manySearch = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("search-1").roles(Set.of(SEARCH_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("search-2").roles(Set.of(SEARCH_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("search-3").roles(Set.of(SEARCH_ROLE)).build())
            .build();
        ExternalDistributionPlan fewSplits = strategy.planDistribution(
            new ExternalDistributionContext(createExternalSourceExec(), createSplits(2), manySearch, QueryPragmas.EMPTY)
        );
        assertFalse(fewSplits.distributed());
    }

    public void testAdaptiveThresholdUsesEligibleWorkersOnly() {
        // 2 index + 1 search: eligible count is 1, so 2 splits with no breaker is manySplits and distributes.
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("index-2").roles(Set.of(INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("search-1").roles(Set.of(SEARCH_ROLE)).build())
            .build();

        ExternalDistributionPlan plan = strategy.planDistribution(
            new ExternalDistributionContext(createExternalSourceExec(), createSplits(2), nodes, QueryPragmas.EMPTY)
        );

        assertTrue(plan.distributed());
        assertEquals(Set.of("search-1"), plan.nodeAssignments().keySet());
    }

    public void testAssignmentsNeverReferenceIndexNode() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("search-1").roles(Set.of(SEARCH_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("data-1").roles(Set.of(DATA_HOT_NODE_ROLE)).build())
            .build();
        PhysicalPlan planWithAgg = new AggregateExec(
            Source.EMPTY,
            createExternalSourceExec(),
            List.of(),
            List.of(),
            AggregatorMode.SINGLE,
            List.of(),
            null
        );

        ExternalDistributionPlan plan = strategy.planDistribution(
            new ExternalDistributionContext(planWithAgg, createSplits(6), nodes, QueryPragmas.EMPTY)
        );

        assertTrue(plan.distributed());
        assertFalse(plan.nodeAssignments().containsKey("index-1"));
        assertEquals(Set.of("search-1", "data-1"), plan.nodeAssignments().keySet());
    }

    private static ExternalSourceExec createExternalSourceExec() {
        return new ExternalSourceExec(Source.EMPTY, "s3://bucket/data/*.parquet", "parquet", List.of(), Map.of(), Map.of(), null);
    }

    private static List<ExternalSplit> createSplits(int count) {
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add(
                new FileSplit("parquet", StoragePath.of("s3://bucket/file" + i + ".parquet"), 0, 1024, ".parquet", Map.of(), Map.of())
            );
        }
        return splits;
    }

    private static DiscoveryNodes createNodes(int count) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < count; i++) {
            builder.add(DiscoveryNodeUtils.builder("node-" + i).roles(Set.of(DATA_HOT_NODE_ROLE)).build());
        }
        return builder.build();
    }
}
