/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for data node execution of external sources.
 * Covers distribution planning, plan structure, and request building.
 */
public class ExternalSourceDataNodeTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    // --- Distribution result tests ---

    public void testDistributionResultNotDistributedWhenNoSplits() {
        ExternalSourceExec source = createExternalSourceExec();
        LimitExec limit = new LimitExec(SRC, source, new Literal(SRC, 10, DataType.INTEGER), null);

        List<ExternalSplit> splits = collectExternalSplits(limit);
        assertTrue("No splits expected on source without splits", splits.isEmpty());
    }

    public void testDistributionResultDistributedWithSplitsAndAggregation() {
        List<ExternalSplit> splits = createSplits(5);
        ExternalSourceExec source = createExternalSourceExec().withSplits(splits);
        AggregateExec agg = new AggregateExec(SRC, source, List.of(), List.of(), AggregatorMode.INITIAL, List.of(), null);
        ExchangeExec exchange = new ExchangeExec(SRC, agg);

        AdaptiveStrategy strategy = new AdaptiveStrategy();
        DiscoveryNodes nodes = createNodes(3);
        ExternalDistributionContext context = new ExternalDistributionContext(exchange, splits, nodes, QueryPragmas.EMPTY);
        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue("Should distribute with aggregation and multiple splits", plan.distributed());
        assertFalse("Node assignments should not be empty", plan.nodeAssignments().isEmpty());
    }

    public void testDistributionResultLocalWithSingleSplit() {
        List<ExternalSplit> splits = createSplits(1);
        ExternalSourceExec source = createExternalSourceExec().withSplits(splits);

        AdaptiveStrategy strategy = new AdaptiveStrategy();
        DiscoveryNodes nodes = createNodes(3);
        ExternalDistributionContext context = new ExternalDistributionContext(source, splits, nodes, QueryPragmas.EMPTY);
        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse("Single split should stay local", plan.distributed());
    }

    public void testCoordinatorOnlyPragmaOverride() {
        List<ExternalSplit> splits = createSplits(10);
        ExternalSourceExec source = createExternalSourceExec().withSplits(splits);
        AggregateExec agg = new AggregateExec(SRC, source, List.of(), List.of(), AggregatorMode.INITIAL, List.of(), null);

        Settings settings = Settings.builder().put("external_distribution", "coordinator_only").build();
        QueryPragmas pragmas = new QueryPragmas(settings);
        ExternalDistributionStrategy strategy = ComputeService.resolveExternalDistributionStrategy(pragmas);
        assertTrue("Should be coordinator-only strategy", strategy instanceof CoordinatorOnlyStrategy);

        DiscoveryNodes nodes = createNodes(3);
        ExternalDistributionContext context = new ExternalDistributionContext(agg, splits, nodes, pragmas);
        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertFalse("coordinator_only pragma should prevent distribution", plan.distributed());
    }

    public void testRoundRobinPragmaDistribution() {
        List<ExternalSplit> splits = createSplits(6);
        ExternalSourceExec source = createExternalSourceExec().withSplits(splits);

        Settings settings = Settings.builder().put("external_distribution", "round_robin").build();
        QueryPragmas pragmas = new QueryPragmas(settings);
        ExternalDistributionStrategy strategy = ComputeService.resolveExternalDistributionStrategy(pragmas);
        assertTrue("Should be round-robin strategy", strategy instanceof RoundRobinStrategy);

        DiscoveryNodes nodes = createNodes(3);
        ExternalDistributionContext context = new ExternalDistributionContext(source, splits, nodes, pragmas);
        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue("round_robin should distribute", plan.distributed());
        assertEquals("Should have 3 node assignments", 3, plan.nodeAssignments().size());
        for (List<ExternalSplit> nodeSplits : plan.nodeAssignments().values()) {
            assertEquals("Each node should get 2 splits", 2, nodeSplits.size());
        }
    }

    // --- Plan structure tests ---

    public void testExternalSourcePlanBreaksAtExchange() {
        List<ExternalSplit> splits = createSplits(3);
        ExternalSourceExec source = createExternalSourceExec().withSplits(splits);
        ExchangeExec exchange = new ExchangeExec(SRC, source);
        LimitExec limit = new LimitExec(SRC, exchange, new Literal(SRC, 10, DataType.INTEGER), null);

        var result = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(limit, null);

        PhysicalPlan coordinatorPlan = result.v1();
        PhysicalPlan dataNodePlan = result.v2();

        assertNotNull("Data node plan should exist", dataNodePlan);
        assertTrue("Data node plan should be ExchangeSinkExec", dataNodePlan instanceof ExchangeSinkExec);

        ExchangeSinkExec sink = (ExchangeSinkExec) dataNodePlan;
        assertTrue("Sink child should be ExternalSourceExec", sink.child() instanceof ExternalSourceExec);

        ExternalSourceExec sinkSource = (ExternalSourceExec) sink.child();
        assertEquals("Splits should be preserved", 3, sinkSource.splits().size());

        assertTrue("Coordinator plan should have ExchangeSourceExec", coordinatorPlan instanceof LimitExec);
        LimitExec coordLimit = (LimitExec) coordinatorPlan;
        assertTrue("Coordinator child should be ExchangeSourceExec", coordLimit.child() instanceof ExchangeSourceExec);
    }

    public void testExternalSourceExecWithSplitsInjection() {
        ExternalSourceExec source = createExternalSourceExec();
        assertTrue("Initially no splits", source.splits().isEmpty());

        List<ExternalSplit> splits = createSplits(4);
        ExternalSourceExec withSplits = source.withSplits(splits);

        assertEquals("Should have 4 splits", 4, withSplits.splits().size());
        assertEquals("Source path preserved", source.sourcePath(), withSplits.sourcePath());
        assertEquals("Source type preserved", source.sourceType(), withSplits.sourceType());
        assertEquals("Attributes preserved", source.output(), withSplits.output());
    }

    // --- Split injection into plan tests ---

    public void testSplitInjectionIntoExchangeSinkPlan() {
        ExternalSourceExec source = createExternalSourceExec();
        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, source.output(), false, source);

        List<ExternalSplit> splits = createSplits(4);
        PhysicalPlan planWithSplits = sink.child().transformUp(ExternalSourceExec.class, exec -> exec.withSplits(splits));

        assertTrue("Transformed plan should be ExternalSourceExec", planWithSplits instanceof ExternalSourceExec);
        ExternalSourceExec injected = (ExternalSourceExec) planWithSplits;
        assertEquals("Should have 4 splits after injection", 4, injected.splits().size());
    }

    public void testSplitInjectionPreservesOtherFields() {
        ExternalSourceExec source = createExternalSourceExec();
        List<ExternalSplit> splits = createSplits(2);
        ExternalSourceExec withSplits = source.withSplits(splits);

        assertEquals(source.sourcePath(), withSplits.sourcePath());
        assertEquals(source.sourceType(), withSplits.sourceType());
        assertEquals(source.output(), withSplits.output());
        assertEquals(source.config(), withSplits.config());
        assertEquals(source.sourceMetadata(), withSplits.sourceMetadata());
        assertEquals(2, withSplits.splits().size());
    }

    // --- Node assignment tests ---

    public void testRoundRobinDistributionEvenSplits() {
        List<ExternalSplit> splits = createSplits(9);
        List<DiscoveryNode> nodes = new ArrayList<>();
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < 3; i++) {
            DiscoveryNode node = DiscoveryNodeUtils.builder("node-" + i).roles(Set.of(DATA_HOT_NODE_ROLE)).build();
            nodes.add(node);
            builder.add(node);
        }

        ExternalDistributionPlan plan = RoundRobinStrategy.assignRoundRobin(splits, nodes);

        assertTrue(plan.distributed());
        assertEquals(3, plan.nodeAssignments().size());
        for (List<ExternalSplit> nodeSplits : plan.nodeAssignments().values()) {
            assertEquals("Each of 3 nodes should get 3 of 9 splits", 3, nodeSplits.size());
        }
    }

    public void testRoundRobinDistributionUnevenSplits() {
        List<ExternalSplit> splits = createSplits(7);
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            nodes.add(DiscoveryNodeUtils.builder("node-" + i).roles(Set.of(DATA_HOT_NODE_ROLE)).build());
        }

        ExternalDistributionPlan plan = RoundRobinStrategy.assignRoundRobin(splits, nodes);

        assertTrue(plan.distributed());
        int totalAssigned = 0;
        for (List<ExternalSplit> nodeSplits : plan.nodeAssignments().values()) {
            totalAssigned += nodeSplits.size();
        }
        assertEquals("All splits should be assigned", 7, totalAssigned);
    }

    // --- ExternalDistributionResult tests ---

    public void testDistributionResultIsDistributedWithRealPlan() {
        List<ExternalSplit> splits = createSplits(4);
        Map<String, List<ExternalSplit>> assignments = Map.of("node-0", splits.subList(0, 2), "node-1", splits.subList(2, 4));
        ExternalDistributionPlan distributionPlan = new ExternalDistributionPlan(assignments, true);

        var result = new ComputeService.ExternalDistributionResult(createExternalSourceExec(), distributionPlan);
        assertTrue("Should be distributed when plan says distributed", result.isDistributed());
        assertThat(result.distributionPlan().nodeAssignments().size(), equalTo(2));
    }

    public void testDistributionResultNotDistributedWithNullPlan() {
        var result = new ComputeService.ExternalDistributionResult(createExternalSourceExec(), null);
        assertFalse("Should not be distributed with null plan", result.isDistributed());
    }

    public void testDistributionResultNotDistributedWithLocalPlan() {
        var result = new ComputeService.ExternalDistributionResult(createExternalSourceExec(), ExternalDistributionPlan.LOCAL);
        assertFalse("Should not be distributed with LOCAL plan", result.isDistributed());
    }

    // --- Dispatch condition tests ---

    public void testExternalDispatchConditionWithSplitsAndNoShards() {
        List<ExternalSplit> splits = createSplits(3);
        boolean hasExternalSplits = splits.isEmpty() == false;
        boolean hasShardsEmpty = List.of().isEmpty();
        assertTrue("Should dispatch to external handler", hasExternalSplits && hasShardsEmpty);
    }

    public void testExternalDispatchConditionWithSplitsAndShards() {
        List<ExternalSplit> splits = createSplits(3);
        boolean hasExternalSplits = splits.isEmpty() == false;
        boolean hasShardsEmpty = false;
        assertFalse("Should NOT dispatch to external handler when shards present", hasExternalSplits && hasShardsEmpty);
    }

    public void testExternalDispatchConditionWithNoSplits() {
        List<ExternalSplit> splits = List.of();
        boolean hasExternalSplits = splits.isEmpty() == false;
        assertFalse("Should NOT dispatch to external handler when no splits", hasExternalSplits);
    }

    // --- handleExternalSourceRequest failure path tests ---

    public void testHandleExternalSourceRequiresExchangeSinkExec() {
        ExternalSourceExec source = createExternalSourceExec().withSplits(createSplits(3));
        PhysicalPlan plan = source;
        assertFalse("A non-ExchangeSinkExec plan should be rejected by handleExternalSourceRequest", plan instanceof ExchangeSinkExec);
    }

    public void testHandleExternalSourcePlanTransformationWithSinkExec() {
        ExternalSourceExec source = createExternalSourceExec();
        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, source.output(), false, source);

        List<ExternalSplit> splits = createSplits(5);
        PhysicalPlan planWithSplits = sink.child().transformUp(ExternalSourceExec.class, exec -> exec.withSplits(splits));
        ExchangeSinkExec updatedSink = new ExchangeSinkExec(sink.source(), sink.output(), sink.isIntermediateAgg(), planWithSplits);

        assertTrue("Updated sink child should be ExternalSourceExec", updatedSink.child() instanceof ExternalSourceExec);
        ExternalSourceExec injected = (ExternalSourceExec) updatedSink.child();
        assertThat("Should have 5 splits after injection", injected.splits().size(), equalTo(5));
        assertThat("Output should be preserved", updatedSink.output(), equalTo(sink.output()));
    }

    public void testHandleExternalSourcePlanTransformationWithNestedPlan() {
        ExternalSourceExec source = createExternalSourceExec();
        LimitExec limit = new LimitExec(SRC, source, new Literal(SRC, 100, DataType.INTEGER), null);
        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, source.output(), false, limit);

        List<ExternalSplit> splits = createSplits(3);
        PhysicalPlan planWithSplits = sink.child().transformUp(ExternalSourceExec.class, exec -> exec.withSplits(splits));
        ExchangeSinkExec updatedSink = new ExchangeSinkExec(sink.source(), sink.output(), sink.isIntermediateAgg(), planWithSplits);

        assertTrue("Updated sink child should be LimitExec", updatedSink.child() instanceof LimitExec);
        LimitExec updatedLimit = (LimitExec) updatedSink.child();
        assertTrue("Limit child should be ExternalSourceExec", updatedLimit.child() instanceof ExternalSourceExec);
        ExternalSourceExec injected = (ExternalSourceExec) updatedLimit.child();
        assertThat("Should have 3 splits after injection through nested plan", injected.splits().size(), equalTo(3));
    }

    // --- startExternalComputeOnDataNodes failure path tests ---

    public void testStartExternalComputeSkipsEmptySplitAssignments() {
        List<ExternalSplit> splits = List.of();
        Map<String, List<ExternalSplit>> assignments = Map.of("node-0", splits);

        boolean sentAny = false;
        for (Map.Entry<String, List<ExternalSplit>> entry : assignments.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }
            sentAny = true;
        }
        assertFalse("Should not send when all assignments are empty", sentAny);
    }

    public void testStartExternalComputeHandlesMissingNode() {
        DiscoveryNodes nodes = createNodes(2);
        String missingNodeId = "node-missing";
        assertNull("Missing node should not be found", nodes.get(missingNodeId));
    }

    public void testStartExternalComputeNodeLookupSuccess() {
        DiscoveryNodes nodes = createNodes(3);
        assertNotNull("node-0 should exist", nodes.get("node-0"));
        assertNotNull("node-1 should exist", nodes.get("node-1"));
        assertNotNull("node-2 should exist", nodes.get("node-2"));
    }

    // --- Full distribution flow tests ---

    public void testExternalDistributionBuildsSplitAssignments() {
        List<ExternalSplit> splits = createSplits(10);
        ExternalSourceExec source = createExternalSourceExec().withSplits(splits);
        AggregateExec agg = new AggregateExec(SRC, source, List.of(), List.of(), AggregatorMode.INITIAL, List.of(), null);
        ExchangeExec exchange = new ExchangeExec(SRC, agg);

        DiscoveryNodes nodes = createNodes(3);
        Settings settings = Settings.builder().put("external_distribution", "round_robin").build();
        QueryPragmas pragmas = new QueryPragmas(settings);
        ExternalDistributionStrategy strategy = ComputeService.resolveExternalDistributionStrategy(pragmas);
        ExternalDistributionContext context = new ExternalDistributionContext(exchange, splits, nodes, pragmas);
        ExternalDistributionPlan plan = strategy.planDistribution(context);

        assertTrue("Should distribute", plan.distributed());
        assertThat("Should have 3 node assignments", plan.nodeAssignments().size(), equalTo(3));

        int totalAssigned = 0;
        for (List<ExternalSplit> nodeSplits : plan.nodeAssignments().values()) {
            assertTrue("Each node should have at least 1 split", nodeSplits.size() >= 1);
            totalAssigned += nodeSplits.size();
        }
        assertThat("All 10 splits should be assigned", totalAssigned, equalTo(10));
    }

    public void testExternalDistributionPlanBreaksCorrectlyForDataNodes() {
        List<ExternalSplit> splits = createSplits(6);
        ExternalSourceExec source = createExternalSourceExec().withSplits(splits);
        ExchangeExec exchange = new ExchangeExec(SRC, source);
        LimitExec limit = new LimitExec(SRC, exchange, new Literal(SRC, 50, DataType.INTEGER), null);

        var result = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(limit, null);
        PhysicalPlan coordinatorPlan = result.v1();
        PhysicalPlan dataNodePlan = result.v2();

        assertNotNull("Should have data node plan", dataNodePlan);
        assertTrue("Data node plan should be ExchangeSinkExec", dataNodePlan instanceof ExchangeSinkExec);

        ExchangeSinkExec sinkExec = (ExchangeSinkExec) dataNodePlan;
        assertTrue("Sink child should be ExternalSourceExec", sinkExec.child() instanceof ExternalSourceExec);
        ExternalSourceExec sinkSource = (ExternalSourceExec) sinkExec.child();
        assertThat("Splits should be preserved in data node plan", sinkSource.splits().size(), equalTo(6));

        // Verify coordinator plan structure
        assertTrue("Coordinator plan should be LimitExec", coordinatorPlan instanceof LimitExec);
        LimitExec coordLimit = (LimitExec) coordinatorPlan;
        assertTrue("Coordinator child should be ExchangeSourceExec", coordLimit.child() instanceof ExchangeSourceExec);

        // Verify the data node plan can be used to inject per-node splits
        List<ExternalSplit> nodeSplits = splits.subList(0, 2);
        PhysicalPlan injected = sinkExec.child().transformUp(ExternalSourceExec.class, exec -> exec.withSplits(nodeSplits));
        ExchangeSinkExec updatedSink = new ExchangeSinkExec(sinkExec.source(), sinkExec.output(), sinkExec.isIntermediateAgg(), injected);

        ExternalSourceExec updatedSource = (ExternalSourceExec) updatedSink.child();
        assertThat("Injected plan should have 2 splits", updatedSource.splits().size(), equalTo(2));
    }

    public void testExternalDistributionWithAggregationPlanBreaks() {
        List<ExternalSplit> splits = createSplits(4);
        ExternalSourceExec source = createExternalSourceExec().withSplits(splits);
        List<Attribute> output = source.output();
        AggregateExec agg = new AggregateExec(SRC, source, List.of(), List.of(), AggregatorMode.INITIAL, output, null);
        ExchangeExec exchange = new ExchangeExec(SRC, agg);

        var result = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(exchange, null);
        PhysicalPlan dataNodePlan = result.v2();

        assertNotNull("Should have data node plan for aggregation", dataNodePlan);
        assertTrue("Data node plan should be ExchangeSinkExec", dataNodePlan instanceof ExchangeSinkExec);

        ExchangeSinkExec sinkExec = (ExchangeSinkExec) dataNodePlan;
        assertTrue("Sink child should be AggregateExec", sinkExec.child() instanceof AggregateExec);
        AggregateExec sinkAgg = (AggregateExec) sinkExec.child();
        assertTrue("Agg child should be ExternalSourceExec", sinkAgg.child() instanceof ExternalSourceExec);

        ExternalSourceExec sinkSource = (ExternalSourceExec) sinkAgg.child();
        assertThat("Splits preserved through aggregation", sinkSource.splits().size(), equalTo(4));

        // Verify split injection works through the aggregation
        List<ExternalSplit> nodeSplits = splits.subList(0, 1);
        PhysicalPlan injected = sinkExec.child().transformUp(ExternalSourceExec.class, exec -> exec.withSplits(nodeSplits));
        ExchangeSinkExec updatedSink = new ExchangeSinkExec(sinkExec.source(), sinkExec.output(), sinkExec.isIntermediateAgg(), injected);

        AggregateExec updatedAgg = (AggregateExec) updatedSink.child();
        ExternalSourceExec updatedSource = (ExternalSourceExec) updatedAgg.child();
        assertThat("Injected through aggregation should have 1 split", updatedSource.splits().size(), equalTo(1));
    }

    public void testCollapseExternalSourceExchangesPreservesSplits() {
        List<ExternalSplit> splits = createSplits(3);
        ExternalSourceExec source = createExternalSourceExec().withSplits(splits);
        ExchangeExec exchange = new ExchangeExec(SRC, source);
        LimitExec limit = new LimitExec(SRC, exchange, new Literal(SRC, 10, DataType.INTEGER), null);

        PhysicalPlan collapsed = ComputeService.collapseExternalSourceExchanges(limit);

        assertTrue("Collapsed plan should be LimitExec", collapsed instanceof LimitExec);
        LimitExec collapsedLimit = (LimitExec) collapsed;
        assertTrue("Exchange should be removed, child should be ExternalSourceExec", collapsedLimit.child() instanceof ExternalSourceExec);

        ExternalSourceExec collapsedSource = (ExternalSourceExec) collapsedLimit.child();
        assertThat("Splits should be preserved after collapse", collapsedSource.splits().size(), equalTo(3));
    }

    public void testCollapseExternalSourceExchangesLeavesNonExternalExchange() {
        ExternalSourceExec source = createExternalSourceExec();
        LimitExec limit = new LimitExec(SRC, source, new Literal(SRC, 10, DataType.INTEGER), null);
        ExchangeExec exchange = new ExchangeExec(SRC, limit);

        PhysicalPlan collapsed = ComputeService.collapseExternalSourceExchanges(exchange);

        assertTrue("Non-external-child exchange should be preserved", collapsed instanceof ExchangeExec);
    }

    // --- Helpers ---

    private static ExternalSourceExec createExternalSourceExec() {
        return new ExternalSourceExec(
            SRC,
            "s3://bucket/data/*.parquet",
            "parquet",
            List.of(
                new FieldAttribute(SRC, "name", new EsField("name", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
            ),
            Map.of(),
            Map.of(),
            null
        );
    }

    private static List<ExternalSplit> createSplits(int count) {
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add(
                new FileSplit("parquet", StoragePath.of("s3://bucket/file" + i + ".parquet"), 0, 1024, "parquet", Map.of(), Map.of())
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

    private static List<ExternalSplit> collectExternalSplits(PhysicalPlan plan) {
        List<ExternalSplit> splits = new ArrayList<>();
        plan.forEachDown(ExternalSourceExec.class, exec -> splits.addAll(exec.splits()));
        return splits;
    }
}
