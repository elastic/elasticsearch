/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.List;

/**
 * Tests for the plan-splitting logic used by partitioned coordinator execution.
 * <p>
 * {@link ComputeService#hasGroupedFinalAgg} detects whether a plan is eligible for partitioning.
 * {@link ComputeService#splitCoordinatorPlanForPartitioning} splits it into a per-partition
 * "final plan" and a global "output plan". These tests verify the split produces the correct
 * plan structure for different plan shapes (with and without TopN).
 */
public class ComputeServiceTests extends ESTestCase {

    /**
     * Tests that splitCoordinatorPlanForPartitioning correctly splits the plan
     * so that TopN is pushed into the per-partition final plan (partial TopN)
     * AND kept in the output plan (global merge TopN).
     *
     * The input plan tree is:
     * <pre>
     *   OutputExec
     *     └── ProjectExec
     *           └── TopNExec
     *                 └── AggregateExec(FINAL)
     *                       └── ExchangeSourceExec
     * </pre>
     *
     * Expected split:
     *   - Final plan: ExchangeSinkExec → TopNExec → AggregateExec(FINAL) → ExchangeSourceExec
     *   - Output plan: OutputExec → ProjectExec → TopNExec → ExchangeSourceExec
     */
    public void testSplitCoordinatorPlanPutsTopNInBothPlans() {
        // Build attributes
        Attribute groupAttr = new ReferenceAttribute(Source.EMPTY, "group", DataType.KEYWORD);
        Attribute countAttr = new ReferenceAttribute(Source.EMPTY, "c", DataType.LONG);
        List<Attribute> aggOutputAttrs = List.of(countAttr, groupAttr);
        List<Attribute> intermediateAttrs = List.of(
            new ReferenceAttribute(Source.EMPTY, "c$count", DataType.LONG),
            new ReferenceAttribute(Source.EMPTY, "c$seen", DataType.BOOLEAN),
            new ReferenceAttribute(Source.EMPTY, "group", DataType.KEYWORD)
        );

        // ExchangeSourceExec (bottom of the tree)
        ExchangeSourceExec exchangeSource = new ExchangeSourceExec(Source.EMPTY, intermediateAttrs, true);

        // AggregateExec(FINAL) with groupings
        List<Expression> groupings = List.of(groupAttr);
        List<NamedExpression> aggregates = List.of(countAttr, groupAttr);
        AggregateExec aggExec = new AggregateExec(
            Source.EMPTY,
            exchangeSource,
            groupings,
            aggregates,
            AggregatorMode.FINAL,
            intermediateAttrs,
            null
        );

        // TopNExec (SORT c DESC LIMIT 10)
        TopNExec topNExec = new TopNExec(
            Source.EMPTY,
            aggExec,
            List.of(new Order(Source.EMPTY, countAttr, Order.OrderDirection.DESC, Order.NullsPosition.LAST)),
            new Literal(Source.EMPTY, 10, DataType.INTEGER),
            null
        );

        // ProjectExec
        ProjectExec projectExec = new ProjectExec(Source.EMPTY, topNExec, List.of(countAttr, groupAttr));

        // OutputExec (root)
        OutputExec outputExec = new OutputExec(Source.EMPTY, projectExec, page -> {});

        // Verify hasGroupedFinalAgg detects this correctly
        assertTrue("Plan should be detected as having a grouped FINAL agg", ComputeService.hasGroupedFinalAgg(outputExec));

        // Now split the plan
        var split = ComputeService.splitCoordinatorPlanForPartitioning(outputExec);
        assertNotNull("Split should succeed", split);

        PhysicalPlan finalPlan = split.v1();
        PhysicalPlan outputPlan = split.v2();

        // The final plan should be: ExchangeSinkExec → TopNExec → AggregateExec(FINAL) → ExchangeSourceExec
        // TopN is pushed into the final plan as a partial TopN (per-partition top-K)
        assertThat("Final plan root should be ExchangeSinkExec", finalPlan, org.hamcrest.Matchers.instanceOf(ExchangeSinkExec.class));
        ExchangeSinkExec finalSink = (ExchangeSinkExec) finalPlan;
        assertThat(
            "Final plan child should be TopNExec (partial TopN per partition)",
            finalSink.child(),
            org.hamcrest.Matchers.instanceOf(TopNExec.class)
        );
        TopNExec finalTopN = (TopNExec) finalSink.child();
        assertThat(
            "TopN's child in final plan should be AggregateExec",
            finalTopN.child(),
            org.hamcrest.Matchers.instanceOf(AggregateExec.class)
        );
        AggregateExec finalAgg = (AggregateExec) finalTopN.child();
        assertEquals("Final agg mode should be FINAL", AggregatorMode.FINAL, finalAgg.getMode());
        assertThat(
            "Final agg child should be ExchangeSourceExec",
            finalAgg.child(),
            org.hamcrest.Matchers.instanceOf(ExchangeSourceExec.class)
        );

        // The output plan should also contain a TopNExec (global merge TopN on K * numPartitions rows)
        assertThat("Output plan root should be OutputExec", outputPlan, org.hamcrest.Matchers.instanceOf(OutputExec.class));
        OutputExec outputRoot = (OutputExec) outputPlan;

        // Walk the output plan: OutputExec → ProjectExec → TopNExec → ExchangeSourceExec
        assertThat(outputRoot.child(), org.hamcrest.Matchers.instanceOf(ProjectExec.class));
        ProjectExec outputProject = (ProjectExec) outputRoot.child();
        assertThat(outputProject.child(), org.hamcrest.Matchers.instanceOf(TopNExec.class));
        TopNExec outputTopN = (TopNExec) outputProject.child();
        assertThat(
            "Output TopN's child should be ExchangeSourceExec (reads merged partition results)",
            outputTopN.child(),
            org.hamcrest.Matchers.instanceOf(ExchangeSourceExec.class)
        );
    }

    /**
     * Tests that when there's no TopNExec, only the AggregateExec goes into the final plan
     * and everything else stays in the output plan.
     *
     * The input plan tree is:
     * <pre>
     *   OutputExec
     *     └── ProjectExec
     *           └── AggregateExec(FINAL)
     *                 └── ExchangeSourceExec
     * </pre>
     *
     * Expected split:
     *   - Final plan: ExchangeSinkExec → AggregateExec(FINAL) → ExchangeSourceExec
     *   - Output plan: OutputExec → ProjectExec → ExchangeSourceExec
     */
    public void testSplitCoordinatorPlanWithoutTopN() {
        Attribute groupAttr = new ReferenceAttribute(Source.EMPTY, "group", DataType.KEYWORD);
        Attribute countAttr = new ReferenceAttribute(Source.EMPTY, "c", DataType.LONG);
        List<Attribute> intermediateAttrs = List.of(
            new ReferenceAttribute(Source.EMPTY, "c$count", DataType.LONG),
            new ReferenceAttribute(Source.EMPTY, "c$seen", DataType.BOOLEAN),
            new ReferenceAttribute(Source.EMPTY, "group", DataType.KEYWORD)
        );

        ExchangeSourceExec exchangeSource = new ExchangeSourceExec(Source.EMPTY, intermediateAttrs, true);

        AggregateExec aggExec = new AggregateExec(
            Source.EMPTY,
            exchangeSource,
            List.of(groupAttr),
            List.of(countAttr, groupAttr),
            AggregatorMode.FINAL,
            intermediateAttrs,
            null
        );

        ProjectExec projectExec = new ProjectExec(Source.EMPTY, aggExec, List.of(countAttr, groupAttr));
        OutputExec outputExec = new OutputExec(Source.EMPTY, projectExec, page -> {});

        var split = ComputeService.splitCoordinatorPlanForPartitioning(outputExec);
        assertNotNull("Split should succeed", split);

        // Final plan should contain only AggregateExec (no TopN to push)
        ExchangeSinkExec finalSink = (ExchangeSinkExec) split.v1();
        assertThat(finalSink.child(), org.hamcrest.Matchers.instanceOf(AggregateExec.class));

        // Output plan should contain ProjectExec → ExchangeSourceExec
        OutputExec outputRoot = (OutputExec) split.v2();
        assertThat(outputRoot.child(), org.hamcrest.Matchers.instanceOf(ProjectExec.class));
        assertThat(((ProjectExec) outputRoot.child()).child(), org.hamcrest.Matchers.instanceOf(ExchangeSourceExec.class));
    }
}
