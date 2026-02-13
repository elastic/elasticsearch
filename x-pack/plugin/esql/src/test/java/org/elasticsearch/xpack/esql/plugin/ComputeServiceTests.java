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
 * Tests for {@link ComputeService#splitCoordinatorPlanForPartitioning} and
 * {@link ComputeService#hasGroupedFinalAgg}.
 */
public class ComputeServiceTests extends ESTestCase {

    /**
     * Tests that splitCoordinatorPlanForPartitioning correctly splits the plan
     * so that TopN and nodes above the AggregateExec are in the output plan,
     * not in the per-partition final plan.
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
     *   - Final plan: ExchangeSinkExec → AggregateExec(FINAL) → ExchangeSourceExec
     *   - Output plan: OutputExec → ProjectExec → TopNExec → ExchangeSourceExec
     */
    public void testSplitCoordinatorPlanPutsTopNInOutputPlan() {
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

        // The final plan should be: ExchangeSinkExec → AggregateExec(FINAL) → ExchangeSourceExec
        // It should NOT contain TopNExec or ProjectExec
        assertThat("Final plan root should be ExchangeSinkExec", finalPlan, org.hamcrest.Matchers.instanceOf(ExchangeSinkExec.class));
        ExchangeSinkExec finalSink = (ExchangeSinkExec) finalPlan;
        assertThat(
            "Final plan child should be AggregateExec, not TopNExec or ProjectExec - "
                + "TopN must be in the output plan so it applies globally, not per-partition",
            finalSink.child(),
            org.hamcrest.Matchers.instanceOf(AggregateExec.class)
        );
        AggregateExec finalAgg = (AggregateExec) finalSink.child();
        assertEquals("Final agg mode should be FINAL", AggregatorMode.FINAL, finalAgg.getMode());
        assertThat(
            "Final agg child should be ExchangeSourceExec",
            finalAgg.child(),
            org.hamcrest.Matchers.instanceOf(ExchangeSourceExec.class)
        );

        // The output plan should contain the TopNExec (global TopN applied after merging partition results)
        assertThat("Output plan root should be OutputExec", outputPlan, org.hamcrest.Matchers.instanceOf(OutputExec.class));
        OutputExec outputRoot = (OutputExec) outputPlan;

        // Walk the output plan to find TopNExec
        boolean foundTopN = false;
        PhysicalPlan current = outputRoot.child();
        while (current != null) {
            if (current instanceof TopNExec) {
                foundTopN = true;
                break;
            }
            if (current.children().size() == 1) {
                current = current.children().get(0);
            } else {
                break;
            }
        }
        assertTrue("Output plan must contain TopNExec to apply the LIMIT globally across all partitions", foundTopN);
    }
}
