/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.ReductionPlan;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class NodeReduceSplitTests extends ESTestCase {

    private final FieldAttribute sortKey = field("ts", DataType.DATETIME);
    private final FieldAttribute other = field("a", DataType.LONG);
    private final List<Order> order = List.of(new Order(Source.EMPTY, sortKey, Order.OrderDirection.DESC, Order.NullsPosition.FIRST));

    /** The node reduce plan is everything above the NODE exchange, the data plan is the fragment below it. */
    public void testSplit() {
        FragmentExec fragment = fragment(List.of(sortKey, other));
        ExchangeExec nodeExchange = nodeExchange(fragment.output(), fragment);
        TopNExec nodeCut = new TopNExec(Source.EMPTY, nodeExchange, order, EsqlTestUtils.of(10), null).withSortedInput();
        ExchangeSinkExec plan = new ExchangeSinkExec(Source.EMPTY, nodeCut.output(), false, nodeCut);

        ReductionPlan split = NodeReduceSplit.split(plan).orElseThrow();

        TopNExec reduceCut = (TopNExec) split.nodeReducePlan().child();
        assertThat(reduceCut.order(), equalTo(order));
        ExchangeSourceExec reduceSource = (ExchangeSourceExec) reduceCut.child();
        assertThat(reduceSource.output(), equalTo(nodeExchange.output()));

        assertThat(split.dataNodePlan().output(), equalTo(nodeExchange.output()));
        assertThat(split.dataNodePlan().child(), sameInstance(fragment));
    }

    public void testNoNodeExchange() {
        FragmentExec fragment = fragment(List.of(sortKey, other));
        assertThat(
            NodeReduceSplit.split(new ExchangeSinkExec(Source.EMPTY, fragment.output(), false, fragment)),
            equalTo(Optional.empty())
        );
    }

    public void testTwoNodeExchanges() {
        FragmentExec fragment = fragment(List.of(sortKey, other));
        ExchangeExec inner = nodeExchange(fragment.output(), fragment);
        ExchangeExec outer = nodeExchange(inner.output(), inner);
        // the inner one is never reached, the outer one wraps no fragment
        Exception e = expectThrows(
            EsqlIllegalArgumentException.class,
            () -> NodeReduceSplit.split(new ExchangeSinkExec(Source.EMPTY, outer.output(), false, outer))
        );
        assertThat(e.getMessage(), containsString("must wrap a fragment"));

        PhysicalPlan sibling = new TopNExec(Source.EMPTY, inner, order, EsqlTestUtils.of(10), null);
        ExchangeExec second = nodeExchange(fragment.output(), fragment(List.of(sortKey, other)));
        PhysicalPlan twoBranches = new MergeExec(Source.EMPTY, List.of(sibling, second), sibling.output(), MergeExec.Kind.UNION);
        e = expectThrows(
            EsqlIllegalArgumentException.class,
            () -> NodeReduceSplit.split(new ExchangeSinkExec(Source.EMPTY, twoBranches.output(), false, twoBranches))
        );
        assertThat(e.getMessage(), containsString("single NODE exchange"));
    }

    public void testOutputMismatch() {
        FragmentExec fragment = fragment(List.of(sortKey, other));
        ExchangeExec exchange = nodeExchange(List.of(sortKey), fragment);
        Exception e = expectThrows(
            EsqlIllegalArgumentException.class,
            () -> NodeReduceSplit.split(new ExchangeSinkExec(Source.EMPTY, exchange.output(), false, exchange))
        );
        assertThat(e.getMessage(), containsString("does not match its fragment output"));
    }

    /** The planned stage runs even when node level reduction is off, the coordinator relies on what it produces. */
    public void testReductionPlanningUsesThePlannedStage() {
        FragmentExec fragment = fragment(List.of(sortKey, other));
        ExchangeExec nodeExchange = nodeExchange(fragment.output(), fragment);
        TopNExec nodeCut = new TopNExec(Source.EMPTY, nodeExchange, order, EsqlTestUtils.of(10), null).withSortedInput();
        ExchangeSinkExec plan = new ExchangeSinkExec(Source.EMPTY, nodeCut.output(), false, nodeCut);

        ReductionPlan reduction = ComputeService.reductionPlan(
            PlannerSettings.DEFAULTS,
            EsqlFlags.DEFAULTS,
            EsqlTestUtils.TEST_CFG,
            EsqlTestUtils.TEST_CFG.newFoldContext(),
            plan,
            false,
            false,
            null
        );
        assertThat(reduction.nodeReducePlan().child(), instanceOf(TopNExec.class));
        assertThat(reduction.dataNodePlan().child(), sameInstance(fragment));
    }

    private FragmentExec fragment(List<Attribute> output) {
        EsRelation relation = new EsRelation(
            Source.EMPTY,
            "idx",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(),
            List.of(sortKey, other)
        );
        TopN topN = new TopN(Source.EMPTY, relation, order, EsqlTestUtils.of(10), false);
        return new FragmentExec(new Project(Source.EMPTY, topN, output));
    }

    private static ExchangeExec nodeExchange(List<Attribute> output, PhysicalPlan child) {
        return new ExchangeExec(Source.EMPTY, output, false, ExchangeExec.Scope.NODE, child);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}
