/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.alias;

public class PushTopNIntoExternalSourceTests extends ESTestCase {

    private static final ReferenceAttribute USER_ID = new ReferenceAttribute(Source.EMPTY, "user_id", DataType.KEYWORD);
    private static final ReferenceAttribute REGION = new ReferenceAttribute(Source.EMPTY, "region", DataType.KEYWORD);
    private static final ReferenceAttribute COUNTER = new ReferenceAttribute(Source.EMPTY, "counter", DataType.INTEGER);

    public void testTopNPushedAscLimit10() {
        TopNExec topN = topN(USER_ID, Order.OrderDirection.ASC, 10, externalAggregate());

        TopNExec result = (TopNExec) applyRule(topN);

        ExternalSourceExec ext = unwrap(result);
        BlockHash.TopNDef def = ext.pushedTopN();
        assertNotNull(def);
        assertEquals(0, def.order());
        assertTrue(def.asc());
        assertFalse(def.nullsFirst());
        assertEquals(10, def.limit());
    }

    public void testTopNPushedDescLimit5() {
        TopNExec topN = topN(USER_ID, Order.OrderDirection.DESC, 5, externalAggregate());

        TopNExec result = (TopNExec) applyRule(topN);
        BlockHash.TopNDef def = unwrap(result).pushedTopN();
        assertNotNull(def);
        assertFalse(def.asc());
        assertEquals(5, def.limit());
    }

    /**
     * Sort key must reference a grouping attribute. Sorting on the aggregation result must NOT push the hint
     * because the rank of a key depends on data that is only known after the aggregation completes.
     */
    public void testTopNNotPushedWhenSortIsOnAggregateResult() {
        Alias countAlias = countStarAlias();
        AggregateExec aggregate = aggregateExec(USER_ID, externalSource(), countAlias);
        // Sort on the aggregate's output reference (the count alias), not the grouping key.
        TopNExec topN = topN(countAlias.toAttribute(), Order.OrderDirection.DESC, 10, aggregate);

        TopNExec result = (TopNExec) applyRule(topN);
        assertNull(unwrap(result).pushedTopN());
    }

    public void testTopNNotPushedWithMultipleGroupings() {
        AggregateExec aggregate = aggregateExec(List.of(USER_ID, REGION), externalSource(), countStarAlias());
        TopNExec topN = topN(USER_ID, Order.OrderDirection.ASC, 10, aggregate);

        TopNExec result = (TopNExec) applyRule(topN);
        assertNull(unwrap(result).pushedTopN());
    }

    public void testTopNNotPushedWithMultipleSortKeys() {
        AggregateExec aggregate = aggregateExec(USER_ID, externalSource(), countStarAlias());
        Order primary = new Order(Source.EMPTY, USER_ID, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        Order secondary = new Order(Source.EMPTY, REGION, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        TopNExec topN = new TopNExec(Source.EMPTY, aggregate, List.of(primary, secondary), literal(10), null);

        TopNExec result = (TopNExec) applyRule(topN);
        assertNull(unwrap(result).pushedTopN());
    }

    /**
     * Only LONG and BYTES_REF groupings have a Top-N {@link BlockHash} implementation today. Other element
     * types (e.g. INT) must not receive the hint, otherwise it would be silently dropped at runtime by
     * {@code BlockHash#build}, masking the absence of the optimization.
     */
    public void testTopNNotPushedForUnsupportedGroupingType() {
        AggregateExec aggregate = aggregateExec(COUNTER, externalSource(), countStarAlias());
        TopNExec topN = topN(COUNTER, Order.OrderDirection.ASC, 10, aggregate);

        TopNExec result = (TopNExec) applyRule(topN);
        assertNull(unwrap(result).pushedTopN());
    }

    public void testRuleIsNoOpWithoutAggregateChild() {
        // TopN directly above ExternalSourceExec (no aggregate) — the rule must not fire.
        ExternalSourceExec ext = externalSource();
        TopNExec topN = topN(USER_ID, Order.OrderDirection.ASC, 10, ext);

        PhysicalPlan result = applyRule(topN);

        TopNExec resultTopN = (TopNExec) result;
        assertSame(ext, resultTopN.child());
        assertNull(((ExternalSourceExec) resultTopN.child()).pushedTopN());
    }

    public void testWithPushedTopNReturnsNewInstance() {
        ExternalSourceExec ext = externalSource();
        BlockHash.TopNDef def = new BlockHash.TopNDef(0, true, false, 7);
        ExternalSourceExec annotated = ext.withPushedTopN(def);

        assertNotSame(ext, annotated);
        assertNull(ext.pushedTopN());
        assertEquals(def, annotated.pushedTopN());
        // Other fields untouched
        assertEquals(ext.sourcePath(), annotated.sourcePath());
        assertEquals(ext.sourceType(), annotated.sourceType());
        assertEquals(ext.output(), annotated.output());
    }

    public void testTopNNotPushedWhenSourceHasPushedFilter() {
        ExternalSourceExec ext = new ExternalSourceExec(
            Source.EMPTY,
            "file:///test.parquet",
            "parquet",
            List.of(USER_ID, REGION),
            Map.of(),
            Map.of(),
            "some_pushed_filter",
            null
        );
        AggregateExec agg = aggregateExec(USER_ID, ext, countStarAlias());
        TopNExec topN = topN(USER_ID, Order.OrderDirection.ASC, 10, agg);

        PhysicalPlan result = applyRule(topN);
        assertSame("rule must be no-op when source has a pushed filter", topN, result);
    }

    // --- helpers ---

    private static TopNExec topN(Attribute sortAttr, Order.OrderDirection direction, int limit, PhysicalPlan child) {
        Order order = new Order(Source.EMPTY, sortAttr, direction, Order.NullsPosition.LAST);
        return new TopNExec(Source.EMPTY, child, List.of(order), literal(limit), null);
    }

    private static AggregateExec externalAggregate() {
        return aggregateExec(USER_ID, externalSource(), countStarAlias());
    }

    private static AggregateExec aggregateExec(Attribute groupingAttr, PhysicalPlan child, NamedExpression... aggregates) {
        return aggregateExec(List.of(groupingAttr), child, aggregates);
    }

    private static AggregateExec aggregateExec(List<Attribute> groupingAttrs, PhysicalPlan child, NamedExpression... aggregates) {
        // The grouping is the attribute itself (no Alias wrapper) so that Expressions.attribute(group) returns it.
        List<Expression> groupings = groupingAttrs.stream().<Expression>map(a -> a).toList();
        List<NamedExpression> aggList = List.of(aggregates);
        List<Attribute> intermediateAttrs = AbstractPhysicalOperationProviders.intermediateAttributes(aggList, groupings);
        return new AggregateExec(Source.EMPTY, child, groupings, aggList, AggregatorMode.SINGLE, intermediateAttrs, null);
    }

    private static Alias countStarAlias() {
        return alias("c", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")));
    }

    private static ExternalSourceExec externalSource() {
        return new ExternalSourceExec(Source.EMPTY, "file:///test.parquet", "parquet", List.of(USER_ID, REGION), Map.of(), Map.of(), null);
    }

    private static ExternalSourceExec unwrap(TopNExec topN) {
        AggregateExec agg = (AggregateExec) topN.child();
        return (ExternalSourceExec) agg.child();
    }

    private static Literal literal(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    private static PhysicalPlan applyRule(TopNExec topNExec) {
        PushTopNIntoExternalSource rule = new PushTopNIntoExternalSource();
        LocalPhysicalOptimizerContext ctx = new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            null,
            FoldContext.small(),
            null
        );
        return rule.apply(topNExec, ctx);
    }
}
