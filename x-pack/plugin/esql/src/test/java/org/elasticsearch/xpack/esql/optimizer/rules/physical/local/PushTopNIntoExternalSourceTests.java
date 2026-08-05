/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
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
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.alias;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.hasSize;

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
        assertEquals(1, def.sortKeys().size());
        assertEquals(0, def.primaryKey().groupingIndex());
        assertTrue(def.primaryKey().asc());
        assertFalse(def.primaryKey().nullsFirst());
        assertEquals(10, def.limit());
    }

    public void testTopNPushedDescLimit5() {
        TopNExec topN = topN(USER_ID, Order.OrderDirection.DESC, 5, externalAggregate());

        TopNExec result = (TopNExec) applyRule(topN);
        BlockHash.TopNDef def = unwrap(result).pushedTopN();
        assertNotNull(def);
        assertFalse(def.primaryKey().asc());
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

    /**
     * A single sort key that references one of two grouping attributes — the rule must fire with a
     * single-entry {@link BlockHash.TopNDef} pointing at the matched grouping index.
     */
    public void testTopNPushedWithMultipleGroupingsSingleSortKey() {
        AggregateExec aggregate = aggregateExec(List.of(USER_ID, REGION), externalSource(), countStarAlias());
        TopNExec topN = topN(USER_ID, Order.OrderDirection.ASC, 10, aggregate);

        TopNExec result = (TopNExec) applyRule(topN);
        BlockHash.TopNDef def = unwrap(result).pushedTopN();
        assertNotNull(def);
        assertEquals(1, def.sortKeys().size());
        assertEquals(0, def.primaryKey().groupingIndex()); // USER_ID is grouping[0]
        assertTrue(def.primaryKey().asc());
        assertEquals(10, def.limit());
    }

    /**
     * Both sort keys reference grouping attributes — the rule must fire with a 2-entry {@link BlockHash.TopNDef}.
     */
    public void testTopNPushedWith2KeySort() {
        AggregateExec aggregate = aggregateExec(List.of(USER_ID, REGION), externalSource(), countStarAlias());
        Order primary = new Order(Source.EMPTY, USER_ID, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        Order secondary = new Order(Source.EMPTY, REGION, Order.OrderDirection.DESC, Order.NullsPosition.FIRST);
        TopNExec topN = new TopNExec(Source.EMPTY, aggregate, List.of(primary, secondary), literal(10), null);

        TopNExec result = (TopNExec) applyRule(topN);
        BlockHash.TopNDef def = unwrap(result).pushedTopN();
        assertNotNull(def);
        assertEquals(2, def.sortKeys().size());
        assertEquals(0, def.sortKeys().get(0).groupingIndex()); // USER_ID is grouping[0]
        assertTrue(def.sortKeys().get(0).asc());
        assertFalse(def.sortKeys().get(0).nullsFirst());
        assertEquals(1, def.sortKeys().get(1).groupingIndex()); // REGION is grouping[1]
        assertFalse(def.sortKeys().get(1).asc());
        assertTrue(def.sortKeys().get(1).nullsFirst());
        assertEquals(10, def.limit());
    }

    /**
     * The second sort key references a field that is NOT one of the grouping attributes — the rule must not fire.
     */
    public void testTopNNotPushedWhenSortKeyNotInGroupings() {
        // USER_ID is the only grouping but the sort also references REGION which is not a grouping attribute.
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
        BlockHash.TopNDef def = new BlockHash.TopNDef(List.of(new BlockHash.SortKey(0, true, false)), 7);
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

    // --- full-pipeline tests ---

    /**
     * Full-pipeline test: parse → analyze → logical optimize → {@link LocalMapper} → {@link LocalPhysicalPlanOptimizer}.
     *
     * <p>Verifies end-to-end that {@link PushTopNIntoExternalSource} fires and annotates the
     * {@link ExternalSourceExec} when the query is
     * {@code EXTERNAL "..." | STATS count(*) BY user_id, region | SORT user_id ASC, region ASC | LIMIT 10}.
     * Uses {@link LocalMapper} (the data-node path) so that {@code TopNExec → AggregateExec(INITIAL) →
     * ExternalSourceExec} appears in one contiguous plan tree where the rule can see both nodes.
     */
    public void testFullPipelineMultiKeySortPushesTopNAnnotation() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        String path = "file:///test.parquet";
        List<Attribute> schema = List.of(referenceAttribute("user_id", DataType.KEYWORD), referenceAttribute("region", DataType.KEYWORD));

        LogicalPlan analyzed = EsqlTestUtils.analyzer()
            .externalSourceUnresolved(path, schema)
            .query("EXTERNAL \"" + path + "\" | STATS count(*) BY user_id, region | SORT user_id ASC, region ASC | LIMIT 10");

        LogicalPlan logicallyOptimized = new LogicalPlanOptimizer(
            new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), TransportVersion.current())
        ).optimize(analyzed);

        PhysicalPlan physicalPlan = LocalMapper.INSTANCE.map(logicallyOptimized);
        PhysicalPlan localPlan = new LocalPhysicalPlanOptimizer(
            new LocalPhysicalOptimizerContext(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(true),
                EsqlTestUtils.TEST_CFG,
                FoldContext.small(),
                EsqlTestUtils.TEST_SEARCH_STATS
            )
        ).localOptimize(physicalPlan);

        List<ExternalSourceExec> externalSources = new ArrayList<>();
        localPlan.forEachDown(ExternalSourceExec.class, externalSources::add);
        assertThat("expected exactly one ExternalSourceExec", externalSources, hasSize(1));

        BlockHash.TopNDef topN = externalSources.get(0).pushedTopN();
        assertNotNull("PushTopNIntoExternalSource must have fired and set pushedTopN on ExternalSourceExec", topN);
        assertEquals("expected 2 sort keys (user_id, region)", 2, topN.sortKeys().size());
        assertEquals("limit must match the query LIMIT", 10, topN.limit());
        // user_id is grouping[0], region is grouping[1] — matching order in the BY clause
        assertEquals("primary key must reference grouping index 0 (user_id)", 0, topN.sortKeys().get(0).groupingIndex());
        assertTrue("user_id sorts ascending", topN.sortKeys().get(0).asc());
        assertEquals("secondary key must reference grouping index 1 (region)", 1, topN.sortKeys().get(1).groupingIndex());
        assertTrue("region sorts ascending", topN.sortKeys().get(1).asc());
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
