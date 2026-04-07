/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.operator.topn.TopNOperator.InputOrdering;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.List;
import java.util.Map;

public class ExternalDistributionTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    // --- Mapper tests ---

    public void testMapperWrapsExternalRelationInFragmentExec() {
        ExternalRelation external = createExternalRelation();

        Mapper mapper = new Mapper();
        PhysicalPlan physicalPlan = mapper.map(new Versioned<>(external, TransportVersion.current()));

        assertTrue("Expected FragmentExec, got: " + physicalPlan.getClass().getSimpleName(), physicalPlan instanceof FragmentExec);
        FragmentExec fragment = (FragmentExec) physicalPlan;
        assertTrue(
            "Expected ExternalRelation inside fragment, got: " + fragment.fragment().getClass().getSimpleName(),
            fragment.fragment() instanceof ExternalRelation
        );
    }

    public void testMapperCreatesDistributedAggForExternalSource() {
        ExternalRelation external = createExternalRelation();
        List<Expression> groupings = List.of();
        List<? extends NamedExpression> aggregates = List.of();
        Aggregate aggregate = new Aggregate(SRC, external, groupings, aggregates);

        Mapper mapper = new Mapper();
        PhysicalPlan physicalPlan = mapper.map(new Versioned<>(aggregate, TransportVersion.current()));

        assertTrue("Expected AggregateExec at top, got: " + physicalPlan.getClass().getSimpleName(), physicalPlan instanceof AggregateExec);
        AggregateExec aggExec = (AggregateExec) physicalPlan;
        assertEquals(AggregatorMode.FINAL, aggExec.getMode());
        assertTrue(
            "Expected ExchangeExec child, got: " + aggExec.child().getClass().getSimpleName(),
            aggExec.child() instanceof ExchangeExec
        );
        ExchangeExec exchange = (ExchangeExec) aggExec.child();
        assertTrue(
            "Expected FragmentExec child, got: " + exchange.child().getClass().getSimpleName(),
            exchange.child() instanceof FragmentExec
        );
        FragmentExec fragment = (FragmentExec) exchange.child();
        assertTrue(
            "Expected Aggregate inside fragment, got: " + fragment.fragment().getClass().getSimpleName(),
            fragment.fragment() instanceof Aggregate
        );
    }

    public void testMapperInsertsExchangeForLimitAboveExternalSource() {
        ExternalRelation external = createExternalRelation();
        Limit limit = new Limit(SRC, new Literal(SRC, 10, DataType.INTEGER), external);

        Mapper mapper = new Mapper();
        PhysicalPlan physicalPlan = mapper.map(new Versioned<>(limit, TransportVersion.current()));

        assertTrue("Expected LimitExec at top, got: " + physicalPlan.getClass().getSimpleName(), physicalPlan instanceof LimitExec);
        LimitExec limitExec = (LimitExec) physicalPlan;
        assertTrue(
            "Expected ExchangeExec child, got: " + limitExec.child().getClass().getSimpleName(),
            limitExec.child() instanceof ExchangeExec
        );
        ExchangeExec exchange = (ExchangeExec) limitExec.child();
        assertTrue(
            "Expected FragmentExec child, got: " + exchange.child().getClass().getSimpleName(),
            exchange.child() instanceof FragmentExec
        );
        FragmentExec fragment = (FragmentExec) exchange.child();
        assertTrue(
            "Expected Limit inside fragment, got: " + fragment.fragment().getClass().getSimpleName(),
            fragment.fragment() instanceof Limit
        );
    }

    public void testMapperInsertsExchangeForTopNAboveExternalSource() {
        ExternalRelation external = createExternalRelation();
        Attribute nameAttr = external.output().get(0);
        Order order = new Order(SRC, nameAttr, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        TopN topN = new TopN(SRC, external, List.of(order), new Literal(SRC, 10, DataType.INTEGER), false);

        Mapper mapper = new Mapper();
        PhysicalPlan physicalPlan = mapper.map(new Versioned<>(topN, TransportVersion.current()));

        assertTrue("Expected TopNExec at top, got: " + physicalPlan.getClass().getSimpleName(), physicalPlan instanceof TopNExec);
        TopNExec topNExec = (TopNExec) physicalPlan;
        assertTrue(
            "Expected ExchangeExec child, got: " + topNExec.child().getClass().getSimpleName(),
            topNExec.child() instanceof ExchangeExec
        );
        ExchangeExec exchange = (ExchangeExec) topNExec.child();
        assertTrue(
            "Expected FragmentExec child, got: " + exchange.child().getClass().getSimpleName(),
            exchange.child() instanceof FragmentExec
        );
        FragmentExec fragment = (FragmentExec) exchange.child();
        assertTrue(
            "Expected TopN inside fragment, got: " + fragment.fragment().getClass().getSimpleName(),
            fragment.fragment() instanceof TopN
        );
    }

    // --- Context validation tests ---

    public void testContextRejectsNullSplits() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new ExternalDistributionContext(createExternalSourceExec(), null, null, QueryPragmas.EMPTY)
        );
    }

    public void testContextRejectsNullPlan() {
        expectThrows(IllegalArgumentException.class, () -> new ExternalDistributionContext(null, List.of(), null, QueryPragmas.EMPTY));
    }

    // --- Strategy resolution tests ---

    public void testResolveStrategyAdaptiveDefault() {
        ExternalDistributionStrategy strategy = ComputeService.resolveExternalDistributionStrategy(QueryPragmas.EMPTY);
        assertTrue(strategy instanceof AdaptiveStrategy);
    }

    public void testResolveStrategyCoordinatorOnly() {
        Settings settings = Settings.builder().put("external_distribution", "coordinator_only").build();
        QueryPragmas pragmas = new QueryPragmas(settings);
        ExternalDistributionStrategy strategy = ComputeService.resolveExternalDistributionStrategy(pragmas);
        assertTrue(strategy instanceof CoordinatorOnlyStrategy);
    }

    public void testResolveStrategyRoundRobin() {
        Settings settings = Settings.builder().put("external_distribution", "round_robin").build();
        QueryPragmas pragmas = new QueryPragmas(settings);
        ExternalDistributionStrategy strategy = ComputeService.resolveExternalDistributionStrategy(pragmas);
        assertTrue(strategy instanceof RoundRobinStrategy);
    }

    // --- Exchange collapsing tests (safety net for PR5) ---

    public void testCollapseExternalSourceExchangeRemovesExchange() {
        ExternalSourceExec externalSource = createExternalSourceExec();
        ExchangeExec exchange = new ExchangeExec(SRC, externalSource);
        LimitExec limit = new LimitExec(SRC, exchange, new Literal(SRC, 10, DataType.INTEGER), null);

        PhysicalPlan collapsed = ComputeService.collapseExternalSourceExchanges(limit);

        assertTrue("Expected LimitExec at top", collapsed instanceof LimitExec);
        LimitExec collapsedLimit = (LimitExec) collapsed;
        assertTrue(
            "Expected ExternalSourceExec directly under LimitExec, got: " + collapsedLimit.child().getClass().getSimpleName(),
            collapsedLimit.child() instanceof ExternalSourceExec
        );
    }

    public void testCollapseExternalSourceExchangePreservesNonExternalExchanges() {
        ExternalSourceExec externalSource = createExternalSourceExec();
        AggregateExec agg = new AggregateExec(SRC, externalSource, List.of(), List.of(), AggregatorMode.INITIAL, List.of(), null);
        ExchangeExec exchange = new ExchangeExec(SRC, agg);

        PhysicalPlan collapsed = ComputeService.collapseExternalSourceExchanges(exchange);

        assertTrue("Exchange wrapping non-ExternalSourceExec should be preserved", collapsed instanceof ExchangeExec);
    }

    public void testCollapseDoesNothingWhenNoExchanges() {
        ExternalSourceExec externalSource = createExternalSourceExec();
        LimitExec limit = new LimitExec(SRC, externalSource, new Literal(SRC, 10, DataType.INTEGER), null);

        PhysicalPlan collapsed = ComputeService.collapseExternalSourceExchanges(limit);

        assertTrue(collapsed instanceof LimitExec);
        LimitExec collapsedLimit = (LimitExec) collapsed;
        assertTrue(collapsedLimit.child() instanceof ExternalSourceExec);
    }

    public void testCollapseWithSplitsOnExternalSource() {
        List<ExternalSplit> splits = List.of(
            new FileSplit("parquet", StoragePath.of("s3://bucket/file1.parquet"), 0, 1024, ".parquet", Map.of(), Map.of()),
            new FileSplit("parquet", StoragePath.of("s3://bucket/file2.parquet"), 0, 1024, ".parquet", Map.of(), Map.of())
        );
        ExternalSourceExec externalSource = createExternalSourceExec().withSplits(splits);
        ExchangeExec exchange = new ExchangeExec(SRC, externalSource);
        LimitExec limit = new LimitExec(SRC, exchange, new Literal(SRC, 10, DataType.INTEGER), null);

        PhysicalPlan collapsed = ComputeService.collapseExternalSourceExchanges(limit);

        assertTrue(collapsed instanceof LimitExec);
        LimitExec collapsedLimit = (LimitExec) collapsed;
        assertTrue(collapsedLimit.child() instanceof ExternalSourceExec);
        ExternalSourceExec collapsedSource = (ExternalSourceExec) collapsedLimit.child();
        assertEquals(2, collapsedSource.splits().size());
    }

    public void testCollapseFragmentExecExchangeRemovesExchange() {
        ExternalRelation external = createExternalRelation();
        FragmentExec fragment = new FragmentExec(external);
        ExchangeExec exchange = new ExchangeExec(SRC, fragment);
        LimitExec limit = new LimitExec(SRC, exchange, new Literal(SRC, 10, DataType.INTEGER), null);

        PhysicalPlan collapsed = ComputeService.collapseExternalSourceExchanges(limit);

        assertTrue("Expected LimitExec at top", collapsed instanceof LimitExec);
        LimitExec collapsedLimit = (LimitExec) collapsed;
        assertTrue(
            "Expected FragmentExec directly under LimitExec, got: " + collapsedLimit.child().getClass().getSimpleName(),
            collapsedLimit.child() instanceof FragmentExec
        );
    }

    // --- localPlan expansion tests ---

    public void testLocalPlanExpandsFragmentExecToExternalSourceExec() {
        ExternalRelation external = createExternalRelation();
        FragmentExec fragment = new FragmentExec(external);
        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, external.output(), false, fragment);

        PhysicalPlan expanded = runLocalPlan(sink);

        assertTrue("Expected ExchangeSinkExec at top", expanded instanceof ExchangeSinkExec);
        ExchangeSinkExec expandedSink = (ExchangeSinkExec) expanded;
        assertTrue(
            "Expected ExternalSourceExec after expansion, got: " + expandedSink.child().getClass().getSimpleName(),
            expandedSink.child() instanceof ExternalSourceExec
        );
        ExternalSourceExec sourceExec = (ExternalSourceExec) expandedSink.child();
        assertEquals("s3://bucket/data/*.parquet", sourceExec.sourcePath());
        assertEquals("parquet", sourceExec.sourceType());
    }

    public void testLocalPlanExpandsAggregateFragmentContainsExternalSource() {
        ExternalRelation external = createExternalRelation();
        Aggregate aggregate = new Aggregate(SRC, external, List.of(), List.of());
        FragmentExec fragment = new FragmentExec(aggregate);
        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, aggregate.output(), true, fragment);

        PhysicalPlan expanded = runLocalPlan(sink);

        assertTrue("Expected ExchangeSinkExec at top", expanded instanceof ExchangeSinkExec);
        ExchangeSinkExec expandedSink = (ExchangeSinkExec) expanded;
        boolean hasNoFragmentExec = expandedSink.anyMatch(n -> n instanceof FragmentExec) == false;
        assertTrue("FragmentExec should be fully expanded", hasNoFragmentExec);
    }

    public void testLocalPlanExpandsLimitFragmentToLimitOverExternalSource() {
        ExternalRelation external = createExternalRelation();
        Limit limit = new Limit(SRC, new Literal(SRC, 10, DataType.INTEGER), external);
        FragmentExec fragment = new FragmentExec(limit);
        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, external.output(), false, fragment);

        PhysicalPlan expanded = runLocalPlan(sink);

        assertTrue("Expected ExchangeSinkExec at top", expanded instanceof ExchangeSinkExec);
        ExchangeSinkExec expandedSink = (ExchangeSinkExec) expanded;
        PhysicalPlan child = expandedSink.child();
        assertTrue("Expected LimitExec after expansion, got: " + child.getClass().getSimpleName(), child instanceof LimitExec);
        LimitExec limitExec = (LimitExec) child;
        assertTrue(
            "Expected ExternalSourceExec under LimitExec, got: " + limitExec.child().getClass().getSimpleName(),
            limitExec.child() instanceof ExternalSourceExec
        );
    }

    public void testLocalPlanExpandsTopNFragmentContainsExternalSource() {
        ExternalRelation external = createExternalRelation();
        Attribute nameAttr = external.output().get(0);
        Order order = new Order(SRC, nameAttr, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        TopN topN = new TopN(SRC, external, List.of(order), new Literal(SRC, 10, DataType.INTEGER), false);
        FragmentExec fragment = new FragmentExec(topN);
        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, external.output(), false, fragment);

        PhysicalPlan expanded = runLocalPlan(sink);

        assertTrue("Expected ExchangeSinkExec at top", expanded instanceof ExchangeSinkExec);
        ExchangeSinkExec expandedSink = (ExchangeSinkExec) expanded;
        boolean hasExternalSource = expandedSink.anyMatch(n -> n instanceof ExternalSourceExec);
        assertTrue("Expanded plan should contain ExternalSourceExec", hasExternalSource);
        boolean hasNoFragmentExec = expandedSink.anyMatch(n -> n instanceof FragmentExec) == false;
        assertTrue("FragmentExec should be fully expanded", hasNoFragmentExec);
    }

    public void testLocalPlanExpandsFilterFragmentContainsExternalSource() {
        ExternalRelation external = createExternalRelation();
        Attribute nameAttr = external.output().get(0);
        Expression filterCondition = new Literal(SRC, true, DataType.BOOLEAN);
        Filter filter = new Filter(SRC, external, filterCondition);
        FragmentExec fragment = new FragmentExec(filter);
        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, external.output(), false, fragment);

        PhysicalPlan expanded = runLocalPlan(sink);

        assertTrue("Expected ExchangeSinkExec at top", expanded instanceof ExchangeSinkExec);
        ExchangeSinkExec expandedSink = (ExchangeSinkExec) expanded;
        boolean hasNoFragmentExec = expandedSink.anyMatch(n -> n instanceof FragmentExec) == false;
        assertTrue("FragmentExec should be fully expanded", hasNoFragmentExec);
    }

    public void testEndToEndMapperThenLocalPlanExpandsFragmentExec() {
        ExternalRelation external = createExternalRelation();
        Limit limit = new Limit(SRC, new Literal(SRC, 10, DataType.INTEGER), external);

        Mapper mapper = new Mapper();
        PhysicalPlan coordPlan = mapper.map(new Versioned<>(limit, TransportVersion.current()));

        assertTrue("Expected LimitExec at top", coordPlan instanceof LimitExec);
        LimitExec limitExec = (LimitExec) coordPlan;
        assertTrue("Expected ExchangeExec child", limitExec.child() instanceof ExchangeExec);
        ExchangeExec exchange = (ExchangeExec) limitExec.child();
        assertTrue("Expected FragmentExec child", exchange.child() instanceof FragmentExec);
        FragmentExec fragment = (FragmentExec) exchange.child();

        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, fragment.output(), false, fragment);
        PhysicalPlan dataNodePlan = runLocalPlan(sink);

        assertTrue("Expected ExchangeSinkExec at top", dataNodePlan instanceof ExchangeSinkExec);
        ExchangeSinkExec expandedSink = (ExchangeSinkExec) dataNodePlan;
        boolean hasExternalSource = expandedSink.anyMatch(n -> n instanceof ExternalSourceExec);
        assertTrue("Expanded data node plan should contain ExternalSourceExec", hasExternalSource);
        boolean hasNoFragmentExec = expandedSink.anyMatch(n -> n instanceof FragmentExec) == false;
        assertTrue("FragmentExec should be fully expanded on data node", hasNoFragmentExec);
    }

    // --- Coordinator-only split injection tests ---

    public void testCoordinatorOnlySplitInjectionAfterLocalPlanExpansion() {
        ExternalRelation external = createExternalRelation();
        Limit limit = new Limit(SRC, new Literal(SRC, 10, DataType.INTEGER), external);
        FragmentExec fragment = new FragmentExec(limit);
        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, external.output(), false, fragment);

        PhysicalPlan expanded = runLocalPlan(sink);

        assertTrue("Expected ExchangeSinkExec at top", expanded instanceof ExchangeSinkExec);
        ExchangeSinkExec expandedSink = (ExchangeSinkExec) expanded;
        boolean hasExternalSource = expandedSink.anyMatch(n -> n instanceof ExternalSourceExec);
        assertTrue("Expanded plan should contain ExternalSourceExec", hasExternalSource);

        List<ExternalSplit> splits = List.of(
            new FileSplit("parquet", StoragePath.of("s3://bucket/file1.parquet"), 0, 1024, ".parquet", Map.of(), Map.of()),
            new FileSplit("parquet", StoragePath.of("s3://bucket/file2.parquet"), 0, 1024, ".parquet", Map.of(), Map.of())
        );
        PhysicalPlan withSplits = expanded.transformUp(
            ExternalSourceExec.class,
            exec -> exec.splits().isEmpty() ? exec.withSplits(splits) : exec
        );

        final List<ExternalSourceExec> sources = new java.util.ArrayList<>();
        withSplits.forEachDown(ExternalSourceExec.class, sources::add);
        assertFalse("Should have at least one ExternalSourceExec", sources.isEmpty());
        assertEquals("Splits should be injected", 2, sources.get(0).splits().size());
    }

    // --- Field retention through local optimization tests ---

    /**
     * Verifies that external source fields survive local plan optimization with empty SearchStats.
     * ReplaceFieldWithConstantOrNull must not replace external fields with null.
     * Without the fix, the optimizer inserts Eval(field=null) + Project nodes above the source.
     */
    public void testLocalPlanRetainsExternalSourceFields() {
        ExternalRelation external = createMultiFieldExternalRelation();
        Attribute salaryAttr = external.output().get(1);
        Expression filterCondition = new GreaterThan(SRC, salaryAttr, new Literal(SRC, 50000, DataType.INTEGER), null);
        Filter filter = new Filter(SRC, external, filterCondition);
        FragmentExec fragment = new FragmentExec(filter);
        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, external.output(), false, fragment);

        PhysicalPlan expanded = runLocalPlan(sink);

        ExchangeSinkExec expandedSink = (ExchangeSinkExec) expanded;
        // The filter must survive -- without the fix, the field reference in the filter condition
        // is replaced with null by ReplaceFieldWithConstantOrNull, causing PruneFilters to remove it
        boolean hasFilter = expandedSink.anyMatch(n -> n instanceof FilterExec);
        assertTrue("Filter on external field must survive local optimization with empty SearchStats", hasFilter);
    }

    /**
     * Verifies that a filter referencing external source fields survives local optimization.
     * With empty SearchStats, the filter condition must not be replaced with null.
     */
    public void testLocalPlanRetainsFilterOnExternalFields() {
        ExternalRelation external = createMultiFieldExternalRelation();
        Attribute salaryAttr = external.output().get(1);
        Expression filterCondition = new GreaterThan(SRC, salaryAttr, new Literal(SRC, 50000, DataType.INTEGER), null);
        Filter filter = new Filter(SRC, external, filterCondition);
        FragmentExec fragment = new FragmentExec(filter);
        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, external.output(), false, fragment);

        PhysicalPlan expanded = runLocalPlan(sink);

        ExchangeSinkExec expandedSink = (ExchangeSinkExec) expanded;
        boolean hasFilter = expandedSink.anyMatch(n -> n instanceof FilterExec);
        assertTrue("Filter should survive optimization, not be pruned", hasFilter);
    }

    /**
     * Verifies that eval expressions referencing external source fields produce non-null results.
     */
    public void testLocalPlanRetainsEvalOnExternalFields() {
        ExternalRelation external = createMultiFieldExternalRelation();
        Attribute salaryAttr = external.output().get(1);
        var alias = new org.elasticsearch.xpack.esql.core.expression.Alias(SRC, "abs_salary", new Abs(SRC, salaryAttr));
        Eval eval = new Eval(SRC, external, List.of(alias));
        FragmentExec fragment = new FragmentExec(eval);
        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, eval.output(), false, fragment);

        PhysicalPlan expanded = runLocalPlan(sink);

        ExchangeSinkExec expandedSink = (ExchangeSinkExec) expanded;
        boolean hasEval = expandedSink.anyMatch(n -> n instanceof EvalExec);
        assertTrue("Eval should survive optimization", hasEval);
        boolean hasExternalSource = expandedSink.anyMatch(n -> n instanceof ExternalSourceExec);
        assertTrue("ExternalSourceExec should be present", hasExternalSource);
    }

    /**
     * Verifies that collapseExternalSourceExchanges resets TopNExec InputOrdering
     * when the exchange is removed and the TopN sits directly above a FragmentExec.
     */
    public void testCollapseResetsTopNInputOrdering() {
        ExternalRelation external = createExternalRelation();
        TopN topN = new TopN(
            SRC,
            external,
            List.of(new Order(SRC, external.output().get(0), Order.OrderDirection.ASC, Order.NullsPosition.LAST)),
            new Literal(SRC, 10, DataType.INTEGER),
            false
        );
        FragmentExec fragment = new FragmentExec(topN);
        ExchangeExec exchange = new ExchangeExec(SRC, fragment);
        TopNExec topNExec = new TopNExec(
            SRC,
            exchange,
            List.of(new Order(SRC, external.output().get(0), Order.OrderDirection.ASC, Order.NullsPosition.LAST)),
            new Literal(SRC, 10, DataType.INTEGER),
            null
        ).withSortedInput();

        assertEquals("Precondition: TopNExec should have SORTED ordering", InputOrdering.SORTED, topNExec.inputOrdering());

        PhysicalPlan collapsed = ComputeService.collapseExternalSourceExchanges(topNExec);

        assertTrue("Expected TopNExec at top", collapsed instanceof TopNExec);
        TopNExec collapsedTopN = (TopNExec) collapsed;
        assertEquals("InputOrdering should be reset to NOT_SORTED after collapse", InputOrdering.NOT_SORTED, collapsedTopN.inputOrdering());
        assertTrue("Expected FragmentExec child", collapsedTopN.child() instanceof FragmentExec);
    }

    // --- Helpers ---

    private static PhysicalPlan runLocalPlan(PhysicalPlan plan) {
        var config = EsqlTestUtils.TEST_CFG;
        return PlannerUtils.localPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            config,
            config.newFoldContext(),
            plan,
            SearchStats.EMPTY,
            null
        );
    }

    private static ExternalRelation createExternalRelation() {
        List<Attribute> output = List.of(
            new FieldAttribute(SRC, "name", new EsField("name", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
        );
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, "parquet", "s3://bucket/data/*.parquet");
        return new ExternalRelation(SRC, "s3://bucket/data/*.parquet", metadata, output, null);
    }

    private static ExternalRelation createMultiFieldExternalRelation() {
        List<Attribute> output = List.of(
            new FieldAttribute(SRC, "name", new EsField("name", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)),
            new FieldAttribute(SRC, "salary", new EsField("salary", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)),
            new FieldAttribute(SRC, "age", new EsField("age", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
        );
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, "parquet", "s3://bucket/data/*.parquet");
        return new ExternalRelation(SRC, "s3://bucket/data/*.parquet", metadata, output, null);
    }

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
}
