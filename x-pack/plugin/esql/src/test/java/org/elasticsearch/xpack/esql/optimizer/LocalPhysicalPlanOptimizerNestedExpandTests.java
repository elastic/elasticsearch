/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertExternalFieldExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushFiltersToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushLimitToExternalSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushNestedExpandToExternalSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushStatsToExternalSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushTopNIntoExternalSource;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.NestedExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.rule.Rule;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.alias;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * A dataset's {@code NESTED_EXPAND} through the whole local physical optimizer: where {@link PushNestedExpandToExternalSource}
 * sits among the other rules, and what the plans that reach it come out as. The statistics and limit rules guard an
 * expanding scan too, but only the batch order keeps them from meeting one in a query, so these tests pin that order.
 */
public class LocalPhysicalPlanOptimizerNestedExpandTests extends ESTestCase {

    private static final FieldAttribute ID = field("id", DataType.LONG);
    private static final FieldAttribute PRICE = field("price", DataType.LONG);
    private static final FieldAttribute QTY = field("items.qty", DataType.LONG);

    /**
     * S4: the rule runs in a batch after the one holding the statistics, top-N, filter and limit pushdowns, and before
     * deferred extraction, which declines the expanding scan it leaves.
     */
    public void testRuleRunsAfterThePushdownRules() {
        List<RuleExecutor.Batch<PhysicalPlan>> batches = new LocalPhysicalPlanOptimizer(context()).batches();
        int expandBatch = batchOf(batches, PushNestedExpandToExternalSource.class);
        for (Class<?> pushdown : List.of(
            PushStatsToExternalSource.class,
            PushTopNIntoExternalSource.class,
            PushFiltersToSource.class,
            PushLimitToExternalSource.class
        )) {
            assertThat(pushdown.getSimpleName(), batchOf(batches, pushdown), lessThan(expandBatch));
        }
        List<Rule<?, PhysicalPlan>> rules = List.of(batches.get(expandBatch).rules());
        assertThat(indexOf(rules, PushNestedExpandToExternalSource.class), lessThan(indexOf(rules, InsertExternalFieldExtraction.class)));
    }

    /**
     * S4: {@code NESTED_EXPAND | STATS COUNT(*)} keeps its aggregation over the expanding scan, never the footer's row
     * count, while the same aggregation over the scan alone is answered from the footer (positive control).
     */
    public void testCountAfterExpansionIsNotTheFooter() {
        ExternalSourceExec scan = scan();
        PhysicalPlan plain = optimize(countStar(scan));
        assertThat(plain.anyMatch(LocalSourceExec.class::isInstance), equalTo(true));

        PhysicalPlan expanded = optimize(countStar(expand(scan)));
        assertThat(expanded.anyMatch(LocalSourceExec.class::isInstance), equalTo(false));
        AggregateExec aggregate = as(expanded, AggregateExec.class);
        assertNotNull(as(aggregate.child(), ExternalSourceExec.class).expansion());
    }

    /**
     * S2, the plan the N1 decision names: {@code WHERE price > 100 | NESTED_EXPAND items | LIMIT 3}. The reader answers
     * the filter, so it is pushed and no FilterExec stays; the limit then sits on the scan and is pushed; and the rule
     * turns that pushed limit, which counted the reader's filtered rows, into the parent limit.
     */
    public void testPushedFilterAndLimitBecomeTheParentLimit() {
        Expression filter = new GreaterThan(Source.EMPTY, PRICE, new Literal(Source.EMPTY, 100L, DataType.LONG));
        LimitExec copy = new LimitExec(Source.EMPTY, new FilterExec(Source.EMPTY, scan(), filter), limit(3), null);
        PhysicalPlan plan = new LimitExec(Source.EMPTY, expand(copy), limit(3), null);

        LimitExec top = as(optimize(plan), LimitExec.class);
        ExternalSourceExec folded = as(top.child(), ExternalSourceExec.class);
        assertNotNull(folded.pushedFilter());
        assertThat(folded.pushedExpressions(), equalTo(List.of(filter)));
        assertThat(folded.expansion().parentLimit(), equalTo(3));
        assertThat(folded.pushedLimit(), equalTo(FormatReader.NO_LIMIT));
    }

    private static PhysicalPlan optimize(PhysicalPlan plan) {
        return new LocalPhysicalPlanOptimizer(context()).localOptimize(plan);
    }

    private static int batchOf(List<RuleExecutor.Batch<PhysicalPlan>> batches, Class<?> rule) {
        for (int b = 0; b < batches.size(); b++) {
            if (indexOf(List.of(batches.get(b).rules()), rule) >= 0) {
                return b;
            }
        }
        throw new AssertionError("no batch holds [" + rule.getSimpleName() + "]");
    }

    private static int indexOf(List<Rule<?, PhysicalPlan>> rules, Class<?> rule) {
        for (int r = 0; r < rules.size(); r++) {
            if (rule.isInstance(rules.get(r))) {
                return r;
            }
        }
        return -1;
    }

    private static AggregateExec countStar(PhysicalPlan child) {
        Alias count = alias("c", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")));
        List<NamedExpression> aggregates = List.of(count);
        List<Attribute> intermediate = AbstractPhysicalOperationProviders.intermediateAttributes(aggregates, List.of());
        return new AggregateExec(Source.EMPTY, child, List.of(), aggregates, AggregatorMode.SINGLE, intermediate, null);
    }

    private static ExternalSourceExec scan() {
        return new ExternalSourceExec(
            Source.EMPTY,
            "file:///orders.parquet",
            "parquet",
            List.of(ID, PRICE),
            Map.of(),
            Map.of(SourceStatisticsSerializer.STATS_ROW_COUNT, 1000L),
            null
        );
    }

    private static NestedExpandExec expand(PhysicalPlan child) {
        return new NestedExpandExec(Source.EMPTY, child, "items", List.of(QTY), List.of("qty"));
    }

    private static Literal limit(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    private static LocalPhysicalOptimizerContext context() {
        FormatReaderRegistry registry = new FormatReaderRegistry(null);
        registry.registerLazy("parquet", (settings, blockFactory) -> new StubReader(), null, null);
        return new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            null,
            FoldContext.small(),
            null,
            new ExternalOptimizerContext(registry)
        );
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    /**
     * A reader that answers every pushed filter exactly and every aggregate from its footer, so whatever the optimizer
     * does not push or answer is the plan's own decision, not the reader's.
     */
    private static class StubReader implements NoConfigFormatReader {
        @Override
        public AggregatePushdownSupport aggregatePushdownSupport() {
            return (aggregates, groupings) -> groupings.isEmpty()
                ? AggregatePushdownSupport.Pushability.YES
                : AggregatePushdownSupport.Pushability.NO;
        }

        @Override
        public FilterPushdownSupport filterPushdownSupport() {
            return filters -> new FilterPushdownSupport.PushdownResult("opaque", filters, List.of());
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String formatName() {
            return "parquet";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }
}
