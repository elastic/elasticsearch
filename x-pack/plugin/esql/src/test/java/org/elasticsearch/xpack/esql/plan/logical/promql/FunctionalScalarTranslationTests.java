/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PackDimsAgg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesFunction;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.hamcrest.Matchers.containsString;

/** Scalar conversion counts finished vector samples and supplies NaN on missing evaluation steps. */
public class FunctionalScalarTranslationTests extends ESTestCase {
    private static final String BOUNDS = "start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:02:00Z\" step=1m";

    /** Full series identity must survive until scalar counts samples, even when two series have equal values. */
    public void testScalarCountsFinishedSeries() {
        LogicalPlan plan = analyze("scalar(network.bytes_in)", BOUNDS);
        List<Aggregate> scalars = plan.collect(Aggregate.class)
            .stream()
            .filter(aggregate -> aggregate.aggregates().stream().anyMatch(expression -> expression.anyMatch(Scalar.class::isInstance)))
            .toList();
        assertEquals(1, scalars.size());
        Aggregate scalar = scalars.getFirst();
        assertFalse(scalar instanceof TimeSeriesAggregate);
        assertEquals(1, scalar.groupings().size());
        assertEquals(DataType.DATETIME, scalar.groupings().getFirst().dataType());
        Filter present = (Filter) scalar.child();
        assertTrue(present.condition() instanceof IsNotNull);
        UnpackDims unpack = present.collect(UnpackDims.class).getFirst();
        assertEquals(1, unpack.dims().size());
        assertEquals(DataType.TIMESERIES_DATA_TYPE, unpack.dims().getFirst().dataType());
        Aggregate identity = (Aggregate) unpack.child();
        Alias grouping = (Alias) identity.groupings()
            .stream()
            .filter(group -> group instanceof Alias alias && alias.toAttribute().semanticEquals(unpack.packed()))
            .findFirst()
            .orElseThrow();
        TimeSeriesAggregate perSeries = (TimeSeriesAggregate) identity.child();
        Alias definition = (Alias) perSeries.aggregates()
            .stream()
            .filter(aggregate -> aggregate.toAttribute().semanticEquals(grouping.child()))
            .findFirst()
            .orElseThrow();
        PackDimsAgg identityValue = (PackDimsAgg) definition.child();
        assertEquals(1, identityValue.fields().size());
        assertEquals(DataType.TIMESERIES_DATA_TYPE, identityValue.fields().getFirst().dataType());
        assertTrue(present.output().stream().anyMatch(attribute -> attribute.dataType() == DataType.TIMESERIES_DATA_TYPE));
        assertEquals(List.of("result", "step"), plan.output().stream().map(Attribute::name).toList());
    }

    /** Explicit bounds provide precisely the requested clock, including steps absent from every input series. */
    public void testBoundedScalarSuppliesRequestedSteps() {
        LogicalPlan plan = analyze("scalar(network.bytes_in)", BOUNDS);
        assertScalarDefaultSelection(plan);
        assertEquals(1, plan.collect(MvExpand.class).size());
        Row grid = plan.collect(Row.class).getFirst();
        long first = Instant.parse("2025-01-01T00:00:00Z").toEpochMilli();
        assertEquals(List.of(first, first + 60_000L, first + 120_000L), ((Literal) grid.fields().getFirst().child()).value());
        assertContainsNaN(plan);
    }

    /** Histograms do not contribute to scalar cardinality, but their observed and requested steps still produce NaN. */
    public void testScalarIgnoresHistogramSamples() {
        for (String bounds : List.of(BOUNDS, "step=1m")) {
            LogicalPlan plan = prototype().addIndex("exp_histo", "exp_histo_sample-mappings.json", IndexMode.TIME_SERIES)
                .query("PROMQL index=exp_histo " + bounds + " result=(scalar(responseTime offset 1h)) | LIMIT 10");
            assertConsistent(plan);
            assertHistogramScalar(plan);
            assertEquals(List.of("result", "step"), plan.output().stream().map(Attribute::name).toList());
            if (bounds.equals(BOUNDS)) {
                assertEquals(1, plan.collect(MvExpand.class).size());
            } else {
                assertTrue(plan.collect(MvExpand.class).isEmpty());
                assertEquals(2, plan.collect(EsRelation.class).size());
                assertSourceClockOffset(plan, Duration.ofHours(1));
            }
        }
    }

    /** A relabeled histogram contributes no numeric sample to scalar conversion. */
    public void testHistogramScalarIgnoresRelabeledSamples() {
        LogicalPlan plan = prototype().addIndex("exp_histo", "exp_histo_sample-mappings.json", IndexMode.TIME_SERIES)
            .query(
                "PROMQL index=exp_histo "
                    + BOUNDS
                    + " result=(scalar(label_replace(responseTime, \"instance\", \"\", \"instance\", \".*\"))) | LIMIT 10"
            );
        assertConsistent(plan);
        assertHistogramScalar(plan);
    }

    /** Invalid child functions must retain their diagnostic rather than fail while reading an unresolved sample's type. */
    public void testInvalidScalarInputRetainsVerificationError() {
        VerificationException error = expectThrows(
            VerificationException.class,
            () -> prototype().addK8s()
                .query("PROMQL index=k8s " + BOUNDS + " result=(scalar(histogram_count(network.bytes_in))) | LIMIT 10")
        );
        assertThat(error.getMessage(), containsString("must be [exponential_histogram or tdigest]"));
    }

    /** Empty indices still produce scalar NaN on an explicit clock, rather than an empty vector result. */
    public void testBoundedEmptySourceHasScalarClock() {
        LogicalPlan plan = prototype().addEmptyIndex()
            .unmappedResolution(UnmappedResolution.NULLIFY)
            .query("PROMQL index=empty_index " + BOUNDS + " result=(scalar(foo)) | LIMIT 10");
        assertConsistent(plan);
        assertTrue(plan.collect(EsRelation.class).isEmpty());
        assertScalarDefaultSelection(plan);
        assertEquals(1, plan.collect(MvExpand.class).size());
        assertEquals(List.of("result", "step"), plan.output().stream().map(Attribute::name).toList());
        assertContainsNaN(plan);
    }

    /** Without explicit bounds, both value and missing-step defaults use finite source-driven clocks with the same offset. */
    public void testUnboundedScalarRetainsSourceClockOffset() {
        LogicalPlan plan = analyze("scalar(network.bytes_in offset 1h)", "step=1m");
        assertTrue(plan.collect(MvExpand.class).isEmpty());
        assertEquals(2, plan.collect(EsRelation.class).size());
        assertSourceClockOffset(plan, Duration.ofHours(1));
    }

    /** A source-free scalar operand must share the shifted vector clock rather than remove unmatched vector steps. */
    public void testUnboundedSourceFreeScalarSharesVectorClock() {
        LogicalPlan plan = analyze("(network.bytes_in offset 1h) / scalar(vector(1))", "step=1m");
        assertTrue(plan.collect(MvExpand.class).isEmpty());
        assertEquals(3, plan.collect(EsRelation.class).size());
        assertSourceClockOffset(plan, Duration.ofHours(1));
    }

    /** Different selector offsets contribute one common clock each, and repeated offsets cannot duplicate scalar defaults. */
    public void testUnboundedScalarUnionsDistinctSourceClocks() {
        LogicalPlan plan = analyze(
            "scalar((network.bytes_in{cluster=\"missing\"} offset 1h) "
                + "or (network.bytes_in{cluster=\"also_missing\"} offset 1h) or network.bytes_in{cluster=\"missing\"})",
            "step=1m"
        );
        assertTrue(plan.collect(MvExpand.class).isEmpty());
        List<UnionAll> clocks = plan.collect(UnionAll.class)
            .stream()
            .filter(union -> union.output().size() == 1 && union.output().getFirst().dataType() == DataType.DATETIME)
            .toList();
        assertEquals(plan.toString(), 1, clocks.size());
        UnionAll clock = clocks.getFirst();
        assertEquals(2, clock.children().size());
        for (LogicalPlan branch : clock.children()) {
            assertEquals(1, branch.collect(EsRelation.class).size());
        }
        assertTrue(
            plan.collect(Aggregate.class)
                .stream()
                .anyMatch(aggregate -> aggregate.child() == clock && aggregate.groupings().equals(clock.output()))
        );
    }

    /** Source-free vectors still need cardinality evaluation, including set operations that leave no samples. */
    public void testBoundedSourceFreeScalarExpressions() {
        for (String expression : List.of("scalar(vector(1))", "scalar(vector(1) > 2)", "scalar(vector(1) unless vector(2))")) {
            LogicalPlan plan = prototype().addEmptyIndex()
                .unmappedResolution(UnmappedResolution.NULLIFY)
                .query("PROMQL index=empty_index " + BOUNDS + " result=(" + expression + ") | LIMIT 10");
            assertConsistent(plan);
            assertScalarDefaultSelection(plan);
            assertTrue(plan.collect(EsRelation.class).isEmpty());
            assertEquals(List.of("result", "step"), plan.output().stream().map(Attribute::name).toList());
        }
    }

    /** Disabled and older clusters keep the legacy scalar plan and do not acquire typed helpers. */
    public void testLegacyScalarTranslationIsUnchanged() {
        for (TestAnalyzer analyzer : List.of(
            prototype().flags(EsqlFlags.DEFAULTS),
            prototype().minimumTransportVersion(TransportVersionUtils.getPreviousVersion(featureVersion()))
        )) {
            LogicalPlan plan = analyzer.addK8s().query("PROMQL index=k8s " + BOUNDS + " result=(scalar(network.bytes_in)) | LIMIT 10");
            assertConsistent(plan);
            assertTrue(plan.collect(TopNBy.class).isEmpty());
            plan.forEachExpressionDown(TimeSeriesFunction.class, function -> fail("Legacy scalar unexpectedly uses typed metadata"));
        }
    }

    /** Grid allocation must fail against the configured planning budget before allocating millions of boxed steps. */
    public void testEvaluationGridHonorsConfiguredMemoryLimit() {
        String query = "PROMQL index=k8s start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T01:00:00Z\" "
            + "step=1ms result=(vector(1)) | LIMIT 10";
        for (EsqlFlags flags : List.of(EsqlFlags.DEFAULTS, EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(true))) {
            var analyzer = prototype().flags(flags)
                .addK8s()
                .configuration(
                    configuration(new QueryPragmas(Settings.builder().put(QueryPragmas.FOLD_LIMIT.getKey(), "1kb").build()), query)
                );
            var failure = expectThrows(FoldContext.FoldTooMuchMemoryException.class, () -> analyzer.query(query));
            assertThat(failure.getMessage(), containsString("Folding query used more than 1kb"));
        }
    }

    /** An empty raw vector needs no grid; scalar over that vector must still account for its requested NaN steps. */
    public void testEmptySelectorsDoNotAllocateUnusedEvaluationGrid() {
        String bounds = "start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T01:00:00Z\" step=1ms";
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.FOLD_LIMIT.getKey(), "1kb").build());
        for (String expression : List.of("foo", "avg(foo) by(job)")) {
            String raw = "PROMQL index=empty_index " + bounds + " result=(" + expression + ") | LIMIT 10";
            for (TestAnalyzer analyzer : List.of(
                prototype(),
                prototype().flags(EsqlFlags.DEFAULTS),
                prototype().minimumTransportVersion(TransportVersionUtils.getPreviousVersion(featureVersion()))
            )) {
                LogicalPlan plan = analyzer.addEmptyIndex()
                    .unmappedResolution(UnmappedResolution.NULLIFY)
                    .configuration(configuration(pragmas, raw))
                    .query(raw);
                assertConsistent(plan);
                assertTrue(plan.collect(MvExpand.class).isEmpty());
                assertEquals(1, plan.collect(LocalRelation.class).size());
                LocalRelation empty = plan.collect(LocalRelation.class).getFirst();
                assertEquals(EmptyLocalSupplier.EMPTY, empty.supplier());
                // NULLIFY resolves an absent BY label to NULL; AcrossSeriesAggregate omits it from the declared output.
                List<String> columns = expression.equals("foo")
                    ? List.of("result", "step", MetadataAttribute.TIMESERIES)
                    : List.of("result", "step");
                assertEquals(columns, plan.output().stream().map(Attribute::name).toList());
                assertEquals(columns, empty.output().stream().map(Attribute::name).toList());
            }
        }
        String scalar = "PROMQL index=empty_index " + bounds + " result=(scalar(foo)) | LIMIT 10";
        var analyzer = prototype().addEmptyIndex()
            .unmappedResolution(UnmappedResolution.NULLIFY)
            .configuration(configuration(pragmas, scalar));
        expectThrows(FoldContext.FoldTooMuchMemoryException.class, () -> analyzer.query(scalar));
    }

    /** Independent scalar branches share one command budget rather than each receiving its own allocation allowance. */
    public void testScalarGridsSharePlanningBudget() {
        String single = "PROMQL index=k8s " + BOUNDS + " result=(scalar(network.bytes_in)) | LIMIT 10";
        // Measure one real grid allocation so the test does not depend on reference width or object-layout estimates.
        long[] allocated = new long[1];
        FoldContext measured = new FoldContext(Long.MAX_VALUE) {
            @Override
            public void trackAllocation(Source source, long bytes) {
                super.trackAllocation(source, bytes);
                allocated[0] += bytes;
            }
        };
        PromqlCommand command = TEST_PARSER.parseQuery(single).collect(PromqlCommand.class).getFirst();
        PromqlLogicalPlanBuilder.buildLocalRelation(command, measured);
        assertTrue(allocated[0] > 0);
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.FOLD_LIMIT.getKey(), allocated[0] + "b").build());
        assertConsistent(prototype().addK8s().configuration(configuration(pragmas, single)).query(single));
        String combined = "PROMQL index=k8s " + BOUNDS + " result=(scalar(network.bytes_in) + scalar(network.bytes_in)) | LIMIT 10";
        var analyzer = prototype().addK8s().configuration(configuration(pragmas, combined));
        expectThrows(FoldContext.FoldTooMuchMemoryException.class, () -> analyzer.query(combined));
    }

    /** Materializing timestamps before the Unix epoch must neither stop early nor lose the final step identity. */
    public void testEvaluationGridCrossesUnixEpoch() {
        String query = "PROMQL index=k8s start=\"1969-12-31T23:59:00Z\" end=\"1970-01-01T00:01:00Z\" "
            + "step=1m result=(vector(1)) | LIMIT 10";
        for (EsqlFlags flags : List.of(EsqlFlags.DEFAULTS, EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(true))) {
            LogicalPlan plan = prototype().flags(flags).addK8s().query(query);
            assertConsistent(plan);
            Row grid = plan.collect(Row.class).getFirst();
            assertEquals(List.of(-60_000L, 0L, 60_000L), ((Literal) grid.fields().getFirst().child()).value());
            assertEquals(List.of("result", "step"), plan.output().stream().map(Attribute::name).toList());
        }
    }

    private static LogicalPlan analyze(String expression, String bounds) {
        LogicalPlan plan = prototype().addK8s().query("PROMQL index=k8s " + bounds + " result=(" + expression + ") | LIMIT 10");
        assertConsistent(plan);
        return plan;
    }

    private static TestAnalyzer prototype() {
        return analyzer().minimumTransportVersion(featureVersion()).flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(true));
    }

    private static TransportVersion featureVersion() {
        return TransportVersion.fromName("promql_functional_timeseries");
    }

    private static void assertConsistent(LogicalPlan plan) {
        Failures failures = new Failures();
        plan.forEachUp(node -> PlanConsistencyChecker.checkPlan(node, failures));
        assertFalse(failures.toString(), failures.hasFailures());
        assertTrue(plan.output().stream().allMatch(Attribute::resolved));
    }

    private static void assertContainsNaN(LogicalPlan plan) {
        var literals = new ArrayList<Literal>();
        plan.forEachExpressionDown(Literal.class, literals::add);
        assertTrue(literals.stream().anyMatch(literal -> literal.value() instanceof Double value && value.isNaN()));
    }

    private static void assertScalarDefaultSelection(LogicalPlan plan) {
        List<TopNBy> selections = plan.collect(TopNBy.class);
        assertEquals(1, selections.size());
        TopNBy selection = selections.getFirst();
        assertEquals(1, selection.limitPerGroup().fold(FoldContext.small()));
        assertEquals(1, selection.groupings().size());
        assertEquals(DataType.DATETIME, selection.groupings().getFirst().dataType());
        assertEquals(1, selection.order().size());
        Order order = selection.order().getFirst();
        assertEquals(Order.OrderDirection.ASC, order.direction());
        UnionAll union = (UnionAll) selection.child();
        Attribute step = union.output().stream().filter(selection.groupings().getFirst()::semanticEquals).findFirst().orElseThrow();
        Attribute priority = union.output().stream().filter(order.child()::semanticEquals).findFirst().orElseThrow();
        int stepColumn = union.output().indexOf(step);
        int priorityColumn = union.output().indexOf(priority);
        assertEquals(DataType.INTEGER, priority.dataType());
        assertEquals(2, union.children().size());
        for (int i = 0; i < union.children().size(); i++) {
            LogicalPlan branch = union.children().get(i);
            assertEquals(DataType.DATETIME, branch.output().get(stepColumn).dataType());
            var definitions = new ArrayList<Alias>();
            branch.forEachExpressionDown(Alias.class, definitions::add);
            Alias branchPriority = definitions.stream()
                .filter(alias -> alias.toAttribute().semanticEquals(branch.output().get(priorityColumn)))
                .findFirst()
                .orElseThrow();
            assertEquals(i, branchPriority.child().fold(FoldContext.small()));
            for (int column = 0; column < union.output().size(); column++) {
                assertFalse(union.output().get(column).semanticEquals(branch.output().get(column)));
            }
        }
        assertContainsNaN(union.children().getLast());
    }

    private static Aggregate assertHistogramScalar(LogicalPlan plan) {
        List<Aggregate> scalars = plan.collect(Aggregate.class)
            .stream()
            .filter(aggregate -> aggregate.aggregates().stream().anyMatch(expression -> expression.anyMatch(e -> e instanceof Scalar)))
            .toList();
        assertEquals(1, scalars.size());
        Aggregate aggregate = scalars.getFirst();
        var scalar = new ArrayList<Scalar>();
        aggregate.forEachExpressionDown(Scalar.class, scalar::add);
        assertEquals(1, scalar.size());
        Literal sample = (Literal) scalar.getFirst().field();
        assertEquals(DataType.DOUBLE, sample.dataType());
        assertNull(sample.value());
        Filter present = (Filter) aggregate.child();
        assertTrue(((IsNotNull) present.condition()).field().dataType().isHistogram());
        assertContainsNaN(plan);
        return aggregate;
    }

    private static void assertSourceClockOffset(LogicalPlan plan, Duration offset) {
        List<TimeSeriesAggregate> aggregates = plan.collect(TimeSeriesAggregate.class);
        assertFalse(aggregates.isEmpty());
        for (TimeSeriesAggregate aggregate : aggregates) {
            var definitions = new ArrayList<Alias>();
            aggregate.child().forEachExpressionDown(Alias.class, definitions::add);
            assertTrue(
                aggregate.toString(),
                definitions.stream()
                    .anyMatch(
                        alias -> alias.toAttribute().semanticEquals(aggregate.timestamp())
                            && alias.child() instanceof Add add
                            && add.children().stream().anyMatch(child -> child instanceof Literal literal && offset.equals(literal.value()))
                    )
            );
        }
    }
}
