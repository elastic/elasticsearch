/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.hamcrest.Matchers.instanceOf;

public class PushStatsToExternalSourceTests extends ESTestCase {

    public void testCountStarPushedDown() {
        Map<String, Object> metadata = statsMetadata(1000L, null, null, null);
        AggregateExec agg = aggregateExec(externalSource(metadata), countStarAlias());

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
        LocalSourceExec local = (LocalSourceExec) result;
        assertEquals(1, local.output().size());
        Page page = local.supplier().get();
        assertNotNull(page);
        assertEquals(1, page.getPositionCount());
        Block block = page.getBlock(0);
        assertThat(block, instanceOf(LongBlock.class));
        assertEquals(1000L, ((LongBlock) block).getLong(0));
    }

    public void testCountFieldPushedDown() {
        Map<String, Object> metadata = statsMetadata(1000L, "age", 50L, null);
        ReferenceAttribute field = new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER);
        AggregateExec agg = aggregateExec(externalSource(metadata), countFieldAlias(field));

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
        LocalSourceExec local = (LocalSourceExec) result;
        Page page = local.supplier().get();
        assertEquals(950L, ((LongBlock) page.getBlock(0)).getLong(0));
    }

    public void testCountFieldWithoutNullCountNotPushed() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 1000L);
        ReferenceAttribute field = new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER);
        AggregateExec agg = aggregateExec(externalSource(metadata), countFieldAlias(field));

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testMinPushedDown() {
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L, null);
        metadata.put("_stats.columns.age.min", 18);
        ReferenceAttribute field = new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER);
        Alias minAlias = new Alias(Source.EMPTY, "m", new Min(Source.EMPTY, field));
        AggregateExec agg = aggregateExec(externalSource(metadata), minAlias);

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
    }

    public void testMaxPushedDown() {
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L, null);
        metadata.put("_stats.columns.age.max", 99);
        ReferenceAttribute field = new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER);
        Alias maxAlias = new Alias(Source.EMPTY, "m", new Max(Source.EMPTY, field));
        AggregateExec agg = aggregateExec(externalSource(metadata), maxAlias);

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
    }

    public void testMultipleAggsPushedDown() {
        Map<String, Object> metadata = statsMetadata(500L, "score", 10L, null);
        metadata.put("_stats.columns.score.min", 1.0);
        metadata.put("_stats.columns.score.max", 100.0);

        ReferenceAttribute field = new ReferenceAttribute(Source.EMPTY, "score", DataType.DOUBLE);
        Alias countAlias = countStarAlias();
        Alias minAlias = new Alias(Source.EMPTY, "mn", new Min(Source.EMPTY, field));
        Alias maxAlias = new Alias(Source.EMPTY, "mx", new Max(Source.EMPTY, field));

        AggregateExec agg = aggregateExec(externalSource(metadata), countAlias, minAlias, maxAlias);

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
        LocalSourceExec local = (LocalSourceExec) result;
        assertEquals(3, local.output().size());
    }

    public void testNotPushedWithGroupings() {
        Map<String, Object> metadata = statsMetadata(1000L, null, null, null);
        ExternalSourceExec ext = externalSource(metadata);
        ReferenceAttribute groupField = new ReferenceAttribute(Source.EMPTY, "dept", DataType.KEYWORD);
        AggregateExec agg = new AggregateExec(
            Source.EMPTY,
            ext,
            List.of(groupField),
            List.of(countStarAlias()),
            AggregatorMode.SINGLE,
            List.of(),
            null
        );

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testNotPushedWithoutStats() {
        Map<String, Object> metadata = Map.of();
        AggregateExec agg = aggregateExec(externalSource(metadata), countStarAlias());

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testNotPushedWithMultipleSplitsWithoutStats() {
        Map<String, Object> metadata = statsMetadata(1000L, null, null, null);
        ExternalSourceExec ext = externalSourceWithSplits(metadata, null, null);
        AggregateExec agg = aggregateExec(ext, countStarAlias());

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testPushedWithMultipleSplitsWithStats() {
        Map<String, Object> stats1 = statsMetadata(100L, null, null, null);
        Map<String, Object> stats2 = statsMetadata(200L, null, null, null);
        Map<String, Object> stats3 = statsMetadata(300L, null, null, null);
        ExternalSourceExec ext = externalSourceWithSplits(Map.of(), stats1, stats2, stats3);
        AggregateExec agg = aggregateExec(ext, countStarAlias());

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
        LocalSourceExec local = (LocalSourceExec) result;
        Page page = local.supplier().get();
        assertEquals(600L, ((LongBlock) page.getBlock(0)).getLong(0));
    }

    public void testNotPushedWithMixedStatsAndNoStats() {
        Map<String, Object> stats1 = statsMetadata(100L, null, null, null);
        ExternalSourceExec ext = externalSourceWithSplits(Map.of(), stats1, null);
        AggregateExec agg = aggregateExec(ext, countStarAlias());

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testMinMaxPushedWithMultipleSplitsWithStats() {
        Map<String, Object> stats1 = statsMetadata(100L, "age", 5L, null);
        stats1.put("_stats.columns.age.min", 18);
        stats1.put("_stats.columns.age.max", 50);

        Map<String, Object> stats2 = statsMetadata(200L, "age", 10L, null);
        stats2.put("_stats.columns.age.min", 22);
        stats2.put("_stats.columns.age.max", 65);

        ExternalSourceExec ext = externalSourceWithSplits(Map.of(), stats1, stats2);

        ReferenceAttribute ageField = new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER);
        AggregateExec agg = aggregateExec(
            ext,
            new Alias(Source.EMPTY, "mn", new Min(Source.EMPTY, ageField)),
            new Alias(Source.EMPTY, "mx", new Max(Source.EMPTY, ageField))
        );

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
        LocalSourceExec local = (LocalSourceExec) result;
        assertEquals(2, local.output().size());
        Page page = local.supplier().get();
        assertEquals(18, ((IntBlock) page.getBlock(0)).getInt(0));
        assertEquals(65, ((IntBlock) page.getBlock(1)).getInt(0));
    }

    public void testPushedWithSingleSplit() {
        Map<String, Object> metadata = statsMetadata(1000L, null, null, null);
        ExternalSourceExec ext = externalSourceWithSplits(metadata, (Map<String, Object>) null);
        AggregateExec agg = aggregateExec(ext, countStarAlias());

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
    }

    public void testMinWithoutStatsNotPushed() {
        Map<String, Object> metadata = statsMetadata(100L, null, null, null);
        ReferenceAttribute field = new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER);
        Alias minAlias = new Alias(Source.EMPTY, "m", new Min(Source.EMPTY, field));
        AggregateExec agg = aggregateExec(externalSource(metadata), minAlias);

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testMaxWithoutStatsNotPushed() {
        Map<String, Object> metadata = statsMetadata(100L, null, null, null);
        ReferenceAttribute field = new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER);
        Alias maxAlias = new Alias(Source.EMPTY, "m", new Max(Source.EMPTY, field));
        AggregateExec agg = aggregateExec(externalSource(metadata), maxAlias);

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testSerializerRoundTrip() {
        Map<String, Object> original = new HashMap<>();
        original.put("existing_key", "value");

        Map<String, Object> enriched = SourceStatisticsSerializer.embedStatistics(original, new SourceStatistics() {
            @Override
            public OptionalLong rowCount() {
                return OptionalLong.of(42);
            }

            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.of(1024);
            }

            @Override
            public Optional<Map<String, SourceStatistics.ColumnStatistics>> columnStatistics() {
                return Optional.of(Map.of("col1", new SourceStatistics.ColumnStatistics() {
                    @Override
                    public OptionalLong nullCount() {
                        return OptionalLong.of(5);
                    }

                    @Override
                    public OptionalLong distinctCount() {
                        return OptionalLong.empty();
                    }

                    @Override
                    public Optional<Object> minValue() {
                        return Optional.of(10);
                    }

                    @Override
                    public Optional<Object> maxValue() {
                        return Optional.of(100);
                    }
                }));
            }
        });

        assertEquals("value", enriched.get("existing_key"));
        assertEquals(42L, enriched.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertEquals(1024L, enriched.get(SourceStatisticsSerializer.STATS_SIZE_BYTES));
        assertEquals(OptionalLong.of(42), SourceStatisticsSerializer.extractRowCount(enriched));
        assertEquals(OptionalLong.of(5), SourceStatisticsSerializer.extractColumnNullCount(enriched, "col1"));
        assertEquals(Optional.of(10), SourceStatisticsSerializer.extractColumnMin(enriched, "col1"));
        assertEquals(Optional.of(100), SourceStatisticsSerializer.extractColumnMax(enriched, "col1"));
    }

    // --- EvalExec intermediate node tests ---

    /**
     * EVAL age_years = age | STATS count(*)
     * count(*) doesn't reference any aliased column so it should still push down.
     */
    public void testCountStarPushedThroughEval() {
        Map<String, Object> metadata = statsMetadata(1000L, "age", 0L, null);
        EvalExec eval = evalWithSimpleAlias(externalSource(metadata), "age_years", "age");
        AggregateExec agg = aggregateExec(eval, countStarAlias());

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
        LocalSourceExec local = (LocalSourceExec) result;
        Page page = local.supplier().get();
        assertEquals(1000L, ((LongBlock) page.getBlock(0)).getLong(0));
    }

    /**
     * EVAL age_years = age | STATS count(age_years)
     * count(age_years) references an alias for "age"; should resolve and push.
     */
    public void testCountFieldPushedThroughEval() {
        Map<String, Object> metadata = statsMetadata(1000L, "age", 50L, null);
        ReferenceAttribute ageRef = new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER);
        Alias evalAlias = new Alias(Source.EMPTY, "age_years", ageRef);
        EvalExec eval = new EvalExec(Source.EMPTY, externalSource(metadata), List.of(evalAlias));
        ReferenceAttribute aliasedField = (ReferenceAttribute) evalAlias.toAttribute();
        AggregateExec agg = aggregateExec(eval, countFieldAlias(aliasedField));

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
        LocalSourceExec local = (LocalSourceExec) result;
        Page page = local.supplier().get();
        assertEquals(950L, ((LongBlock) page.getBlock(0)).getLong(0));
    }

    /**
     * EVAL age_years = age | STATS min(age_years)
     * min(age_years) should resolve to min(age) via alias and push.
     */
    public void testMinPushedThroughEval() {
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L, null);
        metadata.put("_stats.columns.age.min", 18);
        ReferenceAttribute ageRef = new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER);
        Alias evalAlias = new Alias(Source.EMPTY, "age_years", ageRef);
        EvalExec eval = new EvalExec(Source.EMPTY, externalSource(metadata), List.of(evalAlias));
        ReferenceAttribute aliasedField = (ReferenceAttribute) evalAlias.toAttribute();
        AggregateExec agg = aggregateExec(eval, new Alias(Source.EMPTY, "m", new Min(Source.EMPTY, aliasedField)));

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
    }

    /**
     * EVAL age_years = age | STATS max(age_years)
     * max(age_years) should resolve to max(age) via alias and push.
     */
    public void testMaxPushedThroughEval() {
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L, null);
        metadata.put("_stats.columns.age.max", 99);
        ReferenceAttribute ageRef = new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER);
        Alias evalAlias = new Alias(Source.EMPTY, "age_years", ageRef);
        EvalExec eval = new EvalExec(Source.EMPTY, externalSource(metadata), List.of(evalAlias));
        ReferenceAttribute aliasedField = (ReferenceAttribute) evalAlias.toAttribute();
        AggregateExec agg = aggregateExec(eval, new Alias(Source.EMPTY, "m", new Max(Source.EMPTY, aliasedField)));

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
    }

    /**
     * EVAL age_years = age | STATS count(*), min(age_years), max(age_years)
     * All three aggregates should resolve through the alias and push.
     */
    public void testMultipleAggsPushedThroughEval() {
        Map<String, Object> metadata = statsMetadata(500L, "age", 10L, null);
        metadata.put("_stats.columns.age.min", 1);
        metadata.put("_stats.columns.age.max", 100);
        ReferenceAttribute ageRef = new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER);
        Alias evalAlias = new Alias(Source.EMPTY, "age_years", ageRef);
        EvalExec eval = new EvalExec(Source.EMPTY, externalSource(metadata), List.of(evalAlias));
        ReferenceAttribute aliasedField = (ReferenceAttribute) evalAlias.toAttribute();
        AggregateExec agg = aggregateExec(
            eval,
            countStarAlias(),
            new Alias(Source.EMPTY, "mn", new Min(Source.EMPTY, aliasedField)),
            new Alias(Source.EMPTY, "mx", new Max(Source.EMPTY, aliasedField))
        );

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
        LocalSourceExec local = (LocalSourceExec) result;
        assertEquals(3, local.output().size());
    }

    /**
     * EVAL computed_val = abs(age) | STATS min(computed_val)
     * The eval field is not a simple attribute alias (it's a function call),
     * so no alias map entry is created and pushdown should fail.
     */
    public void testNotPushedThroughEvalWithComputedExpression() {
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L, null);
        metadata.put("_stats.columns.age.min", 18);
        ExternalSourceExec ext = externalSource(metadata);
        ReferenceAttribute ageRef = new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER);
        Alias computedAlias = new Alias(Source.EMPTY, "computed_val", new Abs(Source.EMPTY, ageRef));
        EvalExec eval = new EvalExec(Source.EMPTY, ext, List.of(computedAlias));
        ReferenceAttribute computedRef = (ReferenceAttribute) computedAlias.toAttribute();
        AggregateExec agg = aggregateExec(eval, new Alias(Source.EMPTY, "m", new Min(Source.EMPTY, computedRef)));

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(AggregateExec.class));
    }

    /**
     * EVAL age_years = age | STATS count(*) BY age_years
     * Grouped aggregates should not be pushed regardless of intermediate nodes.
     */
    public void testNotPushedWithGroupingsThroughEval() {
        Map<String, Object> metadata = statsMetadata(1000L, null, null, null);
        ReferenceAttribute ageRef = new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER);
        Alias evalAlias = new Alias(Source.EMPTY, "age_years", ageRef);
        EvalExec eval = new EvalExec(Source.EMPTY, externalSource(metadata), List.of(evalAlias));
        ReferenceAttribute groupField = (ReferenceAttribute) evalAlias.toAttribute();
        AggregateExec agg = new AggregateExec(
            Source.EMPTY,
            eval,
            List.of(groupField),
            List.of(countStarAlias()),
            AggregatorMode.SINGLE,
            List.of(),
            null
        );

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(AggregateExec.class));
    }

    // --- ProjectExec intermediate node tests ---

    /**
     * RENAME salary AS pay | STATS min(pay)
     * ProjectExec renames salary to pay; min(pay) should resolve to min(salary).
     */
    public void testMinPushedThroughProject() {
        Map<String, Object> metadata = statsMetadata(100L, "salary", 0L, null);
        metadata.put("_stats.columns.salary.min", 30000);
        ExternalSourceExec ext = externalSource(metadata);
        ReferenceAttribute salaryRef = new ReferenceAttribute(Source.EMPTY, "salary", DataType.INTEGER);
        Alias renameAlias = new Alias(Source.EMPTY, "pay", salaryRef);
        ProjectExec project = new ProjectExec(Source.EMPTY, ext, List.of(renameAlias));
        ReferenceAttribute payRef = (ReferenceAttribute) renameAlias.toAttribute();
        AggregateExec agg = aggregateExec(project, new Alias(Source.EMPTY, "m", new Min(Source.EMPTY, payRef)));

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
    }

    /**
     * RENAME salary AS pay | STATS min(pay), max(pay)
     * Both aggregates should resolve through the project rename.
     */
    public void testMultipleAggsPushedThroughProject() {
        Map<String, Object> metadata = statsMetadata(100L, "salary", 0L, null);
        metadata.put("_stats.columns.salary.min", 30000);
        metadata.put("_stats.columns.salary.max", 150000);
        ExternalSourceExec ext = externalSource(metadata);
        ReferenceAttribute salaryRef = new ReferenceAttribute(Source.EMPTY, "salary", DataType.INTEGER);
        Alias renameAlias = new Alias(Source.EMPTY, "pay", salaryRef);
        ProjectExec project = new ProjectExec(Source.EMPTY, ext, List.of(renameAlias));
        ReferenceAttribute payRef = (ReferenceAttribute) renameAlias.toAttribute();
        AggregateExec agg = aggregateExec(
            project,
            new Alias(Source.EMPTY, "mn", new Min(Source.EMPTY, payRef)),
            new Alias(Source.EMPTY, "mx", new Max(Source.EMPTY, payRef))
        );

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
        LocalSourceExec local = (LocalSourceExec) result;
        assertEquals(2, local.output().size());
    }

    /**
     * RENAME salary AS pay | STATS count(*)
     * count(*) doesn't reference columns, so project intermediate should not prevent pushdown.
     */
    public void testCountStarPushedThroughProject() {
        Map<String, Object> metadata = statsMetadata(1000L, null, null, null);
        ExternalSourceExec ext = externalSource(metadata);
        ReferenceAttribute salaryRef = new ReferenceAttribute(Source.EMPTY, "salary", DataType.INTEGER);
        Alias renameAlias = new Alias(Source.EMPTY, "pay", salaryRef);
        ProjectExec project = new ProjectExec(Source.EMPTY, ext, List.of(renameAlias));
        AggregateExec agg = aggregateExec(project, countStarAlias());

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(LocalSourceExec.class));
        LocalSourceExec local = (LocalSourceExec) result;
        Page page = local.supplier().get();
        assertEquals(1000L, ((LongBlock) page.getBlock(0)).getLong(0));
    }

    // --- helpers ---

    @SafeVarargs
    @SuppressWarnings("varargs")
    private static ExternalSourceExec externalSourceWithSplits(Map<String, Object> sourceMetadata, Map<String, Object>... perSplitStats) {
        List<Attribute> attrs = List.of(
            new ReferenceAttribute(Source.EMPTY, "x", DataType.INTEGER),
            new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER)
        );
        List<ExternalSplit> splits = new ArrayList<>(perSplitStats.length);
        for (int i = 0; i < perSplitStats.length; i++) {
            splits.add(
                new FileSplit(
                    "parquet",
                    StoragePath.of("file:///split" + (i + 1) + ".parquet"),
                    0,
                    100,
                    "parquet",
                    Map.of(),
                    Map.of(),
                    null,
                    perSplitStats[i]
                )
            );
        }
        return new ExternalSourceExec(
            Source.EMPTY,
            "file:///test.parquet",
            "parquet",
            attrs,
            Map.of(),
            sourceMetadata,
            null,
            -1,
            null,
            null,
            splits
        );
    }

    private static ExternalSourceExec externalSource(Map<String, Object> sourceMetadata) {
        List<Attribute> attrs = List.of(
            new ReferenceAttribute(Source.EMPTY, "x", DataType.INTEGER),
            new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER),
            new ReferenceAttribute(Source.EMPTY, "score", DataType.DOUBLE),
            new ReferenceAttribute(Source.EMPTY, "salary", DataType.INTEGER)
        );
        return new ExternalSourceExec(Source.EMPTY, "file:///test.parquet", "parquet", attrs, Map.of(), sourceMetadata, null);
    }

    private static AggregateExec aggregateExec(PhysicalPlan child, NamedExpression... aggregates) {
        return new AggregateExec(Source.EMPTY, child, List.of(), List.of(aggregates), AggregatorMode.SINGLE, List.of(), null);
    }

    private static EvalExec evalWithSimpleAlias(ExternalSourceExec child, String aliasName, String originalName) {
        ReferenceAttribute originalRef = new ReferenceAttribute(Source.EMPTY, originalName, DataType.INTEGER);
        Alias alias = new Alias(Source.EMPTY, aliasName, originalRef);
        return new EvalExec(Source.EMPTY, child, List.of(alias));
    }

    private static Alias countStarAlias() {
        return new Alias(Source.EMPTY, "c", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")));
    }

    private static Alias countFieldAlias(ReferenceAttribute field) {
        return new Alias(Source.EMPTY, "c", new Count(Source.EMPTY, field));
    }

    private static Map<String, Object> statsMetadata(Long rowCount, String colName, Long nullCount, Long sizeBytes) {
        Map<String, Object> metadata = new HashMap<>();
        if (rowCount != null) {
            metadata.put(SourceStatisticsSerializer.STATS_ROW_COUNT, rowCount);
        }
        if (sizeBytes != null) {
            metadata.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, sizeBytes);
        }
        if (colName != null && nullCount != null) {
            metadata.put("_stats.columns." + colName + ".null_count", nullCount);
        }
        return metadata;
    }

    private static PhysicalPlan applyRule(AggregateExec agg) {
        PushStatsToExternalSource rule = new PushStatsToExternalSource();
        return rule.apply(agg);
    }
}
