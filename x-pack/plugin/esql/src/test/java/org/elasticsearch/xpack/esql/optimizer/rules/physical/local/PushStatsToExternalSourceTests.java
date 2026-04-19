/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
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
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.alias;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOrEqualOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

public class PushStatsToExternalSourceTests extends ESTestCase {

    private static final ReferenceAttribute AGE = referenceAttribute("age", DataType.INTEGER);
    private static final ReferenceAttribute SCORE = referenceAttribute("score", DataType.DOUBLE);
    private static final ReferenceAttribute SALARY = referenceAttribute("salary", DataType.INTEGER);

    public void testCountStarPushedDown() {
        var agg = aggregateExec(externalSource(statsMetadata(1000L, null, null, null)), countStarAlias());

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(1, local.output().size());
        Page page = local.supplier().get();
        assertNotNull(page);
        assertEquals(1, page.getPositionCount());
        assertEquals(1000L, as(page.getBlock(0), LongBlock.class).getLong(0));
    }

    public void testCountFieldPushedDown() {
        var agg = aggregateExec(externalSource(statsMetadata(1000L, "age", 50L, null)), countFieldAlias(AGE));

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(950L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    public void testCountFieldWithoutNullCountNotPushed() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 1000L);
        var agg = aggregateExec(externalSource(metadata), countFieldAlias(AGE));

        as(applyRule(agg), AggregateExec.class);
    }

    public void testMinPushedDown() {
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L, null);
        metadata.put("_stats.columns.age.min", 18);
        var agg = aggregateExec(externalSource(metadata), alias("m", new Min(Source.EMPTY, AGE)));

        as(applyRule(agg), LocalSourceExec.class);
    }

    public void testMaxPushedDown() {
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L, null);
        metadata.put("_stats.columns.age.max", 99);
        var agg = aggregateExec(externalSource(metadata), alias("m", new Max(Source.EMPTY, AGE)));

        as(applyRule(agg), LocalSourceExec.class);
    }

    public void testMultipleAggsPushedDown() {
        Map<String, Object> metadata = statsMetadata(500L, "score", 10L, null);
        metadata.put("_stats.columns.score.min", 1.0);
        metadata.put("_stats.columns.score.max", 100.0);

        var agg = aggregateExec(
            externalSource(metadata),
            countStarAlias(),
            alias("mn", new Min(Source.EMPTY, SCORE)),
            alias("mx", new Max(Source.EMPTY, SCORE))
        );

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(3, local.output().size());
    }

    public void testNotPushedWithGroupings() {
        ExternalSourceExec ext = externalSource(statsMetadata(1000L, null, null, null));
        ReferenceAttribute groupField = referenceAttribute("dept", DataType.KEYWORD);
        var agg = new AggregateExec(
            Source.EMPTY,
            ext,
            List.of(groupField),
            List.of(countStarAlias()),
            AggregatorMode.SINGLE,
            List.of(),
            null
        );

        as(applyRule(agg), AggregateExec.class);
    }

    public void testNotPushedWithoutStats() {
        var agg = aggregateExec(externalSource(Map.of()), countStarAlias());

        as(applyRule(agg), AggregateExec.class);
    }

    public void testNotPushedWithMultipleSplitsWithoutStats() {
        ExternalSourceExec ext = externalSourceWithSplits(statsMetadata(1000L, null, null, null), (Map<String, Object>[]) null);
        var agg = aggregateExec(ext, countStarAlias());

        as(applyRule(agg), AggregateExec.class);
    }

    public void testPushedWithMultipleSplitsWithStats() {
        ExternalSourceExec ext = externalSourceWithSplits(
            Map.of(),
            statsMetadata(100L, null, null, null),
            statsMetadata(200L, null, null, null),
            statsMetadata(300L, null, null, null)
        );
        var agg = aggregateExec(ext, countStarAlias());

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(600L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    public void testNotPushedWithMixedStatsAndNoStats() {
        ExternalSourceExec ext = externalSourceWithSplits(Map.of(), statsMetadata(100L, null, null, null), null);
        var agg = aggregateExec(ext, countStarAlias());

        as(applyRule(agg), AggregateExec.class);
    }

    public void testMinMaxPushedWithMultipleSplitsWithStats() {
        Map<String, Object> stats1 = splitStatsWithMinMax("age", 18, 50, 100L, 5L);
        Map<String, Object> stats2 = splitStatsWithMinMax("age", 22, 65, 200L, 10L);
        ExternalSourceExec ext = externalSourceWithSplits(Map.of(), stats1, stats2);

        var agg = aggregateExec(ext, alias("mn", new Min(Source.EMPTY, AGE)), alias("mx", new Max(Source.EMPTY, AGE)));

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(2, local.output().size());
        Page page = local.supplier().get();
        assertEquals(18, as(page.getBlock(0), IntBlock.class).getInt(0));
        assertEquals(65, as(page.getBlock(1), IntBlock.class).getInt(0));
    }

    public void testPushedWithSingleSplit() {
        ExternalSourceExec ext = externalSourceWithSplits(statsMetadata(1000L, null, null, null), (Map<String, Object>) null);
        var agg = aggregateExec(ext, countStarAlias());

        as(applyRule(agg), LocalSourceExec.class);
    }

    public void testMinWithoutStatsNotPushed() {
        var agg = aggregateExec(externalSource(statsMetadata(100L, null, null, null)), alias("m", new Min(Source.EMPTY, AGE)));

        as(applyRule(agg), AggregateExec.class);
    }

    public void testMaxWithoutStatsNotPushed() {
        var agg = aggregateExec(externalSource(statsMetadata(100L, null, null, null)), alias("m", new Max(Source.EMPTY, AGE)));

        as(applyRule(agg), AggregateExec.class);
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
        assertEquals(Long.valueOf(42L), SourceStatisticsSerializer.extractRowCount(enriched));
        assertEquals(Long.valueOf(5L), SourceStatisticsSerializer.extractColumnNullCount(enriched, "col1"));
        assertEquals(10, SourceStatisticsSerializer.extractColumnMin(enriched, "col1"));
        assertEquals(100, SourceStatisticsSerializer.extractColumnMax(enriched, "col1"));
    }

    // --- EvalExec intermediate node tests ---

    public void testCountStarPushedThroughEval() {
        EvalExec eval = evalWithSimpleAlias(externalSource(statsMetadata(1000L, "age", 0L, null)), "age_years", "age");
        var agg = aggregateExec(eval, countStarAlias());

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(1000L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    public void testCountFieldPushedThroughEval() {
        Alias evalAlias = alias("age_years", AGE);
        EvalExec eval = new EvalExec(Source.EMPTY, externalSource(statsMetadata(1000L, "age", 50L, null)), List.of(evalAlias));
        var agg = aggregateExec(eval, countFieldAlias((ReferenceAttribute) evalAlias.toAttribute()));

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(950L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    public void testMinPushedThroughEval() {
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L, null);
        metadata.put("_stats.columns.age.min", 18);
        Alias evalAlias = alias("age_years", AGE);
        EvalExec eval = new EvalExec(Source.EMPTY, externalSource(metadata), List.of(evalAlias));
        var agg = aggregateExec(eval, alias("m", new Min(Source.EMPTY, evalAlias.toAttribute())));

        as(applyRule(agg), LocalSourceExec.class);
    }

    public void testMaxPushedThroughEval() {
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L, null);
        metadata.put("_stats.columns.age.max", 99);
        Alias evalAlias = alias("age_years", AGE);
        EvalExec eval = new EvalExec(Source.EMPTY, externalSource(metadata), List.of(evalAlias));
        var agg = aggregateExec(eval, alias("m", new Max(Source.EMPTY, evalAlias.toAttribute())));

        as(applyRule(agg), LocalSourceExec.class);
    }

    public void testMultipleAggsPushedThroughEval() {
        Map<String, Object> metadata = statsMetadata(500L, "age", 10L, null);
        metadata.put("_stats.columns.age.min", 1);
        metadata.put("_stats.columns.age.max", 100);
        Alias evalAlias = alias("age_years", AGE);
        EvalExec eval = new EvalExec(Source.EMPTY, externalSource(metadata), List.of(evalAlias));
        ReferenceAttribute aliasedField = (ReferenceAttribute) evalAlias.toAttribute();
        var agg = aggregateExec(
            eval,
            countStarAlias(),
            alias("mn", new Min(Source.EMPTY, aliasedField)),
            alias("mx", new Max(Source.EMPTY, aliasedField))
        );

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(3, local.output().size());
    }

    public void testNotPushedThroughEvalWithComputedExpression() {
        Map<String, Object> metadata = statsMetadata(100L, "age", 0L, null);
        metadata.put("_stats.columns.age.min", 18);
        Alias computedAlias = alias("computed_val", new Abs(Source.EMPTY, AGE));
        EvalExec eval = new EvalExec(Source.EMPTY, externalSource(metadata), List.of(computedAlias));
        var agg = aggregateExec(eval, alias("m", new Min(Source.EMPTY, computedAlias.toAttribute())));

        as(applyRule(agg), AggregateExec.class);
    }

    public void testNotPushedWithGroupingsThroughEval() {
        Alias evalAlias = alias("age_years", AGE);
        EvalExec eval = new EvalExec(Source.EMPTY, externalSource(statsMetadata(1000L, null, null, null)), List.of(evalAlias));
        ReferenceAttribute groupField = (ReferenceAttribute) evalAlias.toAttribute();
        var agg = new AggregateExec(
            Source.EMPTY,
            eval,
            List.of(groupField),
            List.of(countStarAlias()),
            AggregatorMode.SINGLE,
            List.of(),
            null
        );

        as(applyRule(agg), AggregateExec.class);
    }

    // --- ProjectExec intermediate node tests ---

    public void testMinPushedThroughProject() {
        Map<String, Object> metadata = statsMetadata(100L, "salary", 0L, null);
        metadata.put("_stats.columns.salary.min", 30000);
        Alias renameAlias = alias("pay", SALARY);
        ProjectExec project = new ProjectExec(Source.EMPTY, externalSource(metadata), List.of(renameAlias));
        var agg = aggregateExec(project, alias("m", new Min(Source.EMPTY, renameAlias.toAttribute())));

        as(applyRule(agg), LocalSourceExec.class);
    }

    public void testMultipleAggsPushedThroughProject() {
        Map<String, Object> metadata = statsMetadata(100L, "salary", 0L, null);
        metadata.put("_stats.columns.salary.min", 30000);
        metadata.put("_stats.columns.salary.max", 150000);
        Alias renameAlias = alias("pay", SALARY);
        ProjectExec project = new ProjectExec(Source.EMPTY, externalSource(metadata), List.of(renameAlias));
        ReferenceAttribute payRef = (ReferenceAttribute) renameAlias.toAttribute();
        var agg = aggregateExec(project, alias("mn", new Min(Source.EMPTY, payRef)), alias("mx", new Max(Source.EMPTY, payRef)));

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(2, local.output().size());
    }

    public void testCountStarPushedThroughProject() {
        Alias renameAlias = alias("pay", SALARY);
        ProjectExec project = new ProjectExec(Source.EMPTY, externalSource(statsMetadata(1000L, null, null, null)), List.of(renameAlias));
        var agg = aggregateExec(project, countStarAlias());

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(1000L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    // --- filter tests ---

    public void testCountPushedThroughOrFilterAllResolve() {
        Map<String, Object> split1 = splitStatsWithMinMax("age", 30L, 50L, 500L, 0L);
        Map<String, Object> split2 = splitStatsWithMinMax("age", 60L, 80L, 500L, 0L);
        ExternalSourceExec ext = externalSourceWithSplits(Map.of(), split1, split2);
        Expression filterCondition = new Or(Source.EMPTY, greaterThanOf(AGE, of(20L)), lessThanOrEqualOf(AGE, of(90L)));
        var agg = aggregateExec(new FilterExec(Source.EMPTY, ext, filterCondition), countStarAlias());

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(1000L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    public void testCountPushedThroughNotFilter() {
        Map<String, Object> split1 = splitStatsWithMinMax("age", 30L, 50L, 500L, 0L);
        Map<String, Object> split2 = splitStatsWithMinMax("age", 60L, 80L, 500L, 0L);
        ExternalSourceExec ext = externalSourceWithSplits(Map.of(), split1, split2);
        Expression filterCondition = new Not(Source.EMPTY, greaterThanOf(AGE, of(20L)));
        var agg = aggregateExec(new FilterExec(Source.EMPTY, ext, filterCondition), countStarAlias());

        LocalSourceExec local = as(applyRule(agg), LocalSourceExec.class);
        assertEquals(0L, as(local.supplier().get().getBlock(0), LongBlock.class).getLong(0));
    }

    // --- helpers ---

    @SafeVarargs
    @SuppressWarnings("varargs")
    private static ExternalSourceExec externalSourceWithSplits(Map<String, Object> sourceMetadata, Map<String, Object>... perSplitStats) {
        List<ExternalSplit> splits = new ArrayList<>(perSplitStats == null ? 1 : perSplitStats.length);
        if (perSplitStats == null) {
            splits.add(fileSplit(0, null));
            splits.add(fileSplit(1, null));
        } else {
            for (int i = 0; i < perSplitStats.length; i++) {
                splits.add(fileSplit(i, perSplitStats[i]));
            }
        }
        return new ExternalSourceExec(
            Source.EMPTY,
            "file:///test.parquet",
            "parquet",
            defaultAttrs(),
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
        return new ExternalSourceExec(Source.EMPTY, "file:///test.parquet", "parquet", defaultAttrs(), Map.of(), sourceMetadata, null);
    }

    private static List<Attribute> defaultAttrs() {
        return List.of(referenceAttribute("x", DataType.INTEGER), AGE, SCORE, SALARY);
    }

    private static FileSplit fileSplit(int index, Map<String, Object> stats) {
        return new FileSplit(
            "parquet",
            StoragePath.of("file:///split" + (index + 1) + ".parquet"),
            0,
            100,
            "parquet",
            Map.of(),
            Map.of(),
            null,
            stats
        );
    }

    private static AggregateExec aggregateExec(PhysicalPlan child, NamedExpression... aggregates) {
        return new AggregateExec(Source.EMPTY, child, List.of(), List.of(aggregates), AggregatorMode.SINGLE, List.of(), null);
    }

    private static EvalExec evalWithSimpleAlias(ExternalSourceExec child, String aliasName, String originalName) {
        return new EvalExec(Source.EMPTY, child, List.of(alias(aliasName, referenceAttribute(originalName, DataType.INTEGER))));
    }

    private static Alias countStarAlias() {
        return alias("c", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")));
    }

    private static Alias countFieldAlias(ReferenceAttribute field) {
        return alias("c", new Count(Source.EMPTY, field));
    }

    private static Map<String, Object> splitStatsWithMinMax(String colName, Object min, Object max, long rowCount, long nullCount) {
        Map<String, Object> metadata = statsMetadata(rowCount, colName, nullCount, null);
        metadata.put("_stats.columns." + colName + ".min", min);
        metadata.put("_stats.columns." + colName + ".max", max);
        return metadata;
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
        return new PushStatsToExternalSource().apply(agg);
    }
}
