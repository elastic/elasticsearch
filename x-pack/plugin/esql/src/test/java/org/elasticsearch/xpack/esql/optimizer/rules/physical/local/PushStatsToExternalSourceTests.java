/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
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
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public void testNotPushedWithMultipleSplits() {
        Map<String, Object> metadata = statsMetadata(1000L, null, null, null);
        List<Attribute> attrs = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.INTEGER));
        ExternalSourceExec ext = new ExternalSourceExec(
            Source.EMPTY,
            "file:///test.parquet",
            "parquet",
            attrs,
            Map.of(),
            metadata,
            null,
            -1,
            null,
            null,
            List.of(
                new FileSplit("parquet", StoragePath.of("file:///split1.parquet"), 0, 100, "parquet", Map.of(), Map.of()),
                new FileSplit("parquet", StoragePath.of("file:///split2.parquet"), 0, 100, "parquet", Map.of(), Map.of())
            )
        );
        AggregateExec agg = aggregateExec(ext, countStarAlias());

        PhysicalPlan result = applyRule(agg);

        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testPushedWithSingleSplit() {
        Map<String, Object> metadata = statsMetadata(1000L, null, null, null);
        List<Attribute> attrs = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.INTEGER));
        ExternalSourceExec ext = new ExternalSourceExec(
            Source.EMPTY,
            "file:///test.parquet",
            "parquet",
            attrs,
            Map.of(),
            metadata,
            null,
            -1,
            null,
            null,
            List.of(new FileSplit("parquet", StoragePath.of("file:///test.parquet"), 0, 100, "parquet", Map.of(), Map.of()))
        );
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

        Map<String, Object> enriched = SourceStatisticsSerializer.embedStatistics(
            original,
            new org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics() {
                @Override
                public java.util.OptionalLong rowCount() {
                    return java.util.OptionalLong.of(42);
                }

                @Override
                public java.util.OptionalLong sizeInBytes() {
                    return java.util.OptionalLong.of(1024);
                }

                @Override
                public
                    java.util.Optional<Map<String, org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics.ColumnStatistics>>
                    columnStatistics() {
                    return java.util.Optional.of(
                        Map.of("col1", new org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics.ColumnStatistics() {
                            @Override
                            public java.util.OptionalLong nullCount() {
                                return java.util.OptionalLong.of(5);
                            }

                            @Override
                            public java.util.OptionalLong distinctCount() {
                                return java.util.OptionalLong.empty();
                            }

                            @Override
                            public java.util.Optional<Object> minValue() {
                                return java.util.Optional.of(10);
                            }

                            @Override
                            public java.util.Optional<Object> maxValue() {
                                return java.util.Optional.of(100);
                            }
                        })
                    );
                }
            }
        );

        assertEquals("value", enriched.get("existing_key"));
        assertEquals(42L, enriched.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertEquals(1024L, enriched.get(SourceStatisticsSerializer.STATS_SIZE_BYTES));
        assertEquals(java.util.OptionalLong.of(42), SourceStatisticsSerializer.extractRowCount(enriched));
        assertEquals(java.util.OptionalLong.of(5), SourceStatisticsSerializer.extractColumnNullCount(enriched, "col1"));
        assertEquals(java.util.Optional.of(10), SourceStatisticsSerializer.extractColumnMin(enriched, "col1"));
        assertEquals(java.util.Optional.of(100), SourceStatisticsSerializer.extractColumnMax(enriched, "col1"));
    }

    // --- helpers ---

    private static ExternalSourceExec externalSource(Map<String, Object> sourceMetadata) {
        List<Attribute> attrs = List.of(
            new ReferenceAttribute(Source.EMPTY, "x", DataType.INTEGER),
            new ReferenceAttribute(Source.EMPTY, "age", DataType.INTEGER),
            new ReferenceAttribute(Source.EMPTY, "score", DataType.DOUBLE)
        );
        return new ExternalSourceExec(Source.EMPTY, "file:///test.parquet", "parquet", attrs, Map.of(), sourceMetadata, null);
    }

    private static AggregateExec aggregateExec(ExternalSourceExec child, NamedExpression... aggregates) {
        return new AggregateExec(Source.EMPTY, child, List.of(), List.of(aggregates), AggregatorMode.SINGLE, List.of(), null);
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
