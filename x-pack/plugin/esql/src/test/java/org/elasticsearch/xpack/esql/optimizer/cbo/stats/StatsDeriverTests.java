/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.stats;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class StatsDeriverTests extends ESTestCase {
    public void testExternalStatisticsPopulateRelationAndColumnEstimates() {
        Attribute field = attribute("value");
        SourceStatistics.ColumnStatistics column = columnStatistics(20, 40, 80, 800, 1L, 99L);
        ExternalRelation relation = externalRelation(field, sourceStatistics(100, 1_200, Map.of("value", column)));

        PlanStats stats = new StatsDeriver().derive(relation).get(relation);

        assertEquals(100, stats.rows().value(), 0);
        assertEquals(1_200, stats.bytes().value(), 0);
        assertEquals(12, stats.avgRowBytes().value(), 0);
        assertEquals(40, stats.column(field).ndv().value(), 0);
        assertEquals(0.2, stats.column(field).nullFraction().value(), 0);
        assertEquals(10, stats.column(field).averageBytes().value(), 0);
        assertEquals(0.8, stats.column(field).averageValuesPerRow().value(), 0);
        assertEquals(Optional.of(1L), stats.column(field).minValue());
        assertEquals(Optional.of(99L), stats.column(field).maxValue());
    }

    public void testSampleWidthDoesNotBecomeTotalCardinality() {
        Attribute field = attribute("value");
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(
            List.of(field),
            "sampled",
            "path",
            null,
            null,
            Map.of(SourceMetadata.SAMPLE_BYTES_KEY, 500L, SourceMetadata.SAMPLE_ROWS_KEY, 10),
            Map.of()
        );
        ExternalRelation relation = new ExternalRelation(Source.EMPTY, "path", metadata, List.of(field), null, Map.of());

        PlanStats stats = new StatsDeriver().derive(relation).get(relation);

        assertTrue(stats.rows().isUnknown());
        assertTrue(stats.bytes().isUnknown());
        assertEquals(50, stats.avgRowBytes().value(), 0);
        assertEquals(Provenance.SAMPLED, stats.avgRowBytes().provenance());
    }

    public void testLocalRelationDoesNotInvokeItsSupplier() {
        LocalSupplier supplier = mock(LocalSupplier.class);
        LocalRelation relation = new LocalRelation(Source.EMPTY, List.of(attribute("value")), supplier);

        PlanStats stats = new StatsDeriver().derive(relation).get(relation);

        assertTrue(stats.rows().isUnknown());
        verifyNoInteractions(supplier);
    }

    public void testAggregateRowsUseKnownGroupingNdv() {
        Attribute field = attribute("value");
        ExternalRelation input = externalRelation(
            field,
            sourceStatistics(100, 1_200, Map.of("value", columnStatistics(0, 7, 100, 800, 1L, 99L)))
        );
        Aggregate aggregate = new Aggregate(Source.EMPTY, input, List.of(field), List.of(field));

        Map<LogicalPlan, PlanStats> statistics = new StatsDeriver().derive(aggregate);

        assertEquals(7, statistics.get(aggregate).rows().value(), 0);
        assertEquals(7, statistics.get(aggregate).column(field).ndv().value(), 0);
    }

    private static Attribute attribute(String name) {
        return new ReferenceAttribute(Source.EMPTY, name, DataType.LONG);
    }

    private static ExternalRelation externalRelation(Attribute field, SourceStatistics statistics) {
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(List.of(field), "test", "path", statistics, List.of());
        return new ExternalRelation(Source.EMPTY, "path", metadata, List.of(field), null, Map.of());
    }

    private static SourceStatistics sourceStatistics(
        long rowCount,
        long sizeInBytes,
        Map<String, SourceStatistics.ColumnStatistics> columns
    ) {
        return new SourceStatistics() {
            @Override
            public OptionalLong rowCount() {
                return OptionalLong.of(rowCount);
            }

            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.of(sizeInBytes);
            }

            @Override
            public Optional<Map<String, ColumnStatistics>> columnStatistics() {
                return Optional.of(columns);
            }
        };
    }

    private static SourceStatistics.ColumnStatistics columnStatistics(
        long nullCount,
        long distinctCount,
        long valueCount,
        long sizeInBytes,
        Object min,
        Object max
    ) {
        return new SourceStatistics.ColumnStatistics() {
            @Override
            public OptionalLong nullCount() {
                return OptionalLong.of(nullCount);
            }

            @Override
            public OptionalLong distinctCount() {
                return OptionalLong.of(distinctCount);
            }

            @Override
            public Optional<Object> minValue() {
                return Optional.of(min);
            }

            @Override
            public Optional<Object> maxValue() {
                return Optional.of(max);
            }

            @Override
            public OptionalLong valueCount() {
                return OptionalLong.of(valueCount);
            }

            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.of(sizeInBytes);
            }
        };
    }
}
