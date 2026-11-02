/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.stats;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.Row;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

final class MetadataLeafStatsProvider implements LeafStatsProvider {
    @Override
    public PlanStats estimate(LeafPlan leaf) {
        if (leaf instanceof ExternalRelation external) {
            return external(external);
        }
        if (leaf instanceof Row row) {
            return row(row);
        }
        return unknown(leaf.output());
    }

    private static PlanStats external(ExternalRelation relation) {
        SourceMetadata metadata = relation.metadata();
        SourceStatistics statistics = SourceStatisticsSerializer.fromSource(metadata);
        Estimate rows = statistics == null ? Estimate.UNKNOWN : estimate(statistics.rowCount());
        Estimate bytes = statistics == null ? Estimate.UNKNOWN : estimate(statistics.sizeInBytes());
        Map<NameId, ColumnStats> columns = externalColumns(relation.output(), statistics, rows);
        Estimate averageRowBytes;
        if (rows.isUnknown() == false && bytes.isUnknown() == false && rows.value() > 0) {
            averageRowBytes = Estimate.metadata(bytes.value() / rows.value());
        } else if (metadata.sampleBytes() > 0 && metadata.sampleRows() > 0) {
            double sampleWidth = (double) metadata.sampleBytes() / metadata.sampleRows();
            averageRowBytes = Estimate.sampled(sampleWidth, Math.max(1, sampleWidth / 4));
        } else {
            averageRowBytes = StatsSupport.averageRowBytes(relation.output(), columns);
        }
        if (bytes.isUnknown()) {
            bytes = StatsSupport.bytes(rows, averageRowBytes);
        }
        return new PlanStats(rows, averageRowBytes, columns, bytes);
    }

    private static Map<NameId, ColumnStats> externalColumns(List<Attribute> output, SourceStatistics statistics, Estimate rows) {
        Map<String, SourceStatistics.ColumnStatistics> sourceColumns = statistics == null
            ? Map.of()
            : statistics.columnStatistics().orElse(Map.of());
        Map<NameId, ColumnStats> columns = new LinkedHashMap<>(StatsSupport.unknownColumns(output));
        for (Attribute attribute : output) {
            SourceStatistics.ColumnStatistics sourceColumn = sourceColumns.get(attribute.name());
            if (sourceColumn == null) {
                continue;
            }
            Estimate ndv = estimate(sourceColumn.distinctCount());
            Estimate nullFraction = nullFraction(sourceColumn.nullCount(), rows);
            Estimate averageBytes = averageBytes(sourceColumn, rows, attribute.dataType());
            Estimate averageValuesPerRow = averageValuesPerRow(sourceColumn.valueCount(), rows);
            columns.put(
                attribute.id(),
                new ColumnStats(
                    ndv,
                    nullFraction,
                    sourceColumn.minValue(),
                    sourceColumn.maxValue(),
                    averageBytes.isUnknown() ? StatsSupport.withDefaultWidth(attribute).averageBytes() : averageBytes,
                    averageValuesPerRow
                )
            );
        }
        return columns;
    }

    private static PlanStats row(Row row) {
        Map<NameId, ColumnStats> columns = new LinkedHashMap<>(StatsSupport.unknownColumns(row.output()));
        for (Alias field : row.fields()) {
            if (field.child() instanceof Literal literal) {
                Object value = literal.value();
                Estimate nullFraction = Estimate.exact(value == null ? 1 : 0);
                Estimate averageValues = Estimate.exact(value == null ? 0 : 1);
                Optional<Object> bound = Optional.ofNullable(value);
                ColumnStats defaultStats = StatsSupport.withDefaultWidth(field.toAttribute());
                columns.put(
                    field.toAttribute().id(),
                    new ColumnStats(Estimate.exact(1), nullFraction, bound, bound, defaultStats.averageBytes(), averageValues)
                );
            }
        }
        Estimate averageRowBytes = StatsSupport.averageRowBytes(row.output(), columns);
        return new PlanStats(Estimate.exact(1), averageRowBytes, columns);
    }

    private static PlanStats unknown(List<Attribute> output) {
        Map<NameId, ColumnStats> columns = StatsSupport.unknownColumns(output);
        return new PlanStats(Estimate.UNKNOWN, StatsSupport.averageRowBytes(output, columns), columns, Estimate.UNKNOWN);
    }

    private static Estimate estimate(OptionalLong value) {
        return value.isPresent() && value.getAsLong() >= 0 ? Estimate.metadata(value.getAsLong()) : Estimate.UNKNOWN;
    }

    private static Estimate nullFraction(OptionalLong nullCount, Estimate rows) {
        if (nullCount.isEmpty() || nullCount.getAsLong() < 0 || rows.isUnknown()) {
            return Estimate.UNKNOWN;
        }
        if (rows.value() == 0) {
            return nullCount.getAsLong() == 0 ? Estimate.metadata(0) : Estimate.UNKNOWN;
        }
        return Estimate.metadata(Math.min(1, (double) nullCount.getAsLong() / rows.value()));
    }

    /**
     * Average in-memory bytes per value. Source column sizes are encoded sizes (dictionary and run-length encoded
     * values can take a byte or less per value), while the plan holds decoded blocks: fixed-width values take their
     * full width, and every string value carries an offset next to its bytes. Underestimating here lets a build side
     * pass the materialization fit check and then fail at runtime, so the width is raised to the decoded floor.
     */
    private static Estimate averageBytes(SourceStatistics.ColumnStatistics sourceColumn, Estimate rows, DataType type) {
        OptionalLong size = sourceColumn.sizeInBytes();
        if (size.isEmpty() || size.getAsLong() < 0) {
            return Estimate.UNKNOWN;
        }
        OptionalLong values = sourceColumn.valueCount();
        double divisor = values.isPresent() && values.getAsLong() > 0 ? values.getAsLong()
            : rows.isUnknown() == false && rows.value() > 0 ? rows.value()
            : 0;
        if (divisor <= 0) {
            return Estimate.UNKNOWN;
        }
        double encoded = size.getAsLong() / divisor;
        double decoded = DataType.isString(type) ? encoded + Integer.BYTES : Math.max(encoded, type.estimatedSize());
        return Estimate.metadata(decoded);
    }

    private static Estimate averageValuesPerRow(OptionalLong valueCount, Estimate rows) {
        if (valueCount.isEmpty() || valueCount.getAsLong() < 0 || rows.isUnknown() || rows.value() <= 0) {
            return Estimate.UNKNOWN;
        }
        return Estimate.metadata((double) valueCount.getAsLong() / rows.value());
    }
}
