/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import org.apache.iceberg.Metrics;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.xpack.parquet.write.file.ParquetCodecs;
import org.elasticsearch.xpack.parquet.write.file.ParquetReadOptionsFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * The Iceberg metrics of a Parquet file: the record count, and for each column (by field id) its size, its number of values and nulls,
 * and its lower and upper bound. They go into the manifest of the table so that a reader can skip files without opening their footers.
 *
 * <p>Iceberg has {@code ParquetUtil#footerMetrics} for this, and this class does not use it, for two reasons. {@code fileMetrics} opens
 * the footer through Hadoop classes that a node does not have, and {@code footerMetrics} takes a footer object of the Parquet classes
 * of the Iceberg plugin. In a node the Parquet reader plugin and the Iceberg plugin each have their own copy of the Parquet classes, so
 * the footer that this plugin reads is of another class than the one that Iceberg expects, and the call fails to link. The metrics are
 * built here from the footer, and only Iceberg's own types ({@link Metrics}, byte buffers) go across.
 *
 * <p>Only the bounds of integers and timestamps are written. The bounds of a string need a truncation that keeps them valid bounds,
 * and a missing bound only means that a reader cannot skip the file by that column.
 */
final class ParquetFileMetrics {

    private ParquetFileMetrics() {}

    static Metrics read(InputFile file) throws IOException {
        final ParquetReadOptions options = ParquetReadOptionsFactory.create(new ParquetCodecs());
        long rows = 0;
        final Map<Integer, Long> sizes = new HashMap<>();
        final Map<Integer, Long> valueCounts = new HashMap<>();
        final Map<Integer, Long> nullCounts = new HashMap<>();
        final Map<Integer, Long> lower = new HashMap<>();
        final Map<Integer, Long> upper = new HashMap<>();
        final Map<Integer, Type.PrimitiveType> boundTypes = new HashMap<>();
        try (ParquetFileReader reader = ParquetFileReader.open(file, options)) {
            for (BlockMetaData block : reader.getFooter().getBlocks()) {
                rows += block.getRowCount();
                for (ColumnChunkMetaData column : block.getColumns()) {
                    final PrimitiveType type = column.getPrimitiveType();
                    if (type.getId() == null) {
                        continue; // a column with no field id is not a column of the table
                    }
                    final int id = type.getId().intValue();
                    sizes.merge(id, column.getTotalSize(), Long::sum);
                    valueCounts.merge(id, column.getValueCount(), Long::sum);
                    final Statistics<?> statistics = column.getStatistics();
                    if (statistics == null || statistics.isEmpty()) {
                        continue;
                    }
                    if (statistics.isNumNullsSet()) {
                        nullCounts.merge(id, statistics.getNumNulls(), Long::sum);
                    }
                    // The bounds of a leaf inside a list (more than one level in its path) say nothing about the rows of the column, and
                    // Iceberg keeps none for the elements of a list.
                    final Type.PrimitiveType icebergType = column.getPath().size() == 1 ? boundType(type) : null;
                    if (icebergType != null && statistics.hasNonNullValue()) {
                        boundTypes.put(id, icebergType);
                        final long factor = microsPerUnit(type);
                        lower.merge(id, ((Number) statistics.genericGetMin()).longValue() * factor, Math::min);
                        upper.merge(id, ((Number) statistics.genericGetMax()).longValue() * factor, Math::max);
                    }
                }
            }
        }
        return new Metrics(rows, sizes, valueCounts, nullCounts, Map.of(), bounds(lower, boundTypes), bounds(upper, boundTypes));
    }

    /** The Iceberg type of a column whose bounds are written, or null. */
    private static Type.PrimitiveType boundType(PrimitiveType type) {
        return switch (type.getPrimitiveTypeName()) {
            case INT64 -> type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
                ? Types.TimestampType.withZone()
                : Types.LongType.get();
            case INT32 -> Types.IntegerType.get();
            default -> null;
        };
    }

    /** An Iceberg timestamp has the precision of microseconds, and a Parquet timestamp can have another unit. */
    private static long microsPerUnit(PrimitiveType type) {
        if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp) {
            return switch (timestamp.getUnit()) {
                case MILLIS -> 1000;
                case MICROS -> 1;
                case NANOS -> throw new IllegalArgumentException("a timestamp in nanoseconds is not supported by a table of the format 2");
            };
        }
        return 1;
    }

    private static Map<Integer, ByteBuffer> bounds(Map<Integer, Long> values, Map<Integer, Type.PrimitiveType> types) {
        final Map<Integer, ByteBuffer> bounds = new HashMap<>();
        for (Map.Entry<Integer, Long> value : values.entrySet()) {
            final Type.PrimitiveType type = types.get(value.getKey());
            final Object converted = type.typeId() == Type.TypeID.INTEGER ? (Object) Math.toIntExact(value.getValue()) : value.getValue();
            bounds.put(value.getKey(), Conversions.toByteBuffer(type, converted));
        }
        return bounds;
    }
}
