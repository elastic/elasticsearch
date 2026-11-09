/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetPositionalReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What the footer of a Parquet file says, reduced to what a mount and an engine need: the leaf columns of the schema and the number of
 * rows of each row group. The footer itself comes from the ES|QL reader, which parses it.
 */
public final class ParquetFileInfo {

    /**
     * A leaf column of the schema.
     *
     * @param path         the dotted path of the column, which is the name that the ES|QL reader gives it
     * @param physicalType the Parquet physical type
     * @param logical      what the annotations of the column mean
     * @param required     whether the column and all its parents are {@code REQUIRED}
     * @param flat         whether the column is a direct child of the root, so not part of a struct, list or map
     * @param repeated     whether a value is repeated for a row, which is what a list does
     */
    public record LeafColumn(
        String path,
        PrimitiveType.PrimitiveTypeName physicalType,
        LogicalKind logical,
        boolean required,
        boolean flat,
        boolean repeated
    ) {}

    /**
     * What the footer says about the chunk of an integer column in one row group.
     *
     * @param hasMinMax whether the footer has a usable minimum and maximum, which the parquet reader drops when the sort order of the
     *                  column is not reliable
     * @param min       the smallest value, as the file stores it
     * @param max       the largest value, as the file stores it
     * @param nullCount the number of missing values, or {@code -1} if the footer does not say
     */
    public record ChunkStats(boolean hasMinMax, long min, long max, long nullCount) {}

    /**
     * A row group: its number of rows, and the statistics of its chunks of the top-level {@code INT32} and {@code INT64} columns,
     * by the path of the column.
     */
    public record RowGroup(int numRows, Map<String, ChunkStats> chunks) {}

    private final String location;
    private final long length;
    private final long rowCount;
    @Nullable
    private final String createdBy;
    private final List<LeafColumn> columns;
    private final List<RowGroup> rowGroups;

    private ParquetFileInfo(
        String location,
        long length,
        long rowCount,
        @Nullable String createdBy,
        List<LeafColumn> columns,
        List<RowGroup> rowGroups
    ) {
        this.location = location;
        this.length = length;
        this.rowCount = rowCount;
        this.createdBy = createdBy;
        this.columns = List.copyOf(columns);
        this.rowGroups = List.copyOf(rowGroups);
    }

    /** Reads the footer of the file through the ES|QL reader. */
    public static ParquetFileInfo read(String location, StorageObject file) throws IOException {
        ParquetMetadata footer = ParquetPositionalReader.readFooter(file, ParquetBlocks.factory(), Settings.EMPTY);
        return of(location, file.length(), footer);
    }

    public static ParquetFileInfo of(String location, long length, ParquetMetadata footer) {
        List<LeafColumn> columns = new ArrayList<>();
        for (ColumnDescriptor descriptor : footer.getFileMetaData().getSchema().getColumns()) {
            PrimitiveType type = descriptor.getPrimitiveType();
            columns.add(
                new LeafColumn(
                    String.join(".", descriptor.getPath()),
                    type.getPrimitiveTypeName(),
                    LogicalKind.of(type),
                    descriptor.getMaxDefinitionLevel() == 0,
                    descriptor.getPath().length == 1,
                    descriptor.getMaxRepetitionLevel() > 0
                )
            );
        }
        Set<String> required = new HashSet<>();
        for (LeafColumn column : columns) {
            if (column.required()) {
                required.add(column.path());
            }
        }
        List<RowGroup> rowGroups = new ArrayList<>();
        long rows = 0;
        for (BlockMetaData block : footer.getBlocks()) {
            if (block.getRowCount() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("a row group of [" + location + "] has [" + block.getRowCount() + "] rows");
            }
            rowGroups.add(new RowGroup((int) block.getRowCount(), chunkStats(block, required)));
            rows += block.getRowCount();
        }
        return new ParquetFileInfo(location, length, rows, footer.getFileMetaData().getCreatedBy(), columns, rowGroups);
    }

    private static Map<String, ChunkStats> chunkStats(BlockMetaData block, Set<String> required) {
        Map<String, ChunkStats> chunks = new HashMap<>();
        for (ColumnChunkMetaData chunk : block.getColumns()) {
            if (chunk.getPath().size() != 1) {
                continue;
            }
            PrimitiveType.PrimitiveTypeName type = chunk.getPrimitiveType().getPrimitiveTypeName();
            if (type != PrimitiveType.PrimitiveTypeName.INT32 && type != PrimitiveType.PrimitiveTypeName.INT64) {
                continue;
            }
            String path = chunk.getPath().toDotString();
            Statistics<?> statistics = chunk.getStatistics();
            boolean hasMinMax = statistics != null && statistics.hasNonNullValue();
            long min = 0;
            long max = 0;
            if (hasMinMax) {
                min = ((Number) statistics.genericGetMin()).longValue();
                max = ((Number) statistics.genericGetMax()).longValue();
            }
            // a required column has no missing value, whatever the footer says about the count
            long nullCount = required.contains(path)
                ? 0
                : (statistics != null && statistics.isNumNullsSet() ? statistics.getNumNulls() : -1);
            chunks.put(path, new ChunkStats(hasMinMax, min, max, nullCount));
        }
        return chunks;
    }

    public String location() {
        return location;
    }

    public long length() {
        return length;
    }

    public long rowCount() {
        return rowCount;
    }

    /** The writer of the file as the footer names it, or {@code null}. */
    @Nullable
    public String createdBy() {
        return createdBy;
    }

    public List<LeafColumn> columns() {
        return columns;
    }

    public List<RowGroup> rowGroups() {
        return rowGroups;
    }

    /** The leaf column with the given dotted path, or {@code null}. */
    @Nullable
    public LeafColumn column(String path) {
        for (LeafColumn column : columns) {
            if (column.path().equals(path)) {
                return column;
            }
        }
        return null;
    }

    /** The number of rows of the row groups before the given one, which is the file row of its first row. */
    public long firstRowOf(int rowGroup) {
        long rows = 0;
        for (int i = 0; i < rowGroup; i++) {
            rows += rowGroups.get(i).numRows();
        }
        return rows;
    }

    /**
     * Checks that another file can be served together with this one, whose columns gave the mapping: every mapped column must exist in
     * the other file with the same type and meaning. A mount checks the files of one request with it, and the engine checks the files
     * of an index when it attaches them.
     *
     * @throws IllegalArgumentException with the first difference
     */
    public void requireCompatibleWith(ParquetFileInfo other, Collection<String> mappedColumns) {
        for (String path : mappedColumns) {
            LeafColumn expected = column(path);
            LeafColumn found = other.column(path);
            if (found == null) {
                throw new IllegalArgumentException(
                    "[" + other.location() + "] has no column [" + path + "], which [" + location() + "] has"
                );
            }
            if (found.physicalType() != expected.physicalType() || found.logical() != expected.logical()) {
                throw new IllegalArgumentException(
                    "column ["
                        + path
                        + "] of ["
                        + other.location()
                        + "] is ["
                        + found.physicalType()
                        + " "
                        + found.logical()
                        + "], but it is ["
                        + expected.physicalType()
                        + " "
                        + expected.logical()
                        + "] in ["
                        + location()
                        + "]"
                );
            }
            if (found.flat() == false || found.repeated()) {
                throw new IllegalArgumentException("column [" + path + "] of [" + other.location() + "] is nested or repeated");
            }
        }
    }

    /**
     * What a Parquet column means, for the types that the mapping cares about. The schema of the footer carries the converted types of
     * old writers as logical type annotations too, so this is the one place that reads them.
     */
    public enum LogicalKind {
        /** No annotation: a plain integer, floating point number or byte array. */
        NONE,
        STRING,
        ENUM,
        JSON,
        DATE,
        TIMESTAMP_MILLIS,
        TIMESTAMP_MICROS,
        TIMESTAMP_NANOS,
        INT_8,
        INT_16,
        INT_32,
        INT_64,
        UINT_8,
        UINT_16,
        UINT_32,
        UINT_64,
        DECIMAL,
        TIME,
        /** An annotation that the mapping does not know. */
        OTHER;

        static LogicalKind of(PrimitiveType column) {
            LogicalTypeAnnotation annotation = column.getLogicalTypeAnnotation();
            if (annotation == null) {
                return NONE;
            }
            if (annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                return STRING;
            } else if (annotation instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
                return ENUM;
            } else if (annotation instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation) {
                return JSON;
            } else if (annotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                return DATE;
            } else if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                return DECIMAL;
            } else if (annotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
                return TIME;
            } else if (annotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp) {
                return switch (timestamp.getUnit()) {
                    case MILLIS -> TIMESTAMP_MILLIS;
                    case MICROS -> TIMESTAMP_MICROS;
                    case NANOS -> TIMESTAMP_NANOS;
                };
            } else if (annotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation integer) {
                boolean signed = integer.isSigned();
                return switch (integer.getBitWidth()) {
                    case 8 -> signed ? INT_8 : UINT_8;
                    case 16 -> signed ? INT_16 : UINT_16;
                    case 32 -> signed ? INT_32 : UINT_32;
                    case 64 -> signed ? INT_64 : UINT_64;
                    default -> OTHER;
                };
            }
            // UNKNOWN (a column of nulls), UUID, geometry and the like are not annotations that the mapping knows
            return OTHER;
        }
    }
}
