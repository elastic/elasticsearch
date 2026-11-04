/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Util;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads the footer of a Parquet file without Hadoop and without reading any data. The footer is the last part of the file: the
 * Thrift-encoded metadata, then its length as a 4-byte little-endian integer, then the magic {@code PAR1}.
 *
 * <p>The reader asks for a tail of the file that is usually long enough for the whole footer, so a footer costs one request, and
 * asks for the rest in a second request only when the footer is longer.
 */
public final class ParquetFooterReader {

    /** The size of the first request for the tail of the file. A footer of a few hundred columns and row groups fits in it. */
    static final int DEFAULT_TAIL_GUESS = 512 * 1024;

    private static final byte[] MAGIC = { 'P', 'A', 'R', '1' };
    private static final int TRAILER = 8;

    private ParquetFooterReader() {}

    public static ParquetFileInfo read(ParquetObject object) throws IOException {
        return read(object, DEFAULT_TAIL_GUESS);
    }

    static ParquetFileInfo read(ParquetObject object, int tailGuess) throws IOException {
        long length = object.length();
        if (length < MAGIC.length + TRAILER) {
            throw new IOException("[" + object.location() + "] is too short to be a Parquet file: " + length + " bytes");
        }
        int tailLength = (int) Math.min(length, tailGuess);
        byte[] tail = object.readRange(length - tailLength, tailLength);
        for (int i = 0; i < MAGIC.length; i++) {
            if (tail[tail.length - MAGIC.length + i] != MAGIC[i]) {
                throw new IOException("[" + object.location() + "] does not end with the Parquet magic");
            }
        }
        int footerLength = readLittleEndianInt(tail, tail.length - TRAILER);
        if (footerLength <= 0 || footerLength + (long) TRAILER > length - MAGIC.length) {
            throw new IOException("[" + object.location() + "] has an invalid footer length [" + footerLength + "]");
        }
        byte[] footer;
        if (footerLength + TRAILER <= tail.length) {
            footer = new byte[footerLength];
            System.arraycopy(tail, tail.length - TRAILER - footerLength, footer, 0, footerLength);
        } else {
            footer = object.readRange(length - TRAILER - footerLength, footerLength);
        }
        FileMetaData metadata = Util.readFileMetaData(new ByteArrayInputStream(footer));
        return convert(object, length, metadata);
    }

    private static int readLittleEndianInt(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF) | ((bytes[offset + 1] & 0xFF) << 8) | ((bytes[offset + 2] & 0xFF) << 16) | ((bytes[offset + 3] & 0xFF)
            << 24);
    }

    private static ParquetFileInfo convert(ParquetObject object, long length, FileMetaData metadata) throws IOException {
        List<ParquetFileInfo.LeafColumn> columns = new ArrayList<>();
        List<SchemaElement> schema = metadata.getSchema();
        if (schema.isEmpty()) {
            throw new IOException("[" + object.location() + "] has an empty schema");
        }
        walk(schema, 0, "", true, 0, columns, true);

        List<ParquetFileInfo.RowGroup> rowGroups = new ArrayList<>();
        long rows = 0;
        for (RowGroup rowGroup : metadata.getRow_groups()) {
            List<ColumnChunk> chunks = rowGroup.getColumns();
            if (chunks.size() != columns.size()) {
                throw new IOException(
                    "[" + object.location() + "] has a row group with [" + chunks.size() + "] chunks for [" + columns.size() + "] columns"
                );
            }
            List<ParquetFileInfo.Chunk> converted = new ArrayList<>(chunks.size());
            for (int columnIndex = 0; columnIndex < chunks.size(); columnIndex++) {
                ColumnChunk chunk = chunks.get(columnIndex);
                ColumnMetaData columnMetaData = chunk.getMeta_data();
                if (columnMetaData == null) {
                    throw new EOFException("[" + object.location() + "] has a column chunk without metadata, which is not supported");
                }
                long start = columnMetaData.getData_page_offset();
                if (columnMetaData.isSetDictionary_page_offset()
                    && columnMetaData.getDictionary_page_offset() > 0
                    && columnMetaData.getDictionary_page_offset() < start) {
                    start = columnMetaData.getDictionary_page_offset();
                }
                ChunkStatistics statistics = ChunkStatistics.of(columnMetaData, columns.get(columnIndex));
                converted.add(
                    new ParquetFileInfo.Chunk(
                        start,
                        columnMetaData.getTotal_compressed_size(),
                        columnMetaData.getTotal_uncompressed_size(),
                        columnMetaData.getNum_values(),
                        columnMetaData.getCodec().getValue(),
                        statistics.min(),
                        statistics.max(),
                        statistics.nullCount()
                    )
                );
            }
            long numRows = rowGroup.getNum_rows();
            if (numRows > Integer.MAX_VALUE) {
                throw new IOException("[" + object.location() + "] has a row group of [" + numRows + "] rows");
            }
            rows += numRows;
            rowGroups.add(new ParquetFileInfo.RowGroup((int) numRows, converted));
        }
        if (rows != metadata.getNum_rows()) {
            throw new IOException(
                "[" + object.location() + "] says it has [" + metadata.getNum_rows() + "] rows but its row groups have [" + rows + "]"
            );
        }
        return new ParquetFileInfo(object.location(), length, rows, metadata.getCreated_by(), columns, rowGroups);
    }

    /**
     * The statistics of a column chunk that a doc values skipper can use: the range of an integer column and the number of missing
     * values. A statistic that is absent, that has an unexpected size, or whose sort order is not the order of the values that we serve
     * becomes the widest range, so that a skipper never claims more than the footer proves.
     */
    record ChunkStatistics(long min, long max, long nullCount) {

        static final ChunkStatistics UNKNOWN = new ChunkStatistics(
            ColumnSnapshot.UNKNOWN_MIN,
            ColumnSnapshot.UNKNOWN_MAX,
            ColumnSnapshot.UNKNOWN_NULL_COUNT
        );

        static ChunkStatistics of(ColumnMetaData metaData, ParquetFileInfo.LeafColumn column) {
            if (metaData.isSetStatistics() == false) {
                return UNKNOWN;
            }
            Statistics statistics = metaData.getStatistics();
            long nullCount = statistics.isSetNull_count() ? statistics.getNull_count() : ColumnSnapshot.UNKNOWN_NULL_COUNT;
            int width = switch (column.physicalType()) {
                case 1 -> 4; // INT32
                case 2 -> 8; // INT64
                default -> 0;
            };
            if (width == 0 || column.logical() == LogicalKind.UINT_64) {
                return new ChunkStatistics(ColumnSnapshot.UNKNOWN_MIN, ColumnSnapshot.UNKNOWN_MAX, nullCount);
            }
            byte[] min;
            byte[] max;
            if (statistics.isSetMin_value() && statistics.isSetMax_value()) {
                // these follow the sort order of the column, which is the unsigned order for an unsigned annotation
                min = statistics.getMin_value();
                max = statistics.getMax_value();
            } else if (statistics.isSetMin() && statistics.isSetMax() && isUnsigned(column.logical()) == false) {
                // the older fields always use the signed order, so they only fit a column that is not unsigned
                min = statistics.getMin();
                max = statistics.getMax();
            } else {
                return new ChunkStatistics(ColumnSnapshot.UNKNOWN_MIN, ColumnSnapshot.UNKNOWN_MAX, nullCount);
            }
            if (min.length != width || max.length != width) {
                return new ChunkStatistics(ColumnSnapshot.UNKNOWN_MIN, ColumnSnapshot.UNKNOWN_MAX, nullCount);
            }
            boolean zeroExtend = column.logical() == LogicalKind.UINT_32;
            long low = decode(min, zeroExtend);
            long high = decode(max, zeroExtend);
            if (low > high) {
                return new ChunkStatistics(ColumnSnapshot.UNKNOWN_MIN, ColumnSnapshot.UNKNOWN_MAX, nullCount);
            }
            return new ChunkStatistics(low, high, nullCount);
        }

        private static boolean isUnsigned(LogicalKind kind) {
            return switch (kind) {
                case UINT_8, UINT_16, UINT_32, UINT_64 -> true;
                case NONE, STRING, ENUM, JSON, DATE, TIMESTAMP_MILLIS, TIMESTAMP_MICROS, TIMESTAMP_NANOS, INT_8, INT_16, INT_32, INT_64,
                    DECIMAL, TIME, OTHER -> false;
            };
        }

        private static long decode(byte[] bytes, boolean zeroExtend) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            if (bytes.length == 4) {
                int value = buffer.getInt();
                return zeroExtend ? value & 0xFFFFFFFFL : value;
            }
            return buffer.getLong();
        }
    }

    /**
     * Walks the depth-first list of schema elements below {@code position} and adds the leaf columns. Returns the position after the
     * subtree.
     */
    private static int walk(
        List<SchemaElement> schema,
        int position,
        String parentPath,
        boolean parentsRequired,
        int repetitionDepth,
        List<ParquetFileInfo.LeafColumn> columns,
        boolean parentIsRoot
    ) {
        SchemaElement element = schema.get(position);
        int children = element.isSetNum_children() ? element.getNum_children() : 0;
        boolean isRoot = position == 0;
        FieldRepetitionType repetition = element.getRepetition_type();
        boolean required = isRoot || repetition == null || repetition == FieldRepetitionType.REQUIRED;
        int depth = repetitionDepth + (repetition == FieldRepetitionType.REPEATED ? 1 : 0);
        String path = isRoot ? "" : (parentPath.isEmpty() ? element.getName() : parentPath + "." + element.getName());
        int following = position + 1;
        if (children == 0 && isRoot == false) {
            columns.add(
                new ParquetFileInfo.LeafColumn(
                    columns.size(),
                    path,
                    element.getType().getValue(),
                    element.isSetConverted_type() ? element.getConverted_type().getValue() : -1,
                    LogicalKind.of(element),
                    parentsRequired && required,
                    parentIsRoot,
                    depth > 0
                )
            );
            return following;
        }
        for (int child = 0; child < children; child++) {
            following = walk(schema, following, path, parentsRequired && required, depth, columns, isRoot);
        }
        return following;
    }
}
