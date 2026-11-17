/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Set;

/**
 * Page-at-a-time decoder for the one LIST shape vector search reads, {@code LIST<FLOAT>} as {@code dense_vector}:
 * a single repetition level and plain-encoded values. {@link ParquetColumnDecoding#readListColumn} drives parquet-mr's
 * {@code ColumnReader} one value at a time, about ten interface calls per element for the level cursor, the value
 * and the consume; on a 50,000 x 384 embedding scan that protocol was 90% of the read on both runtimes. This reader
 * decodes a page's repetition and definition levels into arrays once, then walks them with plain array loads and
 * reads the floats straight from the page's value bytes.
 *
 * <p>The row semantics are those of {@code readListRow}, kept element for element: a row starts at repetition level
 * zero; an element whose definition level reaches the maximum appends a value; a list with no values (null or
 * empty) is a null position; a null element inside a list is dropped and announced once through the loss warnings;
 * a row that starts at a non-zero repetition level is a corruption, fatal under a strict policy and otherwise
 * discarded and charged to the {@link ParquetColumnDecoding.ListCorruptionHandler}; the footer row count must
 * exhaust the chunk's levels exactly. Anything this reader does not cover ({@link #supports} says what) keeps the
 * general path.
 */
final class FloatListPageReader {

    private static final Set<Encoding> SUPPORTED_ENCODINGS = Set.of(Encoding.PLAIN, Encoding.RLE);

    private final PageReader pageReader;
    private final int maxDefLevel;
    private final int maxRepLevel;
    private final int nullElementDef;
    private final ParquetColumnDecoding.ListCorruptionHandler corruptionHandler;
    private final String columnName;
    private final String fileLocation;
    private final int rowGroupOrdinal;
    private final long rowsBeforeGroup;
    private final long totalValueCount;
    private long consumedValueCount;
    private long rowCount;

    // The current page: levels decoded up front, values read on demand.
    private int[] rep = new int[0];
    private int[] def = new int[0];
    private int pageValueCount;
    private int pos;
    // Whether every definition level of the current page is the maximum, the common page; then rows are cut on the
    // repetition levels alone.
    private boolean pageAllDefined;
    // Value bytes of the current page: a heap array when the page has one, else the buffer; both indexed absolutely.
    private ByteBuffer values;
    private byte[] valueArray;
    private int valueIndex;
    private int valueEnd;
    private boolean exhausted;

    // The batch being decoded: values appended in order, trimmed to the count when the estimate was far off.
    private static final int TRIM_SLACK = 1024;
    private float[] out;
    private int outCount;

    /**
     * Whether this reader can decode the column chunk: one repetition level, physical {@code FLOAT} read as
     * {@code dense_vector} with no declared coercion, and only the encodings it understands (plain values, RLE
     * hybrid levels: a dictionary-encoded or delta-encoded chunk, or the legacy bit-packed level encoding, keeps
     * the general path). The definition level width is bounded so a packed group fits one machine word.
     */
    static boolean supports(ColumnInfo info, ColumnChunkMetaData chunk) {
        if (info.maxRepLevel() != 1 || info.maxDefLevel() < 1 || info.maxDefLevel() > 255) {
            return false;
        }
        if (info.esqlType() != DataType.DENSE_VECTOR || info.parquetType() != PrimitiveType.PrimitiveTypeName.FLOAT) {
            return false;
        }
        if (info.fileEsqlType() != null && info.fileEsqlType() != DataType.DENSE_VECTOR) {
            return false;
        }
        if (chunk == null || chunk.getEncodings() == null || chunk.getEncodings().isEmpty()) {
            return false;
        }
        return SUPPORTED_ENCODINGS.containsAll(chunk.getEncodings());
    }

    FloatListPageReader(
        PageReader pageReader,
        ColumnInfo info,
        ParquetColumnDecoding.ListCorruptionHandler corruptionHandler,
        String columnName,
        String fileLocation,
        int rowGroupOrdinal,
        long rowsBeforeGroup
    ) {
        this.pageReader = pageReader;
        this.maxDefLevel = info.maxDefLevel();
        this.maxRepLevel = info.maxRepLevel();
        this.nullElementDef = info.descriptor().getPrimitiveType().getRepetition() == Type.Repetition.OPTIONAL ? maxDefLevel - 1 : -1;
        this.corruptionHandler = corruptionHandler;
        this.columnName = columnName;
        this.fileLocation = fileLocation;
        this.rowGroupOrdinal = rowGroupOrdinal;
        this.rowsBeforeGroup = rowsBeforeGroup;
        this.totalValueCount = pageReader.getTotalValueCount();
    }

    /**
     * Reads {@code rows} lists into a {@link FloatBlock} of as many positions; see the class comment for the row rules.
     * The values go straight into one {@code float[]} with the position offsets beside it, the shape of the block
     * itself, rather than through a builder: at hundreds of values per row the per-value builder call was most of
     * the read without a JIT.
     */
    Block readBatch(int rows, BlockFactory blockFactory, SkipWarnings lossWarnings) {
        if (rows == 0) {
            try (var builder = blockFactory.newFloatBlockBuilder(0)) {
                return builder.build();
            }
        }
        int estimate = rows;
        if (ensureLevels()) {
            int dims = valuesInCurrentRow();
            if (dims > 0) {
                estimate = (int) Math.min(Integer.MAX_VALUE, (long) rows * dims);
            }
        }
        out = new float[estimate];
        outCount = 0;
        try {
            int[] firstValueIndexes = new int[rows + 1];
            BitSet nulls = null;
            boolean dropped = false;
            for (int row = 0; row < rows; row++) {
                int before = outCount;
                firstValueIndexes[row] = before;
                dropped |= readRow(true);
                if (outCount == before) {
                    // A null position still owns one value slot in an array block.
                    if (nulls == null) {
                        nulls = new BitSet(rows);
                    }
                    nulls.set(row);
                    if (outCount == out.length) {
                        out = Arrays.copyOf(out, out.length + Math.max(16, out.length >> 1));
                    }
                    out[outCount++] = 0f;
                }
            }
            firstValueIndexes[rows] = outCount;
            float[] values = out.length - outCount > TRIM_SLACK ? Arrays.copyOf(out, outCount) : out;
            Block block = blockFactory.newFloatArrayBlock(values, rows, firstValueIndexes, nulls, Block.MvOrdering.UNORDERED);
            if (dropped) {
                lossWarnings.addOnce(ParquetColumnDecoding.nullListElementsMessage(columnName));
            }
            return block;
        } finally {
            out = null;
        }
    }

    /** Advances past {@code rows} lists without producing a block. */
    void skipRows(int rows) {
        for (int row = 0; row < rows; row++) {
            readRow(false);
        }
    }

    void validateExhausted() {
        if (consumedValueCount < totalValueCount) {
            throw corruption(
                "footer row count was exhausted after ["
                    + rowCount
                    + "] rows but ["
                    + (totalValueCount - consumedValueCount)
                    + "] level values remain"
            );
        }
    }

    /**
     * Reads one row into {@link #out} (or skips it when {@code emit} is false). Returns whether a null element was
     * dropped. The row is consumed as runs, one per page it touches: a run whose levels are all defined is a block
     * copy of floats; any other run falls back to element-wise handling of the null elements it contains.
     */
    private boolean readRow(boolean emit) {
        ensureRowStart();
        boolean dropped = false;
        boolean first = true;
        while (first || (hasRemaining() && currentRepetitionLevel() > 0)) {
            // The run: from pos to the end of this row within the current page.
            final int[] r = rep;
            final int[] d = def;
            final int start = pos;
            final int limit = pageValueCount;
            int end = start + 1;
            boolean allDefined;
            if (pageAllDefined) {
                allDefined = true;
                while (end < limit && r[end] > 0) {
                    end++;
                }
            } else {
                allDefined = d[start] >= maxDefLevel;
                while (end < limit && r[end] > 0) {
                    allDefined &= d[end] >= maxDefLevel;
                    end++;
                }
            }
            // A page cannot hold more levels than the chunk has left; the general path reports that as corruption
            // when it reads past the footer count, so cap the run the same way.
            long remaining = totalValueCount - consumedValueCount;
            if (end - start > remaining) {
                end = start + (int) remaining;
            }
            int count = end - start;
            if (allDefined) {
                if (emit) {
                    appendFloats(count);
                } else {
                    skipFloats(count);
                }
            } else {
                for (int i = start; i < end; i++) {
                    int elementDef = d[i];
                    if (elementDef >= maxDefLevel) {
                        if (emit) {
                            appendFloats(1);
                        } else {
                            skipFloats(1);
                        }
                    } else {
                        dropped |= elementDef == nullElementDef;
                    }
                }
            }
            pos = end;
            consumedValueCount += count;
            first = false;
        }
        rowCount++;
        return dropped;
    }

    private void appendFloats(int count) {
        int at = valueIndex;
        int bytes = count * Float.BYTES;
        if (at + bytes > valueEnd) {
            throw corruption("page value data ends before its last value");
        }
        valueIndex = at + bytes;
        int n = outCount;
        if (n + count > out.length) {
            out = Arrays.copyOf(out, Math.max(out.length + (out.length >> 1), n + count));
        }
        float[] o = out;
        byte[] a = valueArray;
        if (a != null) {
            for (int i = 0; i < count; i++, at += Float.BYTES) {
                o[n + i] = Float.intBitsToFloat(
                    (a[at] & 0xFF) | (a[at + 1] & 0xFF) << 8 | (a[at + 2] & 0xFF) << 16 | (a[at + 3] & 0xFF) << 24
                );
            }
        } else {
            for (int i = 0; i < count; i++, at += Float.BYTES) {
                o[n + i] = values.getFloat(at);
            }
        }
        outCount = n + count;
    }

    private void skipFloats(int count) {
        int bytes = count * Float.BYTES;
        if (valueIndex + bytes > valueEnd) {
            throw corruption("page value data ends before its last value");
        }
        valueIndex += bytes;
    }

    private void ensureRowStart() {
        int repetitionLevel = currentRepetitionLevel();
        if (repetitionLevel == 0) {
            return;
        }
        if (corruptionHandler.isStrict()) {
            throw corruption("row [" + (rowCount + 1) + "] starts at repetition level [" + repetitionLevel + "] instead of [0]");
        }
        long discarded = 0;
        do {
            if (def[pos] >= maxDefLevel) {
                skipFloats(1);
            }
            consume();
            discarded++;
        } while (hasRemaining() && currentRepetitionLevel() > 0);
        if (hasRemaining() == false) {
            throw corruption(
                "orphan continuation fragment before row ["
                    + (rowCount + 1)
                    + "] consumed the remaining ["
                    + discarded
                    + "] values without reaching repetition level [0]"
            );
        }
        corruptionHandler.recoveredOrphan(columnName, rowGroupOrdinal, rowCount + 1, rowsBeforeGroup + rowCount + 1, discarded);
    }

    private boolean hasRemaining() {
        return consumedValueCount < totalValueCount;
    }

    private int currentRepetitionLevel() {
        ensureRemaining("read a repetition level");
        return rep[pos];
    }

    private void consume() {
        pos++;
        consumedValueCount++;
    }

    /** Number of defined values in the row that starts at the current position, within the current page. */
    private int valuesInCurrentRow() {
        int count = 0;
        for (int i = pos; i < pageValueCount && (i == pos || rep[i] > 0); i++) {
            if (def[i] >= maxDefLevel) {
                count++;
            }
        }
        return count;
    }

    /**
     * Makes the levels at {@code pos} valid, loading the next page when the current one is spent. The caller has
     * established that levels remain by count; a missing page is the same corruption the general path reports.
     */
    private void ensureRemaining(String action) {
        if (hasRemaining() == false) {
            throw corruption(
                "cannot "
                    + action
                    + ": footer row count exceeds the ["
                    + totalValueCount
                    + "] available level values after ["
                    + rowCount
                    + "] rows"
            );
        }
        if (ensureLevels() == false) {
            throw corruption(
                "cannot " + action + ": the column chunk ended after [" + consumedValueCount + "] of [" + totalValueCount + "] level values"
            );
        }
    }

    /** True when {@code pos} indexes a decoded page, loading one if needed; false once the chunk has no more pages. */
    private boolean ensureLevels() {
        while (pos >= pageValueCount) {
            if (exhausted) {
                return false;
            }
            DataPage page = pageReader.readPage();
            if (page == null) {
                exhausted = true;
                return false;
            }
            loadPage(page);
        }
        return true;
    }

    private void loadPage(DataPage page) {
        int count = page.getValueCount();
        if (rep.length < count) {
            rep = new int[count];
            def = new int[count];
        }
        try {
            if (page instanceof DataPageV1 v1) {
                if (v1.getRlEncoding() != Encoding.RLE || v1.getDlEncoding() != Encoding.RLE || v1.getValueEncoding() != Encoding.PLAIN) {
                    throw corruption(
                        "page encodings [" + v1.getRlEncoding() + "/" + v1.getDlEncoding() + "/" + v1.getValueEncoding() + "]"
                    );
                }
                ByteBuffer bytes = v1.getBytes().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
                decodeLengthPrefixed(bytes, maxRepLevel, count, rep, "repetition");
                pageAllDefined = decodeLengthPrefixed(bytes, maxDefLevel, count, def, "definition") == maxDefLevel;
                values = bytes.slice().order(ByteOrder.LITTLE_ENDIAN);
            } else if (page instanceof DataPageV2 v2) {
                if (v2.getDataEncoding() != Encoding.PLAIN) {
                    throw corruption("page value encoding [" + v2.getDataEncoding() + "]");
                }
                decode(v2.getRepetitionLevels().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN), maxRepLevel, count, rep, "repetition");
                pageAllDefined = decode(
                    v2.getDefinitionLevels().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN),
                    maxDefLevel,
                    count,
                    def,
                    "definition"
                ) == maxDefLevel;
                values = v2.getData().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
            } else {
                throw new IllegalArgumentException("Unexpected page type: " + page.getClass().getName());
            }
        } catch (IOException e) {
            throw ParquetReadFailures.wrap(e, "Failed to read page bytes of LIST column [" + columnName + "]");
        }
        if (values.hasArray()) {
            valueArray = values.array();
            valueIndex = values.arrayOffset() + values.position();
            valueEnd = values.arrayOffset() + values.limit();
        } else {
            valueArray = null;
            valueIndex = values.position();
            valueEnd = values.limit();
        }
        pageValueCount = count;
        pos = 0;
    }

    /** V1 pages prefix each level stream with its byte length. Returns the smallest level, as {@link #decode} does. */
    private int decodeLengthPrefixed(ByteBuffer bytes, int maxLevel, int count, int[] out, String kind) {
        if (bytes.remaining() < Integer.BYTES) {
            throw corruption("truncated level stream");
        }
        int length = bytes.getInt();
        if (length < 0 || bytes.remaining() < length) {
            throw corruption("truncated level stream");
        }
        ByteBuffer payload = bytes.slice(bytes.position(), length).order(ByteOrder.LITTLE_ENDIAN);
        bytes.position(bytes.position() + length);
        return decode(payload, maxLevel, count, out, kind);
    }

    /**
     * Decodes {@code count} levels of the RLE / bit-packing hybrid encoding into {@code out} and returns the smallest
     * level seen. A level stream for a maximum of zero is empty and every level is zero. Levels above {@code maxLevel}
     * are corruption; a run is checked once and a packed group only when its width admits such a value, so the check
     * is not a pass over every level. The stream is read from its backing array (a level stream is a few kilobytes,
     * so one is copied out of a buffer without an array), which is what the buffer's own {@code get} does without a
     * bounds-and-position update per byte.
     */
    private int decode(ByteBuffer in, int maxLevel, int count, int[] out, String kind) {
        if (maxLevel == 0) {
            Arrays.fill(out, 0, count, 0);
            return 0;
        }
        final byte[] a;
        int p;
        final int end;
        if (in.hasArray()) {
            a = in.array();
            p = in.arrayOffset() + in.position();
            end = in.arrayOffset() + in.limit();
        } else {
            a = new byte[in.remaining()];
            in.get(in.position(), a);
            p = 0;
            end = a.length;
        }
        int bitWidth = 32 - Integer.numberOfLeadingZeros(maxLevel);
        int mask = (1 << bitWidth) - 1;
        boolean checkGroups = mask != maxLevel;
        int min = Integer.MAX_VALUE;
        int produced = 0;
        while (produced < count) {
            // Varint header: even is a run of one value, odd is a count of bit-packed groups of eight.
            int header = 0;
            for (int shift = 0;; shift += 7) {
                if (p >= end) {
                    throw corruption("truncated level stream");
                }
                int b = a[p++] & 0xFF;
                header |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    break;
                }
                if (shift >= 28) {
                    throw corruption("level stream header overflow");
                }
            }
            if ((header & 1) == 0) {
                int run = header >>> 1;
                int valueBytes = (bitWidth + 7) >>> 3;
                if (end - p < valueBytes) {
                    throw corruption("truncated level stream");
                }
                int value = 0;
                for (int i = 0; i < valueBytes; i++) {
                    value |= (a[p++] & 0xFF) << (8 * i);
                }
                if (value > maxLevel) {
                    throw levelOutOfRange(kind, value, maxLevel);
                }
                int take = Math.min(run, count - produced);
                if (take > 0) {
                    Arrays.fill(out, produced, produced + take, value);
                    min = Math.min(min, value);
                }
                produced += take;
            } else {
                int groups = header >>> 1;
                for (int g = 0; g < groups && produced < count; g++) {
                    if (end - p < bitWidth) {
                        throw corruption("truncated level stream");
                    }
                    long word = 0;
                    for (int i = 0; i < bitWidth; i++) {
                        word |= (long) (a[p++] & 0xFF) << (8 * i);
                    }
                    int take = Math.min(8, count - produced);
                    for (int i = 0; i < take; i++) {
                        int value = (int) (word >>> (i * bitWidth)) & mask;
                        if (checkGroups && value > maxLevel) {
                            throw levelOutOfRange(kind, value, maxLevel);
                        }
                        out[produced + i] = value;
                        min = Math.min(min, value);
                    }
                    produced += take;
                }
            }
        }
        return min == Integer.MAX_VALUE ? 0 : min;
    }

    private IllegalArgumentException levelOutOfRange(String kind, int value, int maxLevel) {
        return corruption(kind + " level [" + value + "] is outside [0, " + maxLevel + "]");
    }

    private IllegalArgumentException corruption(String detail) {
        return new IllegalArgumentException(
            "Malformed Parquet LIST column ["
                + columnName
                + "] in file ["
                + fileLocation
                + "] row group ["
                + (rowGroupOrdinal + 1)
                + "]: "
                + detail
        );
    }
}
