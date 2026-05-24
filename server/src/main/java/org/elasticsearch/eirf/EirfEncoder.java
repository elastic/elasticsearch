/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Encodes documents into EIRF (Elastic Internal Row Format) batches.
 *
 * <p>Two usage modes are supported, both backed by a shared {@link EirfSchema} and per-document
 * {@link ScratchBuffers}.
 *
 * <p><b>Single partition</b> (legacy):
 * <pre>
 * try (EirfEncoder encoder = new EirfEncoder()) {
 *     encoder.addDocument(source1, XContentType.JSON);
 *     encoder.addDocument(source2, XContentType.JSON);
 *     EirfBatch batch = encoder.build();
 * }
 * </pre>
 *
 * <p><b>Multi-partition</b> (single parse pass per document, row dispatched to one of several
 * destination partitions — typically one per shard — after the document has been fully parsed
 * into scratch and any caller-side decisions, such as routing, have been made):
 * <pre>
 * try (EirfEncoder encoder = new EirfEncoder()) {
 *     encoder.parseToScratch(source, XContentType.JSON, leafSink);
 *     int rowIndex = encoder.commitScratchTo(shardId);
 *     // ...repeat for additional documents...
 *     EirfBatch shardBatch = encoder.buildPartition(shardId);
 * }
 * </pre>
 */
public class EirfEncoder implements Releasable {

    private static final int HEADER_SIZE = 32;
    private static final int INITIAL_CAPACITY = 16;
    private static final int INITIAL_PARTITION_CAPACITY = 4;

    private final EirfSchema schema;
    private final ScratchBuffers scratch;
    private Partition[] partitions;
    /** Cached dotted path per leaf column index. Lazily filled and grown as the schema grows. */
    private String[] cachedPath;
    /** True after {@link #parseToScratch} returns and before {@link #commitScratchTo} is called. */
    private boolean rowStaged;

    public EirfEncoder() {
        this.schema = new EirfSchema();
        this.scratch = new ScratchBuffers(INITIAL_CAPACITY);
        this.cachedPath = new String[INITIAL_CAPACITY];
        this.partitions = new Partition[INITIAL_PARTITION_CAPACITY];
    }

    /**
     * Adds a single document to the encoder's default partition. Equivalent to
     * {@code parseToScratch(source, xContentType, NO_OP_LEAF_SINK); commitScratchTo(DEFAULT_PARTITION);}.
     */
    public void addDocument(BytesReference source, XContentType xContentType, int partition) throws IOException {
        parseToScratch(source, xContentType, LeafSink.NO_OP);
        commitScratchTo(partition);
    }

    /**
     * Parses {@code source} into the encoder's per-document scratch and fires {@code sink} for every
     * primitive leaf value (string / number / boolean — null and array values are intentionally not
     * forwarded; see {@link LeafSink}). The parsed row is held in scratch until the next
     * {@link #commitScratchTo(int)} call.
     *
     * <p>Calling this method twice without an intervening {@code commitScratchTo} discards the
     * previously staged row.
     */
    public void parseToScratch(BytesReference source, XContentType xContentType, LeafSink sink) throws IOException {
        int columnCountBefore = schema.leafCount();
        Arrays.fill(scratch.typeBytes, 0, columnCountBefore, (byte) 0);
        Arrays.fill(scratch.varData, 0, columnCountBefore, null);
        scratch.resetCounters();

        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, xContentType)) {
            // The schema will prevent duplicate columns. No need to double with JSON's internal duplicate prevention.
            parser.allowDuplicateKeys(true);
            parser.nextToken(); // START_OBJECT
            flattenObject(parser, 0, schema, scratch, parser.nextToken(), this, sink);
        }
        rowStaged = true;
    }

    /**
     * Flushes the row currently staged in scratch into the partition identified by
     * {@code partitionKey}, returning the row's index within that partition.
     *
     * @throws IllegalStateException if no row is currently staged.
     */
    public int commitScratchTo(int partitionKey) throws IOException {
        if (rowStaged == false) {
            throw new IllegalStateException("commitScratchTo called without a staged row");
        }
        Partition partition = getOrCreatePartition(partitionKey);
        int columnCount = schema.leafCount();
        int rowStart = (int) partition.rowOutput.position();
        partition.ensureRowCapacity();
        partition.rowOffsets[partition.docCount] = rowStart;
        writeRow(partition.rowOutput, columnCount, scratch);
        partition.rowLengths[partition.docCount] = (int) partition.rowOutput.position() - rowStart;
        int rowIndex = partition.docCount;
        partition.docCount++;
        rowStaged = false;
        return rowIndex;
    }

    /**
     * Builds an {@link EirfBatch} for the partition identified by {@code partitionKey}. Producing a
     * batch consumes that partition's row data; subsequent calls for the same key will produce an
     * empty batch.
     */
    public EirfBatch buildPartition(int partitionKey) {
        Partition partition = getOrCreatePartition(partitionKey);
        ReleasableBytesReference rowBytes = partition.rowOutput.moveToBytesReference();
        BytesReference headerBytes = buildHeader(schema, partition.docCount, partition.rowOffsets, partition.rowLengths, rowBytes.length());
        BytesReference combined = CompositeBytesReference.of(headerBytes, rowBytes);
        return new EirfBatch(combined, rowBytes);
    }

    /**
     * Returns the number of rows committed to the partition identified by {@code partitionKey}.
     * Returns 0 for partitions that have never been written to.
     */
    public int docCount(int partitionKey) {
        Partition partition = partitionKey < partitions.length ? partitions[partitionKey] : null;
        return partition == null ? 0 : partition.docCount;
    }

    /**
     * Returns true if at least one row has been committed to the partition identified by
     * {@code partitionKey}.
     */
    public boolean hasPartition(int partitionKey) {
        Partition partition = partitionKey < partitions.length ? partitions[partitionKey] : null;
        return partition != null && partition.docCount > 0;
    }

    /**
     * Returns the dotted path for the given leaf column. Result is cached: callers may use the
     * column index as a stable key for their own per-column state.
     */
    public String columnPath(int columnIndex) {
        if (columnIndex >= cachedPath.length) {
            int newCap = cachedPath.length;
            while (columnIndex >= newCap) {
                newCap <<= 1;
            }
            cachedPath = Arrays.copyOf(cachedPath, newCap);
        }
        String path = cachedPath[columnIndex];
        if (path == null) {
            path = schema.getFullPath(columnIndex);
            cachedPath[columnIndex] = path;
        }
        return path;
    }

    @Override
    public void close() {
        for (Partition partition : partitions) {
            if (partition != null) {
                partition.rowOutput.close();
            }
        }
        Arrays.fill(partitions, null);
    }

    public static EirfBatch encode(List<BytesReference> sources, XContentType xContentType) throws IOException {
        try (EirfEncoder encoder = new EirfEncoder()) {
            for (BytesReference source : sources) {
                encoder.addDocument(source, xContentType, 0);
            }
            return encoder.buildPartition(0);
        }
    }

    private Partition getOrCreatePartition(int partitionKey) {
        if (partitionKey >= partitions.length) {
            int newCap = partitions.length;
            while (partitionKey >= newCap) {
                newCap <<= 1;
            }
            partitions = Arrays.copyOf(partitions, newCap);
        }
        Partition partition = partitions[partitionKey];
        if (partition == null) {
            partition = new Partition(new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE));
            partitions[partitionKey] = partition;
        }
        return partition;
    }

    /** Per-partition row state: row output stream plus the parallel offsets/lengths arrays for the doc index. */
    private static final class Partition {
        final RecyclerBytesStreamOutput rowOutput;
        int[] rowOffsets;
        int[] rowLengths;
        int docCount;

        Partition(RecyclerBytesStreamOutput rowOutput) {
            this.rowOutput = rowOutput;
            this.rowOffsets = new int[INITIAL_CAPACITY];
            this.rowLengths = new int[INITIAL_CAPACITY];
            this.docCount = 0;
        }

        void ensureRowCapacity() {
            if (docCount >= rowOffsets.length) {
                int newCap = rowOffsets.length << 1;
                rowOffsets = Arrays.copyOf(rowOffsets, newCap);
                rowLengths = Arrays.copyOf(rowLengths, newCap);
            }
        }
    }

    /**
     * Sink fired for every primitive leaf value during {@link #parseToScratch}.
     *
     * <p>Invoked for {@code VALUE_STRING}, {@code VALUE_NUMBER}, and {@code VALUE_BOOLEAN} tokens
     * directly under an object. Not invoked for {@code VALUE_NULL} (matching today's
     * {@code RoutingHashBuilder.extractItem} behavior, which skips nulls), nor for elements inside
     * arrays or empty objects encoded as {@code KEY_VALUE} leaves.
     *
     * <p>The encoder dispatches differently based on {@link #passRawText()}: sinks that want the
     * raw UTF-8 byte slice for every primitive (e.g. routing-path hashing) get
     * {@link #onTextPrimitive} for every leaf, and sinks that want typed values (e.g. tsid
     * dimensions) get {@link #onLongPrimitive} / {@link #onDoublePrimitive} /
     * {@link #onBooleanPrimitive} for numeric and boolean leaves with no wasted
     * {@code parser.optimizedText().bytes()} call.
     *
     * <p>Arrays at leaf positions are signalled via {@link #onArrayLeaf} regardless, so the caller
     * can react (e.g. throw to abandon batch encoding for the bulk).
     */
    public interface LeafSink {

        LeafSink NO_OP = () -> false;

        /**
         * Returns true if this sink wants the parser's UTF-8 text bytes for every primitive leaf
         * (via {@link #onTextPrimitive}), false if it wants typed values (via
         * {@link #onLongPrimitive} / {@link #onDoublePrimitive} / {@link #onBooleanPrimitive}) for
         * numeric and boolean leaves. The encoder reads this once per document.
         */
        boolean passRawText();

        /**
         * Called in raw-text mode ({@link #passRawText()} = {@code true}) for every primitive leaf,
         * and in typed mode for {@code STRING} leaves and unrecognized number types (BIG_DECIMAL /
         * BIG_INTEGER) that the encoder narrows to a string representation.
         *
         * @param columnIndex schema leaf index (stable across documents in this encoder)
         * @param dottedPath cached dotted path for the column
         * @param type the {@link EirfType} byte assigned to the value
         * @param textBytes UTF-8 byte slice of the parser token's textual form
         */
        default void onTextPrimitive(int columnIndex, String dottedPath, byte type, XContentString.UTF8Bytes textBytes) {}

        /**
         * Called in typed mode ({@link #passRawText()} = {@code false}) for {@code INT} and
         * {@code LONG} primitives. The encoder narrows numerics that fit in the int range to
         * {@code EirfType.INT}; subclasses can dispatch further on {@code type} if they want to
         * feed e.g. {@code addIntDimension} vs {@code addLongDimension}.
         */
        default void onLongPrimitive(int columnIndex, String dottedPath, byte type, long value) {}

        /**
         * Called in typed mode for {@code FLOAT} and {@code DOUBLE} primitives. The value passed is
         * the actual {@code double} (already reconstructed for narrowed {@code FLOAT} columns),
         * not raw bits.
         */
        default void onDoublePrimitive(int columnIndex, String dottedPath, byte type, double value) {}

        /** Called in typed mode for boolean primitives. */
        default void onBooleanPrimitive(int columnIndex, String dottedPath, boolean value) {}

        /**
         * Invoked once per array encountered as a direct leaf value under an object. The encoder
         * still encodes the array into scratch as a {@code FIXED_ARRAY} or {@code UNION_ARRAY};
         * this hook simply tells the caller that a complex value was seen at that column so it can
         * decide how to react.
         */
        default void onArrayLeaf(int columnIndex, String dottedPath) {}
    }

    static final class ScratchBuffers {
        byte[] typeBytes;
        byte[] fixedData; // 8 bytes per slot (even for 4-byte types, for simplicity)
        Object[] varData; // XContentString.UTF8Bytes or BytesReference

        // Row-level counters accumulated during parsing, used by writeRow to avoid recomputation.
        int totalVarSize;
        int varColumnCount;
        int scalarFixedSize;

        // Tracks which columns have been set in the current document, to detect duplicates.
        FixedBitSet columnsSet;

        // Reusable buffers for array element parsing. Null until first array is encountered,
        // then created and reused. Set to null while borrowed by parseArray to handle reentrance.
        byte[] arrayElemTypes;
        long[] arrayElemNumeric;
        Object[] arrayElemVar;

        ScratchBuffers(int capacity) {
            this.typeBytes = new byte[capacity];
            this.fixedData = new byte[capacity * 8];
            this.varData = new Object[capacity];
            this.columnsSet = new FixedBitSet(Math.max(capacity, 64));
        }

        void ensureCapacity(int needed) {
            if (needed <= typeBytes.length) return;
            int cap = typeBytes.length;
            while (cap <= needed) {
                cap <<= 1;
            }
            typeBytes = Arrays.copyOf(typeBytes, cap);
            fixedData = Arrays.copyOf(fixedData, cap * 8);
            varData = Arrays.copyOf(varData, cap);
            columnsSet = FixedBitSet.ensureCapacity(columnsSet, cap);
        }

        void resetCounters() {
            totalVarSize = 0;
            varColumnCount = 0;
            scalarFixedSize = 0;
            columnsSet.clear();
        }
    }

    private static void flattenObject(
        XContentParser parser,
        int parentNonLeafIdx,
        EirfSchema schema,
        ScratchBuffers scratch,
        XContentParser.Token firstToken,
        EirfEncoder encoder,
        LeafSink sink
    ) throws IOException {
        XContentParser.Token token = firstToken;
        while (token != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            String fieldName = parser.currentName();
            token = parser.nextToken();

            if (token == XContentParser.Token.START_OBJECT) {
                // Peek inside the object. An empty object is encoded as a zero-byte KEY_VALUE leaf
                // Non-empty objects take the normal non-leaf + recursive flatten path.
                XContentParser.Token inner = parser.nextToken();
                if (inner == XContentParser.Token.END_OBJECT) {
                    int emptyColIdx = schema.appendLeaf(fieldName, parentNonLeafIdx);
                    scratch.ensureCapacity(emptyColIdx + 1);
                    if (scratch.columnsSet.getAndSet(emptyColIdx)) {
                        throw new IllegalArgumentException("Duplicate field [" + fieldName + "]");
                    }
                    scratch.typeBytes[emptyColIdx] = EirfType.KEY_VALUE;
                    scratch.varData[emptyColIdx] = BytesArray.EMPTY;
                    scratch.varColumnCount++;
                } else {
                    int nonLeafIdx = schema.appendNonLeaf(fieldName, parentNonLeafIdx);
                    flattenObject(parser, nonLeafIdx, schema, scratch, inner, encoder, sink);
                }
                token = parser.nextToken();
                continue;
            }

            int colIdx = schema.appendLeaf(fieldName, parentNonLeafIdx);
            scratch.ensureCapacity(colIdx + 1);
            if (scratch.columnsSet.getAndSet(colIdx)) {
                throw new IllegalArgumentException("Duplicate field [" + fieldName + "]");
            }

            boolean firePathSink = sink != LeafSink.NO_OP;
            boolean rawTextMode = firePathSink && sink.passRawText();
            switch (token) {
                case START_ARRAY -> {
                    PackedArray arr = parseArray(parser, scratch);
                    scratch.typeBytes[colIdx] = arr.arrayType;
                    scratch.varData[colIdx] = new BytesArray(arr.packed);
                    scratch.totalVarSize += arr.packed.length;
                    scratch.varColumnCount++;
                    if (firePathSink) {
                        sink.onArrayLeaf(colIdx, encoder.columnPath(colIdx));
                    }
                }
                case VALUE_STRING -> {
                    scratch.typeBytes[colIdx] = EirfType.STRING;
                    XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                    scratch.varData[colIdx] = str;
                    scratch.totalVarSize += str.length();
                    scratch.varColumnCount++;
                    if (firePathSink) {
                        // Strings flow through onTextPrimitive in both modes — there's no typed value to
                        // give consumers besides the bytes themselves.
                        sink.onTextPrimitive(colIdx, encoder.columnPath(colIdx), EirfType.STRING, str);
                    }
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> {
                            long val = parser.longValue();
                            byte type;
                            if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                                type = EirfType.INT;
                                scratch.typeBytes[colIdx] = type;
                                writeIntToFixed(scratch.fixedData, colIdx, (int) val);
                                scratch.scalarFixedSize += 4;
                            } else {
                                type = EirfType.LONG;
                                scratch.typeBytes[colIdx] = type;
                                writeLongToFixed(scratch.fixedData, colIdx, val);
                                scratch.scalarFixedSize += 8;
                            }
                            if (rawTextMode) {
                                sink.onTextPrimitive(colIdx, encoder.columnPath(colIdx), type, parser.optimizedText().bytes());
                            } else if (firePathSink) {
                                sink.onLongPrimitive(colIdx, encoder.columnPath(colIdx), type, val);
                            }
                        }
                        case FLOAT, DOUBLE -> {
                            double val = parser.doubleValue();
                            float fval = (float) val;
                            byte type;
                            if ((double) fval == val) {
                                type = EirfType.FLOAT;
                                scratch.typeBytes[colIdx] = type;
                                writeIntToFixed(scratch.fixedData, colIdx, Float.floatToRawIntBits(fval));
                                scratch.scalarFixedSize += 4;
                            } else {
                                type = EirfType.DOUBLE;
                                scratch.typeBytes[colIdx] = type;
                                writeLongToFixed(scratch.fixedData, colIdx, Double.doubleToRawLongBits(val));
                                scratch.scalarFixedSize += 8;
                            }
                            if (rawTextMode) {
                                sink.onTextPrimitive(colIdx, encoder.columnPath(colIdx), type, parser.optimizedText().bytes());
                            } else if (firePathSink) {
                                sink.onDoublePrimitive(colIdx, encoder.columnPath(colIdx), type, val);
                            }
                        }
                        default -> {
                            // BIG_INTEGER / BIG_DECIMAL fall back to a string column. Both modes funnel
                            // through onTextPrimitive in this case (typed sinks treat this as
                            // "unrecognized" and may signal fallback).
                            scratch.typeBytes[colIdx] = EirfType.STRING;
                            XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                            scratch.varData[colIdx] = str;
                            scratch.totalVarSize += str.length();
                            scratch.varColumnCount++;
                            if (firePathSink) {
                                sink.onTextPrimitive(colIdx, encoder.columnPath(colIdx), EirfType.STRING, str);
                            }
                        }
                    }
                }
                case VALUE_BOOLEAN -> {
                    boolean v = parser.booleanValue();
                    byte type = v ? EirfType.TRUE : EirfType.FALSE;
                    scratch.typeBytes[colIdx] = type;
                    if (rawTextMode) {
                        // Non-JSON formats render booleans differently (YAML "yes"/"True", CBOR/SMILE
                        // binary tags exposed as canonical text). Routing-hash parity with
                        // RoutingHashBuilder depends on hashing the parser's canonical text bytes.
                        sink.onTextPrimitive(colIdx, encoder.columnPath(colIdx), type, parser.optimizedText().bytes());
                    } else if (firePathSink) {
                        sink.onBooleanPrimitive(colIdx, encoder.columnPath(colIdx), v);
                    }
                }
                case VALUE_NULL -> scratch.typeBytes[colIdx] = EirfType.NULL;
                default -> throw new IllegalStateException("Unexpected token: " + token);
            }
            token = parser.nextToken();
        }
    }

    private record PackedArray(byte arrayType, byte[] packed) {}

    /**
     * Parses an array from the parser (positioned after START_ARRAY) and packs it into
     * either FIXED_ARRAY (all elements same type) or UNION_ARRAY (mixed types) format.
     *
     * @param scratch if non-null, array element buffers are borrowed from scratch to avoid allocation.
     *                Null is passed for recursive calls where the buffers are already in use.
     */
    private static PackedArray parseArray(XContentParser parser, ScratchBuffers scratch) throws IOException {
        byte[] elemTypes;
        long[] elemNumeric;
        Object[] elemVar;
        boolean borrowed = scratch != null && scratch.arrayElemTypes != null;
        if (borrowed) {
            elemTypes = scratch.arrayElemTypes;
            elemNumeric = scratch.arrayElemNumeric;
            elemVar = scratch.arrayElemVar;
            scratch.arrayElemTypes = null;
            scratch.arrayElemNumeric = null;
            scratch.arrayElemVar = null;
        } else {
            elemTypes = new byte[16];
            elemNumeric = new long[16];
            elemVar = new Object[16];
        }

        int count = 0;
        boolean forceUnion = false;
        try {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (count >= elemTypes.length) {
                    int newCap = elemTypes.length * 2;
                    elemTypes = Arrays.copyOf(elemTypes, newCap);
                    elemNumeric = Arrays.copyOf(elemNumeric, newCap);
                    elemVar = Arrays.copyOf(elemVar, newCap);
                }
                switch (token) {
                    case START_OBJECT -> {
                        elemTypes[count] = EirfType.KEY_VALUE;
                        elemVar[count] = serializeKeyValue(parser);
                        forceUnion = true;
                    }
                    case START_ARRAY -> {
                        PackedArray nested = parseArray(parser, scratch);
                        elemTypes[count] = nested.arrayType;
                        elemVar[count] = nested.packed;
                        forceUnion = true;
                    }
                    case VALUE_STRING -> {
                        elemTypes[count] = EirfType.STRING;
                        elemVar[count] = parser.optimizedText().bytes();
                    }
                    case VALUE_NUMBER -> {
                        XContentParser.NumberType numType = parser.numberType();
                        switch (numType) {
                            case INT, LONG -> {
                                long val = parser.longValue();
                                if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                                    elemTypes[count] = EirfType.INT;
                                    elemNumeric[count] = val;
                                } else {
                                    elemTypes[count] = EirfType.LONG;
                                    elemNumeric[count] = val;
                                }
                            }
                            case FLOAT, DOUBLE -> {
                                double val = parser.doubleValue();
                                float fval = (float) val;
                                if ((double) fval == val) {
                                    elemTypes[count] = EirfType.FLOAT;
                                    elemNumeric[count] = Float.floatToRawIntBits(fval);
                                } else {
                                    elemTypes[count] = EirfType.DOUBLE;
                                    elemNumeric[count] = Double.doubleToRawLongBits(val);
                                }
                            }
                            default -> {
                                elemTypes[count] = EirfType.STRING;
                                elemVar[count] = parser.optimizedText().bytes();
                            }
                        }
                    }
                    case VALUE_BOOLEAN -> elemTypes[count] = parser.booleanValue() ? EirfType.TRUE : EirfType.FALSE;
                    case VALUE_NULL -> elemTypes[count] = EirfType.NULL;
                    default -> throw new IllegalStateException("Unexpected token in array: " + token);
                }
                count++;
            }

            boolean useFixed = false;
            byte sharedType = 0;
            if (forceUnion == false && count > 0) {
                sharedType = elemTypes[0];
                useFixed = true;
                for (int i = 1; i < count; i++) {
                    if (elemTypes[i] != sharedType) {
                        useFixed = false;
                        break;
                    }
                }
                // FIXED_ARRAY is byte-length-terminated with no element count, so a zero-data-size shared
                // type (NULL/TRUE/FALSE) would be indistinguishable from an empty array. Force UNION in
                // that case so each element contributes its type byte and the reader can iterate.
                // TODO: We will likely switch this to an element count of fixed_arrays for space. Tracked in meta issues
                if (useFixed && EirfType.elemDataSize(sharedType) == 0) {
                    useFixed = false;
                }
            }

            byte[] packed;
            byte arrayType;
            if (useFixed) {
                packed = packFixedArray(sharedType, elemNumeric, elemVar, count);
                arrayType = EirfType.FIXED_ARRAY;
            } else {
                packed = packUnionArray(elemTypes, elemNumeric, elemVar, count);
                arrayType = EirfType.UNION_ARRAY;
            }
            return new PackedArray(arrayType, packed);
        } finally {
            if (scratch != null) {
                Arrays.fill(elemVar, 0, count, null);
                scratch.arrayElemTypes = elemTypes;
                scratch.arrayElemNumeric = elemNumeric;
                scratch.arrayElemVar = elemVar;
            }
        }
    }

    /**
     * Packs a union array: per element: type(1) + data. No count byte — byte length terminates.
     */
    static byte[] packUnionArray(byte[] elemTypes, long[] elemNumeric, Object[] elemVar, int count) {
        int size = 0;
        for (int i = 0; i < count; i++) {
            size += 1; // type byte
            size += elemDataSize(elemTypes[i], elemVar[i]);
        }

        // TODO: Eventually expose a recycler here and use a recycling bytes stream output instance
        byte[] packed = new byte[size];
        int pos = 0;
        for (int i = 0; i < count; i++) {
            packed[pos++] = elemTypes[i];
            pos = writeElemData(packed, pos, elemTypes[i], elemNumeric[i], elemVar[i]);
        }
        return packed;
    }

    /**
     * Packs a fixed array: element_type(1) + per element: data only. No count byte — byte length terminates.
     */
    static byte[] packFixedArray(byte sharedType, long[] elemNumeric, Object[] elemVar, int count) {
        int size = 1; // shared type byte
        for (int i = 0; i < count; i++) {
            size += elemDataSize(sharedType, elemVar[i]);
        }

        byte[] packed = new byte[size];
        packed[0] = sharedType;
        int pos = 1;
        for (int i = 0; i < count; i++) {
            pos = writeElemData(packed, pos, sharedType, elemNumeric[i], elemVar[i]);
        }
        return packed;
    }

    private static int elemDataSize(byte type, Object varData) {
        return switch (type) {
            case EirfType.INT, EirfType.FLOAT -> 4;
            case EirfType.LONG, EirfType.DOUBLE -> 8;
            case EirfType.STRING -> {
                XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) varData;
                yield 4 + (str != null ? str.length() : 0);
            }
            case EirfType.KEY_VALUE, EirfType.UNION_ARRAY, EirfType.FIXED_ARRAY -> {
                byte[] bytes = (byte[]) varData;
                yield 4 + bytes.length; // 4-byte length prefix + payload
            }
            default -> 0; // NULL, TRUE, FALSE
        };
    }

    private static int writeElemData(byte[] packed, int pos, byte type, long numeric, Object var) {
        switch (type) {
            case EirfType.INT, EirfType.FLOAT -> {
                ByteUtils.writeIntLE((int) numeric, packed, pos);
                pos += 4;
            }
            case EirfType.LONG, EirfType.DOUBLE -> {
                ByteUtils.writeLongLE(numeric, packed, pos);
                pos += 8;
            }
            case EirfType.STRING -> {
                XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) var;
                int len = str.length();
                ByteUtils.writeIntLE(len, packed, pos);
                pos += 4;
                System.arraycopy(str.bytes(), str.offset(), packed, pos, len);
                pos += len;
            }
            case EirfType.KEY_VALUE, EirfType.UNION_ARRAY, EirfType.FIXED_ARRAY -> {
                byte[] bytes = (byte[]) var;
                ByteUtils.writeIntLE(bytes.length, packed, pos);
                pos += 4;
                System.arraycopy(bytes, 0, packed, pos, bytes.length);
                pos += bytes.length;
            }
        }
        return pos;
    }

    /**
     * Serializes an object from the parser into KEY_VALUE binary format.
     * Parser must be positioned after START_OBJECT.
     */
    static byte[] serializeKeyValue(XContentParser parser) throws IOException {
        // TODO: Eventually expose a recycler here and use a recycling instance
        BytesStreamOutput out = new BytesStreamOutput(64);

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            byte[] keyBytes = parser.currentName().getBytes(StandardCharsets.UTF_8);
            token = parser.nextToken(); // value token

            // key_length(i32) + key_bytes
            out.writeIntLE(keyBytes.length);
            out.writeBytes(keyBytes, 0, keyBytes.length);

            // type(1) + value_data
            writeElementValue(out, parser, token);
        }

        return BytesReference.toBytes(out.bytes());
    }

    /**
     * Writes a single element value (type byte + data) into the output stream.
     */
    private static void writeElementValue(BytesStreamOutput out, XContentParser parser, XContentParser.Token token) throws IOException {
        switch (token) {
            case VALUE_STRING -> {
                XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                out.writeByte(EirfType.STRING);
                out.writeIntLE(str.length());
                out.writeBytes(str.bytes(), str.offset(), str.length());
            }
            case VALUE_NUMBER -> {
                XContentParser.NumberType numType = parser.numberType();
                switch (numType) {
                    case INT, LONG -> {
                        long val = parser.longValue();
                        if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                            out.writeByte(EirfType.INT);
                            out.writeIntLE((int) val);
                        } else {
                            out.writeByte(EirfType.LONG);
                            out.writeLongLE(val);
                        }
                    }
                    case FLOAT, DOUBLE -> {
                        double val = parser.doubleValue();
                        float fval = (float) val;
                        if ((double) fval == val) {
                            out.writeByte(EirfType.FLOAT);
                            out.writeIntLE(Float.floatToRawIntBits(fval));
                        } else {
                            out.writeByte(EirfType.DOUBLE);
                            out.writeLongLE(Double.doubleToRawLongBits(val));
                        }
                    }
                    default -> {
                        XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                        out.writeByte(EirfType.STRING);
                        out.writeIntLE(str.length());
                        out.writeBytes(str.bytes(), str.offset(), str.length());
                    }
                }
            }
            case VALUE_BOOLEAN -> out.writeByte(parser.booleanValue() ? EirfType.TRUE : EirfType.FALSE);
            case VALUE_NULL -> out.writeByte(EirfType.NULL);
            case START_OBJECT -> {
                byte[] nested = serializeKeyValue(parser);
                out.writeByte(EirfType.KEY_VALUE);
                out.writeIntLE(nested.length);
                out.writeBytes(nested, 0, nested.length);
            }
            case START_ARRAY -> {
                PackedArray arr = parseArray(parser, null);
                out.writeByte(arr.arrayType);
                out.writeIntLE(arr.packed.length);
                out.writeBytes(arr.packed, 0, arr.packed.length);
            }
            default -> throw new IllegalStateException("Unexpected token: " + token);
        }
    }

    /**
     * Writes a row to output.
     *
     * <p>Row layout: row_flags(u8) | column_count(u16) | var_offset(u16 or i32) | type_bytes | fixed_section | var_section
     */
    static void writeRow(RecyclerBytesStreamOutput output, int columnCount, ScratchBuffers scratch) throws IOException {
        byte[] typeBytes = scratch.typeBytes;
        byte[] fixedData = scratch.fixedData;
        Object[] varData = scratch.varData;

        boolean smallRow = scratch.totalVarSize <= EirfType.SMALL_ROW_MAX_VAR_SIZE;
        int fixedSectionSize = scratch.scalarFixedSize + scratch.varColumnCount * (smallRow ? 4 : 8);

        // row_flags(1) + column_count(2) + var_offset(2 or 4) + type_bytes(columnCount) + fixed_section
        int varOffsetFieldSize = smallRow ? 2 : 4;
        int varOffset = 1 + 2 + varOffsetFieldSize + columnCount + fixedSectionSize;

        // Write row_flags (u8): bit 0 = small_row
        output.writeByte(smallRow ? (byte) 0x01 : (byte) 0x00);

        // Write column_count as u16 LE
        writeShortLE(output, columnCount);

        // Write var_offset
        if (smallRow) {
            writeShortLE(output, varOffset);
        } else {
            output.writeIntLE(varOffset);
        }

        // Write type bytes (unchanged — type codes are the same regardless of row size)
        for (int col = 0; col < columnCount; col++) {
            output.writeByte(typeBytes[col]);
        }

        // Write fixed section
        int varDataOffset = 0;
        for (int col = 0; col < columnCount; col++) {
            byte typeByte = typeBytes[col];
            if (typeByte < EirfType.INT) continue;

            if (typeByte == EirfType.INT || typeByte == EirfType.FLOAT) {
                output.writeBytes(fixedData, col * 8, 4);
            } else if (typeByte == EirfType.LONG || typeByte == EirfType.DOUBLE) {
                output.writeBytes(fixedData, col * 8, 8);
            } else if (EirfType.isVariable(typeByte)) {
                int len = getVarDataLength(typeByte, varData[col]);
                if (smallRow) {
                    // 4-byte entry: u16 offset | u16 length (both LE)
                    writeShortLE(output, varDataOffset);
                    writeShortLE(output, len);
                } else {
                    // 8-byte entry: i32 offset | i32 length (both LE)
                    output.writeIntLE(varDataOffset);
                    output.writeIntLE(len);
                }
                varDataOffset += len;
            }
        }

        // Write var section
        for (int col = 0; col < columnCount; col++) {
            byte typeByte = typeBytes[col];
            if (EirfType.isVariable(typeByte)) {
                writeVarData(output, typeByte, varData[col]);
            }
        }
    }

    static int getVarDataLength(byte typeByte, Object data) {
        if (typeByte == EirfType.STRING) {
            return ((XContentString.UTF8Bytes) data).length();
        } else if (typeByte == EirfType.BINARY) {
            return ((BytesReference) data).length();
        } else if (typeByte == EirfType.UNION_ARRAY || typeByte == EirfType.FIXED_ARRAY || typeByte == EirfType.KEY_VALUE) {
            return ((BytesArray) data).length();
        }
        return 0;
    }

    private static void writeVarData(RecyclerBytesStreamOutput output, byte typeByte, Object data) throws IOException {
        if (typeByte == EirfType.STRING) {
            XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) data;
            output.writeBytes(str.bytes(), str.offset(), str.length());
        } else if (typeByte == EirfType.BINARY) {
            BytesReference ref = (BytesReference) data;
            ref.writeTo(output);
        } else if (typeByte == EirfType.UNION_ARRAY || typeByte == EirfType.FIXED_ARRAY || typeByte == EirfType.KEY_VALUE) {
            BytesArray arr = (BytesArray) data;
            output.writeBytes(arr.array(), arr.arrayOffset(), arr.length());
        }
    }

    static BytesReference buildHeader(EirfSchema schema, int docCount, int[] rowOffsets, int[] rowLengths, int rowDataSize) {
        int nonLeafCount = schema.nonLeafCount();
        int leafCount = schema.leafCount();

        // Compute schema section size (all u16)
        int schemaSize = 2; // non_leaf_count u16
        byte[][] nonLeafNameBytes = new byte[nonLeafCount][];
        for (int i = 0; i < nonLeafCount; i++) {
            nonLeafNameBytes[i] = schema.getNonLeafName(i).getBytes(StandardCharsets.UTF_8);
            schemaSize += 2 + 2 + nonLeafNameBytes[i].length; // parent_index u16 + name_length u16 + name_bytes
        }
        schemaSize += 2; // leaf_count u16
        byte[][] leafNameBytes = new byte[leafCount][];
        for (int i = 0; i < leafCount; i++) {
            leafNameBytes[i] = schema.getLeafName(i).getBytes(StandardCharsets.UTF_8);
            schemaSize += 2 + 2 + leafNameBytes[i].length;
        }

        int docIndexSize = docCount * 8;
        int headerTotal = HEADER_SIZE + schemaSize + docIndexSize;

        byte[] header = new byte[headerTotal];

        int schemaOffset = HEADER_SIZE;
        int docIndexOffset = schemaOffset + schemaSize;
        int dataOffset = headerTotal;
        int totalSize = headerTotal + rowDataSize;

        // Header fields (i32 LE)
        ByteUtils.writeIntLE(EirfBatch.MAGIC_LE, header, 0);
        ByteUtils.writeIntLE(EirfBatch.VERSION, header, 4);
        ByteUtils.writeIntLE(0, header, 8); // flags
        ByteUtils.writeIntLE(docCount, header, 12);
        ByteUtils.writeIntLE(schemaOffset, header, 16);
        ByteUtils.writeIntLE(docIndexOffset, header, 20);
        ByteUtils.writeIntLE(dataOffset, header, 24);
        ByteUtils.writeIntLE(totalSize, header, 28);

        // Schema section: non-leaf fields (u16 LE)
        int pos = schemaOffset;
        writeShortLE(header, pos, nonLeafCount);
        pos += 2;
        for (int i = 0; i < nonLeafCount; i++) {
            writeShortLE(header, pos, schema.getNonLeafParent(i));
            pos += 2;
            writeShortLE(header, pos, nonLeafNameBytes[i].length);
            pos += 2;
            System.arraycopy(nonLeafNameBytes[i], 0, header, pos, nonLeafNameBytes[i].length);
            pos += nonLeafNameBytes[i].length;
        }

        // Schema section: leaf fields (u16 LE)
        writeShortLE(header, pos, leafCount);
        pos += 2;
        for (int i = 0; i < leafCount; i++) {
            writeShortLE(header, pos, schema.getLeafParent(i));
            pos += 2;
            writeShortLE(header, pos, leafNameBytes[i].length);
            pos += 2;
            System.arraycopy(leafNameBytes[i], 0, header, pos, leafNameBytes[i].length);
            pos += leafNameBytes[i].length;
        }

        // Doc index section (i32 LE)
        for (int i = 0; i < docCount; i++) {
            ByteUtils.writeIntLE(rowOffsets[i], header, docIndexOffset + i * 8);
            ByteUtils.writeIntLE(rowLengths[i], header, docIndexOffset + i * 8 + 4);
        }

        return new BytesArray(header);
    }

    static void writeLongToFixed(byte[] fixedData, int colIdx, long value) {
        ByteUtils.writeLongLE(value, fixedData, colIdx * 8);
    }

    static void writeIntToFixed(byte[] fixedData, int colIdx, int value) {
        ByteUtils.writeIntLE(value, fixedData, colIdx * 8);
    }

    private static void writeShortLE(byte[] buf, int offset, int value) {
        buf[offset] = (byte) value;
        buf[offset + 1] = (byte) (value >>> 8);
    }

    private static void writeShortLE(RecyclerBytesStreamOutput output, int value) throws IOException {
        output.writeByte((byte) value);
        output.writeByte((byte) (value >>> 8));
    }
}
