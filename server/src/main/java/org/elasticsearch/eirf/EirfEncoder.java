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
 * <p>Supports both incremental and batch usage:
 * <pre>
 * // Incremental
 * try (EirfEncoder encoder = new EirfEncoder()) {
 *     encoder.addDocument(source1, XContentType.JSON);
 *     encoder.addDocument(source2, XContentType.JSON);
 *     EirfBatch batch = encoder.build();
 * }
 *
 * // Batch convenience
 * EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);
 * </pre>
 */
public class EirfEncoder implements Releasable {

    private static final int HEADER_SIZE = 32;
    private static final int INITIAL_CAPACITY = 16;

    private final EirfSchema schema;
    private final ScratchBuffers scratch;
    private final RecyclerBytesStreamOutput rowOutput;

    private int[] rowOffsets;
    private int[] rowLengths;
    private int docCount;

    public EirfEncoder() {
        this(new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE));
    }

    public EirfEncoder(final RecyclerBytesStreamOutput rowOutput) {
        this.schema = new EirfSchema();
        this.scratch = new ScratchBuffers(INITIAL_CAPACITY);
        this.rowOutput = rowOutput;
        this.rowOffsets = new int[INITIAL_CAPACITY];
        this.rowLengths = new int[INITIAL_CAPACITY];
        this.docCount = 0;
    }

    /**
     * Adds a single document to the batch.
     */
    public void addDocument(BytesReference source, XContentType xContentType) throws IOException {
        int columnCountBefore = schema.leafCount();
        Arrays.fill(scratch.typeBytes, 0, columnCountBefore, (byte) 0);
        Arrays.fill(scratch.varData, 0, columnCountBefore, null);
        scratch.resetCounters();

        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, xContentType)) {
            // The schema will prevent duplicate columns. No need to double with JSON's internal duplicate prevention.
            parser.allowDuplicateKeys(true);
            parser.nextToken(); // START_OBJECT
            flattenObject(parser, 0, schema, scratch);
        }

        if (docCount >= rowOffsets.length) {
            int newCap = rowOffsets.length << 1;
            rowOffsets = Arrays.copyOf(rowOffsets, newCap);
            rowLengths = Arrays.copyOf(rowLengths, newCap);
        }

        int columnCount = schema.leafCount();
        int rowStart = (int) rowOutput.position();
        rowOffsets[docCount] = rowStart;
        writeRow(rowOutput, columnCount, scratch);
        rowLengths[docCount] = (int) rowOutput.position() - rowStart;
        docCount++;
    }

    /**
     * Builds the final {@link EirfBatch} from all accumulated documents.
     */
    public EirfBatch build() {
        ReleasableBytesReference rowBytes = rowOutput.moveToBytesReference();
        BytesReference headerBytes = buildHeader(schema, docCount, rowOffsets, rowLengths, rowBytes.length());
        BytesReference combined = CompositeBytesReference.of(headerBytes, rowBytes);
        return new EirfBatch(combined, rowBytes);
    }

    public int docCount() {
        return docCount;
    }

    @Override
    public void close() {
        rowOutput.close();
    }

    public static EirfBatch encode(List<BytesReference> sources, XContentType xContentType) throws IOException {
        try (EirfEncoder encoder = new EirfEncoder()) {
            for (BytesReference source : sources) {
                encoder.addDocument(source, xContentType);
            }
            return encoder.build();
        }
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

    private static void flattenObject(XContentParser parser, int parentNonLeafIdx, EirfSchema schema, ScratchBuffers scratch)
        throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            String fieldName = parser.currentName();
            token = parser.nextToken();

            if (token == XContentParser.Token.START_OBJECT) {
                int nonLeafIdx = schema.appendNonLeaf(fieldName, parentNonLeafIdx);
                flattenObject(parser, nonLeafIdx, schema, scratch);
                continue;
            }

            int colIdx = schema.appendLeaf(fieldName, parentNonLeafIdx);
            scratch.ensureCapacity(colIdx + 1);
            if (scratch.columnsSet.getAndSet(colIdx)) {
                throw new IllegalArgumentException("Duplicate field [" + fieldName + "]");
            }

            switch (token) {
                case START_ARRAY -> {
                    PackedArray arr = parseArray(parser, scratch);
                    scratch.typeBytes[colIdx] = arr.arrayType;
                    scratch.varData[colIdx] = new BytesArray(arr.packed);
                    scratch.totalVarSize += arr.packed.length;
                    scratch.varColumnCount++;
                }
                case VALUE_STRING -> {
                    scratch.typeBytes[colIdx] = EirfType.STRING;
                    XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                    scratch.varData[colIdx] = str;
                    scratch.totalVarSize += str.length();
                    scratch.varColumnCount++;
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> {
                            long val = parser.longValue();
                            if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                                scratch.typeBytes[colIdx] = EirfType.INT;
                                writeIntToFixed(scratch.fixedData, colIdx, (int) val);
                                scratch.scalarFixedSize += 4;
                            } else {
                                scratch.typeBytes[colIdx] = EirfType.LONG;
                                writeLongToFixed(scratch.fixedData, colIdx, val);
                                scratch.scalarFixedSize += 8;
                            }
                        }
                        case FLOAT, DOUBLE -> {
                            double val = parser.doubleValue();
                            float fval = (float) val;
                            if ((double) fval == val) {
                                scratch.typeBytes[colIdx] = EirfType.FLOAT;
                                writeIntToFixed(scratch.fixedData, colIdx, Float.floatToRawIntBits(fval));
                                scratch.scalarFixedSize += 4;
                            } else {
                                scratch.typeBytes[colIdx] = EirfType.DOUBLE;
                                writeLongToFixed(scratch.fixedData, colIdx, Double.doubleToRawLongBits(val));
                                scratch.scalarFixedSize += 8;
                            }
                        }
                        default -> {
                            scratch.typeBytes[colIdx] = EirfType.STRING;
                            XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                            scratch.varData[colIdx] = str;
                            scratch.totalVarSize += str.length();
                            scratch.varColumnCount++;
                        }
                    }
                }
                case VALUE_BOOLEAN -> scratch.typeBytes[colIdx] = parser.booleanValue() ? EirfType.TRUE : EirfType.FALSE;
                case VALUE_NULL -> scratch.typeBytes[colIdx] = EirfType.NULL;
                default -> throw new IllegalStateException("Unexpected token: " + token);
            }
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
