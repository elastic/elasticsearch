/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

/**
 * Zero-copy reader for a single row in EIRF format.
 *
 * <p>Row layout:
 * <pre>
 * row_flags(u8) | column_count(u16) | var_offset(u16 or i32) | type_bytes[column_count] | fixed_section | var_section
 * </pre>
 */
public final class EirfRowReader {

    private static final int ROW_FLAGS_OFFSET = 0;
    private static final int ROW_COLUMN_COUNT_OFFSET = ROW_FLAGS_OFFSET + 1;
    // The offset to read the var_offset
    private static final int ROW_VAR_SECTION_OFFSET_OFFSET = ROW_COLUMN_COUNT_OFFSET + 2;

    // Var section offset is u16 so + 2
    private static final int SMALL_ROW_TYPE_BYTES_OFFSET = ROW_VAR_SECTION_OFFSET_OFFSET + 2;
    // Var section offset is i32 so + 4
    private static final int ROW_TYPE_BYTES_OFFSET = ROW_VAR_SECTION_OFFSET_OFFSET + 4;

    private final EirfSchema schema;
    private final BytesReference rowData;
    private final boolean smallRow;
    private final int rowColumnCount;
    private final int typeBytesOffset;
    private final int fixedSectionOffset;
    private final int varSectionOffset;

    // TODO: This class currently does a scan to read every value. We will eventually want to optimize this for sequentially reading over a
    // row with some type of cursor.
    public EirfRowReader(BytesReference rowData, EirfSchema schema) {
        this.rowData = rowData;
        this.schema = schema;

        // TODO: Could consider packing all these reads into one and unpacking the values.
        byte rowFlags = rowData.get(ROW_FLAGS_OFFSET);
        this.smallRow = (rowFlags & 0x01) != 0;
        this.rowColumnCount = EirfBatch.readU16LE(rowData, ROW_COLUMN_COUNT_OFFSET);

        if (smallRow) {
            this.varSectionOffset = EirfBatch.readU16LE(rowData, ROW_VAR_SECTION_OFFSET_OFFSET);
            this.typeBytesOffset = SMALL_ROW_TYPE_BYTES_OFFSET;
        } else {
            this.varSectionOffset = rowData.getIntLE(ROW_VAR_SECTION_OFFSET_OFFSET);
            this.typeBytesOffset = ROW_TYPE_BYTES_OFFSET;
        }
        this.fixedSectionOffset = typeBytesOffset + rowColumnCount;
    }

    public int columnCount() {
        return rowColumnCount;
    }

    public boolean isSmallRow() {
        return smallRow;
    }

    public EirfSchema schema() {
        return schema;
    }

    public byte getTypeByte(int col) {
        if (col >= rowColumnCount) {
            return EirfType.ABSENT;
        }
        return rowData.get(typeBytesOffset + col);
    }

    public boolean isAbsent(int col) {
        return getTypeByte(col) == EirfType.ABSENT;
    }

    public boolean isNull(int col) {
        return getTypeByte(col) == EirfType.NULL;
    }

    public boolean getBooleanValue(int col) {
        byte type = getTypeByte(col);
        if (type == EirfType.TRUE) return true;
        if (type == EirfType.FALSE) return false;
        throw new IllegalStateException("Column " + col + " is not a boolean, type=" + EirfType.name(type));
    }

    public int getIntValue(int col) {
        int offset = computeFixedOffset(col);
        return rowData.getIntLE(offset);
    }

    public float getFloatValue(int col) {
        int offset = computeFixedOffset(col);
        return Float.intBitsToFloat(rowData.getIntLE(offset));
    }

    public long getLongValue(int col) {
        int offset = computeFixedOffset(col);
        return rowData.getLongLE(offset);
    }

    public double getDoubleValue(int col) {
        int offset = computeFixedOffset(col);
        return Double.longBitsToDouble(rowData.getLongLE(offset));
    }

    public Text getStringValue(int col) {
        BytesRef ref = getVarBytesRef(col);
        return new Text(new XContentString.UTF8Bytes(ref.bytes, ref.offset, ref.length));
    }

    public BytesRef getBinaryValue(int col) {
        return getVarBytesRef(col);
    }

    public EirfKeyValueReader getKeyValue(int col) {
        BytesRef ref = getVarBytesRef(col);
        return new EirfKeyValueReader(ref.bytes, ref.offset, ref.length);
    }

    public EirfArrayReader getArrayValue(int col) {
        boolean fixed = getTypeByte(col) == EirfType.FIXED_ARRAY;
        BytesRef ref = getVarBytesRef(col);
        return new EirfArrayReader(ref.bytes, ref.offset, ref.length, fixed);
    }

    private BytesRef getVarBytesRef(int col) {
        long packed = readVarRef(col);
        int varOffset = varRefOffset(packed);
        int varLength = varRefLength(packed);
        return rowData.slice(varSectionOffset + varOffset, varLength).toBytesRef();
    }

    /**
     * Reads the var offset and length from the fixed section for the given column.
     * Small row: reads one LE int containing two packed u16 (offset in low 16, length in high 16).
     * Large row: reads one LE long containing two packed i32 (offset in low 32, length in high 32).
     * Returns a packed long: offset in lower 32 bits, length in upper 32 bits.
     */
    private long readVarRef(int col) {
        int offset = computeFixedOffset(col);
        if (smallRow) {
            // Two u16 LE = one i32 LE: low 16 bits = var offset, high 16 bits = var length
            int packed = rowData.getIntLE(offset);
            return (long) (packed & 0xFFFF) | ((long) (packed >>> 16) << 32);
        } else {
            // Two i32 LE = one i64 LE: low 32 bits = var offset, high 32 bits = var length
            return rowData.getLongLE(offset);
        }
    }

    private static int varRefOffset(long packed) {
        return (int) packed;
    }

    private static int varRefLength(long packed) {
        return (int) (packed >>> 32);
    }

    private int computeFixedOffset(int col) {
        int offset = fixedSectionOffset;
        for (int i = 0; i < col; i++) {
            offset += EirfType.fixedSize(rowData.get(typeBytesOffset + i), smallRow);
        }
        return offset;
    }
}
