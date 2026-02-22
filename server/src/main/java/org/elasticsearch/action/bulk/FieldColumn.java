/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentString;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Provides zero-copy read access to a single column in a {@link DocumentBatch}.
 * Values are read directly from the backing byte array via big-endian VarHandles.
 */
public class FieldColumn {

    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private final String fieldPath;
    private final ColumnType columnType;
    private final byte[] data;
    private final int dataOffset;
    private final int dataLength;
    private final int docCount;
    // For fixed-size types, nullBitmask is at the start of column data: ceil(docCount/8) bytes
    // For variable-size types, per-doc layout is: [1 byte present flag][length][value bytes]
    private final int nullBitmaskSize;

    // Cursor for sequential access to variable-length columns.
    // Tracks the byte position of the entry for cursorDocIndex, so that sequential
    // access (0, 1, 2, ...) is O(1) per doc instead of O(n).
    private int cursorDocIndex = -1;
    private int cursorPos;

    FieldColumn(String fieldPath, ColumnType columnType, byte[] data, int dataOffset, int dataLength, int docCount) {
        this.fieldPath = fieldPath;
        this.columnType = columnType;
        this.data = data;
        this.dataOffset = dataOffset;
        this.dataLength = dataLength;
        this.docCount = docCount;
        this.nullBitmaskSize = (docCount + 7) / 8;
    }

    public String fieldPath() {
        return fieldPath;
    }

    public ColumnType columnType() {
        return columnType;
    }

    /**
     * Returns true if the document has a value for this field (not null/absent).
     */
    public boolean isPresent(int docIndex) {
        if (columnType == ColumnType.NULL) return false;
        if (columnType.isFixedSize()) {
            // Fixed-size: check null bitmask at start of column data
            int byteIdx = docIndex / 8;
            int bitIdx = docIndex % 8;
            return (data[dataOffset + byteIdx] & (1 << bitIdx)) != 0;
        } else {
            // Variable-size: scan to find the entry
            return varLenPresent(docIndex);
        }
    }

    public boolean isNull(int docIndex) {
        return !isPresent(docIndex);
    }

    public int intValue(int docIndex) {
        assert columnType == ColumnType.INT;
        int offset = fixedValueOffset(docIndex, 4);
        return (int) INT_HANDLE.get(data, offset);
    }

    public long longValue(int docIndex) {
        assert columnType == ColumnType.LONG;
        int offset = fixedValueOffset(docIndex, 8);
        return (long) LONG_HANDLE.get(data, offset);
    }

    public float floatValue(int docIndex) {
        return Float.intBitsToFloat(intValue(docIndex));
    }

    public double doubleValue(int docIndex) {
        return Double.longBitsToDouble(longValue(docIndex));
    }

    public boolean booleanValue(int docIndex) {
        assert columnType == ColumnType.BOOLEAN;
        int offset = fixedValueOffset(docIndex, 1);
        return data[offset] != 0;
    }

    public String stringValue(int docIndex) {
        assert columnType == ColumnType.STRING;
        int[] offsetAndLen = varLenValueOffsetAndLength(docIndex);
        if (offsetAndLen == null) return null;
        return new String(data, offsetAndLen[0], offsetAndLen[1], StandardCharsets.UTF_8);
    }

    /**
     * Returns a zero-copy {@link XContentString.UTF8Bytes} view of the string value for the given document,
     * slicing directly into the backing byte array without materializing a {@link String}.
     */
    public XContentString.UTF8Bytes stringBytes(int docIndex) {
        assert columnType == ColumnType.STRING;
        int[] offsetAndLen = varLenValueOffsetAndLength(docIndex);
        if (offsetAndLen == null) return null;
        return new XContentString.UTF8Bytes(data, offsetAndLen[0], offsetAndLen[1]);
    }

    public BytesReference binaryValue(int docIndex) {
        assert columnType == ColumnType.BINARY;
        int[] offsetAndLen = varLenValueOffsetAndLength(docIndex);
        if (offsetAndLen == null) return null;
        return new BytesArray(data, offsetAndLen[0], offsetAndLen[1]);
    }

    // ---- Internal offset calculations ----

    /**
     * For fixed-size columns: offset to the value for docIndex.
     * Layout: [nullBitmask (ceil(N/8) bytes)][value0][value1]...[valueN-1]
     */
    private int fixedValueOffset(int docIndex, int valueSize) {
        return dataOffset + nullBitmaskSize + (docIndex * valueSize);
    }

    /**
     * Scans to the start of the entry for the given docIndex, using the cursor to skip
     * already-visited entries. For sequential access patterns (0, 1, 2, ...) this is O(1)
     * per call. For backward jumps, falls back to scanning from the start.
     */
    private int scanTo(int docIndex) {
        int pos;
        int startDoc;
        if (cursorDocIndex >= 0 && docIndex >= cursorDocIndex) {
            pos = cursorPos;
            startDoc = cursorDocIndex;
        } else {
            pos = dataOffset;
            startDoc = 0;
        }
        for (int i = startDoc; i < docIndex; i++) {
            byte present = data[pos];
            pos++;
            if (present != 0) {
                int len = (int) INT_HANDLE.get(data, pos);
                pos += 4 + len;
            }
        }
        cursorDocIndex = docIndex;
        cursorPos = pos;
        return pos;
    }

    /**
     * For variable-size columns: check if doc is present.
     * Layout: sequential entries, each [1 byte present][4 bytes length][value bytes] or [1 byte 0x00] if absent.
     */
    private boolean varLenPresent(int docIndex) {
        int pos = scanTo(docIndex);
        return data[pos] != 0;
    }

    /**
     * For variable-size columns: returns [offset, length] of the value bytes, or null if absent.
     */
    private int[] varLenValueOffsetAndLength(int docIndex) {
        int pos = scanTo(docIndex);
        byte present = data[pos];
        pos++;
        if (present == 0) return null;
        int len = (int) INT_HANDLE.get(data, pos);
        pos += 4;
        return new int[] { pos, len };
    }
}
