/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.common.util.ByteUtils;

import java.nio.charset.StandardCharsets;

/**
 * A forward-only reader over a key-value structure in EIRF format.
 *
 * <p>Layout: a sequence of entries, each being:
 * <pre>
 * key_length(i32 LE) | key_bytes(UTF-8) | value_type(u8) | value_data
 * </pre>
 *
 * <p>Value data sizes follow the same rules as {@link EirfArrayReader} elements:
 * INT/FLOAT=4 bytes LE, LONG/DOUBLE=8 bytes LE, STRING=i32 length LE + UTF-8 bytes,
 * NULL/TRUE/FALSE=0 bytes, KEY_VALUE/UNION_ARRAY/FIXED_ARRAY=i32 length LE + payload bytes.
 *
 * <p>Usage: call {@link #next()} to advance to each entry, then use {@link #key()} and the
 * appropriate value accessor. {@code next()} handles all positioning — value accessors are
 * pure reads and do not advance the cursor.
 */
public final class EirfKeyValueReader {

    private final byte[] data;
    private final int endOffset;

    private int pos;
    private String currentKey;
    private byte currentType;
    private int dataStart; // start of current value's data (past type byte)
    private int dataEnd;   // end of current value's data (next entry starts here)

    /**
     * Creates a key-value reader.
     * @param data the packed KV bytes
     * @param offset start offset in data
     * @param length total byte length of the KV payload
     */
    public EirfKeyValueReader(byte[] data, int offset, int length) {
        this.data = data;
        this.endOffset = offset + length;
        this.pos = offset;
        this.dataEnd = offset;
    }

    /** Creates a key-value reader over the full byte array. */
    public EirfKeyValueReader(byte[] data) {
        this(data, 0, data.length);
    }

    /**
     * Advances to the next key-value entry. Returns false when all bytes have been consumed.
     * Handles all positioning — any unconsumed data from the previous entry is skipped automatically.
     * After calling this, use {@link #key()} to get the key and {@link #type()} to get the
     * value type, then call the appropriate value accessor.
     */
    public boolean next() {
        pos = dataEnd;
        if (pos >= endOffset) {
            return false;
        }
        int keyLen = ByteUtils.readIntLE(data, pos);
        pos += 4;
        currentKey = new String(data, pos, keyLen, StandardCharsets.UTF_8);
        pos += keyLen;
        currentType = data[pos];
        pos++;
        dataStart = pos;
        int size = EirfType.elemDataSize(currentType);
        dataEnd = size >= 0 ? pos + size : pos + 4 + ByteUtils.readIntLE(data, pos);
        return true;
    }

    public String key() {
        return currentKey;
    }

    public byte type() {
        return currentType;
    }

    public int intValue() {
        return ByteUtils.readIntLE(data, dataStart);
    }

    public float floatValue() {
        return Float.intBitsToFloat(ByteUtils.readIntLE(data, dataStart));
    }

    public long longValue() {
        return ByteUtils.readLongLE(data, dataStart);
    }

    public double doubleValue() {
        return Double.longBitsToDouble(ByteUtils.readLongLE(data, dataStart));
    }

    public String stringValue() {
        int len = ByteUtils.readIntLE(data, dataStart);
        return new String(data, dataStart + 4, len, StandardCharsets.UTF_8);
    }

    /**
     * Creates a child {@link EirfKeyValueReader} reader over the current value's payload.
     * The current value must be of type KEY_VALUE.
     */
    public EirfKeyValueReader nestedKeyValue() {
        int len = ByteUtils.readIntLE(data, dataStart);
        int off = dataStart + 4;
        return new EirfKeyValueReader(data, off, len);
    }

    /**
     * Creates a child {@link EirfArrayReader} reader over the current value's payload.
     * The current value must be a UNION_ARRAY or FIXED_ARRAY.
     */
    public EirfArrayReader nestedArray() {
        int len = ByteUtils.readIntLE(data, dataStart);
        int off = dataStart + 4;
        boolean isFixed = currentType == EirfType.FIXED_ARRAY;
        return new EirfArrayReader(data, off, len, isFixed);
    }
}
