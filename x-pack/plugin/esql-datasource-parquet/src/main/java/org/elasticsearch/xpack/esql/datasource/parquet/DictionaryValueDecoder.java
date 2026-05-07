/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.Dictionary;
import org.elasticsearch.xpack.esql.core.util.Check;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Bulk decoder for RLE_DICTIONARY-encoded Parquet values. Reads a 1-byte bit width header,
 * then decodes RLE hybrid-encoded dictionary indices and gathers typed values from the
 * {@link Dictionary}.
 *
 * <p>In addition to the typed read methods that resolve indices through the dictionary,
 * the decoder also exposes raw indices via {@code readIndices} and the decoded dictionary
 * via {@code getDictionaryBytesRefs}, which together let callers emit an ordinal-backed
 * {@code OrdinalBytesRefBlock} without materializing every value as a separate {@code BytesRef}.
 *
 * <p>Lifetime / threading: pages emitted by {@code PageColumnReader} can be consumed
 * asynchronously by a different driver thread, possibly after the row group has been
 * released. The cached {@code BytesRef[]} returned by {@code getDictionaryBytesRefs} is built
 * by calling {@link org.apache.parquet.io.api.Binary#getBytes()}, whose contract in
 * parquet-mr is to return a JVM-owned, freshly-allocated {@code byte[]} (it does not return a
 * view into a shared buffer). Downstream consumers that intend to outlive the row group
 * should still copy the bytes into ESQL-managed storage (e.g. via
 * {@link org.elasticsearch.common.util.BytesRefArray#append}) before publishing.
 */
final class DictionaryValueDecoder {

    private ByteBuffer in;
    private int bitWidth;

    private boolean rleMode;
    private int rleLeft;
    private int rleValue;

    private byte[] packedBytes;
    private int packedByteOffset;
    private int packedRemaining;
    private final int[] groupValues = new int[8];
    private int groupNext = 8;
    private BytesRef[] cachedDictBytesRefs;
    private Dictionary cachedDict;

    void init(ByteBuffer valueBytes) {
        ByteBuffer dup = valueBytes.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        bitWidth = dup.get() & 0xFF;
        Check.isTrue(bitWidth <= 32, "Dictionary index bit width must be <= 32, got [{}]", bitWidth);
        in = dup.slice().order(ByteOrder.LITTLE_ENDIAN);
        rleMode = true;
        rleLeft = 0;
        rleValue = 0;
        packedByteOffset = 0;
        packedRemaining = 0;
        groupNext = 8;
    }

    void readInts(int[] values, int offset, int count, Dictionary dict) {
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToInt(nextIndex());
        }
    }

    void readLongs(long[] values, int offset, int count, Dictionary dict) {
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToLong(nextIndex());
        }
    }

    void readFloats(double[] values, int offset, int count, Dictionary dict) {
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToFloat(nextIndex());
        }
    }

    void readDoubles(double[] values, int offset, int count, Dictionary dict) {
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToDouble(nextIndex());
        }
    }

    void readBooleans(boolean[] values, int offset, int count, Dictionary dict) {
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToBoolean(nextIndex());
        }
    }

    void readBinaries(BytesRef[] values, int offset, int count, Dictionary dict) {
        BytesRef[] entries = getDictionaryBytesRefs(dict);
        for (int i = 0; i < count; i++) {
            values[offset + i] = entries[nextIndex()];
        }
    }

    void readIndices(int[] indices, int offset, int count) {
        for (int i = 0; i < count; i++) {
            indices[offset + i] = nextIndex();
        }
    }

    BytesRef[] getDictionaryBytesRefs(Dictionary dict) {
        Check.notNull(dict, "dictionary");
        assert cachedDict == null || cachedDict == dict;
        if (cachedDictBytesRefs == null) {
            int size = dict.getMaxId() + 1;
            BytesRef[] entries = new BytesRef[size];
            for (int i = 0; i < size; i++) {
                byte[] bytes = dict.decodeToBinary(i).getBytes();
                entries[i] = new BytesRef(bytes, 0, bytes.length);
            }
            cachedDictBytesRefs = entries;
            cachedDict = dict;
        }
        return cachedDictBytesRefs;
    }

    void skip(int count) {
        int remaining = count;
        while (remaining > 0) {
            ensureRunLoaded();
            if (rleMode) {
                int take = Math.min(remaining, rleLeft);
                rleLeft -= take;
                remaining -= take;
            } else {
                remaining -= skipPackedValues(remaining);
            }
        }
    }

    private int skipPackedValues(int remaining) {
        int skipped = 0;
        while (skipped < remaining) {
            if (packedRemaining == 0) {
                return skipped;
            }
            if (groupNext >= 8) {
                if (packedRemaining == 0) {
                    return skipped;
                }
                loadPackedGroup();
            }
            int inGroupAvail = 8 - groupNext;
            int take = Math.min(inGroupAvail, remaining - skipped);
            groupNext += take;
            packedRemaining -= take;
            skipped += take;
        }
        return skipped;
    }

    int nextIndex() {
        while (true) {
            ensureRunLoaded();
            if (rleMode) {
                if (rleLeft > 0) {
                    rleLeft--;
                    return rleValue;
                }
                startNextRun();
                continue;
            }
            if (packedRemaining > 0) {
                if (groupNext >= 8) {
                    loadPackedGroup();
                }
                int v = groupValues[groupNext++];
                packedRemaining--;
                return v;
            }
            startNextRun();
        }
    }

    private void ensureRunLoaded() {
        if (rleMode && rleLeft > 0) {
            return;
        }
        if (rleMode == false && packedRemaining > 0) {
            return;
        }
        startNextRun();
    }

    private void startNextRun() {
        Check.isTrue(in.hasRemaining(), "Truncated dictionary index stream");
        int header = readUnsignedVarInt();
        if ((header & 1) == 0) {
            rleMode = true;
            rleLeft = header >>> 1;
            rleValue = readIntLittleEndianPadded();
            packedRemaining = 0;
            groupNext = 8;
        } else {
            rleMode = false;
            int numGroups = header >>> 1;
            packedRemaining = numGroups * 8;
            int byteLen = numGroups * bitWidth;
            Check.isTrue(in.remaining() >= byteLen, "Truncated bit-packed dictionary index data");
            if (packedBytes == null || packedBytes.length < byteLen) {
                packedBytes = new byte[byteLen];
            }
            in.get(packedBytes, 0, byteLen);
            packedByteOffset = 0;
            groupNext = 8;
            rleLeft = 0;
        }
    }

    private void loadPackedGroup() {
        if (bitWidth == 0) {
            Arrays.fill(groupValues, 0);
        } else {
            unpack8Values(packedBytes, packedByteOffset, groupValues, 0, bitWidth);
            packedByteOffset += bitWidth;
        }
        groupNext = 0;
    }

    private int readUnsignedVarInt() {
        int shift = 0;
        int result = 0;
        while (true) {
            Check.isTrue(in.hasRemaining(), "Truncated varint in dictionary index stream");
            int b = in.get() & 0xFF;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            Check.isTrue(shift <= 35, "Varint overflow in dictionary index stream");
        }
    }

    private int readIntLittleEndianPadded() {
        int n = (bitWidth + 7) >>> 3;
        if (n == 0) {
            return 0;
        }
        Check.isTrue(in.remaining() >= n, "Truncated padded dictionary index value");
        int v = 0;
        for (int i = 0; i < n; i++) {
            v |= (in.get() & 0xFF) << (8 * i);
        }
        return v;
    }

    @SuppressWarnings("fallthrough")
    private static void unpack8Values(byte[] packed, int byteOffset, int[] out, int outOffset, int bitWidth) {
        switch (bitWidth) {
            case 0 -> {
                for (int i = 0; i < 8; i++) {
                    out[outOffset + i] = 0;
                }
            }
            case 1 -> {
                int b = packed[byteOffset] & 0xFF;
                for (int i = 0; i < 8; i++) {
                    out[outOffset + i] = (b >>> i) & 1;
                }
            }
            case 2 -> {
                int word = (packed[byteOffset] & 0xFF) | ((packed[byteOffset + 1] & 0xFF) << 8);
                for (int i = 0; i < 8; i++) {
                    out[outOffset + i] = (word >>> (i * 2)) & 0x3;
                }
            }
            case 4 -> {
                int word = (packed[byteOffset] & 0xFF) | ((packed[byteOffset + 1] & 0xFF) << 8) | ((packed[byteOffset + 2] & 0xFF) << 16)
                    | ((packed[byteOffset + 3] & 0xFF) << 24);
                for (int i = 0; i < 8; i++) {
                    out[outOffset + i] = (word >>> (i * 4)) & 0xF;
                }
            }
            case 8 -> {
                for (int i = 0; i < 8; i++) {
                    out[outOffset + i] = packed[byteOffset + i] & 0xFF;
                }
            }
            case 16 -> {
                for (int i = 0; i < 8; i++) {
                    int base = byteOffset + i * 2;
                    out[outOffset + i] = (packed[base] & 0xFF) | ((packed[base + 1] & 0xFF) << 8);
                }
            }
            default -> {
                int bitBase = byteOffset * 8;
                for (int i = 0; i < 8; i++) {
                    out[outOffset + i] = readBitsLE(packed, bitBase + i * bitWidth, bitWidth);
                }
            }
        }
    }

    private static int readBitsLE(byte[] data, int bitOffset, int width) {
        int byteIdx = bitOffset >>> 3;
        int bitInByte = bitOffset & 7;
        long word = 0;
        int bytesNeeded = ((bitInByte + width) + 7) >>> 3;
        for (int i = 0; i < bytesNeeded && (byteIdx + i) < data.length; i++) {
            word |= (long) (data[byteIdx + i] & 0xFF) << (i * 8);
        }
        return (int) ((word >>> bitInByte) & (width == 32 ? 0xFFFFFFFFL : ((1L << width) - 1)));
    }
}
