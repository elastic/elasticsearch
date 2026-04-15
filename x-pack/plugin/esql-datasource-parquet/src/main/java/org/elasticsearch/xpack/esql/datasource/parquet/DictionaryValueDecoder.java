/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Bulk decoder for RLE_DICTIONARY-encoded Parquet values. Reads a 1-byte bit width header,
 * then decodes RLE hybrid–encoded dictionary indices and gathers typed values from the
 * {@link Dictionary}.
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

    private int[] indexScratch;

    void init(ByteBuffer valueBytes) {
        ByteBuffer dup = valueBytes.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        bitWidth = dup.get() & 0xFF;
        if (bitWidth > 32) {
            throw new IllegalArgumentException("Dictionary index bit width must be <= 32, got " + bitWidth);
        }
        in = dup.slice().order(ByteOrder.LITTLE_ENDIAN);
        rleMode = true;
        rleLeft = 0;
        rleValue = 0;
        packedByteOffset = 0;
        packedRemaining = 0;
        groupNext = 8;
    }

    void readInts(int[] values, int offset, int count, Dictionary dict) {
        readIndicesBulk(count);
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToInt(indexScratch[i]);
        }
    }

    void readLongs(long[] values, int offset, int count, Dictionary dict) {
        readIndicesBulk(count);
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToLong(indexScratch[i]);
        }
    }

    void readFloats(double[] values, int offset, int count, Dictionary dict) {
        readIndicesBulk(count);
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToFloat(indexScratch[i]);
        }
    }

    void readDoubles(double[] values, int offset, int count, Dictionary dict) {
        readIndicesBulk(count);
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToDouble(indexScratch[i]);
        }
    }

    void readBooleans(boolean[] values, int offset, int count, Dictionary dict) {
        readIndicesBulk(count);
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToBoolean(indexScratch[i]);
        }
    }

    void readBinaries(BytesRef[] values, int offset, int count, Dictionary dict) {
        readIndicesBulk(count);
        for (int i = 0; i < count; i++) {
            Binary bin = dict.decodeToBinary(indexScratch[i]);
            byte[] bytes = bin.getBytes();
            values[offset + i] = new BytesRef(bytes, 0, bytes.length);
        }
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

    // --- Selective read methods: consume totalCount indices but only store selected positions ---

    void readIntsSelective(int[] output, int outOffset, RowSelection selection, int totalCount, Dictionary dict) {
        int outIdx = outOffset;
        int selIdx = selection.nextSelected(0);
        for (int i = 0; i < totalCount; i++) {
            int idx = nextIndex();
            if (i == selIdx) {
                output[outIdx++] = dict.decodeToInt(idx);
                selIdx = selection.nextSelected(selIdx + 1);
            }
        }
    }

    void readLongsSelective(long[] output, int outOffset, RowSelection selection, int totalCount, Dictionary dict) {
        int outIdx = outOffset;
        int selIdx = selection.nextSelected(0);
        for (int i = 0; i < totalCount; i++) {
            int idx = nextIndex();
            if (i == selIdx) {
                output[outIdx++] = dict.decodeToLong(idx);
                selIdx = selection.nextSelected(selIdx + 1);
            }
        }
    }

    void readFloatsSelective(double[] output, int outOffset, RowSelection selection, int totalCount, Dictionary dict) {
        int outIdx = outOffset;
        int selIdx = selection.nextSelected(0);
        for (int i = 0; i < totalCount; i++) {
            int idx = nextIndex();
            if (i == selIdx) {
                output[outIdx++] = dict.decodeToFloat(idx);
                selIdx = selection.nextSelected(selIdx + 1);
            }
        }
    }

    void readDoublesSelective(double[] output, int outOffset, RowSelection selection, int totalCount, Dictionary dict) {
        int outIdx = outOffset;
        int selIdx = selection.nextSelected(0);
        for (int i = 0; i < totalCount; i++) {
            int idx = nextIndex();
            if (i == selIdx) {
                output[outIdx++] = dict.decodeToDouble(idx);
                selIdx = selection.nextSelected(selIdx + 1);
            }
        }
    }

    void readBooleansSelective(boolean[] output, int outOffset, RowSelection selection, int totalCount, Dictionary dict) {
        int outIdx = outOffset;
        int selIdx = selection.nextSelected(0);
        for (int i = 0; i < totalCount; i++) {
            int idx = nextIndex();
            if (i == selIdx) {
                output[outIdx++] = dict.decodeToBoolean(idx);
                selIdx = selection.nextSelected(selIdx + 1);
            }
        }
    }

    void readBinariesSelective(BytesRef[] output, int outOffset, RowSelection selection, int totalCount, Dictionary dict) {
        int outIdx = outOffset;
        int selIdx = selection.nextSelected(0);
        for (int i = 0; i < totalCount; i++) {
            int idx = nextIndex();
            if (i == selIdx) {
                Binary bin = dict.decodeToBinary(idx);
                byte[] bytes = bin.getBytes();
                output[outIdx++] = new BytesRef(bytes, 0, bytes.length);
                selIdx = selection.nextSelected(selIdx + 1);
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

    private void readIndicesBulk(int count) {
        ensureScratch(count);
        for (int i = 0; i < count; i++) {
            indexScratch[i] = nextIndex();
        }
    }

    private void ensureScratch(int count) {
        if (indexScratch == null || indexScratch.length < count) {
            indexScratch = new int[count];
        }
    }

    private int nextIndex() {
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
        if (in.hasRemaining() == false) {
            throw new IllegalStateException("Truncated dictionary index stream");
        }
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
            if (in.remaining() < byteLen) {
                throw new IllegalStateException("Truncated bit-packed dictionary index data");
            }
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
            if (in.hasRemaining() == false) {
                throw new IllegalStateException("Truncated varint in dictionary index stream");
            }
            int b = in.get() & 0xFF;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift > 35) {
                throw new IllegalStateException("Varint overflow in dictionary index stream");
            }
        }
    }

    private int readIntLittleEndianPadded() {
        int n = (bitWidth + 7) >>> 3;
        if (n == 0) {
            return 0;
        }
        if (in.remaining() < n) {
            throw new IllegalStateException("Truncated padded dictionary index value");
        }
        int v = 0;
        for (int i = 0; i < n; i++) {
            v |= (in.get() & 0xFF) << (8 * i);
        }
        return v;
    }

    private static void unpack8Values(byte[] packed, int byteOffset, int[] out, int outOffset, int bitWidth) {
        for (int i = 0; i < 8; i++) {
            out[outOffset + i] = readBitsLE(packed, byteOffset * 8 + i * bitWidth, bitWidth);
        }
    }

    private static int readBitsLE(byte[] data, int bitOffset, int width) {
        int result = 0;
        for (int b = 0; b < width; b++) {
            int abs = bitOffset + b;
            int byteIdx = abs / 8;
            int bitInByte = abs % 8;
            int bit = (data[byteIdx] >>> bitInByte) & 1;
            result |= (bit << b);
        }
        return result;
    }
}
