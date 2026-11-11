/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.Dictionary;
import org.elasticsearch.compute.data.UninitializedArrays;
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

    /**
     * Trailing zero bytes appended to {@link #packedBytes} so that {@link #readBitsLE} (and the
     * bulk unpack paths) can always read a full 8-byte little-endian word starting at any byte
     * offset within the encoded run without bounds-checking. The bit-width is capped at 32, so a
     * group of 8 values occupies at most {@code 32 bytes}; a single value spans at most
     * {@code 5 bytes}. 7 bytes of padding is sufficient to make any 8-byte read from a valid byte
     * offset safe.
     */
    private static final int PACKED_BYTES_PADDING = 7;

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

    /**
     * Reusable scratch buffer for the bulk decode path used by the typed read methods
     * ({@link #readInts}, {@link #readLongs}, etc.). The bulk path decodes indices in one pass
     * into this buffer and then runs a tight gather loop through the {@link Dictionary},
     * eliminating the per-value RLE/bit-packed state-machine branches that {@link #nextIndex}
     * has to walk for every element.
     */
    private int[] scratchIndices;

    void init(ByteBuffer valueBytes) {
        ByteBuffer dup = valueBytes.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        bitWidth = dup.get() & 0xFF;
        if (bitWidth > 32) {
            throw new IllegalArgumentException("Dictionary index bit width must be <= 32, got [" + bitWidth + "]");
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
        int[] indices = scratchIndices(count);
        readIndicesBulk(indices, 0, count);
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToInt(indices[i]);
        }
    }

    void readLongs(long[] values, int offset, int count, Dictionary dict) {
        int[] indices = scratchIndices(count);
        readIndicesBulk(indices, 0, count);
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToLong(indices[i]);
        }
    }

    void readFloats(double[] values, int offset, int count, Dictionary dict) {
        int[] indices = scratchIndices(count);
        readIndicesBulk(indices, 0, count);
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToFloat(indices[i]);
        }
    }

    void readDoubles(double[] values, int offset, int count, Dictionary dict) {
        int[] indices = scratchIndices(count);
        readIndicesBulk(indices, 0, count);
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToDouble(indices[i]);
        }
    }

    void readBooleans(boolean[] values, int offset, int count, Dictionary dict) {
        int[] indices = scratchIndices(count);
        readIndicesBulk(indices, 0, count);
        for (int i = 0; i < count; i++) {
            values[offset + i] = dict.decodeToBoolean(indices[i]);
        }
    }

    void readBinaries(BytesRef[] values, int offset, int count, Dictionary dict) {
        BytesRef[] entries = getDictionaryBytesRefs(dict);
        int[] indices = scratchIndices(count);
        readIndicesBulk(indices, 0, count);
        for (int i = 0; i < count; i++) {
            values[offset + i] = entries[indices[i]];
        }
    }

    void readIndices(int[] indices, int offset, int count) {
        readIndicesBulk(indices, offset, count);
    }

    /**
     * Bulk decode {@code count} dictionary indices into {@code dst} starting at {@code offset}.
     * Avoids the per-value {@link #nextIndex} state machine (three branches per element) by
     * dispatching on the current run type once and consuming as many values as possible in a
     * tight loop. For bit-packed runs, full 8-value groups are decoded directly into
     * {@code dst}; the final partial group, if any, is routed through {@link #groupValues} so
     * the per-value path can resume from the same state on a subsequent call.
     */
    private void readIndicesBulk(int[] dst, int offset, int count) {
        int pos = offset;
        int remaining = count;
        while (remaining > 0) {
            // Drain any partially-consumed bit-packed group left behind by a previous
            // nextIndex() call before starting a fresh run-aligned decode.
            if (rleMode == false && groupNext < 8) {
                int take = Math.min(remaining, 8 - groupNext);
                System.arraycopy(groupValues, groupNext, dst, pos, take);
                groupNext += take;
                packedRemaining -= take;
                pos += take;
                remaining -= take;
                continue;
            }
            ensureRunLoaded();
            if (rleMode) {
                int take = Math.min(remaining, rleLeft);
                Arrays.fill(dst, pos, pos + take, rleValue);
                rleLeft -= take;
                pos += take;
                remaining -= take;
            } else {
                // Bit-packed run. The drain branch above ensures groupNext == 8 here. Each
                // loadPackedGroup() unpacks 8 values into groupValues without changing
                // packedRemaining, and consumers (drain branch above, nextIndex, the tail
                // below) decrement packedRemaining by the number of values they actually take
                // from groupValues. Combined with packedRemaining being initialized to a
                // multiple of 8 in startNextRun, this means packedRemaining is a non-negative
                // multiple of 8 whenever groupNext == 8.
                int wantFromRun = Math.min(remaining, packedRemaining);
                int fullGroups = wantFromRun >>> 3;
                if (bitWidth == 0) {
                    // No bits to read — every index in the run is zero. Skip packedBytes.
                    int consumedFull = fullGroups << 3;
                    Arrays.fill(dst, pos, pos + consumedFull, 0);
                    pos += consumedFull;
                    remaining -= consumedFull;
                    packedRemaining -= consumedFull;
                } else {
                    for (int g = 0; g < fullGroups; g++) {
                        unpack8Values(packedBytes, packedByteOffset, dst, pos, bitWidth);
                        packedByteOffset += bitWidth;
                        pos += 8;
                        remaining -= 8;
                        packedRemaining -= 8;
                    }
                }
                // Tail: we still need < 8 values from the run (either because count < 8 or
                // the run had < 8 values left). Load one group into groupValues so future
                // per-value nextIndex() callers see consistent state.
                int tail = wantFromRun - (fullGroups << 3);
                if (tail > 0) {
                    loadPackedGroup();
                    System.arraycopy(groupValues, 0, dst, pos, tail);
                    groupNext = tail;
                    packedRemaining -= tail;
                    pos += tail;
                    remaining -= tail;
                }
            }
        }
    }

    private int[] scratchIndices(int count) {
        int[] s = scratchIndices;
        if (s == null || s.length < count) {
            // Uninitialized allocation: every slot is overwritten by readIndicesBulk before
            // the gather loop reads it, so leaving the bytes uninitialized is safe and avoids
            // the zeroing memset on the hot path.
            s = UninitializedArrays.newIntArray(count);
            scratchIndices = s;
        }
        return s;
    }

    BytesRef[] getDictionaryBytesRefs(Dictionary dict) {
        // Unlike the byte-content guards below, a null dictionary here is not a corrupt-file symptom: a
        // dictionary-encoded page always has its dictionary loaded by the reader before indices are
        // decoded (a file missing its dictionary page fails earlier, at dictionary-page read). So a null
        // here means our reader failed to wire the dictionary — an internal invariant, kept server-class.
        Check.notNull(dict, "dictionary");
        assert cachedDict == null || cachedDict == dict;
        if (cachedDictBytesRefs == null) {
            int size = dict.getMaxId() + 1;
            BytesRef[] entries = new BytesRef[size];
            for (int i = 0; i < size; i++) {
                byte[] bytes = dict.decodeToBinary(i).getBytes();
                // Note: this decoder is type-agnostic; the same dictionary bytes back INT96 timestamps,
                // decimals and float16 (read via readBinaries), so UTF-8 sanitization must NOT happen here.
                // KEYWORD callers sanitize in PageColumnReader's ordinal/fallback paths instead.
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
        if (in.hasRemaining() == false) {
            throw new IllegalArgumentException("Truncated dictionary index stream");
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
                throw new IllegalArgumentException("Truncated bit-packed dictionary index data");
            }
            if (packedBytes == null || packedBytes.length < byteLen + PACKED_BYTES_PADDING) {
                // Allocate with trailing padding so readBitsLE can do an unchecked 8-byte read at
                // any byte offset within [0, byteLen). The padding bytes are explicitly zeroed
                // below so the mask in readBitsLE drops them.
                packedBytes = UninitializedArrays.newByteArray(byteLen + PACKED_BYTES_PADDING);
            }
            in.get(packedBytes, 0, byteLen);
            // Zero the padding region — UninitializedArrays.newByteArray may return arbitrary
            // bytes there, and a previous, longer run could leave non-zero data past byteLen.
            Arrays.fill(packedBytes, byteLen, byteLen + PACKED_BYTES_PADDING, (byte) 0);
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
                throw new IllegalArgumentException("Truncated varint in dictionary index stream");
            }
            int b = in.get() & 0xFF;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift > 35) {
                throw new IllegalArgumentException("Varint overflow in dictionary index stream");
            }
        }
    }

    private int readIntLittleEndianPadded() {
        int n = (bitWidth + 7) >>> 3;
        if (n == 0) {
            return 0;
        }
        if (in.remaining() < n) {
            throw new IllegalArgumentException("Truncated padded dictionary index value");
        }
        int v = 0;
        for (int i = 0; i < n; i++) {
            v |= (in.get() & 0xFF) << (8 * i);
        }
        return v;
    }

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
                // Eight 2-bit values occupy exactly 2 bytes; VH_LE_SHORT reads them as one
                // little-endian short. The & 0xFFFF widens it to a positive int for shifting.
                int word = ((short) BitUtil.VH_LE_SHORT.get(packed, byteOffset)) & 0xFFFF;
                for (int i = 0; i < 8; i++) {
                    out[outOffset + i] = (word >>> (i * 2)) & 0x3;
                }
            }
            case 4 -> {
                // Eight 4-bit values occupy exactly 4 bytes; one little-endian int read.
                int word = (int) BitUtil.VH_LE_INT.get(packed, byteOffset);
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
                // Eight 16-bit values, each one little-endian short read at byteOffset + i * 2.
                for (int i = 0; i < 8; i++) {
                    out[outOffset + i] = ((short) BitUtil.VH_LE_SHORT.get(packed, byteOffset + i * 2)) & 0xFFFF;
                }
            }
            // For bit widths 3/5/6/7, eight values occupy 24/40/48/56 bits — always strictly
            // less than 64. We can read the full group as one little-endian long and shift out
            // each value. The PACKED_BYTES_PADDING trailing zeros allocated by startNextRun()
            // make the 8-byte read safe at any byteOffset that addresses a valid group.
            case 3 -> unpack8FromSingleLong(packed, byteOffset, out, outOffset, 3, 0x7L);
            case 5 -> unpack8FromSingleLong(packed, byteOffset, out, outOffset, 5, 0x1FL);
            case 6 -> unpack8FromSingleLong(packed, byteOffset, out, outOffset, 6, 0x3FL);
            case 7 -> unpack8FromSingleLong(packed, byteOffset, out, outOffset, 7, 0x7FL);
            default -> {
                int bitBase = byteOffset * 8;
                for (int i = 0; i < 8; i++) {
                    out[outOffset + i] = readBitsLE(packed, bitBase + i * bitWidth, bitWidth);
                }
            }
        }
    }

    private static void unpack8FromSingleLong(byte[] packed, int byteOffset, int[] out, int outOffset, int width, long mask) {
        long word = readLittleEndianLong(packed, byteOffset);
        out[outOffset] = (int) (word & mask);
        out[outOffset + 1] = (int) ((word >>> width) & mask);
        out[outOffset + 2] = (int) ((word >>> (2 * width)) & mask);
        out[outOffset + 3] = (int) ((word >>> (3 * width)) & mask);
        out[outOffset + 4] = (int) ((word >>> (4 * width)) & mask);
        out[outOffset + 5] = (int) ((word >>> (5 * width)) & mask);
        out[outOffset + 6] = (int) ((word >>> (6 * width)) & mask);
        out[outOffset + 7] = (int) ((word >>> (7 * width)) & mask);
    }

    private static int readBitsLE(byte[] data, int bitOffset, int width) {
        int byteIdx = bitOffset >>> 3;
        int bitInByte = bitOffset & 7;
        // Single 8-byte little-endian read covers any (bitInByte + width) up to 64 bits.
        // packedBytes is padded with PACKED_BYTES_PADDING trailing zero bytes (see startNextRun),
        // so reading 8 bytes from byteIdx never walks past the array; the trailing zeros do not
        // affect the result because the mask below isolates only the requested bits.
        long word = readLittleEndianLong(data, byteIdx);
        // Shift by 64 is undefined in Java, but width is always in [0, 32] here, so the mask
        // computation is safe; the width == 32 case yields 0xFFFFFFFFL.
        long mask = width == 32 ? 0xFFFFFFFFL : ((1L << width) - 1);
        return (int) ((word >>> bitInByte) & mask);
    }

    /**
     * Reads 8 bytes from {@code data} starting at {@code offset} as a little-endian {@code long}.
     * The caller must ensure {@code offset + 8 <= data.length}; this is guaranteed for
     * {@code packedBytes} because {@link #startNextRun()} allocates it with
     * {@link #PACKED_BYTES_PADDING} trailing bytes.
     */
    private static long readLittleEndianLong(byte[] data, int offset) {
        return (long) BitUtil.VH_LE_LONG.get(data, offset);
    }
}
