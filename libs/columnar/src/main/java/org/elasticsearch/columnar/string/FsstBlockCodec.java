/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.columnar.substrate.internal.ByteArrayInts;

/**
 * Block-level codec that encodes and decodes FSST-compressed blocks. An FSST block's wire layout
 * is:
 *
 * <pre>
 * [1 byte] marker = {@link ValueStream#FSST}
 * [N bytes] serialised symbol table (see {@link FsstSymbolTable#writeTo})
 * [1 byte]  compressed-length width (1, 2, or 4)
 * [count * width bytes] per-value compressed lengths, little-endian
 * [total bytes] FSST-compressed payload: values concatenated, each independently compressed
 * </pre>
 *
 * <p>Instances are created via {@link FsstBlockCodecBuilder}. {@link #encodeBlock} writes the full
 * block into a caller-supplied buffer pre-sized with {@link #maxEncodedSize}. {@link #decodeBlock}
 * grows and returns its {@code decodedBytes} buffer; the caller must store the returned reference.
 *
 * <p>Instances are not thread-safe and are intended to be owned by a single
 * {@link ValueStream.Writer}. The internal scratch arrays ({@code payloadScratch},
 * {@code compLengths}) and the embedded {@link FsstSymbolTableBuilder} are reused across blocks to
 * avoid per-block heap allocation.
 */
final class FsstBlockCodec {

    final int maxSymbolLength;
    private final FsstSymbolTableBuilder tableBuilder;
    private byte[] payloadScratch = new byte[0];
    private int[] compLengths = new int[0];

    private FsstBlockCodec(int maxSymbolLength) {
        this.maxSymbolLength = maxSymbolLength;
        this.tableBuilder = new FsstSymbolTableBuilder().maxSymbolLength(maxSymbolLength);
    }

    static FsstBlockCodec of(int maxSymbolLength) {
        return new FsstBlockCodec(maxSymbolLength);
    }

    /**
     * Builds a symbol table from the block data, delegating to the embedded
     * {@link FsstSymbolTableBuilder}. Returns {@code null} when the block's bigram distribution is
     * too uniform to benefit from FSST (see {@link FsstSymbolTableBuilder#build}).
     */
    FsstSymbolTable buildTable(byte[] data, int totalLength, int[] pendingLengths, int count) {
        return tableBuilder.data(data, totalLength).lengths(pendingLengths, count).build();
    }

    /**
     * Returns the maximum number of bytes {@link #encodeBlock} may write for the given symbol
     * table, value count, and total uncompressed length.
     *
     * <p>Callers must size {@code dst} to at least this before calling {@link #encodeBlock}.
     */
    static int maxEncodedSize(FsstSymbolTable fsst, int count, int pendingLength) {
        // marker + symbol table + width byte + lengths (max 4 bytes each) + payload (worst case 2x)
        return 1 + fsst.serializedSize() + 1 + count * 4 + pendingLength * 2;
    }

    /**
     * Encodes {@code count} values into an FSST block written to {@code dst[dstOff..]}. The values
     * are supplied as {@code pendingBytes[0..pendingLength)} (concatenated) with per-value lengths
     * in {@code pendingLengths[0..count)}.
     *
     * <p>Uses instance scratch arrays to avoid per-call allocation. The caller must size
     * {@code dst} to at least {@code dstOff + }{@link #maxEncodedSize}{@code (fsst, count,
     * pendingLength)}.
     *
     * @return number of bytes written
     */
    int encodeBlock(byte[] pendingBytes, int pendingLength, int[] pendingLengths, int count, FsstSymbolTable fsst, byte[] dst, int dstOff) {
        payloadScratch = ArrayUtil.growNoCopy(payloadScratch, pendingLength * 2 + count);
        compLengths = ArrayUtil.growNoCopy(compLengths, count);

        int totalCompressed = 0;
        int maxCompLen = 0;
        int srcAt = 0;
        for (int i = 0; i < count; i++) {
            final int len = fsst.encode(pendingBytes, srcAt, pendingLengths[i], payloadScratch, totalCompressed);
            compLengths[i] = len;
            totalCompressed += len;
            maxCompLen = Math.max(maxCompLen, len);
            srcAt += pendingLengths[i];
        }

        final int lengthWidth = ByteArrayInts.widthFor(maxCompLen);

        dst[dstOff] = ValueStream.FSST;
        int at = dstOff + 1;
        at += fsst.writeTo(dst, at);
        dst[at++] = (byte) lengthWidth;
        for (int i = 0; i < count; i++) {
            ByteArrayInts.writeIntLE(compLengths[i], lengthWidth, dst, at);
            at += lengthWidth;
        }
        System.arraycopy(payloadScratch, 0, dst, at, totalCompressed);
        at += totalCompressed;
        return at - dstOff;
    }

    /**
     * Decodes an FSST block from {@code src[offset..]}. The decoded values are written into
     * {@code decodedBytes}; {@code starts[0..count)} and {@code lengths[0..count)} are filled to
     * index into the returned buffer.
     *
     * <p>The caller must store the returned reference: if {@code decodedBytes} was too small it
     * will have been replaced with a larger array.
     *
     * @return a byte array containing the decoded values; may be a grown replacement for
     *         {@code decodedBytes}
     */
    static byte[] decodeBlock(byte[] src, int offset, int count, byte[] decodedBytes, int[] starts, int[] lengths) {
        final int[] cursor = { offset + 1 }; // skip FSST marker byte
        final FsstSymbolTable fsst = FsstSymbolTable.readFrom(src, cursor);
        final int lengthWidth = src[cursor[0]++] & 0xFF;

        final int[] compressedLengths = new int[count];
        int totalCompressed = 0;
        for (int i = 0; i < count; i++) {
            compressedLengths[i] = ByteArrayInts.readIntLE(src, cursor[0], lengthWidth);
            cursor[0] += lengthWidth;
            totalCompressed += compressedLengths[i];
        }

        // Worst-case decoded: each compressed byte could expand to MAX_SYMBOL_LENGTH bytes.
        decodedBytes = ArrayUtil.growNoCopy(decodedBytes, totalCompressed * FsstSymbolTable.MAX_SYMBOL_LENGTH + count);

        int compAt = cursor[0];
        int decodedAt = 0;
        for (int i = 0; i < count; i++) {
            final int decodedLen = fsst.decode(src, compAt, compressedLengths[i], decodedBytes, decodedAt);
            starts[i] = decodedAt;
            lengths[i] = decodedLen;
            decodedAt += decodedLen;
            compAt += compressedLengths[i];
        }
        return decodedBytes;
    }
}
