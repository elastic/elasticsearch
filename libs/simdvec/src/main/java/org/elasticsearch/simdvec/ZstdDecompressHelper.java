/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

import static org.elasticsearch.simdvec.internal.vectorization.JdkFeatures.SUPPORTS_HEAP_SEGMENTS;

/**
 * Helper for Zstd decompression that resolves the source data through the best available path
 * (MemorySegmentAccessInput, DirectAccessInput, or heap copy) and on JDK 22+ decompresses
 * directly into a heap byte array to avoid a final native-to-heap copy.
 */
public final class ZstdDecompressHelper {

    static final MethodHandle DECOMPRESS = Zstd.decompressHandle();

    private ZstdDecompressHelper() {}

    /**
     * Decompresses data from the input into the given {@link BytesRef}. Sets {@code bytes.bytes},
     * {@code bytes.offset}, and {@code bytes.length} to reflect the decompressed sub-range.
     *
     * @return the total number of decompressed bytes (i.e. {@code originalLength})
     */
    public static int decompress(
        DataInput in,
        int compressedLength,
        int originalLength,
        int offset,
        int length,
        BytesRef bytes,
        NativeAccess nativeAccess,
        Zstd zstd,
        byte[] copyBuffer
    ) throws IOException {
        if (SUPPORTS_HEAP_SEGMENTS) {
            return decompressToHeap(in, compressedLength, originalLength, offset, length, bytes, nativeAccess, zstd, copyBuffer);
        } else {
            return decompressToNative(in, compressedLength, originalLength, offset, length, bytes, nativeAccess, zstd, copyBuffer);
        }
    }

    /**
     * JDK 22+ path: decompress directly into the BytesRef's byte array using a heap-backed
     * MemorySegment as the destination. Avoids the intermediate CloseableByteBuffer and the
     * final native-to-heap copy.
     */
    private static int decompressToHeap(
        DataInput in,
        int compressedLength,
        int originalLength,
        int offset,
        int length,
        BytesRef bytes,
        NativeAccess nativeAccess,
        Zstd zstd,
        byte[] copyBuffer
    ) throws IOException {
        bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, originalLength);
        MemorySegment destSegment = MemorySegment.ofArray(bytes.bytes).asSlice(0, originalLength);
        int decompressedLen = decompressFromBestSource(in, compressedLength, destSegment, originalLength, nativeAccess, zstd, copyBuffer);
        bytes.offset = offset;
        bytes.length = length;
        return decompressedLen;
    }

    /**
     * JDK 21 path: decompress into a native CloseableByteBuffer, then copy the requested
     * sub-range into the BytesRef's byte array.
     */
    private static int decompressToNative(
        DataInput in,
        int compressedLength,
        int originalLength,
        int offset,
        int length,
        BytesRef bytes,
        NativeAccess nativeAccess,
        Zstd zstd,
        byte[] copyBuffer
    ) throws IOException {
        try (CloseableByteBuffer dest = nativeAccess.newConfinedBuffer(originalLength)) {
            // OLD MemorySegment destSegment = MemorySegment.ofBuffer(dest.buffer());
            MemorySegment destSegment = MemorySegment.ofBuffer(dest.buffer()).asSlice(0, originalLength);
            int decompressedLen = decompressFromBestSource(
                in,
                compressedLength,
                destSegment,
                originalLength,
                nativeAccess,
                zstd,
                copyBuffer
            );
            bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, length);
            dest.buffer().get(offset, bytes.bytes, 0, length);
            bytes.offset = 0;
            bytes.length = length;
            return decompressedLen;
        }
    }

    /**
     * Resolves the compressed source through the best available path and decompresses into destSegment.
     * Priority: MemorySegmentAccessInput > DirectAccessInput > heap copy.
     */
    private static int decompressFromBestSource(
        DataInput in,
        int compressedLength,
        MemorySegment destSegment,
        int destSize,
        NativeAccess nativeAccess,
        Zstd zstd,
        byte[] copyBuffer
    ) throws IOException {
        if (in instanceof IndexInput indexIn && in instanceof MemorySegmentAccessInput msai) {
            int result = decompressFromSegment(indexIn, msai, compressedLength, destSegment, destSize, zstd);
            if (result >= 0) {
                return result;
            }
        }

        if (in instanceof IndexInput indexIn && in instanceof DirectAccessInput directIn) {
            int result = decompressFromDirect(indexIn, directIn, compressedLength, destSegment, destSize, zstd);
            if (result >= 0) {
                return result;
            }
        }

        return copyAndDecompress(in, compressedLength, destSegment, destSize, nativeAccess, zstd, copyBuffer);
    }

    // Returns the decompressed length, or -1 if the segment slice was unavailable.
    private static int decompressFromSegment(
        IndexInput indexIn,
        MemorySegmentAccessInput msai,
        int compressedLength,
        MemorySegment destSegment,
        int destSize,
        Zstd zstd
    ) throws IOException {
        long startOffset = indexIn.getFilePointer();
        MemorySegment srcSegment = msai.segmentSliceOrNull(startOffset, compressedLength);
        if (srcSegment == null) {
            return -1;
        }
        int decompressedLen = invokeDecompress(zstd, destSegment, destSize, srcSegment, compressedLength);
        indexIn.seek(startOffset + compressedLength);
        return decompressedLen;
    }

    // Returns the decompressed length, or -1 if the direct buffer slice was unavailable.
    private static int decompressFromDirect(
        IndexInput indexIn,
        DirectAccessInput directIn,
        int compressedLength,
        MemorySegment destSegment,
        int destSize,
        Zstd zstd
    ) throws IOException {
        long startOffset = indexIn.getFilePointer();
        int[] resultHolder = { -1 };
        boolean directSuccess = directIn.withByteBufferSlice(startOffset, compressedLength, srcBuf -> {
            MemorySegment srcSegment = MemorySegment.ofBuffer(srcBuf);
            resultHolder[0] = invokeDecompress(zstd, destSegment, destSize, srcSegment, compressedLength);
        });
        if (directSuccess) {
            indexIn.seek(startOffset + compressedLength);
            return resultHolder[0];
        }
        return -1;
    }

    private static int copyAndDecompress(
        DataInput in,
        int compressedLength,
        MemorySegment destSegment,
        int destSize,
        NativeAccess nativeAccess,
        Zstd zstd,
        byte[] copyBuffer
    ) throws IOException {
        try (CloseableByteBuffer src = nativeAccess.newConfinedBuffer(compressedLength)) {
            var srcBuf = src.buffer();
            while (srcBuf.position() < compressedLength) {
                int numBytes = Math.min(copyBuffer.length, compressedLength - srcBuf.position());
                in.readBytes(copyBuffer, 0, numBytes);
                srcBuf.put(copyBuffer, 0, numBytes);
            }
            srcBuf.flip();
            // OLD MemorySegment srcSegment = MemorySegment.ofBuffer(srcBuf);
            MemorySegment srcSegment = MemorySegment.ofBuffer(srcBuf).asSlice(0, compressedLength);
            return invokeDecompress(zstd, destSegment, destSize, srcSegment, compressedLength);
        }
    }

    private static int invokeDecompress(Zstd zstd, MemorySegment dst, int dstSize, MemorySegment src, int srcSize) {
        try {
            return (int) DECOMPRESS.invokeExact(zstd, dst, dstSize, src, srcSize);
        } catch (Throwable t) {
            if (t instanceof Error e) throw e;
            if (t instanceof RuntimeException e) throw e;
            throw new RuntimeException(t);
        }
    }
}
