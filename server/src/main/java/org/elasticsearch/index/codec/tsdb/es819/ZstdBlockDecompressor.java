/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;

import java.io.IOException;

/**
 * A {@link BlockDecompressor} backed by zstd that reuses the native access handle, zstd instance,
 * copy buffer, and native src/dest buffers across multiple {@link #decompress} calls, reallocating
 * only when a larger buffer is needed.
 */
final class ZstdBlockDecompressor extends BlockDecompressor {

    private final NativeAccess nativeAccess;
    private final Zstd zstd;
    private final byte[] copyBuffer = new byte[4096];
    private CloseableByteBuffer src;
    private CloseableByteBuffer dest;

    ZstdBlockDecompressor() {
        this.nativeAccess = NativeAccess.instance();
        this.zstd = nativeAccess.getZstd();
    }

    private CloseableByteBuffer ensureCapacity(CloseableByteBuffer buffer, int requiredCapacity) {
        if (buffer != null && buffer.buffer().capacity() >= requiredCapacity) {
            buffer.buffer().clear();
            return buffer;
        }
        if (buffer != null) {
            buffer.close();
        }
        return nativeAccess.newConfinedBuffer(requiredCapacity);
    }

    @Override
    void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
        if (originalLength == 0) {
            bytes.offset = 0;
            bytes.length = 0;
            return;
        }

        final int compressedLength = in.readVInt();

        src = ensureCapacity(src, compressedLength);
        dest = ensureCapacity(dest, originalLength);

        while (src.buffer().position() < compressedLength) {
            final int numBytes = Math.min(copyBuffer.length, compressedLength - src.buffer().position());
            in.readBytes(copyBuffer, 0, numBytes);
            src.buffer().put(copyBuffer, 0, numBytes);
        }
        src.buffer().flip();

        final int decompressedLen = zstd.decompress(dest, src);
        if (decompressedLen != originalLength) {
            throw new CorruptIndexException("Expected " + originalLength + " decompressed bytes, got " + decompressedLen, in);
        }

        bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, length);
        dest.buffer().get(offset, bytes.bytes, 0, length);
        bytes.offset = 0;
        bytes.length = length;
    }

    @Override
    public void close() throws IOException {
        try {
            if (src != null) {
                src.close();
            }
        } finally {
            if (dest != null) {
                dest.close();
            }
        }
    }
}
