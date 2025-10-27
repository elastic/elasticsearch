/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.zstd;

import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;

import java.io.IOException;

public final class ZstdDecompressor extends Decompressor {

    // Buffer for copying between the DataInput and native memory. No hard science behind this number, it just tries to be high enough
    // to benefit from bulk copying and low enough to keep heap usage under control.
    final byte[] copyBuffer = new byte[4096];

    public ZstdDecompressor() {
    }

    @Override
    public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
        if (originalLength == 0) {
            bytes.offset = 0;
            bytes.length = 0;
            return;
        }

        final NativeAccess nativeAccess = NativeAccess.instance();
        final Zstd zstd = nativeAccess.getZstd();

        final int compressedLength = in.readVInt();

        try (
            CloseableByteBuffer src = nativeAccess.newConfinedBuffer(compressedLength);
            CloseableByteBuffer dest = nativeAccess.newConfinedBuffer(originalLength)
        ) {

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
    }

    @Override
    public Decompressor clone() {
        return new ZstdDecompressor();
    }
}
