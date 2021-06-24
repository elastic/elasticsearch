/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.compress.DeflateCompressor;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;

import java.io.IOException;

public interface TransportDecompressor extends Releasable {

    int decompress(BytesReference bytesReference) throws IOException;

    ReleasableBytesReference pollDecompressedPage(boolean isEOS);

    @Override
    void close();

    byte[] DEFLATE_HEADER = DeflateCompressor.HEADER;
    byte[] LZ4_HEADER = new byte[]{'L', 'Z', '4', '\0'};
    int HEADER_LENGTH = 4;

    static TransportDecompressor getDecompressor(PageCacheRecycler recycler, BytesReference bytes) throws IOException {
        if (bytes.length() < DeflateCompressor.HEADER.length) {
            return null;
        }
        byte firstByte = bytes.get(0);
        byte[] header;
        if (firstByte == DEFLATE_HEADER[0]) {
            header = DEFLATE_HEADER;
        } else if (firstByte == LZ4_HEADER[0]) {
            header = LZ4_HEADER;
        } else {
            throw createIllegalState(bytes);
        }

        for (int i = 1; i < HEADER_LENGTH; ++i) {
            if (bytes.get(i) != header[i]) {
                throw createIllegalState(bytes);
            }
        }

        if (header == DEFLATE_HEADER) {
            return new DeflateTransportDecompressor(recycler);
        } else {
            return new Lz4TransportDecompressor(recycler);
        }
    }

    private static IllegalStateException createIllegalState(BytesReference bytes) {
        int maxToRead = Math.min(bytes.length(), 10);
        StringBuilder sb = new StringBuilder("stream marked as compressed, but no compressor found, first [")
            .append(maxToRead).append("] content bytes out of [").append(bytes.length())
            .append("] readable bytes with message size [").append(bytes.length()).append("] ").append("] are [");
        for (int i = 0; i < maxToRead; i++) {
            sb.append(bytes.get(i)).append(",");
        }
        sb.append("]");
        return new IllegalStateException(sb.toString());
    }
}
