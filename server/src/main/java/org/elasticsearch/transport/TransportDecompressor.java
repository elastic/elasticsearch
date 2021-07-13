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
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;

import java.io.IOException;

public interface TransportDecompressor extends Releasable {

    /**
     * Decompress the provided bytes
     *
     * @param bytesReference to decompress
     * @return number of compressed bytes consumed
     */
    int decompress(BytesReference bytesReference) throws IOException;

    ReleasableBytesReference pollDecompressedPage(boolean isEOS);

    @Override
    void close();

    static TransportDecompressor getDecompressor(PageCacheRecycler recycler, BytesReference bytes) throws IOException {
        if (bytes.length() < Compression.Scheme.HEADER_LENGTH) {
            return null;
        }

        if (Compression.Scheme.isDeflate(bytes)) {
            return new DeflateTransportDecompressor(recycler);
        } else if (Compression.Scheme.isLZ4(bytes)) {
            return new Lz4TransportDecompressor(recycler);
        } else {
            throw createIllegalState(bytes);
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
