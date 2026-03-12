/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.ArrayDeque;

public abstract class TransportDecompressor implements Releasable {

    protected int pageOffset = 0;
    protected int pageLength = 0;
    protected boolean hasSkippedHeader = false;

    protected final ArrayDeque<Recycler.V<BytesRef>> pages = new ArrayDeque<>(4);

    private final Recycler<BytesRef> recycler;

    protected TransportDecompressor(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
    }

    /**
     * Decompress the provided bytes
     *
     * @param bytesReference to decompress
     * @return number of compressed bytes consumed
     */
    public abstract int decompress(BytesReference bytesReference) throws IOException;

    public ReleasableBytesReference pollDecompressedPage(boolean isEOS) {
        if (pages.isEmpty()) {
            return null;
        } else if (pages.size() == 1) {
            if (isEOS) {
                return pollLastPage();
            } else {
                return null;
            }
        } else {
            Recycler.V<BytesRef> page = pages.pollFirst();
            return new ReleasableBytesReference(new BytesArray(page.v()), page);
        }
    }

    public abstract Compression.Scheme getScheme();

    @Override
    public void close() {
        for (Recycler.V<BytesRef> page : pages) {
            page.close();
        }
    }

    static TransportDecompressor getDecompressor(Recycler<BytesRef> recycler, BytesReference bytes) {
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

    protected ReleasableBytesReference pollLastPage() {
        Recycler.V<BytesRef> page = pages.pollFirst();
        BytesArray delegate = new BytesArray(page.v().bytes, page.v().offset, pageOffset);
        ReleasableBytesReference reference = new ReleasableBytesReference(delegate, page);
        pageLength = 0;
        pageOffset = 0;
        return reference;
    }

    protected boolean maybeAddNewPage() {
        if (pageOffset == pageLength) {
            Recycler.V<BytesRef> newPage = recycler.obtain();
            pageOffset = 0;
            pageLength = newPage.v().length;
            assert newPage.v().length > 0;
            pages.add(newPage);
            return true;
        }
        return false;
    }

    private static IllegalStateException createIllegalState(BytesReference bytes) {
        int maxToRead = Math.min(bytes.length(), 10);
        StringBuilder sb = new StringBuilder("stream marked as compressed, but no compressor found, first [").append(maxToRead)
            .append("] content bytes out of [")
            .append(bytes.length())
            .append("] readable bytes with message size [")
            .append(bytes.length())
            .append("] ")
            .append("] are [");
        for (int i = 0; i < maxToRead; i++) {
            sb.append(bytes.get(i)).append(",");
        }
        sb.append("]");
        return new IllegalStateException(sb.toString());
    }
}
