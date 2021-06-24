/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import net.jpountz.lz4.LZ4FrameInputStream;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageCacheRecycler;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;

public class Lz4TransportDecompressor implements TransportDecompressor {

    private final LZ4FrameInputStream inputStream;
    private final ExpandableStream expandableStream;
    private final PageCacheRecycler recycler;
    private final ArrayDeque<Recycler.V<byte[]>> pages;
    private int pageOffset = PageCacheRecycler.BYTE_PAGE_SIZE;
    private boolean hasSkippedHeader = false;

    public Lz4TransportDecompressor(PageCacheRecycler recycler) throws IOException {
        this.recycler = recycler;
        expandableStream = new ExpandableStream();
        inputStream = new LZ4FrameInputStream(expandableStream);
        pages = new ArrayDeque<>(4);
    }

    @Override
    public int decompress(BytesReference bytesReference) throws IOException {
        final StreamInput underlyingStream = bytesReference.streamInput();
        this.expandableStream.nextStream(underlyingStream);

        if (hasSkippedHeader == false) {
            hasSkippedHeader = true;
            int headerLength = TransportDecompressor.HEADER_LENGTH;
            bytesReference = bytesReference.slice(headerLength, bytesReference.length() - headerLength);
        }

        boolean continueDecompressing = true;
        while (continueDecompressing) {
            final Recycler.V<byte[]> page;
            final boolean isNewPage = pageOffset == PageCacheRecycler.BYTE_PAGE_SIZE;
            if (isNewPage) {
                pageOffset = 0;
                page = recycler.bytePage(false);
            } else {
                page = pages.getLast();
            }
            byte[] output = page.v();
            int bytesDecompressed;
            try {
                bytesDecompressed = inputStream.read(output, pageOffset, PageCacheRecycler.BYTE_PAGE_SIZE - pageOffset);
                pageOffset += bytesDecompressed;
                if (isNewPage) {
                    if (bytesDecompressed == 0) {
                        page.close();
                        pageOffset = PageCacheRecycler.BYTE_PAGE_SIZE;
                    } else {
                        pages.add(page);
                    }
                }
            } catch (IOException e) {
                throw new IOException("Exception while LZ4 decompressing bytes", e);
            }
            if (bytesDecompressed == 0) {
                continueDecompressing = false;
            }
        }

        assert underlyingStream.available() == 0;

        return bytesReference.length();
    }

    @Override
    public ReleasableBytesReference pollDecompressedPage(boolean isEOS) {
        if (pages.isEmpty()) {
            return null;
        } else if (pages.size() == 1) {
            if (isEOS) {
                Recycler.V<byte[]> page = pages.pollFirst();
                ReleasableBytesReference reference = new ReleasableBytesReference(new BytesArray(page.v(), 0, pageOffset), page);
                pageOffset = 0;
                return reference;
            } else {
                return null;
            }
        } else {
            Recycler.V<byte[]> page = pages.pollFirst();
            return new ReleasableBytesReference(new BytesArray(page.v()), page);
        }
    }

    @Override
    public void close() {
        try {
            inputStream.close();
        } catch (IOException e) {
            assert false : "Exception should not be thrown.";
        }
        for (Recycler.V<byte[]> page : pages) {
            page.close();
        }
    }

    private static class ExpandableStream extends InputStream {

        private StreamInput current;

        private void nextStream(StreamInput next) {
            current = next;
        }

        @Override
        public int read() throws IOException {
            return Math.max(0, current.read());
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return Math.max(0, current.read(b, off, len));
        }
    }
}
