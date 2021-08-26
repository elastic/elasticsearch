/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageCacheRecycler;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class DeflateTransportDecompressor implements TransportDecompressor {

    private final Inflater inflater;
    private final PageCacheRecycler recycler;
    private final ArrayDeque<Recycler.V<byte[]>> pages;
    private int pageOffset = PageCacheRecycler.BYTE_PAGE_SIZE;
    private boolean hasSkippedHeader = false;

    public DeflateTransportDecompressor(PageCacheRecycler recycler) {
        this.recycler = recycler;
        inflater = new Inflater(true);
        pages = new ArrayDeque<>(4);
    }

    @Override
    public int decompress(BytesReference bytesReference) throws IOException {
        int bytesConsumed = 0;
        if (hasSkippedHeader == false) {
            hasSkippedHeader = true;
            int headerLength = Compression.Scheme.HEADER_LENGTH;
            bytesReference = bytesReference.slice(headerLength, bytesReference.length() - headerLength);
            bytesConsumed += headerLength;
        }

        BytesRefIterator refIterator = bytesReference.iterator();
        BytesRef ref;
        while ((ref = refIterator.next()) != null) {
            inflater.setInput(ref.bytes, ref.offset, ref.length);
            bytesConsumed += ref.length;
            boolean continueInflating = true;
            while (continueInflating) {
                final boolean isNewPage = pageOffset == PageCacheRecycler.BYTE_PAGE_SIZE;
                if (isNewPage) {
                    pageOffset = 0;
                    pages.add(recycler.bytePage(false));
                }
                final Recycler.V<byte[]> page = pages.getLast();

                byte[] output = page.v();
                try {
                    int bytesInflated = inflater.inflate(output, pageOffset, PageCacheRecycler.BYTE_PAGE_SIZE - pageOffset);
                    pageOffset += bytesInflated;
                    if (isNewPage) {
                        if (bytesInflated == 0) {
                            Recycler.V<byte[]> removed = pages.pollLast();
                            assert removed == page;
                            removed.close();
                            pageOffset = PageCacheRecycler.BYTE_PAGE_SIZE;
                        }
                    }
                } catch (DataFormatException e) {
                    throw new IOException("Exception while inflating bytes", e);
                }
                if (inflater.needsInput()) {
                    continueInflating = false;
                }
                if (inflater.finished()) {
                    bytesConsumed -= inflater.getRemaining();
                    continueInflating = false;
                }
                assert inflater.needsDictionary() == false;
            }
        }

        return bytesConsumed;
    }

    public boolean isEOS() {
        return inflater.finished();
    }

    @Override
    public ReleasableBytesReference pollDecompressedPage(boolean isEOS) {
        if (pages.isEmpty()) {
            return null;
        } else if (pages.size() == 1) {
            if (isEOS) {
                assert isEOS();
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
    public Compression.Scheme getScheme() {
        return Compression.Scheme.DEFLATE;
    }

    @Override
    public void close() {
        inflater.end();
        for (Recycler.V<byte[]> page : pages) {
            page.close();
        }
    }
}
