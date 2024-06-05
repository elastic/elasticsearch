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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageCacheRecycler;

import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class DeflateTransportDecompressor extends TransportDecompressor {

    private final Inflater inflater;

    public DeflateTransportDecompressor(Recycler<BytesRef> recycler) {
        super(recycler);
        inflater = new Inflater(true);
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
                final boolean isNewPage = maybeAddNewPage();
                final Recycler.V<BytesRef> page = pages.getLast();

                BytesRef output = page.v();
                try {
                    int bytesInflated = inflater.inflate(output.bytes, output.offset + pageOffset, pageLength - pageOffset);
                    pageOffset += bytesInflated;
                    if (isNewPage) {
                        if (bytesInflated == 0) {
                            Recycler.V<BytesRef> removed = pages.pollLast();
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
    public Compression.Scheme getScheme() {
        return Compression.Scheme.DEFLATE;
    }

    @Override
    public void close() {
        inflater.end();
        super.close();
    }
}
