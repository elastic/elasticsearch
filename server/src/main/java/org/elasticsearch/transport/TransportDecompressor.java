/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageCacheRecycler;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class TransportDecompressor implements Closeable {

    private final Inflater inflater;
    private final PageCacheRecycler recycler;
    private final ArrayDeque<Recycler.V<byte[]>> pages;
    private int pageOffset = PageCacheRecycler.BYTE_PAGE_SIZE;
    private boolean hasReadHeader = false;

    public TransportDecompressor(PageCacheRecycler recycler) {
        this.recycler = recycler;
        inflater = new Inflater(true);
        pages = new ArrayDeque<>(4);
    }

    public int decompress(BytesReference bytesReference) throws IOException {
        int bytesConsumed = 0;
        if (hasReadHeader == false) {
            if (CompressorFactory.COMPRESSOR.isCompressed(bytesReference) == false) {
                int maxToRead = Math.min(bytesReference.length(), 10);
                StringBuilder sb = new StringBuilder("stream marked as compressed, but no compressor found, first [")
                    .append(maxToRead).append("] content bytes out of [").append(bytesReference.length())
                    .append("] readable bytes with message size [").append(bytesReference.length()).append("] ").append("] are [");
                for (int i = 0; i < maxToRead; i++) {
                    sb.append(bytesReference.get(i)).append(",");
                }
                sb.append("]");
                throw new IllegalStateException(sb.toString());
            }
            hasReadHeader = true;
            int headerLength = CompressorFactory.COMPRESSOR.headerLength();
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
                final Recycler.V<byte[]> page;
                final boolean isNewPage = pageOffset == PageCacheRecycler.BYTE_PAGE_SIZE;
                if (isNewPage) {
                    pageOffset = 0;
                    page = recycler.bytePage(false);
                } else {
                    page = pages.getLast();
                }
                byte[] output = page.v();
                try {
                    int bytesInflated = inflater.inflate(output, pageOffset, PageCacheRecycler.BYTE_PAGE_SIZE - pageOffset);
                    pageOffset += bytesInflated;
                    if (isNewPage) {
                        if (bytesInflated == 0) {
                            page.close();
                            pageOffset = PageCacheRecycler.BYTE_PAGE_SIZE;
                        } else {
                            pages.add(page);
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

    public boolean canDecompress(int bytesAvailable) {
        return hasReadHeader || bytesAvailable >= CompressorFactory.COMPRESSOR.headerLength();
    }

    public boolean isEOS() {
        return inflater.finished();
    }

    public ReleasableBytesReference pollDecompressedPage() {
        if (pages.isEmpty()) {
            return null;
        } else if (pages.size() == 1) {
            if (isEOS()) {
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
        inflater.end();
        for (Recycler.V<byte[]> page : pages) {
            page.close();
        }
    }
}
