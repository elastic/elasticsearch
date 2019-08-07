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
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageCacheRecycler;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class TransportDecompressor {

    private final Inflater inflater;
    private final PageCacheRecycler recycler;
    private final ArrayDeque<Recycler.V<byte[]>> pages;
    private int pageOffset = PageCacheRecycler.BYTE_PAGE_SIZE;
    private int uncompressedBytes = 0;

    public TransportDecompressor(PageCacheRecycler recycler) {
        this.recycler = recycler;
        inflater = new Inflater(false);
        pages = new ArrayDeque<>(4);
    }

    public int decompress(BytesReference bytesReference) throws IOException {
        int initialUncompressedBytes = this.uncompressedBytes;
        BytesRefIterator refIterator = bytesReference.iterator();
        BytesRef ref;
        while ((ref = refIterator.next()) != null) {
            inflater.setInput(ref.bytes);
            boolean continueInflating = true;
            while (continueInflating) {
                if (pageOffset == PageCacheRecycler.BYTE_PAGE_SIZE) {
                    pageOffset = 0;
                    pages.add(recycler.bytePage(false));
                }
                byte[] output = pages.getLast().v();
                try {
                    int bytesInflated = inflater.inflate(output, pageOffset, PageCacheRecycler.BYTE_PAGE_SIZE - pageOffset);
                    pageOffset += bytesInflated;
                    this.uncompressedBytes += bytesInflated;
                } catch (DataFormatException e) {
                    throw new IOException("Exception while inflating bytes", e);
                }
                if (inflater.needsInput() || inflater.finished() || inflater.needsDictionary()) {
                    continueInflating = false;
                }
            }
        }

        return uncompressedBytes - initialUncompressedBytes;
    }

    public ReleasableBytesReference pollDecompressedPage() {
        Recycler.V<byte[]> page = pages.pollLast();
        if (page == null) {
            return null;
        } else {
            return new ReleasableBytesReference(new BytesArray(page.v()), page);
        }
    }

    public void close() {
        inflater.end();
        for (Recycler.V<byte[]> page : pages) {
            page.close();
        }
    }
}
