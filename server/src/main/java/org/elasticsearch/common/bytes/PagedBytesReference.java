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

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.PageCacheRecycler;

import java.io.IOException;
import java.util.Objects;

/**
 * A page based bytes reference, internally holding the bytes in a paged
 * data structure.
 */
public class PagedBytesReference extends AbstractBytesReference {

    private static final int PAGE_SIZE = PageCacheRecycler.BYTE_PAGE_SIZE;

    private final ByteArray byteArray;
    private final int offset;
    private final int length;

    public PagedBytesReference(ByteArray byteArray, int length) {
        this(byteArray, 0, length);
    }

    private PagedBytesReference(ByteArray byteArray, int from, int length) {
        this.byteArray = byteArray;
        this.offset = from;
        this.length = length;
    }

    @Override
    public byte get(int index) {
        return byteArray.get(offset + index);
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public BytesReference slice(int from, int length) {
        Objects.checkFromIndexSize(from, length, this.length);
        return new PagedBytesReference(byteArray, offset + from, length);
    }

    @Override
    public BytesRef toBytesRef() {
        BytesRef bref = new BytesRef();
        // if length <= pagesize this will dereference the page, or materialize the byte[]
        byteArray.get(offset, length, bref);
        return bref;
    }

    @Override
    public final BytesRefIterator iterator() {
        final int offset = this.offset;
        final int length = this.length;
        // this iteration is page aligned to ensure we do NOT materialize the pages from the ByteArray
        // we calculate the initial fragment size here to ensure that if this reference is a slice we are still page aligned
        // across the entire iteration. The first page is smaller if our offset != 0 then we start in the middle of the page
        // otherwise we iterate full pages until we reach the last chunk which also might end within a page.
        final int initialFragmentSize = offset != 0 ? PAGE_SIZE - (offset % PAGE_SIZE) : PAGE_SIZE;
        return new BytesRefIterator() {
            int position = 0;
            int nextFragmentSize = Math.min(length, initialFragmentSize);
            // this BytesRef is reused across the iteration on purpose - BytesRefIterator interface was designed for this
            final BytesRef slice = new BytesRef();

            @Override
            public BytesRef next() throws IOException {
                if (nextFragmentSize != 0) {
                    final boolean materialized = byteArray.get(offset + position, nextFragmentSize, slice);
                    assert materialized == false : "iteration should be page aligned but array got materialized";
                    position += nextFragmentSize;
                    final int remaining = length - position;
                    nextFragmentSize = Math.min(remaining, PAGE_SIZE);
                    return slice;
                } else {
                    assert nextFragmentSize == 0 : "fragmentSize expected [0] but was: [" + nextFragmentSize + "]";
                    return null; // we are done with this iteration
                }
            }
        };
    }

    @Override
    public long ramBytesUsed() {
        return byteArray.ramBytesUsed();
    }
}
