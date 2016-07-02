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
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;

import java.io.IOException;
import java.util.Arrays;

/**
 * A composite {@link BytesReference} that allows joining multiple bytes references
 * into one without copying.
 *
 * Note, {@link #toBytesRef()} will materialize all pages in this BytesReference.
 */
public final class CompositeBytesReference extends BytesReference {

    private final BytesReference[] references;
    private final int[] offsets;
    private final int length;
    private final long ramBytesUsed;

    public CompositeBytesReference(BytesReference... references) {
        this.references = references;
        int i = 0;
        int offset = 0;
        offsets = new int[references.length];
        long ramBytesUsed = 0;
        for (BytesReference reference : references) {
            if (reference == null) {
                throw new IllegalArgumentException("references must not be null");
            }
            offsets[i++] = offset;
            offset += reference.length();
            ramBytesUsed += reference.ramBytesUsed();
        }
        this.ramBytesUsed = ramBytesUsed;
        length = offset;
    }


    @Override
    public byte get(int index) {
        int i = getOffsetIndex(index);
        return references[i].get(index - (int)offsets[i]);
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public BytesReference slice(int from, int length) {
        final int to = from + length;
        final int limit = getOffsetIndex(from + length);
        final int start = getOffsetIndex(from);
        BytesReference[] inSlice = new BytesReference[1 + (limit - start)];
        for (int i = 0, j = start; i < inSlice.length; i++) {
            inSlice[i] = references[j++];
        }
        int inSliceOffset = from - offsets[start];
        if (inSlice.length == 1) {
            return inSlice[0].slice(inSliceOffset, length);
        }
        // now adjust slices in front and at the end
        inSlice[0] = inSlice[0].slice(inSliceOffset, inSlice[0].length() - inSliceOffset);
        inSlice[inSlice.length-1] = inSlice[inSlice.length-1].slice(0, to - offsets[limit]);
        return new CompositeBytesReference(inSlice);
    }

    private final int getOffsetIndex(int offset) {
        int i = Arrays.binarySearch(offsets, offset);
        if (i < 0) {
            i = (-(i+1)) - 1;
        }
        return i;
    }

    @Override
    public BytesRef toBytesRef() {
        BytesRefBuilder builder = new BytesRefBuilder();
        builder.grow(length());
        BytesRef spare;
        BytesRefIterator iterator = iterator();
        try {
            while ((spare = iterator.next()) != null) {
                builder.append(spare);
            }
        } catch (IOException ex) {
            throw new AssertionError("won't happen", ex);
        }
        return builder.toBytesRef();
    }

    @Override
    public BytesRefIterator iterator() {
        if (references.length > 0) {
            return new BytesRefIterator() {
                int index = 0;
                private BytesRefIterator current = references[index++].iterator();
                @Override
                public BytesRef next() throws IOException {
                    BytesRef next = current.next();
                    if (next == null) {
                        while (index < references.length) {
                            current = references[index++].iterator();
                            next = current.next();
                            if (next != null) {
                                break;
                            }
                        }
                    }
                    return next;
                }
            };
        } else {
            return () -> null;
        }

    }

    @Override
    public long ramBytesUsed() {
       return ramBytesUsed;
    }
}
