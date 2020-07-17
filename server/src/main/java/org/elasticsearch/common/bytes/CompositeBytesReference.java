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
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A composite {@link BytesReference} that allows joining multiple bytes references
 * into one without copying.
 *
 * Note, {@link #toBytesRef()} will materialize all pages in this BytesReference.
 */
public final class CompositeBytesReference extends AbstractBytesReference {

    private final BytesReference[] references;
    private final int[] offsets;
    private final int length;
    private final long ramBytesUsed;

    public static BytesReference of(BytesReference... references) {
        switch (references.length) {
            case 0:
                return BytesArray.EMPTY;
            case 1:
                return references[0];
            default:
                return new CompositeBytesReference(references);
        }
    }

    private CompositeBytesReference(BytesReference... references) {
        assert references.length > 1
                : "Should not build composite reference from less than two references but received [" + references.length + "]";
        this.references = Objects.requireNonNull(references, "references must not be null");
        this.offsets = new int[references.length];
        long ramBytesUsed = 0;
        int offset = 0;
        for (int i = 0; i < references.length; i++) {
            BytesReference reference = references[i];
            if (reference == null) {
                throw new IllegalArgumentException("references must not be null");
            }
            offsets[i] = offset; // we use the offsets to seek into the right BytesReference for random access and slicing
            offset += reference.length();
            ramBytesUsed += reference.ramBytesUsed();
        }
        this.ramBytesUsed = ramBytesUsed
            + (Integer.BYTES * offsets.length + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER) // offsets
            + (references.length * RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER) // references
            + Integer.BYTES // length
            + Long.BYTES; // ramBytesUsed
        length = offset;
    }


    @Override
    public byte get(int index) {
        final int i = getOffsetIndex(index);
        return references[i].get(index - offsets[i]);
    }

    @Override
    public int indexOf(byte marker, int from) {
        final int remainingBytes = Math.max(length - from, 0);
        Objects.checkFromIndexSize(from, remainingBytes, length);

        int result = -1;
        if (length == 0) {
            return result;
        }

        final int firstReferenceIndex = getOffsetIndex(from);
        for (int i = firstReferenceIndex; i < references.length; ++i) {
            final BytesReference reference = references[i];
            final int internalFrom;
            if (i == firstReferenceIndex) {
                internalFrom = from - offsets[firstReferenceIndex];
            } else {
                internalFrom = 0;
            }
            result = reference.indexOf(marker, internalFrom);
            if (result != -1) {
                result += offsets[i];
                break;
            }
        }
        return result;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public BytesReference slice(int from, int length) {
        Objects.checkFromIndexSize(from, length, this.length);

        if (length == 0) {
            return BytesArray.EMPTY;
        }

        // for slices we only need to find the start and the end reference
        // adjust them and pass on the references in between as they are fully contained
        final int to = from + length;
        final int limit = getOffsetIndex(to - 1);
        final int start = getOffsetIndex(from);
        final BytesReference[] inSlice = new BytesReference[1 + (limit - start)];
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

    private int getOffsetIndex(int offset) {
        final int i = Arrays.binarySearch(offsets, offset);
        return i < 0 ? (-(i + 1)) - 1 : i;
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
            throw new AssertionError("won't happen", ex); // this is really an error since we don't do IO in our bytesreferences
        }
        return builder.toBytesRef();
    }

    @Override
    public BytesRefIterator iterator() {
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
    }

    @Override
    public long ramBytesUsed() {
       return ramBytesUsed;
    }
}
