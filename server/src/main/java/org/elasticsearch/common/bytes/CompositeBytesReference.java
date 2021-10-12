/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * A composite {@link BytesReference} that allows joining multiple bytes references
 * into one without copying.
 *
 * Note, {@link #toBytesRef()} will materialize all pages in this BytesReference.
 */
public final class CompositeBytesReference extends AbstractBytesReference {

    private final BytesReference[] references;
    private final int[] offsets; // we use the offsets to seek into the right BytesReference for random access and slicing
    private final int length;
    private final long ramBytesUsed;

    /**
     * @param references The references to concatenate, none of which may be {@code null}. This array may be mutated, reordering its
     *                   contents, if any of its elements have zero length.
     * @return a {@link BytesReference} representing the concatenation of the given objects.
     */
    public static BytesReference of(BytesReference... references) {
        if (references.length == 0) {
            return BytesArray.EMPTY;
        } else if (references.length == 1) {
            return references[0];
        }
        final int[] offsets = new int[references.length];
        long ramBytesUsed = 0;
        int offset = 0;
        int emptyRefsCount = 0;
        for (int i = 0; i < references.length; i++) {
            final BytesReference reference = references[i];
            if (reference == null) {
                throw new IllegalArgumentException("references must not be null");
            }
            if (reference.length() == 0) {
                emptyRefsCount += 1;
                ramBytesUsed += BytesArray.EMPTY.ramBytesUsed();
                continue;
            }
            if (emptyRefsCount > 0) {
                // shuffle nonempty refs downwards
                references[i - emptyRefsCount] = references[i];
            }
            offsets[i - emptyRefsCount] = offset;
            offset += reference.length();
            if (offset <= 0) {
                throw new IllegalArgumentException("CompositeBytesReference cannot hold more than 2GB");
            }
            ramBytesUsed += reference.ramBytesUsed();
        }
        if (emptyRefsCount == references.length) {
            return BytesArray.EMPTY;
        } else if (emptyRefsCount == references.length - 1) {
            return references[0];
        }
        for (int i = references.length - emptyRefsCount; i < references.length; i++) {
            // fill in the empty refs at the end
            references[i] = BytesArray.EMPTY;
            offsets[i] = offset;
        }

        //noinspection IntegerMultiplicationImplicitCastToLong
        return new CompositeBytesReference(
            references,
            offsets,
            offset,
            ramBytesUsed
                + (Integer.BYTES * offsets.length + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER) // offsets
                + (references.length * RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER) // references
                + Integer.BYTES // length
                + Long.BYTES// ramBytesUsed
        );    }

    private CompositeBytesReference(BytesReference[] references, int[] offsets, int length, long ramBytesUsed) {
        assert references != null && offsets != null;
        assert references.length > 1
                : "Should not build composite reference from less than two references but received [" + references.length + "]";
        assert Arrays.stream(references).allMatch(Objects::nonNull);
        assert IntStream.range(1, references.length).allMatch(i -> references[i].length() == 0 || references[i - 1].length() != 0);
        assert offsets[0] == 0;
        assert IntStream.range(1, references.length).allMatch(i -> offsets[i] - offsets[i - 1] == references[i - 1].length());
        assert length == Arrays.stream(references).mapToLong(BytesReference::length).sum();
        assert ramBytesUsed > Arrays.stream(references).mapToLong(BytesReference::ramBytesUsed).sum();
        this.references = Objects.requireNonNull(references, "references must not be null");
        this.offsets = offsets;
        this.length = length;
        this.ramBytesUsed = ramBytesUsed;
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
        if (from == 0 && this.length == length) {
            return this;
        }
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
        System.arraycopy(references, start, inSlice, 0, inSlice.length);
        int inSliceOffset = from - offsets[start];
        if (inSlice.length == 1) {
            return inSlice[0].slice(inSliceOffset, length);
        }
        // now adjust slices in front and at the end
        inSlice[0] = inSlice[0].slice(inSliceOffset, inSlice[0].length() - inSliceOffset);
        inSlice[inSlice.length-1] = inSlice[inSlice.length-1].slice(0, to - offsets[limit]);
        return CompositeBytesReference.of(inSlice);
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
            throw new AssertionError("won't happen", ex); // this is really an error since we don't do IO in our BytesReferences
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
                assert next == null || next.length > 0;
                return next;
            }
        };
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        for (BytesReference reference : references) {
            reference.writeTo(os);
        }
    }

    @Override
    public long ramBytesUsed() {
       return ramBytesUsed;
    }
}
