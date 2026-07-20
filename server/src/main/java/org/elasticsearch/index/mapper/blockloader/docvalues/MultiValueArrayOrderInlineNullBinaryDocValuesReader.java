/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * Reader for the {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ArrayOrderInlineNull ArrayOrderInlineNull} binary
 * doc-values format, where values are stored in document order with nulls encoded inline. This is the inline-null counterpart of
 * {@link MultiValueSeparateCountBinaryDocValuesReader}.
 * <p>
 * Holds reusable scratch state, so a single instance is created per iterator and reused across documents.
 */
public final class MultiValueArrayOrderInlineNullBinaryDocValuesReader {

    private final BytesRef scratch = new BytesRef();
    private final ByteArrayStreamInput in = new ByteArrayStreamInput();

    public MultiValueArrayOrderInlineNullBinaryDocValuesReader() {}

    /**
     * Tests {@code predicate} against each non-null value of an {@code ArrayOrderInlineNull} blob, returning {@code true} on the first
     * match. {@code count} is the total number of slots (including null slots) carried by the companion {@code .counts} field. A blob is
     * only ever present when at least one non-null value exists, so this is never called for an all-null or empty array; those documents
     * carry a {@code .counts} value but no blob and are filtered out by the caller before reaching here.
     */
    public boolean match(BytesRef bytes, long count, Predicate<BytesRef> predicate) throws IOException {
        if (count == 1) {
            // A single non-null value is stored raw (no length prefix), exactly like SeparateCount.
            return predicate.test(bytes);
        }
        scratch.bytes = bytes.bytes;
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        for (int v = 0; v < count; v++) {
            int encodedLength = in.readVInt();
            if (encodedLength == 0) {
                continue; // null slot: zero following bytes, nothing to test
            }
            scratch.length = encodedLength - 1; // real values are stored with an L+1 length prefix (0 is reserved for null)
            scratch.offset = in.getPosition();
            in.setPosition(scratch.offset + scratch.length);
            if (predicate.test(scratch)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Appends the minimum non-null value of an {@code ArrayOrderInlineNull} blob.
     */
    public void readMin(BytesRef bytes, long count, BlockLoader.BytesRefBuilder builder) throws IOException {
        readExtreme(bytes, count, builder, (a, b) -> a.compareTo(b) < 0);
    }

    /**
     * Appends the maximum non-null value of an {@code ArrayOrderInlineNull} blob.
     */
    public void readMax(BytesRef bytes, long count, BlockLoader.BytesRefBuilder builder) throws IOException {
        readExtreme(bytes, count, builder, (a, b) -> a.compareTo(b) > 0);
    }

    private void readExtreme(BytesRef bytes, long count, BlockLoader.BytesRefBuilder builder, BiPredicate<BytesRef, BytesRef> predicate)
        throws IOException {
        if (count == 1) {
            // A single non-null value is stored raw (no length prefix), exactly like SeparateCount.
            builder.appendBytesRef(bytes);
            return;
        }

        // Points to the current extreme (min or max).
        BytesRef extreme = null;

        // Point scratch at the blob's backing array and seek the stream input to the start of the slots.
        scratch.bytes = bytes.bytes;
        in.reset(bytes.bytes, bytes.offset, bytes.length);

        for (int v = 0; v < count; v++) {
            // Each slot is a VInt length prefix; 0 means a null slot with no following bytes.
            int encodedLength = in.readVInt();
            if (encodedLength == 0) {
                continue; // null slot dropped
            }

            // Decode the value into scratch (length is stored as L+1) and advance the stream past its bytes.
            scratch.length = encodedLength - 1; // real values are stored with an L+1 length prefix (0 is reserved for null)
            scratch.offset = in.getPosition();
            in.setPosition(scratch.offset + scratch.length);

            // Keep the value if it is the first non-null slot or beats the current extreme; copy out since scratch is reused.
            if (extreme == null || predicate.test(scratch, extreme)) {
                extreme = new BytesRef(scratch.bytes, scratch.offset, scratch.length);
            }
        }

        // A blob is only ever written when at least one non-null value exists, so extreme is always set here (matches SeparateCount).
        builder.appendBytesRef(extreme);
    }

    /**
     * Counts non-null slots of an {@code ArrayOrderInlineNull} blob for single-value functions (LENGTH/BYTE_LENGTH), which only need to
     * know whether the effective arity is 0, 1, or more than 1 (nulls are dropped, so a slot count of 2 with one null is single-valued).
     * When exactly one non-null slot is found, {@code out} is set to a view of its bytes. Scanning stops on the second non-null slot,
     * returning {@code 2} to signal "multi-valued". {@code count} is the total slot count from the {@code .counts} field.
     */
    public int nonNullCount(BytesRef bytes, long count, BytesRef out) throws IOException {
        if (count == 1) {
            // A single non-null value is stored raw (no length prefix), exactly like SeparateCount.
            out.bytes = bytes.bytes;
            out.offset = bytes.offset;
            out.length = bytes.length;
            return 1;
        }

        // Seek the stream input to the start of the slots and count non-null slots as we go.
        in.reset(bytes.bytes, bytes.offset, bytes.length);

        int nonNull = 0;
        for (int v = 0; v < count; v++) {
            // Each slot is a VInt length prefix; 0 means a null slot with no following bytes.
            int encodedLength = in.readVInt();
            if (encodedLength == 0) {
                continue; // null slot
            }

            // Decode the slot's bounds (length is stored as L+1) and advance the stream past its bytes.
            int length = encodedLength - 1; // real values are stored with an L+1 length prefix (0 is reserved for null)
            int offset = in.getPosition();
            in.setPosition(offset + length);

            nonNull++;

            if (nonNull == 1) {
                // First non-null slot: record it in out in case it turns out to be the only one.
                out.bytes = bytes.bytes;
                out.offset = offset;
                out.length = length;
            } else {
                return 2; // multi-valued; the caller only needs to know the arity is > 1
            }
        }

        return nonNull;
    }
}
