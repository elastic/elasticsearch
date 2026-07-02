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

import java.io.IOException;
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
}
