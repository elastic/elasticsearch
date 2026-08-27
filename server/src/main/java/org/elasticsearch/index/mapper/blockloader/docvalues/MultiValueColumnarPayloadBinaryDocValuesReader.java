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
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * Reader for the {@link org.elasticsearch.index.mapper.ColumnarBinaryDocValuesField ColumnarBinaryDocValuesField} payload, where a
 * document's slot count travels in the blob ahead of its slots and nulls are encoded inline. The columnar counterpart of
 * {@link MultiValueArrayOrderInlineNullBinaryDocValuesReader}, and simpler for carrying its own count: there is no companion field to
 * advance on and no single-value blob stored raw, so every document decodes exactly one way.
 * <p>
 * Holds reusable scratch state, so a single instance is created per iterator and reused across documents.
 */
public final class MultiValueColumnarPayloadBinaryDocValuesReader {

    private final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();

    public MultiValueColumnarPayloadBinaryDocValuesReader() {}

    /** Tests {@code predicate} against each non-null value, returning {@code true} on the first match. */
    public boolean match(BytesRef bytes, Predicate<BytesRef> predicate) throws IOException {
        for (int slot = decoder.reset(bytes); slot > 0; slot--) {
            final BytesRef value = decoder.next();
            if (value != null && predicate.test(value)) {
                return true;
            }
        }
        return false;
    }

    /** Appends the non-null values in document order, or a null when the document has none. */
    public void read(BytesRef bytes, BlockLoader.BytesRefBuilder builder) throws IOException {
        final int slots = decoder.reset(bytes);
        // Two passes rather than buffering the values: the builder needs the arity up front to choose between a
        // bare value and a position entry, and a second walk of the lengths is cheaper than copying the bytes.
        int nonNull = 0;
        for (int slot = 0; slot < slots; slot++) {
            if (decoder.next() != null) {
                nonNull++;
            }
        }
        if (nonNull == 0) {
            builder.appendNull();
            return;
        }
        decoder.reset(bytes);
        if (nonNull > 1) {
            builder.beginPositionEntry();
        }
        for (int slot = 0; slot < slots; slot++) {
            final BytesRef value = decoder.next();
            if (value != null) {
                builder.appendBytesRef(value);
            }
        }
        if (nonNull > 1) {
            builder.endPositionEntry();
        }
    }

    /** Appends the minimum non-null value. */
    public void readMin(BytesRef bytes, BlockLoader.BytesRefBuilder builder) throws IOException {
        readExtreme(bytes, builder, (a, b) -> a.compareTo(b) < 0);
    }

    /** Appends the maximum non-null value. */
    public void readMax(BytesRef bytes, BlockLoader.BytesRefBuilder builder) throws IOException {
        readExtreme(bytes, builder, (a, b) -> a.compareTo(b) > 0);
    }

    private void readExtreme(BytesRef bytes, BlockLoader.BytesRefBuilder builder, BiPredicate<BytesRef, BytesRef> predicate)
        throws IOException {
        BytesRef extreme = null;
        for (int slot = decoder.reset(bytes); slot > 0; slot--) {
            final BytesRef value = decoder.next();
            // Copy out: the decoder points into the blob but reuses one BytesRef across slots.
            if (value != null && (extreme == null || predicate.test(value, extreme))) {
                extreme = BytesRef.deepCopyOf(value);
            }
        }
        if (extreme == null) {
            builder.appendNull();
        } else {
            builder.appendBytesRef(extreme);
        }
    }

    /**
     * Counts non-null slots for single-value functions (LENGTH/BYTE_LENGTH), which only need to know whether the effective arity is 0, 1
     * or more than 1 — nulls are dropped, so a document of two slots with one null is single-valued. When exactly one non-null slot is
     * found, {@code out} is set to a view of its bytes. Scanning stops on the second, returning {@code 2}.
     */
    public int nonNullCount(BytesRef bytes, BytesRef out) throws IOException {
        int nonNull = 0;
        for (int slot = decoder.reset(bytes); slot > 0; slot--) {
            final BytesRef value = decoder.next();
            if (value == null) {
                continue;
            }
            if (++nonNull == 1) {
                out.bytes = value.bytes;
                out.offset = value.offset;
                out.length = value.length;
            } else {
                return 2; // multi-valued; the caller only needs to know the arity is > 1
            }
        }
        return nonNull;
    }
}
