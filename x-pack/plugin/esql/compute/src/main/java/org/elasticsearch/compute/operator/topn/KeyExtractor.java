/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

/**
 * Extracts keys into a {@link BreakingBytesRefBuilder}.
 * <p>
 *     The keys are encoded as a sequence of bytes:
 * </p>
 * <pre>{@code
 *    non_null<bytes_for_key>
 *    null
 *    non_null<bytes_for_key>
 * }</pre>
 * <p>
 *     Where {@code non_null} is {@code 0x01} for "nulls first" or {@code 0x02} for "nulls last.
 *     And {@code null} is {@code 0x02} for "nulls first" or {@code 0x01} for "nulls last.
 * </p>
 */
interface KeyExtractor {
    void writeKey(BreakingBytesRefBuilder key, int position);

    /**
     * Build a {@link KeyExtractor} extractor for the provided configuration.
     *
     * @param elementType elements of the key
     * @param encoder     encoder appropriate for the data in the key. Callers need not
     *                    call {@link TopNEncoder#toSortable} on the provided encoder.
     * @param ascending   Are we sorting ascending ({@code true}) or descending ({@code false}).
     *                    This must be sent to both to call {@link TopNEncoder#toSortable} and
     *                    to handle multivalued fields.
     * @param nul         The byte that should be used for {@code null} values.
     * @param nonNul      The byte that should prefix non-{@code null} values.
     * @param block       The {@link Block} we're extracting from.
     */
    static KeyExtractor extractorFor(ElementType elementType, TopNEncoder encoder, boolean ascending, byte nul, byte nonNul, Block block) {
        if (false == (elementType == block.elementType() || ElementType.NULL == block.elementType())) {
            throw new IllegalArgumentException("Expected [" + elementType + "] but was [" + block.elementType() + "]");
        }
        encoder = encoder.toSortable(ascending);
        return switch (block.elementType()) {
            case BOOLEAN -> KeyExtractorForBoolean.extractorFor(encoder, ascending, nul, nonNul, (BooleanBlock) block);
            case BYTES_REF -> KeyExtractorForBytesRef.extractorFor(encoder, ascending, nul, nonNul, (BytesRefBlock) block);
            case INT -> KeyExtractorForInt.extractorFor(encoder, ascending, nul, nonNul, (IntBlock) block);
            case LONG -> KeyExtractorForLong.extractorFor(encoder, ascending, nul, nonNul, (LongBlock) block);
            case FLOAT -> KeyExtractorForFloat.extractorFor(encoder, ascending, nul, nonNul, (FloatBlock) block);
            case DOUBLE -> KeyExtractorForDouble.extractorFor(encoder, ascending, nul, nonNul, (DoubleBlock) block);
            case NULL -> new KeyExtractorForNull(nul);
            default -> {
                assert false : "No key extractor for [" + block.elementType() + "]";
                throw new UnsupportedOperationException("No key extractor for [" + block.elementType() + "]");
            }
        };
    }
}
