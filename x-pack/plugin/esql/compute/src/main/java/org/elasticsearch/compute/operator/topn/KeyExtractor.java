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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

/**
 * Extracts keys into a {@link BreakingBytesRefBuilder}.
 */
interface KeyExtractor {
    int writeKey(BreakingBytesRefBuilder key, int position);

    static KeyExtractor extractorFor(ElementType elementType, TopNEncoder encoder, boolean ascending, byte nul, byte nonNul, Block block) {
        if (false == (elementType == block.elementType() || ElementType.NULL == block.elementType())) {
            throw new IllegalArgumentException("Expected [" + elementType + "] but was [" + block.elementType() + "]");
        }
        return switch (block.elementType()) {
            case BOOLEAN -> KeyExtractorForBoolean.extractorFor(encoder, ascending, nul, nonNul, (BooleanBlock) block);
            case BYTES_REF -> KeyExtractorForBytesRef.extractorFor(encoder, ascending, nul, nonNul, (BytesRefBlock) block);
            case INT -> KeyExtractorForInt.extractorFor(encoder, ascending, nul, nonNul, (IntBlock) block);
            case LONG -> KeyExtractorForLong.extractorFor(encoder, ascending, nul, nonNul, (LongBlock) block);
            case DOUBLE -> KeyExtractorForDouble.extractorFor(encoder, ascending, nul, nonNul, (DoubleBlock) block);
            case NULL -> new KeyExtractorForNull(nul);
            default -> {
                assert false : "No key extractor for [" + block.elementType() + "]";
                throw new UnsupportedOperationException("No key extractor for [" + block.elementType() + "]");
            }
        };
    }
}
