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
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

/**
 * Extracts values into a {@link BreakingBytesRefBuilder}.
 */
interface ValueExtractor {
    void writeValue(BreakingBytesRefBuilder values, int position);

    static ValueExtractor extractorFor(ElementType elementType, TopNEncoder encoder, boolean inKey, Block block) {
        if (false == (elementType == block.elementType() || ElementType.NULL == block.elementType())) {
            throw new IllegalArgumentException("Expected [" + elementType + "] but was [" + block.elementType() + "]");
        }
        return switch (block.elementType()) {
            case BOOLEAN -> ValueExtractorForBoolean.extractorFor(encoder, inKey, (BooleanBlock) block);
            case BYTES_REF -> ValueExtractorForBytesRef.extractorFor(encoder, inKey, (BytesRefBlock) block);
            case INT -> ValueExtractorForInt.extractorFor(encoder, inKey, (IntBlock) block);
            case LONG -> ValueExtractorForLong.extractorFor(encoder, inKey, (LongBlock) block);
            case DOUBLE -> ValueExtractorForDouble.extractorFor(encoder, inKey, (DoubleBlock) block);
            case NULL -> new ValueExtractorForNull();
            case DOC -> new ValueExtractorForDoc(encoder, ((DocBlock) block).asVector());
            default -> {
                assert false : "No value extractor for [" + block.elementType() + "]";
                throw new UnsupportedOperationException("No value extractor for [" + block.elementType() + "]");
            }
        };
    }
}
