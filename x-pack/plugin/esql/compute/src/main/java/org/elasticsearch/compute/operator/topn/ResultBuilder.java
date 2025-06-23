/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.core.Releasable;

/**
 * Builds {@link Block}s from keys and values encoded into {@link BytesRef}s.
 */
interface ResultBuilder extends Releasable {
    /**
     * Called for each sort key before {@link #decodeValue} to consume the sort key and
     * store the value of the key for {@link #decodeValue} can use it to reconstruct
     * the value. This will only be called if the value is part of the key.
     */
    void decodeKey(BytesRef keys);

    /**
     * Called once per row to decode the value and write to the internal {@link Block.Builder}.
     * If the value is part of the key then {@link #decodeKey} will be called first and
     * implementations can store keys in that method and reuse them in this method. Most
     * implementations don't write single valued fields that appear in the key and instead
     * use the value form {@link #decodeKey}.
     */
    void decodeValue(BytesRef values);

    /**
     * Build the result block.
     */
    Block build();

    static ResultBuilder resultBuilderFor(
        BlockFactory blockFactory,
        ElementType elementType,
        TopNEncoder encoder,
        boolean inKey,
        int positions
    ) {
        return switch (elementType) {
            case BOOLEAN -> new ResultBuilderForBoolean(blockFactory, encoder, inKey, positions);
            case BYTES_REF -> new ResultBuilderForBytesRef(blockFactory, encoder, inKey, positions);
            case INT -> new ResultBuilderForInt(blockFactory, encoder, inKey, positions);
            case LONG -> new ResultBuilderForLong(blockFactory, encoder, inKey, positions);
            case FLOAT -> new ResultBuilderForFloat(blockFactory, encoder, inKey, positions);
            case DOUBLE -> new ResultBuilderForDouble(blockFactory, encoder, inKey, positions);
            case NULL -> new ResultBuilderForNull(blockFactory);
            case DOC -> new ResultBuilderForDoc(blockFactory, positions);
            case AGGREGATE_METRIC_DOUBLE -> new ResultBuilderForAggregateMetricDouble(blockFactory, positions);
            default -> {
                assert false : "Result builder for [" + elementType + "]";
                throw new UnsupportedOperationException("Result builder for [" + elementType + "]");
            }
        };
    }

}
