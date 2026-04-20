/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;

/**
 * Converts Apache Arrow FieldVector to ESQL Blocks.
 * This is the inverse operation of {@link BlockConverter} (Block → Arrow).
 * Together they provide symmetric conversion: Block ↔ Arrow.
 *
 * <p>Note: Timestamp types are converted to milliseconds or nanoseconds.
 * Float types (FLOAT2, FLOAT4) are converted to double (ESQL doesn't have a separate float type).
 */
@FunctionalInterface
public interface ArrowToBlockConverter {

    /**
     * Convert an Arrow FieldVector to an ESQL Block.
     * @param vector the Arrow vector
     * @param factory the block factory for memory management
     * @return the ESQL block
     */
    Block convert(FieldVector vector, BlockFactory factory);

    /**
     * Get a converter for the given Arrow type.
     * @param arrowType the Arrow minor type
     * @return the appropriate converter, or null if the type is not supported
     */
    static ArrowToBlockConverter forType(Types.MinorType arrowType) {
        return ArrowToBlockConverters.forType(arrowType);
    }
}
