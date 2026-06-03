/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;

/**
 * Represents the optimization state for block processing in lookup/enrich operations.
 */
public enum BlockOptimization {
    /**
     * No optimization - used for LOOKUP JOIN where pages are not merged.
     * <p>
     * When {@code mergePages} is false (e.g., LOOKUP JOIN), the lookup service returns
     * multiple pages that are joined by {@link RightChunkedLeftJoin} rather than merged
     * by {@link MergePositionsOperator}.
     * </p>
     */
    NONE,
    /**
     * Dictionary optimization - input block has ordinals (dictionary-encoded), allowing query deduplication.
     * <p>
     * For ENRICH operations, the input page contains exactly one block: the match field values.
     * When this block is dictionary-encoded ({@link OrdinalBytesRefBlock}), we can optimize by:
     * </p>
     * <ul>
     *   <li>{@link EnrichQuerySourceOperator}: queries only the unique dictionary values instead of
     *       all positions, avoiding duplicate queries for repeated match values.</li>
     *   <li>{@link MergePositionsOperator}: uses the ordinals to map query results back to original
     *       positions, expanding the deduplicated results to match the input page's position count.</li>
     * </ul>
     */
    DICTIONARY,
    /**
     * Range optimization - no deduplication, positions map 1:1 to input.
     * <p>
     * Used when the input block is not dictionary-encoded (e.g., non-BytesRef types or
     * BytesRef without ordinals). Each input position generates a separate query, and
     * {@link MergePositionsOperator} uses a simple range {@code [0, positionCount)} to map
     * results directly back to input positions without any expansion.
     * </p>
     */
    RANGE;

    /**
     * Extracts the {@link OrdinalBytesRefBlock} from block 0 of the given page.
     * Used when in DICTIONARY optimization mode to access ordinals and dictionary.
     */
    public static OrdinalBytesRefBlock extractOrdinalBlock(Page page) {
        return ((BytesRefBlock) page.getBlock(0)).asOrdinals();
    }
}
