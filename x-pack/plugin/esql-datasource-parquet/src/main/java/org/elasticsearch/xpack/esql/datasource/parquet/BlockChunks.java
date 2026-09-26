/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * Shared, null-safe concatenation of the per-run/per-bucket {@link Block} chunks produced by the
 * sparse decode paths ({@link PageColumnReader#readBatchSparse} and
 * {@link ParquetColumnExtractor}). Every chunk in a call belongs to the same logical column and
 * therefore shares one {@link ElementType}; the only variation is that an all-null run arrives as a
 * {@link ElementType#NULL} {@code ConstantNullBlock}.
 *
 * <p>The subtlety this helper exists to remove: resolving the builder's element type from the
 * <em>first</em> chunk is wrong when that first chunk is an all-null run. A
 * {@code ConstantNullBlock.Builder} rejects any non-null {@code copyFrom}, so a null-leading run
 * followed by a run with values throws {@code "can't append non-null values to a null block"}. We
 * instead resolve the type from the first non-{@code NULL} chunk, so chunk ordering cannot poison
 * the builder. Typed builders already append nulls for any {@code ConstantNullBlock} chunk, so
 * every mixed ordering (null-leading, null-trailing, interleaved) round-trips correctly.
 *
 * <p>Ownership contract: on success every input chunk is closed and the caller owns the returned
 * block. On a throw the helper closes nothing — the caller's existing catch/finally is responsible
 * for releasing the chunks (see {@link PageColumnReader#readBatchSparse} and
 * {@link ParquetColumnExtractor}).
 */
final class BlockChunks {

    private BlockChunks() {}

    /**
     * Concatenates {@code chunks} (all belonging to one column) into a single block whose position
     * count is the sum of each chunk's positions. The result size is derived entirely from the chunks:
     * the typed path's {@code build()} count and the all-null path's
     * {@link BlockFactory#newConstantNullBlock ConstantNullBlock} both use that same summed size, so
     * the two paths cannot diverge. Callers that need a hard output-size invariant assert it themselves
     * against their chunk list (see {@link PageColumnReader#readBatchSparse}); the gather path
     * ({@link ParquetColumnExtractor#stitchAndGather}) deliberately concatenates only the unique
     * per-bucket positions and expands duplicates afterwards, so no fixed count fits here. A fail-fast
     * assert fires if two genuinely different (non-{@code NULL}) element types reach the same concat,
     * which would indicate an upstream decode bug rather than a benign null run.
     *
     * <p>Closes every input chunk on success only; on a throw the caller must release them.
     */
    static Block concat(List<Block> chunks, BlockFactory blockFactory) {
        assert chunks.isEmpty() == false : "concat requires at least one chunk";

        int total = 0;
        ElementType type = ElementType.NULL;
        for (Block b : chunks) {
            total += b.getPositionCount();
            ElementType et = b.elementType();
            if (et != ElementType.NULL) {
                assert type == ElementType.NULL || type == et : "mixed element types " + type + " vs " + et;
                type = et;
            }
        }

        if (type == ElementType.NULL) {
            // Allocate before closing so a breaker trip inside newConstantNullBlock leaves the chunks
            // untouched for the caller to release, honouring the "closes nothing on throw" contract
            // (mirrors the typed path, which closes only after build()).
            Block result = blockFactory.newConstantNullBlock(total);
            closeChunks(chunks);
            return result;
        }

        try (Block.Builder builder = type.newBlockBuilder(total, blockFactory)) {
            for (Block chunk : chunks) {
                builder.copyFrom(chunk, 0, chunk.getPositionCount());
            }
            Block result = builder.build();
            // Close only after build() succeeds so the throw path leaves every chunk for the caller
            // to release (closing incrementally would leave the list half-closed on a mid-loop throw).
            closeChunks(chunks);
            return result;
        }
    }

    private static void closeChunks(List<Block> chunks) {
        for (Block chunk : chunks) {
            Releasables.closeExpectNoException(chunk);
        }
    }
}
