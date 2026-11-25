/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * Computes the block range for a sliced posting list visitor given a doc ID range.
 * <p>
 * Used by both BBQ ({@code SlicedMemorySegmentPostingsVisitor}) and ASH ({@code SlicedAshPostingsVisitor})
 * to determine which blocks of a posting list overlap with a target slice's doc range, and to skip
 * irrelevant leading blocks.
 *
 * @param vectors the number of vectors to process (adjusted for the block range)
 * @param docBase the ordinal base (first ordinal in the first relevant block)
 * @param skipBytes the number of bytes to skip in the IndexInput past irrelevant leading blocks
 */
public record SlicedBlockRange(int vectors, int docBase, long skipBytes) {

    /** Sentinel returned when the slice range contains no vectors. */
    public static final SlicedBlockRange EMPTY = new SlicedBlockRange(0, 0, 0);

    /**
     * Computes the block range for a sliced posting list.
     *
     * @param vectorValues the vector values for ordinal-to-doc translation
     * @param startDocId inclusive start of the slice doc range
     * @param endDocId exclusive end of the slice doc range
     * @param totalVectors total number of vectors in the posting list
     * @param bulkSize the block size (number of vectors per block)
     * @param perVectorBytes the number of bytes per vector in the posting list (codes + corrections)
     * @return the computed block range, or {@link #EMPTY} if the range contains no vectors
     */
    public static SlicedBlockRange compute(
        KnnVectorValues vectorValues,
        int startDocId,
        int endDocId,
        int totalVectors,
        int bulkSize,
        long perVectorBytes
    ) throws IOException {
        int totalBlocks = totalVectors / bulkSize;
        KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
        if (iterator.advance(startDocId) >= endDocId) {
            return EMPTY;
        }
        int minOrd = iterator.index();
        int docId = iterator.advance(endDocId);
        int maxOrd;
        if (docId == DocIdSetIterator.NO_MORE_DOCS) {
            maxOrd = vectorValues.size();
        } else {
            maxOrd = iterator.index();
        }
        // When searching the full segment (startDocId == 0), the doc range may span
        // more ordinals than a single posting list in multi-centroid segments. In that case
        // we clamp to the posting list bounds rather than asserting.
        if (maxOrd - minOrd > totalVectors) {
            maxOrd = Math.min(maxOrd, minOrd + totalVectors);
        }
        int startBlock = minOrd / bulkSize;
        int endBlock = (maxOrd - 1) / bulkSize;
        int vectors;
        if (endBlock == totalBlocks) {
            vectors = totalVectors - startBlock * bulkSize;
        } else {
            vectors = (1 + endBlock - startBlock) * bulkSize;
        }
        int docBase = startBlock * bulkSize;
        long skipBytes = (long) startBlock * bulkSize * perVectorBytes;
        return new SlicedBlockRange(vectors, docBase, skipBytes);
    }
}
