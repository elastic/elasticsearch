/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.Optional;

/**
 * Performs an {@code INNER JOIN} where many "right hand" pages are joined against a "left hand" {@link Page}.
 * Each "left hand" row is output once per "right hand" row pointing at it, and not at all if none does.
 * <p>
 *     The "right hand" pages have the same shape as for {@link RightChunkedLeftJoin}: a non-decreasing
 *     {@code positions} column followed by the right columns. They already hold exactly the matches, so the
 *     output is the "left hand" page filtered by {@code positions} next to the "right hand" columns, which are
 *     reused as they are. Unlike {@link RightChunkedLeftJoin} nothing is padded with nulls, and there is no
 *     trailing output.
 * </p>
 * <pre>{@code
 * "left hand"                 "right hand"                     output
 * | lhdata |             | positions | r1 | r2 |          | lhdata | r1 | r2 |
 * ----------             -----------------------          ---------------------
 * |    l00 |             |         1 |  2 |  3 |          |    l01 |  2 |  3 |
 * |    l01 |             |         1 |  3 |  3 |          |    l01 |  3 |  3 |
 * |    l02 |             |         3 |  9 |  9 |          |    l03 |  9 |  9 |
 * |    l03 |
 * }</pre>
 */
public class RightChunkedInnerJoin implements RightChunkedJoin {
    private final Page leftHand;
    private final int mergedElementCount;
    /**
     * The last position seen, used to detect "right hand" pages that "go backwards".
     */
    private int last = 0;

    public RightChunkedInnerJoin(Page leftHand, int mergedElementCount) {
        this.leftHand = leftHand;
        this.mergedElementCount = mergedElementCount;
    }

    @Override
    public Page join(Page rightHand) {
        if (rightHand.getBlockCount() != mergedElementCount + 1) {
            throw new IllegalArgumentException(
                "expected right hand side with [" + (mergedElementCount + 1) + "] but got [" + rightHand.getBlockCount() + "]"
            );
        }
        IntVector positions = rightHand.<IntBlock>getBlock(0).asVector();
        int count = positions.getPositionCount();
        int[] filter = new int[count];
        boolean identity = count == leftHand.getPositionCount();
        for (int p = 0; p < count; p++) {
            int pos = positions.getInt(p);
            if (pos < last) {
                throw new IllegalArgumentException("positions must be non-decreasing, got [" + pos + "] after [" + last + "]");
            }
            filter[p] = pos;
            identity &= pos == p;
            last = pos;
        }
        Block[] blocks = new Block[leftHand.getBlockCount() + mergedElementCount];
        try {
            int b = 0;
            while (b < leftHand.getBlockCount()) {
                Block left = leftHand.getBlock(b);
                if (identity) {
                    // Every left row matched exactly once and in order: reuse the left blocks.
                    left.mustIncRef();
                    blocks[b] = left;
                } else {
                    blocks[b] = left.filter(true, filter);
                }
                b++;
            }
            int rb = 1; // Skip the positions column
            while (b < blocks.length) {
                Block right = rightHand.getBlock(rb);
                right.mustIncRef();
                blocks[b] = right;
                b++;
                rb++;
            }
            Page result = new Page(blocks);
            blocks = null;
            return result;
        } finally {
            if (blocks != null) {
                Releasables.close(blocks);
            }
        }
    }

    /**
     * Unmatched "left hand" rows are dropped, so there is never trailing output.
     */
    @Override
    public Optional<Page> noMoreRightHandPages() {
        return Optional.empty();
    }

    @Override
    public void releaseOnAnyThread() {
        leftHand.allowPassingToDifferentDriver();
        leftHand.releaseBlocks();
    }

    @Override
    public void close() {
        Releasables.close(leftHand::releaseBlocks);
    }
}
