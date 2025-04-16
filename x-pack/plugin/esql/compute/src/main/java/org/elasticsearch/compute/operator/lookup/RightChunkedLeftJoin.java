/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Optional;
import java.util.stream.IntStream;

/**
 * Performs a {@code LEFT JOIN} where many "right hand" pages are joined
 * against a "left hand" {@link Page}. Each row on the "left hand" page
 * is output at least once whether it appears in the "right hand" or not.
 * And more than once if it appears in the "right hand" pages more than once.
 * <p>
 *     The "right hand" page contains a non-decreasing {@code positions}
 *     column that controls which position in the "left hand" page the row
 *     in the "right hand" page. This'll make more sense with a picture:
 * </p>
 * <pre>{@code
 * "left hand"                 "right hand"
 * | lhdata |             | positions | r1 | r2 |
 * ----------             -----------------------
 * |    l00 |             |         0 |  1 |  2 |
 * |    l01 |             |         1 |  2 |  3 |
 * |    l02 |             |         1 |  3 |  3 |
 * |    ... |             |         3 |  9 |  9 |
 * |    l99 |
 * }</pre>
 * <p>
 *     Joins to:
 * </p>
 * <pre>{@code
 * | lhdata  |  r1  |  r2  |
 * -------------------------
 * |     l00 |    1 |    2 |
 * |     l01 |    2 |    3 |
 * |     l01 |    3 |    3 |   <1>
 * |     l02 | null | null |   <2>
 * |     l03 |    9 |    9 |
 * }</pre>
 * <ol>
 *     <li>{@code l01} is duplicated because it's positions appears twice in
 *     the right hand page.</li>
 *     <li>{@code l02}'s row is filled with {@code null}s because it's position
 *     does not appear in the right hand page.</li>
 * </ol>
 * <p>
 *     This supports joining many "right hand" pages against the same
 *     "left hand" so long as the first value of the next {@code positions}
 *     column is the same or greater than the last value of the previous
 *     {@code positions} column. Large gaps are fine. Starting with the
 *     same number as you ended on is fine. This looks like:
 * </p>
 * <pre>{@code
 * "left hand"                 "right hand"
 * | lhdata |             | positions | r1 | r2 |
 * ----------             -----------------------
 * |    l00 |                    page 1
 * |    l01 |             |         0 |  1 |  2 |
 * |    l02 |             |         1 |  3 |  3 |
 * |    l03 |                    page 2
 * |    l04 |             |         1 |  9 |  9 |
 * |    l05 |             |         2 |  9 |  9 |
 * |    l06 |                    page 3
 * |    ... |             |         5 | 10 | 10 |
 * |    l99 |             |         7 | 11 | 11 |
 * }</pre>
 * <p>
 *     Which makes:
 * </p>
 * <pre>{@code
 * | lhdata  |  r1  |  r2  |
 * -------------------------
 *         page 1
 * |     l00 |    1 |    2 |
 * |     l01 |    3 |    3 |
 *         page 2
 * |     l01 |    9 |    9 |
 * |     l02 |    9 |    9 |
 *         page 3
 * |     l03 | null | null |
 * |     l04 | null | null |
 * |     l05 |   10 |   10 |
 * |     l06 | null | null |
 * |     l07 |   11 |   11 |
 * }</pre>
 * <p>
 *     Note that the output pages are sized by the "right hand" pages with
 *     {@code null}s inserted.
 * </p>
 * <p>
 *     Finally, after all "right hand" pages have been joined this will produce
 *     all remaining "left hand" rows joined against {@code null}.
 *     Another picture:
 * </p>
 * <pre>{@code
 * "left hand"                 "right hand"
 * | lhdata |             | positions | r1 | r2 |
 * ----------             -----------------------
 * |    l00 |                    last page
 * |    l01 |             |        96 |  1 |  2 |
 * |    ... |             |        97 |  1 |  2 |
 * |    l99 |
 * }</pre>
 * <p>
 *     Which makes:
 * </p>
 * <pre>{@code
 * | lhdata  |  r1  |  r2  |
 * -------------------------
 *     last matching page
 * |     l96 |    1 |    2 |
 * |     l97 |    2 |    3 |
 *    trailing nulls page
 * |     l98 | null | null |
 * |     l99 | null | null |
 * }</pre>
 */
public class RightChunkedLeftJoin implements Releasable {
    private final Page leftHand;
    private final int mergedElementCount;
    /**
     * The next position that we'll emit <strong>or</strong> one more than the
     * next position we'll emit. This is used to cover gaps between "right hand"
     * pages and to detect if "right hand" pages "go backwards".
     */
    private int next = 0;

    public RightChunkedLeftJoin(Page leftHand, int mergedElementCounts) {
        this.leftHand = leftHand;
        this.mergedElementCount = mergedElementCounts;
    }

    public Page join(Page rightHand) {
        IntVector positions = rightHand.<IntBlock>getBlock(0).asVector();
        if (positions.getInt(0) < next - 1) {
            throw new IllegalArgumentException("maximum overlap is one position");
        }
        Block[] blocks = new Block[leftHand.getBlockCount() + mergedElementCount];
        if (rightHand.getBlockCount() != mergedElementCount + 1) {
            throw new IllegalArgumentException(
                "expected right hand side with [" + (mergedElementCount + 1) + "] but got [" + rightHand.getBlockCount() + "]"
            );
        }
        IntVector.Builder leftFilterBuilder = null;
        IntVector leftFilter = null;
        IntVector.Builder insertNullsBuilder = null;
        IntVector insertNulls = null;
        try {
            leftFilterBuilder = positions.blockFactory().newIntVectorBuilder(positions.getPositionCount());
            for (int p = 0; p < positions.getPositionCount(); p++) {
                int pos = positions.getInt(p);
                if (pos > next) {
                    if (insertNullsBuilder == null) {
                        insertNullsBuilder = positions.blockFactory().newIntVectorBuilder(pos - next);
                    }
                    for (int missing = next; missing < pos; missing++) {
                        leftFilterBuilder.appendInt(missing);
                        insertNullsBuilder.appendInt(p);
                    }
                }
                leftFilterBuilder.appendInt(pos);
                next = pos + 1;
            }
            leftFilter = leftFilterBuilder.build();
            int[] leftFilterArray = toArray(leftFilter);
            insertNulls = insertNullsBuilder == null ? null : insertNullsBuilder.build();

            int b = 0;
            while (b < leftHand.getBlockCount()) {
                blocks[b] = leftHand.getBlock(b).filter(leftFilterArray);
                b++;
            }
            int rb = 1; // Skip the positions column
            while (b < blocks.length) {
                Block block = rightHand.getBlock(rb);
                if (insertNulls == null) {
                    block.mustIncRef();
                } else {
                    block = block.insertNulls(insertNulls);
                }
                blocks[b] = block;
                b++;
                rb++;
            }
            Page result = new Page(blocks);
            blocks = null;
            return result;
        } finally {
            Releasables.close(
                blocks == null ? null : Releasables.wrap(blocks),
                leftFilter,
                leftFilterBuilder,
                insertNullsBuilder,
                insertNulls
            );
        }
    }

    public Optional<Page> noMoreRightHandPages() {
        if (next == leftHand.getPositionCount()) {
            return Optional.empty();
        }
        BlockFactory factory = leftHand.getBlock(0).blockFactory();
        Block[] blocks = new Block[leftHand.getBlockCount() + mergedElementCount];
        // TODO make a filter that takes a min and max?
        int[] filter = IntStream.range(next, leftHand.getPositionCount()).toArray();
        try {
            int b = 0;
            while (b < leftHand.getBlockCount()) {
                blocks[b] = leftHand.getBlock(b).filter(filter);
                b++;
            }
            while (b < blocks.length) {
                blocks[b] = factory.newConstantNullBlock(leftHand.getPositionCount() - next);
                b++;
            }
            Page result = new Page(blocks);
            blocks = null;
            return Optional.of(result);
        } finally {
            if (blocks != null) {
                Releasables.close(blocks);
            }
        }
    }

    /**
     * Release this on <strong>any</strong> thread, rather than just the thread that built it.
     */
    public void releaseOnAnyThread() {
        leftHand.allowPassingToDifferentDriver();
        leftHand.releaseBlocks();
    }

    @Override
    public void close() {
        Releasables.close(leftHand::releaseBlocks);
    }

    private int[] toArray(IntVector vector) {
        // TODO replace parameter to filter with vector and remove this
        int[] array = new int[vector.getPositionCount()];
        for (int p = 0; p < vector.getPositionCount(); p++) {
            array[p] = vector.getInt(p);
        }
        return array;
    }
}
