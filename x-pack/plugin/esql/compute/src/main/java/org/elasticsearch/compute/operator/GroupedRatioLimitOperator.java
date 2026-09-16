/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;

/**
 * Streaming operator for {@code limit_ratio(r, v)}: retains exactly {@code ceil(r * N)} rows per
 * group of N total rows, using Bresenham-style error accumulation.
 * <p>
 * For each group, two int counters {@code (total, accepted)} are tracked. A row is accepted when
 * {@code ratio * (total + 1) > accepted}, which maintains the invariant
 * {@code accepted == ceil(ratio * total)} after every row. This is O(groups) state — no row
 * buffering.
 * <p>
 * Group keys use list semantics for multivalues: {@code [1,2]} and {@code [2,1]} are different groups.
 */
public class GroupedRatioLimitOperator extends AbstractPageMappingOperator implements Accountable {

    public static final class Factory implements Operator.OperatorFactory {
        private final double ratio;
        private final int[] groupChannels;
        private final List<ElementType> elementTypes;

        public Factory(double ratio, List<Integer> groupChannels, List<ElementType> elementTypes) {
            if (Double.isFinite(ratio) == false) {
                throw new IllegalArgumentException("ratio must be finite, got [" + ratio + "]");
            }
            if (ratio < 0.0) {
                throw new IllegalArgumentException("ratio must not be negative, got [" + ratio + "]");
            }
            this.ratio = ratio;
            this.groupChannels = groupChannels.stream().mapToInt(Integer::intValue).toArray();
            this.elementTypes = elementTypes;
        }

        @Override
        public GroupedRatioLimitOperator get(DriverContext driverContext) {
            BlockFactory blockFactory = driverContext.blockFactory();
            PagedBytesBuilder row = new PagedBytesBuilder(
                blockFactory.bigArrays().recycler(),
                blockFactory.breaker(),
                "group-key-encoder",
                64
            );
            return new GroupedRatioLimitOperator(ratio, new GroupKeyEncoder(groupChannels, elementTypes, row), blockFactory);
        }

        @Override
        public String describe() {
            return "GroupedRatioLimitOperator[ratio=" + ratio + "]";
        }
    }

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(GroupedRatioLimitOperator.class);

    private final double ratio;
    private final GroupKeyEncoder keyEncoder;
    private BytesRefHashTable seenKeys;
    private BigArrays bigArrays;
    /** Number of rows seen so far per group ordinal. */
    private IntArray totals;
    /** Number of rows accepted so far per group ordinal. */
    private IntArray accepteds;

    public GroupedRatioLimitOperator(double ratio, GroupKeyEncoder keyEncoder, BlockFactory blockFactory) {
        if (Double.isFinite(ratio) == false) {
            throw new IllegalArgumentException("ratio must be finite, got [" + ratio + "]");
        }
        if (ratio < 0.0) {
            throw new IllegalArgumentException("ratio must not be negative, got [" + ratio + "]");
        }
        boolean success = false;
        try {
            this.ratio = ratio;
            this.keyEncoder = keyEncoder;
            this.bigArrays = blockFactory.bigArrays();
            this.seenKeys = HashImplFactory.newBytesRefHash(blockFactory);
            this.totals = bigArrays.newIntArray(16, false);
            this.accepteds = bigArrays.newIntArray(16, false);
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(keyEncoder, seenKeys, totals, accepteds);
            }
        }
    }

    @Override
    protected Page process(Page page) {
        try {
            int positionCount = page.getPositionCount();

            if (ratio <= 0.0) {
                return null;
            }

            int acceptedCount = 0;
            int[] accepted = new int[positionCount];

            for (int pos = 0; pos < positionCount; pos++) {
                PagedBytesCursor key = keyEncoder.encode(page, pos);
                long hashOrd = seenKeys.add(key);
                int total;
                int acc;
                long ord;
                if (hashOrd >= 0) {
                    ord = hashOrd;
                    totals = bigArrays.grow(totals, ord + 1);
                    accepteds = bigArrays.grow(accepteds, ord + 1);
                    total = 0;
                    acc = 0;
                    totals.set(ord, 0);
                    accepteds.set(ord, 0);
                } else {
                    ord = -(hashOrd + 1);
                    total = totals.get(ord);
                    acc = accepteds.get(ord);
                }

                // Bresenham: accept if ratio * (total + 1) > accepted
                if (ratio >= 1.0 || ratio * (total + 1) > acc) {
                    totals.set(ord, total + 1);
                    accepteds.set(ord, acc + 1);
                    accepted[acceptedCount++] = pos;
                } else {
                    totals.set(ord, total + 1);
                }
            }

            if (acceptedCount == 0) {
                return null;
            }

            if (acceptedCount == positionCount) {
                return page.shallowCopy();
            } else {
                return page.filter(false, accepted, 0, acceptedCount);
            }
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += seenKeys.ramBytesUsed();
        size += totals.ramBytesUsed();
        size += accepteds.ramBytesUsed();
        size += keyEncoder.ramBytesUsed();
        return size;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(seenKeys, totals, accepteds, keyEncoder, super::close);
    }

    @Override
    public String toString() {
        return "GroupedRatioLimitOperator[ratio="
            + ratio
            + ", groupKeys="
            + Arrays.toString(keyEncoder.groupChannels())
            + ", groups="
            + seenKeys.size()
            + "]";
    }
}
