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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;

/**
 * Stateless hash-sampling row filter: each row is kept or dropped by hashing its key columns.
 * The keep/drop decision needs no per-group state, so unlike a count-based limit this operator
 * runs concurrently on whatever rows it receives.
 * <p>
 * The subset is stable within a process lifetime -- the same key always hashes the same way, so
 * rows are decided identically however pages are partitioned or ordered -- but it is deliberately
 * not stable across restarts: the hash seed varies per JVM instance. Documented as a divergence
 * from hash-stable samplers; see the {@code limit_ratio} function docs.
 * <p>
 * A non-negative ratio {@code r} keeps rows whose sampling offset is below {@code r}, while a
 * negative ratio inverts the selection (offsets at or above {@code 1 + r}). Out-of-range ratios
 * need no clamping: {@code r > 1} keeps everything, {@code r < -1} keeps everything via the
 * inverted branch, and NaN keeps nothing since both comparisons are false.
 * <p>
 * Key columns use list semantics for multivalues: {@code [1,2]} and {@code [2,1]} are different keys.
 */
public class HashRatioLimitOperator extends AbstractPageMappingOperator implements Accountable {

    public static final class Factory implements Operator.OperatorFactory {
        private final double ratio;
        private final int[] keyChannels;
        private final List<ElementType> elementTypes;

        public Factory(double ratio, List<Integer> keyChannels, List<ElementType> elementTypes) {
            this.ratio = ratio;
            this.keyChannels = keyChannels.stream().mapToInt(Integer::intValue).toArray();
            this.elementTypes = elementTypes;
        }

        @Override
        public HashRatioLimitOperator get(DriverContext driverContext) {
            BlockFactory blockFactory = driverContext.blockFactory();
            PagedBytesBuilder row = new PagedBytesBuilder(
                blockFactory.bigArrays().recycler(),
                blockFactory.breaker(),
                "group-key-encoder",
                64
            );
            return new HashRatioLimitOperator(ratio, new GroupKeyEncoder(keyChannels, elementTypes, row));
        }

        @Override
        public String describe() {
            return "HashRatioLimitOperator[ratio=" + ratio + ", keyChannels=" + Arrays.toString(keyChannels) + "]";
        }
    }

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(HashRatioLimitOperator.class);

    private final double ratio;
    private final GroupKeyEncoder keyEncoder;

    public HashRatioLimitOperator(double ratio, GroupKeyEncoder keyEncoder) {
        this.ratio = ratio;
        this.keyEncoder = keyEncoder;
    }

    @Override
    protected Page process(Page page) {
        try {
            int positionCount = page.getPositionCount();
            int acceptedCount = 0;
            int[] accepted = new int[positionCount];
            for (int pos = 0; pos < positionCount; pos++) {
                PagedBytesCursor key = keyEncoder.encode(page, pos);
                if (keep(ratio, key.hashCode())) {
                    accepted[acceptedCount++] = pos;
                }
            }
            if (acceptedCount == 0) {
                return null;
            }
            if (acceptedCount == positionCount) {
                return page.shallowCopy();
            }
            return page.filter(false, accepted, 0, acceptedCount);
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * Keeps a row when its key-hash sampling offset falls in the selected share: below
     * {@code ratio} for a non-negative ratio, at or above {@code 1 + ratio} for a negative one.
     */
    static boolean keep(double ratio, int hash) {
        // Scale the 32-bit hash to a sampling offset in [0, 1). Multiplying by 2^-32 is exact,
        // so every hash maps to a distinct offset with no rounding skew.
        double offset = (hash & 0xFFFFFFFFL) * 0x1p-32;
        return (ratio >= 0 && offset < ratio) || (ratio < 0 && offset >= 1.0 + ratio);
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + keyEncoder.ramBytesUsed();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(keyEncoder, super::close);
    }

    @Override
    public String toString() {
        return "HashRatioLimitOperator[ratio=" + ratio + ", keyChannels=" + Arrays.toString(keyEncoder.groupChannels()) + "]";
    }
}
