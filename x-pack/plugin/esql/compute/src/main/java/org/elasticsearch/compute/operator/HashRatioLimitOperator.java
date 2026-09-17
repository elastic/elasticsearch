/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;

/**
 * Stateless sampling operator for {@code limit_ratio(r, v)} with Prometheus-compatible semantics:
 * each row is kept or dropped by hashing its field key, so the kept subset is stable
 * across steps, runs, and shards. The keep/drop decision needs no per-group state, so unlike
 * a count-based limit this operator runs concurrently on whatever rows it receives.
 * <p>
 * Like Prometheus, a non-negative ratio {@code r} keeps series whose sampling offset is below
 * {@code r}, while a negative ratio keeps the complement (offsets at or above {@code 1 + r}).
 * Out-of-range ratios need no clamping: {@code r > 1} keeps everything, {@code r < -1} keeps
 * everything via the complement branch, and NaN keeps nothing since both comparisons are false.
 * <p>
 * The hashed bytes are our internal field key, not the Prometheus label serialization, so the
 * kept subset has the same statistical properties but is generally a different subset than the
 * one Prometheus keeps.
 */
public class HashRatioLimitOperator extends AbstractPageMappingOperator {

    public static final class Factory implements Operator.OperatorFactory {
        private final double ratio;
        private final int fieldChannel;

        public Factory(double ratio, int fieldChannel) {
            this.ratio = ratio;
            this.fieldChannel = fieldChannel;
        }

        @Override
        public HashRatioLimitOperator get(DriverContext driverContext) {
            return new HashRatioLimitOperator(ratio, fieldChannel);
        }

        @Override
        public String describe() {
            return "HashRatioLimitOperator[ratio=" + ratio + ", fieldChannel=" + fieldChannel + "]";
        }
    }

    /** Fixed hash seed; the sampling offset derivation mirrors Prometheus {@code SampleOffset}. */
    private static final int HASH_SEED = 0;

    private final double ratio;
    private final int fieldChannel;

    public HashRatioLimitOperator(double ratio, int fieldChannel) {
        this.ratio = ratio;
        this.fieldChannel = fieldChannel;
    }

    @Override
    protected Page process(Page page) {
        try {
            int positionCount = page.getPositionCount();
            BytesRefBlock field = page.getBlock(fieldChannel);
            int acceptedCount = 0;
            int[] accepted = new int[positionCount];
            BytesRef scratch = new BytesRef();
            for (int pos = 0; pos < positionCount; pos++) {
                if (keep(ratio, field.getBytesRef(pos, scratch))) {
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
     * Mirrors Prometheus {@code AddRatioSampleWithOffset} with the offset derived from our field
     * key hash instead of the Prometheus label hash.
     */
    static boolean keep(double ratio, BytesRef fieldId) {
        double offset = (StringHelper.murmurhash3_x86_32(fieldId, HASH_SEED) & 0xFFFFFFFFL) * 0x1p-32;
        return (ratio >= 0 && offset < ratio) || (ratio < 0 && offset >= 1.0 + ratio);
    }

    @Override
    public String toString() {
        return "HashRatioLimitOperator[ratio=" + ratio + ", fieldChannel=" + fieldChannel + "]";
    }
}
