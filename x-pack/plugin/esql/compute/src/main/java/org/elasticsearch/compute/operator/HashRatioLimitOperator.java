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
 * each series is kept or dropped by hashing its series identity, so the kept subset is stable
 * across steps, runs, and shards. The keep/drop decision needs no per-group state, so unlike
 * a count-based limit this operator runs concurrently on whatever rows it receives.
 * <p>
 * Like Prometheus, a non-negative ratio {@code r} keeps series whose sampling offset is below
 * {@code r}, while a negative ratio keeps the complement (offsets at or above {@code 1 + r}).
 * Out-of-range ratios need no clamping: {@code r > 1} keeps everything, {@code r < -1} keeps
 * everything via the complement branch, and NaN keeps nothing since both comparisons are false.
 * <p>
 * The hashed bytes are our internal series id, not the Prometheus label serialization, so the
 * kept subset has the same statistical properties but is generally a different subset than the
 * one Prometheus keeps.
 */
public class HashRatioLimitOperator extends AbstractPageMappingOperator {

    public static final class Factory implements Operator.OperatorFactory {
        private final double ratio;
        private final int seriesChannel;

        public Factory(double ratio, int seriesChannel) {
            this.ratio = ratio;
            this.seriesChannel = seriesChannel;
        }

        @Override
        public HashRatioLimitOperator get(DriverContext driverContext) {
            return new HashRatioLimitOperator(ratio, seriesChannel);
        }

        @Override
        public String describe() {
            return "HashRatioLimitOperator[ratio=" + ratio + ", seriesChannel=" + seriesChannel + "]";
        }
    }

    /** Fixed hash seed; the sampling offset derivation mirrors Prometheus {@code SampleOffset}. */
    private static final int HASH_SEED = 0;

    private final double ratio;
    private final int seriesChannel;

    public HashRatioLimitOperator(double ratio, int seriesChannel) {
        this.ratio = ratio;
        this.seriesChannel = seriesChannel;
    }

    @Override
    protected Page process(Page page) {
        try {
            int positionCount = page.getPositionCount();
            BytesRefBlock series = page.getBlock(seriesChannel);
            int acceptedCount = 0;
            int[] accepted = new int[positionCount];
            BytesRef scratch = new BytesRef();
            for (int pos = 0; pos < positionCount; pos++) {
                if (keep(ratio, series.getBytesRef(pos, scratch))) {
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
     * Mirrors Prometheus {@code AddRatioSampleWithOffset} with the offset derived from our series
     * id hash instead of the Prometheus label hash.
     */
    static boolean keep(double ratio, BytesRef seriesId) {
        double offset = (StringHelper.murmurhash3_x86_32(seriesId, HASH_SEED) & 0xFFFFFFFFL) * 0x1p-32;
        return (ratio >= 0 && offset < ratio) || (ratio < 0 && offset >= 1.0 + ratio);
    }

    @Override
    public String toString() {
        return "HashRatioLimitOperator[ratio=" + ratio + ", seriesChannel=" + seriesChannel + "]";
    }
}
