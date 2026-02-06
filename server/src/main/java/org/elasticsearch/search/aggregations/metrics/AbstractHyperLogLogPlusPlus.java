/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntConsumer;

/**
 * Base class for HLL++ algorithms.
 *
 * It contains methods for cloning and serializing the data structure.
 */
public abstract class AbstractHyperLogLogPlusPlus extends AbstractCardinalityAlgorithm implements Releasable {

    public static final boolean LINEAR_COUNTING = false;
    public static final boolean HYPERLOGLOG = true;

    public AbstractHyperLogLogPlusPlus(int precision) {
        super(precision);
    }

    /** Algorithm used in the given bucket */
    protected abstract boolean getAlgorithm(long bucketOrd);

    protected interface LinearCountingView {
        int size();

        void forEachEncoded(IntConsumer consumer);

        void writeTo(StreamOutput out) throws IOException;
    }

    /**
     * Returns a bucket-bound view of linear counting values.
     *
     * The returned view is mutable and reused between calls, so callers must not
     * retain it or use it after another call to {@link #linearCountingView(long)}.
     * Not thread-safe.
     */
    protected abstract LinearCountingView linearCountingView(long bucketOrd);

    /** Get HyperLogLog algorithm */
    protected abstract AbstractHyperLogLog.RunLenIterator getHyperLogLog(long bucketOrd);

    /** Collect a value in the given bucket */
    public abstract void collect(long bucketOrd, long hash);

    /** Clone the data structure at the given bucket */
    public AbstractHyperLogLogPlusPlus clone(long bucketOrd, BigArrays bigArrays) {
        if (getAlgorithm(bucketOrd) == LINEAR_COUNTING) {
            // we use a sparse structure for linear counting
            LinearCountingView lc = linearCountingView(bucketOrd);
            int size = lc.size();
            HyperLogLogPlusPlusSparse clone = new HyperLogLogPlusPlusSparse(precision(), bigArrays, 1);
            clone.ensureCapacity(0, size);
            lc.forEachEncoded(value -> clone.addEncoded(0, value));
            return clone;
        } else {
            HyperLogLogPlusPlus clone = new HyperLogLogPlusPlus(precision(), bigArrays, 1);
            clone.merge(0, this, bucketOrd);
            return clone;
        }
    }

    private Object getComparableData(long bucketOrd) {
        if (getAlgorithm(bucketOrd) == LINEAR_COUNTING) {
            Set<Integer> values = new HashSet<>();
            linearCountingView(bucketOrd).forEachEncoded(values::add);
            return values;
        } else {
            Map<Byte, Integer> values = new HashMap<>();
            AbstractHyperLogLog.RunLenIterator iterator = getHyperLogLog(bucketOrd);
            while (iterator.next()) {
                byte runLength = iterator.value();
                values.merge(runLength, 1, Integer::sum);
            }
            return values;
        }
    }

    public void writeTo(long bucket, StreamOutput out) throws IOException {
        out.writeVInt(precision());
        if (getAlgorithm(bucket) == LINEAR_COUNTING) {
            out.writeBoolean(LINEAR_COUNTING);
            LinearCountingView lc = linearCountingView(bucket);
            int size = lc.size();
            out.writeVLong(size);
            lc.writeTo(out);
        } else {
            out.writeBoolean(HYPERLOGLOG);
            AbstractHyperLogLog.RunLenIterator iterator = getHyperLogLog(bucket);
            while (iterator.next()) {
                out.writeByte(iterator.value());
            }
        }
    }

    public static AbstractHyperLogLogPlusPlus readFrom(StreamInput in, BigArrays bigArrays) throws IOException {
        final int precision = in.readVInt();
        final boolean algorithm = in.readBoolean();
        if (algorithm == LINEAR_COUNTING) {
            // we use a sparse structure for linear counting
            final long size = in.readVLong();
            HyperLogLogPlusPlusSparse counts = new HyperLogLogPlusPlusSparse(precision, bigArrays, 1);
            counts.ensureCapacity(0, size);
            for (long i = 0; i < size; ++i) {
                counts.addEncoded(0, in.readInt());
            }
            return counts;
        } else {
            HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(precision, bigArrays, 1);
            final int registers = 1 << precision;
            for (int i = 0; i < registers; ++i) {
                counts.addRunLen(0, i, in.readByte());
            }
            return counts;
        }
    }

    public boolean equals(long thisBucket, AbstractHyperLogLogPlusPlus other, long otherBucket) {
        return Objects.equals(precision(), other.precision())
            && Objects.equals(getAlgorithm(thisBucket), other.getAlgorithm(otherBucket))
            && Objects.equals(getComparableData(thisBucket), other.getComparableData(otherBucket));
    }

    public int hashCode(long bucket) {
        return Objects.hash(precision(), getAlgorithm(bucket), getComparableData(bucket));
    }
}
