/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;

/**
 * Aggregation state and PromQL {@code histogram_quantile} evaluation for classic cumulative histogram buckets
 * ({@code le} upper bound plus cumulative count), used by {@link ClassicHistogramQuantileAggregator}.
 */
final class ClassicHistogramQuantileStates {
    static final double SMALL_DELTA_TOLERANCE = 1e-12;

    /**
     * Bytes accounted to the circuit breaker for each buffered {@link Bucket}: the bucket object itself plus the
     * reference slot it occupies in the backing {@link ArrayList}. ArrayList growth slack is intentionally not
     * modelled; this is a per-element estimate to keep the unbounded bucket buffers visible to the breaker.
     */
    static final long BUCKET_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Bucket.class)
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF;

    private static final String BREAKER_LABEL = "<classic_histogram_quantile>";

    private ClassicHistogramQuantileStates() {}

    static BytesRef serializeBuckets(List<Bucket> buckets) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(buckets.size());
            for (Bucket bucket : buckets) {
                out.writeDouble(bucket.upperBound());
                out.writeDouble(bucket.count());
            }
            return out.bytes().toBytesRef();
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize histogram buckets", e);
        }
    }

    static List<Bucket> deserializeBuckets(BytesRef bytesRef) {
        ByteArrayStreamInput in = new ByteArrayStreamInput(bytesRef.bytes);
        in.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        try {
            int size = in.readVInt();
            List<Bucket> buckets = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                buckets.add(new Bucket(in.readDouble(), in.readDouble()));
            }
            return buckets;
        } catch (IOException e) {
            throw new IllegalStateException("failed to deserialize histogram buckets", e);
        }
    }

    /**
     * Estimates a quantile from classic histogram buckets using PromQL {@code histogram_quantile} semantics:
     * coalesce duplicate bounds, repair non-monotonic counts, then linearly interpolate within the target bucket.
     *
     * @param quantile fraction in {@code [0, 1]}; out-of-range and NaN inputs follow PromQL edge-case rules
     * @param inputBuckets cumulative buckets sorted by upper bound, ending with {@code +Inf}
     * @return the interpolated estimate, or {@link Double#NaN} when the histogram cannot produce one
     */
    static double bucketQuantile(double quantile, List<Bucket> inputBuckets) {
        if (Double.isNaN(quantile)) {
            return Double.NaN;
        }
        if (quantile < 0) {
            return Double.NEGATIVE_INFINITY;
        }
        if (quantile > 1) {
            return Double.POSITIVE_INFINITY;
        }
        if (inputBuckets.isEmpty()) {
            return Double.NaN;
        }

        List<Bucket> buckets = new ArrayList<>(inputBuckets);
        buckets.sort(Comparator.comparingDouble(Bucket::upperBound));
        if (Double.isInfinite(buckets.getLast().upperBound()) == false || buckets.getLast().upperBound() < 0) {
            return Double.NaN;
        }

        buckets = coalesceBuckets(buckets);
        ensureMonotonicAndIgnoreSmallDeltas(buckets, SMALL_DELTA_TOLERANCE);

        if (buckets.size() < 2) {
            return Double.NaN;
        }
        double observations = buckets.getLast().count();
        if (observations == 0) {
            return Double.NaN;
        }

        double rank = quantile * observations;
        int bucketIndex = searchBucket(buckets, rank);

        if (bucketIndex == buckets.size() - 1) {
            return buckets.get(buckets.size() - 2).upperBound();
        }
        if (bucketIndex == 0 && buckets.getFirst().upperBound() <= 0) {
            return buckets.getFirst().upperBound();
        }

        double bucketStart = 0d;
        double bucketEnd = buckets.get(bucketIndex).upperBound();
        double count = buckets.get(bucketIndex).count();
        if (bucketIndex > 0) {
            Bucket previous = buckets.get(bucketIndex - 1);
            bucketStart = previous.upperBound();
            count -= previous.count();
            rank -= previous.count();
        }
        return bucketStart + (bucketEnd - bucketStart) * (rank / count);
    }

    /**
     * Returns the index of the first bucket whose cumulative count is at least {@code rank}, excluding the
     * sentinel {@code +Inf} bucket at the end of the list.
     */
    private static int searchBucket(List<Bucket> buckets, double rank) {
        int low = 0;
        int high = buckets.size() - 2;
        int result = buckets.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (buckets.get(mid).count() >= rank) {
                result = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return result;
    }

    /**
     * Merges buckets that share the same upper bound by summing their cumulative counts.
     */
    private static List<Bucket> coalesceBuckets(List<Bucket> buckets) {
        List<Bucket> result = new ArrayList<>(buckets.size());
        Bucket previous = buckets.getFirst();
        for (int i = 1; i < buckets.size(); i++) {
            Bucket bucket = buckets.get(i);
            if (bucket.upperBound() == previous.upperBound()) {
                previous = new Bucket(previous.upperBound(), previous.count() + bucket.count());
            } else {
                result.add(previous);
                previous = bucket;
            }
        }
        result.add(previous);
        return result;
    }

    /**
     * Repairs cumulative counts in place so the invariant that each bucket's cumulative count is at least as large
     * as the previous one stays intact, clamping decreases and treating relative differences below {@code tolerance}
     * as flat to match PromQL floating-point repair.
     * <p>
     * Example:
     * </p>
     * <pre>{@code
     * before: (le=1, 2), (le=2, 1), (le=+Inf, 5)
     * after:  (le=1, 2), (le=2, 2), (le=+Inf, 5)
     * }</pre>
     */
    private static void ensureMonotonicAndIgnoreSmallDeltas(List<Bucket> buckets, double tolerance) {
        double previous = buckets.getFirst().count();
        for (int i = 1; i < buckets.size(); i++) {
            Bucket bucket = buckets.get(i);
            double current = bucket.count();
            if (current == previous) {
                continue;
            }
            if (almostEqual(previous, current, tolerance)) {
                buckets.set(i, new Bucket(bucket.upperBound(), previous));
                continue;
            }
            if (current < previous) {
                buckets.set(i, new Bucket(bucket.upperBound(), previous));
                continue;
            }
            previous = current;
        }
    }

    /**
     * Returns whether {@code left} and {@code right} are equal within a relative {@code epsilon} tolerance.
     */
    private static boolean almostEqual(double left, double right, double epsilon) {
        if (left == right) {
            return true;
        }
        double absSum = Math.abs(left) + Math.abs(right);
        double diff = Math.abs(left - right);
        if (left == 0d || right == 0d || absSum < Double.MIN_NORMAL) {
            return diff < epsilon * Double.MIN_NORMAL;
        }
        return diff / Math.min(absSum, Double.MAX_VALUE) < epsilon;
    }

    record Bucket(double upperBound, double count) {}

    static final class SingleState implements AggregatorState {
        private final CircuitBreaker breaker;
        private final double quantile;
        private final List<Bucket> buckets = new ArrayList<>();
        private long reservedBytes;
        private boolean seen;

        SingleState(CircuitBreaker breaker, double quantile) {
            this.breaker = breaker;
            this.quantile = quantile;
        }

        void add(double upperBound, double count) {
            reserve(1);
            buckets.add(new Bucket(upperBound, count));
            seen = true;
        }

        void add(BytesRef serializedState) {
            List<Bucket> incoming = deserializeBuckets(serializedState);
            reserve(incoming.size());
            buckets.addAll(incoming);
            seen = true;
        }

        /**
         * Accounts for {@code count} additional buckets against the request circuit breaker before they are buffered,
         * so an over-large or high-cardinality histogram trips the breaker instead of exhausting the heap.
         */
        private void reserve(int count) {
            long bytes = count * BUCKET_RAM_BYTES_USED;
            breaker.addEstimateBytesAndMaybeBreak(bytes, BREAKER_LABEL);
            reservedBytes += bytes;
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            if (seen == false) {
                blocks[offset] = driverContext.blockFactory().newConstantNullBlock(1);
                return;
            }
            blocks[offset] = driverContext.blockFactory().newConstantBytesRefBlockWith(serializeBuckets(buckets), 1);
        }

        Block evaluateFinal(DriverContext driverContext) {
            if (seen == false) {
                return driverContext.blockFactory().newConstantNullBlock(1);
            }
            return driverContext.blockFactory().newConstantDoubleBlockWith(bucketQuantile(quantile, buckets), 1);
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-reservedBytes);
            reservedBytes = 0;
        }
    }

    static final class GroupingState implements GroupingAggregatorState {
        private final CircuitBreaker breaker;
        private final BigArrays bigArrays;
        private final double quantile;
        private ObjectArray<SingleState> states;

        GroupingState(CircuitBreaker breaker, BigArrays bigArrays, double quantile) {
            this.breaker = breaker;
            this.bigArrays = bigArrays;
            this.quantile = quantile;
            this.states = bigArrays.newObjectArray(1);
        }

        private SingleState getOrAdd(int groupId) {
            states = bigArrays.grow(states, groupId + 1L);
            SingleState state = states.get(groupId);
            if (state == null) {
                state = new SingleState(breaker, quantile);
                states.set(groupId, state);
            }
            return state;
        }

        void add(int groupId, double upperBound, double count) {
            getOrAdd(groupId).add(upperBound, count);
        }

        void add(int groupId, BytesRef serializedState) {
            getOrAdd(groupId).add(serializedState);
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // This state stores null for unseen groups and doesn't require additional tracking.
        }

        void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    SingleState state = groupId < states.size() ? states.get(groupId) : null;
                    if (state == null) {
                        builder.appendNull();
                    } else {
                        builder.appendBytesRef(serializeBuckets(state.buckets));
                    }
                }
                blocks[offset] = builder.build();
            }
        }

        Block evaluateFinal(IntVector selected, DriverContext driverContext) {
            try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    SingleState state = groupId < states.size() ? states.get(groupId) : null;
                    if (state == null) {
                        builder.appendNull();
                    } else {
                        builder.appendDouble(bucketQuantile(state.quantile, state.buckets));
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void close() {
            Releasables.close(
                Releasables.wrap(LongStream.range(0, states.size()).mapToObj(states::get).filter(Objects::nonNull).toList()),
                states
            );
        }
    }
}
