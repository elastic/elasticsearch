/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Aggregation state and PromQL {@code histogram_quantile} evaluation for classic cumulative histogram buckets
 * ({@code le} upper bound plus cumulative count), used by {@link PromqlHistogramQuantileAggregator}.
 * <p>
 * Buckets are aggregated eagerly: each {@link SingleState} keeps a map from upper bound to the summed count for that
 * bound, so equal upper bounds (which are dimensions and therefore frequently repeated) collapse into a single entry
 * instead of being buffered as raw values. This keeps memory bounded by the number of distinct upper bounds and means
 * the intermediate state shipped to the coordinating node is already pre-aggregated.
 */
final class PromqlHistogramQuantileStates {
    static final double SMALL_DELTA_TOLERANCE = 1e-12;

    /**
     * Load factor of the {@link LongDoubleHashMap} backing each {@link SingleState}; matches the hppc default.
     */
    static final double BUCKETS_LOAD_FACTOR = 0.75d;

    /**
     * Bytes charged to the circuit breaker for each distinct bucket upper bound buffered in a {@link SingleState}.
     * The buckets live in a primitive {@link LongDoubleHashMap}, so a populated entry occupies a {@code long} key slot
     * plus a {@code double} value slot (16 bytes) in the open-addressing tables. The map sizes its backing arrays to a
     * power of two of {@code size / loadFactor}, so a table holding {@code N} entries can allocate close to
     * {@code 2 * N / loadFactor} slots right after a rehash. We therefore charge {@code 2 * 16 / loadFactor} per entry:
     * a deliberately conservative estimate that never under-counts the real footprint, rather than an exact allocation
     * tally tracked across rehashes.
     */
    static final long BUCKET_RAM_BYTES_USED = (long) Math.ceil(2 * (Long.BYTES + Double.BYTES) / BUCKETS_LOAD_FACTOR);

    private static final String BREAKER_LABEL = "<promql_histogram_quantile>";

    private PromqlHistogramQuantileStates() {}

    /**
     * Parses a classic histogram bucket upper bound from its {@code le} keyword label. PromQL stores {@code le} as a
     * dimension (a keyword); the bucket terminating every classic histogram is the literal {@code "+Inf"}, and keeping
     * the bound a keyword in storage also avoids {@code NumberFieldMapper} rejecting that non-finite sentinel.
     * <p>
     * The accepted spellings mirror Go's {@code strconv.ParseFloat}, which is what Prometheus' {@code histogram_quantile}
     * uses to parse {@code le}: the special values {@code inf}/{@code infinity} (with an optional sign) and {@code nan}
     * are recognized case-insensitively, and everything else is a finite number. {@link Double#parseDouble} already
     * handles the case-sensitive {@code "Infinity"}/{@code "NaN"} forms, so only the abbreviated {@code "Inf"} forms and
     * case folding need explicit handling.
     *
     * @throws NumberFormatException if the label is not one of those spellings; the message names the offending value
     *         (like Prometheus' "bad bucket label" warning), and callers skip such buckets, as Prometheus does
     */
    static double parseUpperBound(BytesRef le) {
        String text = le.utf8ToString();
        if (text.equalsIgnoreCase("+Inf")
            || text.equalsIgnoreCase("Inf")
            || text.equalsIgnoreCase("Infinity")
            || text.equalsIgnoreCase("+Infinity")) {
            return Double.POSITIVE_INFINITY;
        }
        if (text.equalsIgnoreCase("-Inf") || text.equalsIgnoreCase("-Infinity")) {
            return Double.NEGATIVE_INFINITY;
        }
        if (text.equalsIgnoreCase("NaN")) {
            return Double.NaN;
        }
        try {
            return Double.parseDouble(text);
        } catch (NumberFormatException e) {
            throw new NumberFormatException("bucket label [le] has a malformed value of [" + text + "]");
        }
    }

    /**
     * Estimates a quantile from pre-aggregated classic histogram buckets using PromQL {@code histogram_quantile}
     * semantics: repair non-monotonic counts, then linearly interpolate within the target bucket.
     * <p>
     * Callers must pass at most one bucket per exact upper bound.
     *
     * @param quantile fraction in {@code [0, 1]}; out-of-range and NaN inputs follow PromQL edge-case rules
     * @param inputBuckets pre-aggregated cumulative buckets ending with {@code +Inf}; the method sorts them by upper bound
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
        assert hasDistinctUpperBounds(buckets) : "histogram buckets must be pre-aggregated by upper bound";
        if (Double.isInfinite(buckets.getLast().upperBound()) == false || buckets.getLast().upperBound() < 0) {
            return Double.NaN;
        }

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
     * Returns whether the buckets have already been pre-aggregated by exact upper bound.
     */
    private static boolean hasDistinctUpperBounds(List<Bucket> buckets) {
        long previousKey = Double.doubleToLongBits(buckets.getFirst().upperBound());
        for (int i = 1; i < buckets.size(); i++) {
            long currentKey = Double.doubleToLongBits(buckets.get(i).upperBound());
            if (previousKey == currentKey) {
                return false;
            }
            previousKey = currentKey;
        }
        return true;
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
        private final Warnings warnings;
        private final LongDoubleHashMap buckets = new LongDoubleHashMap();
        private long reservedBytes;

        SingleState(CircuitBreaker breaker, double quantile) {
            this(breaker, quantile, Warnings.NOOP_WARNINGS);
        }

        SingleState(CircuitBreaker breaker, double quantile, Warnings warnings) {
            this.breaker = breaker;
            this.quantile = quantile;
            this.warnings = warnings;
        }

        /**
         * Parses the bucket's {@code le} keyword bound and adds it, or — mirroring Prometheus' {@code histogram_quantile} —
         * records a warning and skips the bucket when the label is not a number.
         */
        void add(BytesRef le, double count) {
            double upperBound;
            try {
                upperBound = parseUpperBound(le);
            } catch (NumberFormatException e) {
                warnings.registerException(e);
                return;
            }
            add(upperBound, count);
        }

        /**
         * Adds the cumulative {@code count} for the given {@code upperBound}, summing into any existing entry with an
         * exactly equal bound. The bound is keyed by its raw bits so equality matches {@link Double#equals} semantics
         * ({@code -0.0} and {@code 0.0} are distinct, every {@code NaN} collapses to one entry).
         */
        void add(double upperBound, double count) {
            long key = Double.doubleToLongBits(upperBound);
            if (buckets.containsKey(key)) {
                buckets.addTo(key, count);
            } else {
                reserve(1);
                buckets.put(key, count);
            }
        }

        /**
         * Merges the pre-aggregated buckets serialized at {@code position} of an intermediate {@link DoubleBlock}, which
         * stores each bucket as two consecutive values: the upper bound followed by its cumulative count.
         */
        void addIntermediate(DoubleBlock block, int position) {
            int start = block.getFirstValueIndex(position);
            int valueCount = block.getValueCount(position);
            assert valueCount % 2 == 0 : "histogram intermediate state must hold (upperBound, count) pairs, got " + valueCount;
            for (int i = 0; i < valueCount; i += 2) {
                add(block.getDouble(start + i), block.getDouble(start + i + 1));
            }
        }

        /**
         * Accounts for {@code count} additional distinct buckets against the request circuit breaker before they are
         * buffered, so a high-cardinality histogram trips the breaker instead of exhausting the heap.
         */
        private void reserve(int count) {
            long bytes = count * BUCKET_RAM_BYTES_USED;
            breaker.addEstimateBytesAndMaybeBreak(bytes, BREAKER_LABEL);
            reservedBytes += bytes;
        }

        private List<Bucket> toBuckets() {
            List<Bucket> result = new ArrayList<>(buckets.size());
            for (LongDoubleCursor cursor : buckets) {
                result.add(new Bucket(Double.longBitsToDouble(cursor.key), cursor.value));
            }
            return result;
        }

        /**
         * Appends this state's buckets as a single multi-value position of {@code (upperBound, count)} pairs.
         */
        private void appendIntermediate(DoubleBlock.Builder builder) {
            builder.beginPositionEntry();
            for (LongDoubleCursor cursor : buckets) {
                builder.appendDouble(Double.longBitsToDouble(cursor.key));
                builder.appendDouble(cursor.value);
            }
            builder.endPositionEntry();
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            if (buckets.isEmpty()) {
                blocks[offset] = driverContext.blockFactory().newConstantNullBlock(1);
                return;
            }
            try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(buckets.size() * 2)) {
                appendIntermediate(builder);
                blocks[offset] = builder.build();
            }
        }

        Block evaluateFinal(DriverContext driverContext) {
            if (buckets.isEmpty()) {
                return driverContext.blockFactory().newConstantNullBlock(1);
            }
            double result = bucketQuantile(quantile, toBuckets());
            // NaN signals "no estimate"; emit null rather than placing NaN in a DoubleBlock, where it would break equality.
            if (Double.isNaN(result)) {
                return driverContext.blockFactory().newConstantNullBlock(1);
            }
            return driverContext.blockFactory().newConstantDoubleBlockWith(result, 1);
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-reservedBytes);
            reservedBytes = 0;
            buckets.release();
        }
    }

    static final class GroupingState implements GroupingAggregatorState {
        private final CircuitBreaker breaker;
        private final BigArrays bigArrays;
        private final double quantile;
        private final Warnings warnings;
        private ObjectArray<SingleState> states;

        GroupingState(CircuitBreaker breaker, BigArrays bigArrays, double quantile) {
            this(breaker, bigArrays, quantile, Warnings.NOOP_WARNINGS);
        }

        GroupingState(CircuitBreaker breaker, BigArrays bigArrays, double quantile, Warnings warnings) {
            this.breaker = breaker;
            this.bigArrays = bigArrays;
            this.quantile = quantile;
            this.warnings = warnings;
            this.states = bigArrays.newObjectArray(1);
        }

        private SingleState getOrAdd(int groupId) {
            states = bigArrays.grow(states, groupId + 1L);
            SingleState state = states.get(groupId);
            if (state == null) {
                state = new SingleState(breaker, quantile, warnings);
                states.set(groupId, state);
            }
            return state;
        }

        /**
         * Parses the bucket's {@code le} keyword bound for {@code groupId} and adds it, or — mirroring Prometheus'
         * {@code histogram_quantile} — records a warning and skips the bucket when the label is not a number.
         */
        void add(int groupId, BytesRef le, double count) {
            getOrAdd(groupId).add(le, count);
        }

        void add(int groupId, double upperBound, double count) {
            getOrAdd(groupId).add(upperBound, count);
        }

        void addIntermediate(int groupId, DoubleBlock block, int position) {
            getOrAdd(groupId).addIntermediate(block, position);
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // This state stores null for unseen groups and doesn't require additional tracking.
        }

        void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    SingleState state = groupId < states.size() ? states.get(groupId) : null;
                    if (state == null || state.buckets.isEmpty()) {
                        builder.appendNull();
                    } else {
                        state.appendIntermediate(builder);
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
                    if (state == null || state.buckets.isEmpty()) {
                        builder.appendNull();
                        continue;
                    }
                    double result = bucketQuantile(state.quantile, state.toBuckets());
                    // NaN signals "no estimate"; emit null rather than placing NaN in a DoubleBlock, where it breaks equality.
                    if (Double.isNaN(result)) {
                        builder.appendNull();
                    } else {
                        builder.appendDouble(result);
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void close() {
            try {
                for (long i = 0; i < states.size(); i++) {
                    SingleState state = states.get(i);
                    if (state != null) {
                        state.close();
                    }
                }
            } finally {
                states.close();
            }
        }
    }
}
