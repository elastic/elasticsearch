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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Aggregation state and evaluation helpers for PromQL functions over classic cumulative histogram buckets
 * ({@code le} upper bound plus cumulative count).
 * <p>
 * Buckets are aggregated eagerly: each {@link AbstractState.Single} keeps a map from upper bound to the summed count for that
 * bound, so equal upper bounds (which are dimensions and therefore frequently repeated) collapse into a single entry
 * instead of being buffered as raw values. This keeps memory bounded by the number of distinct upper bounds and means
 * the intermediate state shipped to the coordinating node is already pre-aggregated.
 */
final class PromqlHistogramStates {
    /**
     * Load factor of the {@link LongDoubleHashMap} backing each {@link AbstractState.Single}; matches the hppc default.
     */
    static final double BUCKETS_LOAD_FACTOR = 0.75d;

    /**
     * Bytes charged to the circuit breaker for each distinct bucket upper bound buffered in an {@link AbstractState.Single}.
     * The buckets live in a primitive {@link LongDoubleHashMap}, so a populated entry occupies a {@code long} key slot
     * plus a {@code double} value slot (16 bytes) in the open-addressing tables. The map sizes its backing arrays to a
     * power of two of {@code size / loadFactor}, so a table holding {@code N} entries can allocate close to
     * {@code 2 * N / loadFactor} slots right after a rehash. We therefore charge {@code 2 * 16 / loadFactor} per entry:
     * a deliberately conservative estimate that never under-counts the real footprint, rather than an exact allocation
     * tally tracked across rehashes.
     */
    static final long BUCKET_RAM_BYTES_USED = (long) Math.ceil(2 * (Long.BYTES + Double.BYTES) / BUCKETS_LOAD_FACTOR);

    private static final String BREAKER_LABEL = "<promql_histogram>";

    private PromqlHistogramStates() {}

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

    record Bucket(double upperBound, double count) {}

    abstract static class AbstractState {
        abstract static class Single implements AggregatorState {
            private final CircuitBreaker breaker;
            private final Warnings warnings;
            private final LongDoubleHashMap buckets = new LongDoubleHashMap();
            private long reservedBytes;

            Single(CircuitBreaker breaker) {
                this(breaker, Warnings.NOOP_WARNINGS);
            }

            Single(CircuitBreaker breaker, Warnings warnings) {
                this.breaker = breaker;
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

            void combineIntermediate(DoubleBlock block) {
                if (block.isNull(0) == false) {
                    addIntermediate(block, 0);
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

            boolean isEmpty() {
                return buckets.isEmpty();
            }

            List<Bucket> toBuckets() {
                List<Bucket> result = new ArrayList<>(buckets.size());
                for (LongDoubleCursor cursor : buckets) {
                    result.add(new Bucket(Double.longBitsToDouble(cursor.key), cursor.value));
                }
                return result;
            }

            abstract double evaluate(List<Bucket> buckets);

            Block evaluateFinal(DriverContext driverContext) {
                if (isEmpty()) {
                    return driverContext.blockFactory().newConstantNullBlock(1);
                }
                double result = evaluate(toBuckets());
                if (Double.isNaN(result)) {
                    return driverContext.blockFactory().newConstantNullBlock(1);
                }
                return driverContext.blockFactory().newConstantDoubleBlockWith(result, 1);
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

            @Override
            public void close() {
                breaker.addWithoutBreaking(-reservedBytes);
                reservedBytes = 0;
                buckets.release();
            }
        }

        abstract static class Grouping implements GroupingAggregatorState {
            private final CircuitBreaker breaker;
            private final BigArrays bigArrays;
            private final Warnings warnings;
            private ObjectArray<Single> states;

            Grouping(CircuitBreaker breaker, BigArrays bigArrays) {
                this(breaker, bigArrays, Warnings.NOOP_WARNINGS);
            }

            Grouping(CircuitBreaker breaker, BigArrays bigArrays, Warnings warnings) {
                this.breaker = breaker;
                this.bigArrays = bigArrays;
                this.warnings = warnings;
                this.states = bigArrays.newObjectArray(1);
            }

            abstract Single newSingleState(CircuitBreaker breaker, Warnings warnings);

            private Single getOrAdd(int groupId) {
                states = bigArrays.grow(states, groupId + 1L);
                Single state = states.get(groupId);
                if (state == null) {
                    state = newSingleState(breaker, warnings);
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

            void combineIntermediate(int groupId, DoubleBlock block, int position) {
                if (block.isNull(position) == false) {
                    addIntermediate(groupId, block, position);
                }
            }

            void combineIntermediate(int positionOffset, IntVector groups, DoubleBlock block) {
                for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
                    combineIntermediate(groups.getInt(groupPosition), block, groupPosition + positionOffset);
                }
            }

            void combineIntermediate(int positionOffset, IntBlock groups, DoubleBlock block) {
                for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
                    if (groups.isNull(groupPosition)) {
                        continue;
                    }
                    int valuesPosition = groupPosition + positionOffset;
                    if (block.isNull(valuesPosition)) {
                        continue;
                    }
                    int groupStart = groups.getFirstValueIndex(groupPosition);
                    int groupEnd = groupStart + groups.getValueCount(groupPosition);
                    for (int g = groupStart; g < groupEnd; g++) {
                        addIntermediate(groups.getInt(g), block, valuesPosition);
                    }
                }
            }

            Single state(int groupId) {
                return groupId < states.size() ? states.get(groupId) : null;
            }

            @Override
            public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
                // This state stores null for unseen groups and doesn't require additional tracking.
            }

            void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
                try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
                    for (int i = 0; i < selected.getPositionCount(); i++) {
                        int groupId = selected.getInt(i);
                        Single state = state(groupId);
                        if (state == null || state.isEmpty()) {
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
                        Single state = state(selected.getInt(i));
                        if (state == null || state.isEmpty()) {
                            builder.appendNull();
                            continue;
                        }
                        double result = state.evaluate(state.toBuckets());
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
                        Single state = states.get(i);
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

    static final class Quantile {
        private static final double SMALL_DELTA_TOLERANCE = 1e-12;

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

        private static void ensureMonotonicAndIgnoreSmallDeltas(List<Bucket> buckets, double tolerance) {
            double previous = buckets.getFirst().count();
            for (int i = 1; i < buckets.size(); i++) {
                Bucket bucket = buckets.get(i);
                double current = bucket.count();
                if (current == previous) {
                    continue;
                }
                if (almostEqual(previous, current, tolerance) || current < previous) {
                    buckets.set(i, new Bucket(bucket.upperBound(), previous));
                    continue;
                }
                previous = current;
            }
        }

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

        static final class SingleState extends AbstractState.Single {
            final double quantile;

            SingleState(CircuitBreaker breaker, double quantile) {
                super(breaker);
                this.quantile = quantile;
            }

            SingleState(CircuitBreaker breaker, double quantile, Warnings warnings) {
                super(breaker, warnings);
                this.quantile = quantile;
            }

            @Override
            double evaluate(List<Bucket> buckets) {
                return bucketQuantile(quantile, buckets);
            }
        }

        static final class GroupingState extends AbstractState.Grouping {
            final double quantile;

            GroupingState(CircuitBreaker breaker, BigArrays bigArrays, double quantile) {
                super(breaker, bigArrays);
                this.quantile = quantile;
            }

            GroupingState(CircuitBreaker breaker, BigArrays bigArrays, double quantile, Warnings warnings) {
                super(breaker, bigArrays, warnings);
                this.quantile = quantile;
            }

            @Override
            SingleState newSingleState(CircuitBreaker breaker, Warnings warnings) {
                return new SingleState(breaker, quantile, warnings);
            }

            @Override
            SingleState state(int groupId) {
                return (SingleState) super.state(groupId);
            }
        }
    }

}
