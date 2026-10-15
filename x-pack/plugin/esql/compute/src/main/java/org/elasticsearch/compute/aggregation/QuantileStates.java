/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.InternalMedianAbsoluteDeviation;
import org.elasticsearch.search.aggregations.metrics.TDigestState;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.LongStream;

public final class QuantileStates {
    public static final double MEDIAN = 50.0;
    public static final double DEFAULT_COMPRESSION = 1000.0;

    private QuantileStates() {}

    private static Double percentileParam(double p) {
        // Percentile must be a double between 0 and 100 inclusive
        // If percentile parameter is wrong, the aggregation will return NULL
        return 0 <= p && p <= 100 ? p : null;
    }

    /**
     * Serializes {@code digest}, optionally followed by the non-finite counts written by {@code nonFiniteCounts}. Both
     * ends of an exchange agree on whether the counts are present, because the non-finite mode is fixed for the whole
     * query by the plan that selects it.
     */
    private static BytesRef serialize(TDigestState digest, CheckedConsumer<StreamOutput, IOException> nonFiniteCounts) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(baos);
        try {
            TDigestState.write(digest, out);
            if (nonFiniteCounts != null) {
                nonFiniteCounts.accept(out);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new BytesRef(baos.toByteArray());
    }

    /**
     * Merges a serialized state into {@code digest}, reading the trailing non-finite counts through
     * {@code nonFiniteCounts} when they are expected. {@link TDigestState#write} emits a self-delimiting frame, so the
     * counts can be read from the same stream once the digest has been consumed.
     */
    private static void deserializeInto(
        CircuitBreaker breaker,
        BytesRef bytesRef,
        TDigestState digest,
        CheckedConsumer<StreamInput, IOException> nonFiniteCounts
    ) {
        ByteArrayStreamInput in = new ByteArrayStreamInput(bytesRef.bytes);
        in.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        try {
            try (TDigestState other = TDigestState.read(breaker, in)) {
                digest.add(other);
            }
            if (nonFiniteCounts != null) {
                // The reader's mode must match the writer's, otherwise there is nothing here to read. The stream is
                // backed by a buffer shared with the other positions of the block and does not bounds-check, so
                // without this a mismatch would silently consume a neighbour's bytes as counts.
                assert in.available() > 0 : "serialized state carries no non-finite counts";
                nonFiniteCounts.accept(in);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Selects the value at rank {@code q} (in {@code [0, 1]}) across {@code digest} and the non-finite counts, over the
     * total order {@code NaN < -Inf < finite < +Inf} that Prometheus sorts by before selecting a quantile. With no
     * non-finite observation this is the plain digest quantile, so ordinary data is unaffected.
     * <p>
     *     The rank {@code q * (total - 1)} generally falls between two observations. When both are finite, the rank is
     *     mapped onto the digest's own quantile scale and resolved by the digest alone, which already interpolates
     *     between its centroids. Otherwise the result is the weighted average of the two bracketing observations. A rank that lands
     *     exactly on an observation returns it as-is; averaging in a neighbour weighted by zero would turn an adjacent infinity into
     *     {@code NaN}, losing the observed value.
     * </p>
     */
    static double quantile(double q, TDigestState digest, long nanCount, long negInfCount, long posInfCount) {
        if (nanCount == 0 && negInfCount == 0 && posInfCount == 0) {
            return digest.quantile(q);
        }
        long finiteCount = digest.size();
        long firstFiniteRank = nanCount + negInfCount;
        long lastFiniteRank = firstFiniteRank + finiteCount - 1;
        long total = firstFiniteRank + finiteCount + posInfCount;
        double rank = q * (total - 1);
        if (finiteCount > 0 && rank >= firstFiniteRank && rank <= lastFiniteRank) {
            return digest.quantile(finiteCount == 1 ? 0 : (rank - firstFiniteRank) / (finiteCount - 1));
        }
        long lowerRank = (long) Math.floor(rank);
        double lower = valueAtRank(lowerRank, nanCount, negInfCount, finiteCount);
        double weight = rank - lowerRank;
        if (weight == 0) {
            return lower;
        }
        double upper = valueAtRank(Math.min(total - 1, lowerRank + 1), nanCount, negInfCount, finiteCount);
        return lower * (1 - weight) + upper * weight;
    }

    /**
     * The observation at {@code rank} of the total order {@code NaN < -Inf < finite < +Inf}, for a rank that is not
     * bracketed by two finite observations. At least one of the two bracketing observations is then non-finite and
     * carries a weight strictly between zero and one, so it alone determines the interpolated result, and any finite
     * stand-in can be returned for a finite rank.
     */
    private static double valueAtRank(long rank, long nanCount, long negInfCount, long finiteCount) {
        if (rank < nanCount) {
            return Double.NaN;
        }
        rank -= nanCount;
        if (rank < negInfCount) {
            return Double.NEGATIVE_INFINITY;
        }
        rank -= negInfCount;
        if (rank < finiteCount) {
            return 0.0;
        }
        return Double.POSITIVE_INFINITY;
    }

    static class SingleState implements AggregatorState {
        private final CircuitBreaker breaker;
        private final TDigestState digest;
        private final Double percentile;
        /**
         * Whether the observations a t-digest cannot hold are counted alongside it. IEEE-754 arithmetic legitimately
         * produces these, and Prometheus ranks rather than discards them. In strict (finite-only) mode the counts stay
         * zero and a non-finite observation is rejected by the digest itself.
         */
        private final boolean allowNonFinite;
        private long nanCount;
        private long negInfCount;
        private long posInfCount;

        SingleState(CircuitBreaker breaker, double percentile) {
            this(breaker, percentile, DEFAULT_COMPRESSION);
        }

        SingleState(CircuitBreaker breaker, double percentile, double tDigestStateCompression) {
            this(breaker, percentile, tDigestStateCompression, false);
        }

        SingleState(CircuitBreaker breaker, double percentile, double tDigestStateCompression, boolean allowNonFinite) {
            this.breaker = breaker;
            this.digest = TDigestState.create(breaker, tDigestStateCompression);
            this.percentile = percentileParam(percentile);
            this.allowNonFinite = allowNonFinite;
        }

        @Override
        public void close() {
            Releasables.close(digest);
        }

        void add(double v) {
            if (allowNonFinite && Double.isFinite(v) == false) {
                if (Double.isNaN(v)) {
                    nanCount++;
                } else if (v == Double.POSITIVE_INFINITY) {
                    posInfCount++;
                } else {
                    negInfCount++;
                }
            } else {
                digest.add(v);
            }
        }

        void add(BytesRef other) {
            deserializeInto(breaker, other, digest, allowNonFinite ? this::readNonFiniteCounts : null);
        }

        private void readNonFiniteCounts(StreamInput in) throws IOException {
            nanCount += in.readVLong();
            negInfCount += in.readVLong();
            posInfCount += in.readVLong();
        }

        private void writeNonFiniteCounts(StreamOutput out) throws IOException {
            out.writeVLong(nanCount);
            out.writeVLong(negInfCount);
            out.writeVLong(posInfCount);
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + 1;
            BytesRef serialized = serialize(this.digest, allowNonFinite ? this::writeNonFiniteCounts : null);
            blocks[offset] = driverContext.blockFactory().newConstantBytesRefBlockWith(serialized, 1);
        }

        Block evaluateMedianAbsoluteDeviation(DriverContext driverContext) {
            BlockFactory blockFactory = driverContext.blockFactory();
            assert percentile == MEDIAN : "Median must be 50th percentile [percentile = " + percentile + "]";
            if (digest.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            double result = InternalMedianAbsoluteDeviation.computeMedianAbsoluteDeviation(digest);
            return blockFactory.newConstantDoubleBlockWith(result, 1);
        }

        Block evaluatePercentile(DriverContext driverContext) {
            BlockFactory blockFactory = driverContext.blockFactory();
            if (percentile == null || (digest.size() == 0 && nanCount == 0 && negInfCount == 0 && posInfCount == 0)) {
                return blockFactory.newConstantNullBlock(1);
            }
            double result = quantile(percentile / 100, digest, nanCount, negInfCount, posInfCount);
            return blockFactory.newConstantDoubleBlockWith(result, 1);
        }
    }

    static class GroupingState implements GroupingAggregatorState {
        private static final int NAN_OFFSET = 0;
        private static final int NEG_INF_OFFSET = 1;
        private static final int POS_INF_OFFSET = 2;
        private static final int NON_FINITE_STRIDE = 3;

        private ObjectArray<TDigestState> digests;
        private final BigArrays bigArrays;
        private final CircuitBreaker breaker;
        private final Double percentile;
        private final double tDigestStateCompression;
        /**
         * Whether the observations the digests cannot hold are counted alongside them, as in {@link SingleState}.
         */
        private final boolean allowNonFinite;
        /**
         * Per-group non-finite counts, backed by a big array so a high-cardinality grouping is accounted against the
         * circuit breaker. A group's three counts are interleaved, so growing the array and resolving a group each touch
         * one array and one cache line. Allocated on the first non-zero count and grown only by writes; a group beyond
         * its end has counted none, so a finite observation never touches it and it stays {@code null} in strict mode.
         */
        private LongArray nonFiniteCounts;

        GroupingState(CircuitBreaker breaker, BigArrays bigArrays, double percentile) {
            this(breaker, bigArrays, percentile, DEFAULT_COMPRESSION);
        }

        GroupingState(CircuitBreaker breaker, BigArrays bigArrays, double percentile, double tDigestStateCompression) {
            this(breaker, bigArrays, percentile, tDigestStateCompression, false);
        }

        GroupingState(
            CircuitBreaker breaker,
            BigArrays bigArrays,
            double percentile,
            double tDigestStateCompression,
            boolean allowNonFinite
        ) {
            this.breaker = breaker;
            this.bigArrays = bigArrays;
            this.digests = bigArrays.newObjectArray(1);
            this.percentile = percentileParam(percentile);
            this.tDigestStateCompression = tDigestStateCompression;
            this.allowNonFinite = allowNonFinite;
        }

        private static long nonFiniteIndex(int groupId, int offset) {
            return (long) groupId * NON_FINITE_STRIDE + offset;
        }

        private long nonFiniteCount(int groupId, int offset) {
            if (nonFiniteCounts == null) {
                return 0;
            }
            long index = nonFiniteIndex(groupId, offset);
            return index < nonFiniteCounts.size() ? nonFiniteCounts.get(index) : 0;
        }

        private void addNonFiniteCount(int groupId, int offset, long count) {
            if (count == 0) {
                return;
            }
            long minSize = nonFiniteIndex(groupId, NON_FINITE_STRIDE);
            nonFiniteCounts = nonFiniteCounts == null ? bigArrays.newLongArray(minSize, true) : bigArrays.grow(nonFiniteCounts, minSize);
            nonFiniteCounts.increment(nonFiniteIndex(groupId, offset), count);
        }

        private void readNonFiniteCounts(StreamInput in, int groupId) throws IOException {
            addNonFiniteCount(groupId, NAN_OFFSET, in.readVLong());
            addNonFiniteCount(groupId, NEG_INF_OFFSET, in.readVLong());
            addNonFiniteCount(groupId, POS_INF_OFFSET, in.readVLong());
        }

        private void writeNonFiniteCounts(StreamOutput out, int groupId) throws IOException {
            out.writeVLong(nonFiniteCount(groupId, NAN_OFFSET));
            out.writeVLong(nonFiniteCount(groupId, NEG_INF_OFFSET));
            out.writeVLong(nonFiniteCount(groupId, POS_INF_OFFSET));
        }

        private TDigestState getOrAddGroup(int groupId) {
            digests = bigArrays.grow(digests, groupId + 1);
            TDigestState qs = digests.get(groupId);
            if (qs == null) {
                qs = TDigestState.create(breaker, tDigestStateCompression);
                digests.set(groupId, qs);
            }
            return qs;
        }

        void add(int groupId, double v) {
            TDigestState digest = getOrAddGroup(groupId);
            if (allowNonFinite && Double.isFinite(v) == false) {
                if (Double.isNaN(v)) {
                    addNonFiniteCount(groupId, NAN_OFFSET, 1);
                } else if (v == Double.POSITIVE_INFINITY) {
                    addNonFiniteCount(groupId, POS_INF_OFFSET, 1);
                } else {
                    addNonFiniteCount(groupId, NEG_INF_OFFSET, 1);
                }
            } else {
                digest.add(v);
            }
        }

        void add(int groupId, TDigestState other) {
            if (other != null) {
                getOrAddGroup(groupId).add(other);
            }
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // We always enable.
        }

        void add(int groupId, BytesRef other) {
            TDigestState digest = getOrAddGroup(groupId);
            deserializeInto(breaker, other, digest, allowNonFinite ? in -> readNonFiniteCounts(in, groupId) : null);
        }

        TDigestState getOrNull(int position) {
            if (position < digests.size()) {
                return digests.get(position);
            } else {
                return null;
            }
        }

        /** Extracts an intermediate view of the contents of this state.  */
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset + 1;
            try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int group = selected.getInt(i);
                    TDigestState state;
                    boolean closeState = false;
                    if (group < digests.size()) {
                        state = getOrNull(group);
                        if (state == null) {
                            state = TDigestState.create(breaker, tDigestStateCompression);
                            closeState = true;
                        }
                    } else {
                        state = TDigestState.create(breaker, tDigestStateCompression);
                        closeState = true;
                    }
                    builder.appendBytesRef(serialize(state, allowNonFinite ? out -> writeNonFiniteCounts(out, group) : null));

                    if (closeState) {
                        state.close();
                    }
                }
                blocks[offset] = builder.build();
            }
        }

        Block evaluateMedianAbsoluteDeviation(IntVector selected, DriverContext driverContext) {
            assert percentile == MEDIAN : "Median must be 50th percentile [percentile = " + percentile + "]";
            try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int si = selected.getInt(i);
                    if (si >= digests.size()) {
                        builder.appendNull();
                        continue;
                    }
                    final TDigestState digest = digests.get(si);
                    if (digest != null && digest.size() > 0) {
                        builder.appendDouble(InternalMedianAbsoluteDeviation.computeMedianAbsoluteDeviation(digest));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        Block evaluatePercentile(IntVector selected, DriverContext driverContext) {
            try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int si = selected.getInt(i);
                    if (si >= digests.size()) {
                        builder.appendNull();
                        continue;
                    }
                    final TDigestState digest = digests.get(si);
                    if (percentile == null || digest == null) {
                        builder.appendNull();
                        continue;
                    }
                    long nan = nonFiniteCount(si, NAN_OFFSET);
                    long negInf = nonFiniteCount(si, NEG_INF_OFFSET);
                    long posInf = nonFiniteCount(si, POS_INF_OFFSET);
                    if (digest.size() == 0 && nan == 0 && negInf == 0 && posInf == 0) {
                        builder.appendNull();
                    } else {
                        // A group observing only non-finite values still holds a value; nulling it would drop the series.
                        builder.appendDouble(quantile(percentile / 100, digest, nan, negInf, posInf));
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void close() {
            Releasables.close(
                Releasables.wrap(LongStream.range(0, digests.size()).mapToObj(i -> digests.get(i)).toList()),
                digests,
                nonFiniteCounts
            );
        }
    }
}
