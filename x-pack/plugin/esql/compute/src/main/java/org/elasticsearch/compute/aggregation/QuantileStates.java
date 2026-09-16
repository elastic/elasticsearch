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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.InternalMedianAbsoluteDeviation;
import org.elasticsearch.search.aggregations.metrics.TDigestState;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.LongStream;

public final class QuantileStates {
    public static final double MEDIAN = 50.0;
    public static final double DEFAULT_COMPRESSION = 1000.0;

    /** Smallest encoding of a non-finite tally: three counts, one byte each. */
    private static final int TALLY_MIN_BYTES = 3;

    private QuantileStates() {}

    private static Double percentileParam(double p) {
        // Percentile must be a double between 0 and 100 inclusive
        // If percentile parameter is wrong, the aggregation will return NULL
        return 0 <= p && p <= 100 ? p : null;
    }

    /**
     * Serializes {@code digest}, optionally followed by a tally written by {@code tally}. Both ends of an exchange
     * agree on whether the tally is present, because the non-finite mode is fixed for the whole query by the plan that
     * selects it.
     */
    private static BytesRef serialize(TDigestState digest, TallyWriter tally) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(baos);
        try {
            TDigestState.write(digest, out);
            if (tally != null) {
                tally.write(out);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new BytesRef(baos.toByteArray());
    }

    /**
     * Merges a serialized state into {@code digest}, reading a trailing tally through {@code tally} when one is
     * expected. {@link TDigestState#write} emits a self-delimiting frame, so the tally can be read from the same
     * stream once the digest has been consumed.
     */
    private static void deserializeInto(CircuitBreaker breaker, BytesRef bytesRef, TDigestState digest, TallyReader tally) {
        ByteArrayStreamInput in = new ByteArrayStreamInput(bytesRef.bytes);
        in.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        try {
            try (TDigestState other = TDigestState.read(breaker, in)) {
                digest.add(other);
            }
            if (tally != null) {
                // The reader's mode must match the writer's, otherwise there is nothing here to read. The stream is
                // backed by a buffer shared with the other positions of the block and does not bounds-check, so
                // without this a mismatch would silently consume a neighbour's bytes as counts.
                assert in.available() >= TALLY_MIN_BYTES : "serialized state carries no non-finite tally";
                tally.read(in);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private interface TallyWriter {
        void write(OutputStreamStreamOutput out) throws IOException;
    }

    private interface TallyReader {
        void read(ByteArrayStreamInput in) throws IOException;
    }

    /**
     * Counts of the observations a t-digest cannot hold. IEEE-754 arithmetic legitimately produces these, and
     * Prometheus ranks rather than discards them, so they are tallied alongside the digest.
     */
    private static final class NonFiniteTally {
        private long nan;
        private long negInf;
        private long posInf;

        boolean isEmpty() {
            return (nan | negInf | posInf) == 0;
        }

        void add(double v) {
            if (Double.isNaN(v)) {
                nan++;
            } else if (v == Double.POSITIVE_INFINITY) {
                posInf++;
            } else {
                negInf++;
            }
        }

        void write(OutputStreamStreamOutput out) throws IOException {
            out.writeVLong(nan);
            out.writeVLong(negInf);
            out.writeVLong(posInf);
        }

        void read(ByteArrayStreamInput in) throws IOException {
            nan += in.readVLong();
            negInf += in.readVLong();
            posInf += in.readVLong();
        }

        double quantile(double q, TDigestState digest) {
            return QuantileStates.quantile(q, digest, nan, negInf, posInf);
        }
    }

    /**
     * Per-group counterpart of {@link NonFiniteTally}, backed by a big array so a high-cardinality grouping is
     * accounted against the circuit breaker. Only allocated in non-finite mode. A group's three counts are
     * interleaved, so growing the array and resolving a group each touch one array and one cache line.
     */
    private static final class NonFiniteTallies implements Releasable {
        private static final int NAN = 0;
        private static final int NEG_INF = 1;
        private static final int POS_INF = 2;
        private static final int STRIDE = 3;

        private final BigArrays bigArrays;
        private LongArray counts;

        NonFiniteTallies(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            this.counts = bigArrays.newLongArray(STRIDE, true);
        }

        private static long index(int groupId, int offset) {
            return (long) groupId * STRIDE + offset;
        }

        /**
         * Counts are read through {@link #get}, which reports an out-of-range group as zero, so growing is left to the
         * paths that write. A finite observation therefore never touches this array.
         */
        private void grow(int groupId) {
            counts = bigArrays.grow(counts, index(groupId, STRIDE));
        }

        private long get(int groupId, int offset) {
            long index = index(groupId, offset);
            return index < counts.size() ? counts.get(index) : 0;
        }

        void add(int groupId, double v) {
            final int offset;
            if (Double.isNaN(v)) {
                offset = NAN;
            } else if (v == Double.POSITIVE_INFINITY) {
                offset = POS_INF;
            } else {
                offset = NEG_INF;
            }
            grow(groupId);
            counts.increment(index(groupId, offset), 1);
        }

        long total(int groupId) {
            return get(groupId, NAN) + get(groupId, NEG_INF) + get(groupId, POS_INF);
        }

        void write(OutputStreamStreamOutput out, int groupId) throws IOException {
            out.writeVLong(get(groupId, NAN));
            out.writeVLong(get(groupId, NEG_INF));
            out.writeVLong(get(groupId, POS_INF));
        }

        void read(ByteArrayStreamInput in, int groupId) throws IOException {
            grow(groupId);
            counts.increment(index(groupId, NAN), in.readVLong());
            counts.increment(index(groupId, NEG_INF), in.readVLong());
            counts.increment(index(groupId, POS_INF), in.readVLong());
        }

        double quantile(double q, TDigestState digest, int groupId) {
            return QuantileStates.quantile(q, digest, get(groupId, NAN), get(groupId, NEG_INF), get(groupId, POS_INF));
        }

        @Override
        public void close() {
            Releasables.close(counts);
        }
    }

    /**
     * Selects the value at rank {@code q} (in {@code [0, 1]}) across {@code digest} and the non-finite counts, over the
     * total order {@code NaN < -Inf < finite < +Inf} that Prometheus sorts by before selecting a quantile. With no
     * non-finite observation this is the plain digest quantile, so ordinary data is unaffected.
     * <p>
     *     The rank {@code q * (total - 1)} generally falls between two observations, and the result is their weighted
     *     average. A rank that lands exactly on an observation returns it as-is; averaging in a neighbour weighted by
     *     zero would turn an adjacent infinity into {@code NaN}, losing the observed value.
     * </p>
     */
    static double quantile(double q, TDigestState digest, long nanCount, long negInfCount, long posInfCount) {
        if (nanCount == 0 && negInfCount == 0 && posInfCount == 0) {
            return digest.quantile(q);
        }
        long finiteCount = digest.size();
        long total = nanCount + negInfCount + finiteCount + posInfCount;
        double rank = q * (total - 1);
        long lowerRank = (long) Math.floor(rank);
        double lower = valueAtRank(lowerRank, digest, nanCount, negInfCount, finiteCount);
        double weight = rank - lowerRank;
        if (weight == 0) {
            return lower;
        }
        double upper = valueAtRank(Math.min(total - 1, lowerRank + 1), digest, nanCount, negInfCount, finiteCount);
        return lower * (1 - weight) + upper * weight;
    }

    /**
     * The observation at {@code rank} of the total order {@code NaN < -Inf < finite < +Inf}. Finite ranks are read off
     * the digest's own quantile scale, so they are approximate in the same way the digest is.
     */
    private static double valueAtRank(long rank, TDigestState digest, long nanCount, long negInfCount, long finiteCount) {
        if (rank < nanCount) {
            return Double.NaN;
        }
        rank -= nanCount;
        if (rank < negInfCount) {
            return Double.NEGATIVE_INFINITY;
        }
        rank -= negInfCount;
        if (rank < finiteCount) {
            return digest.quantile(finiteCount == 1 ? 0 : (double) rank / (finiteCount - 1));
        }
        return Double.POSITIVE_INFINITY;
    }

    static class SingleState implements AggregatorState {
        private final CircuitBreaker breaker;
        private final TDigestState digest;
        private final Double percentile;
        /**
         * Tallies the observations the digest cannot hold, or {@code null} in strict (finite-only) mode, where a
         * non-finite observation is rejected by the digest itself.
         */
        private final NonFiniteTally tally;

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
            this.tally = allowNonFinite ? new NonFiniteTally() : null;
        }

        @Override
        public void close() {
            Releasables.close(digest);
        }

        void add(double v) {
            if (tally != null && Double.isFinite(v) == false) {
                tally.add(v);
            } else {
                digest.add(v);
            }
        }

        void add(BytesRef other) {
            deserializeInto(breaker, other, digest, tally == null ? null : tally::read);
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + 1;
            BytesRef serialized = serialize(this.digest, tally == null ? null : tally::write);
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
            if (percentile == null || (digest.size() == 0 && (tally == null || tally.isEmpty()))) {
                return blockFactory.newConstantNullBlock(1);
            }
            double result = tally == null ? digest.quantile(percentile / 100) : tally.quantile(percentile / 100, digest);
            return blockFactory.newConstantDoubleBlockWith(result, 1);
        }
    }

    static class GroupingState implements GroupingAggregatorState {
        private ObjectArray<TDigestState> digests;
        private final BigArrays bigArrays;
        private final CircuitBreaker breaker;
        private final Double percentile;
        private final double tDigestStateCompression;
        /**
         * Tallies the observations the digests cannot hold, or {@code null} in strict (finite-only) mode, where a
         * non-finite observation is rejected by the digest itself.
         */
        private final NonFiniteTallies tallies;

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
            NonFiniteTallies allocated = null;
            boolean success = false;
            try {
                if (allowNonFinite) {
                    allocated = new NonFiniteTallies(bigArrays);
                }
                success = true;
            } finally {
                if (success == false) {
                    // The digests are already charged to the breaker and this instance never reaches a caller that
                    // could close it, so release them here rather than leaking the charge.
                    Releasables.closeExpectNoException(digests);
                }
            }
            this.tallies = allocated;
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
            if (tallies != null && Double.isFinite(v) == false) {
                tallies.add(groupId, v);
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
            deserializeInto(breaker, other, digest, tallies == null ? null : in -> tallies.read(in, groupId));
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
                    builder.appendBytesRef(serialize(state, tallies == null ? null : out -> tallies.write(out, group)));

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
                    } else if (tallies == null) {
                        if (digest.size() > 0) {
                            builder.appendDouble(digest.quantile(percentile / 100));
                        } else {
                            builder.appendNull();
                        }
                    } else if (digest.size() + tallies.total(si) > 0) {
                        // A group observing only non-finite values still holds a value; nulling it would drop the series.
                        builder.appendDouble(tallies.quantile(percentile / 100, digest, si));
                    } else {
                        builder.appendNull();
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
                tallies
            );
        }
    }
}
