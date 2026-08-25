/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.TDigestState;

import java.util.stream.LongStream;

/**
 * Percentile state that accepts non-finite observations, as IEEE-754 arithmetic requires.
 * <p>
 *     A t-digest can only hold finite values, so {@code NaN} and {@code ±Inf} are tallied separately and the requested
 *     rank is resolved over the total order {@code NaN < -Inf < finite < +Inf}. That order places {@code NaN} lowest,
 *     matching the comparator Prometheus sorts by before selecting a quantile.
 * </p>
 * <p>
 *     When no non-finite value is observed the result is the plain t-digest quantile, so the outcome for ordinary data
 *     is identical to {@link QuantileStates}.
 * </p>
 */
public final class LenientQuantileStates {

    private LenientQuantileStates() {}

    /**
     * Selects the value at rank {@code q} (in {@code [0, 1]}) across {@code digest} and the non-finite tallies.
     * <p>
     *     The rank {@code q * (total - 1)} generally falls between two observations of the total order, and the result
     *     is their weighted average. A rank that lands exactly on an observation returns it as-is; averaging in a
     *     neighbour weighted by zero would turn an adjacent infinity into {@code NaN}, losing the observed value.
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
     * The observation at {@code rank} of the total order {@code NaN < -Inf < finite < +Inf}, which is the order
     * Prometheus sorts by before selecting a quantile. Finite ranks are read off the digest's own quantile scale, so
     * they are approximate in the same way the digest is.
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
        private long nanCount;
        private long negInfCount;
        private long posInfCount;

        SingleState(CircuitBreaker breaker, double percentile, double tDigestStateCompression) {
            this.breaker = breaker;
            this.digest = TDigestState.create(breaker, tDigestStateCompression);
            this.percentile = 0 <= percentile && percentile <= 100 ? percentile : null;
        }

        @Override
        public void close() {
            Releasables.close(digest);
        }

        void add(double v) {
            if (Double.isNaN(v)) {
                nanCount++;
            } else if (v == Double.POSITIVE_INFINITY) {
                posInfCount++;
            } else if (v == Double.NEGATIVE_INFINITY) {
                negInfCount++;
            } else {
                digest.add(v);
            }
        }

        void add(BytesRef other, long otherNanCount, long otherNegInfCount, long otherPosInfCount) {
            try (var otherDigest = QuantileStates.deserializeDigest(breaker, other)) {
                digest.add(otherDigest);
            }
            nanCount += otherNanCount;
            negInfCount += otherNegInfCount;
            posInfCount += otherPosInfCount;
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + 4;
            BlockFactory blockFactory = driverContext.blockFactory();
            blocks[offset + 0] = blockFactory.newConstantBytesRefBlockWith(QuantileStates.serializeDigest(digest), 1);
            blocks[offset + 1] = blockFactory.newConstantLongBlockWith(nanCount, 1);
            blocks[offset + 2] = blockFactory.newConstantLongBlockWith(negInfCount, 1);
            blocks[offset + 3] = blockFactory.newConstantLongBlockWith(posInfCount, 1);
        }

        Block evaluatePercentile(DriverContext driverContext) {
            BlockFactory blockFactory = driverContext.blockFactory();
            if (percentile == null || digest.size() + nanCount + negInfCount + posInfCount == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            double result = quantile(percentile / 100, digest, nanCount, negInfCount, posInfCount);
            return blockFactory.newConstantDoubleBlockWith(result, 1);
        }
    }

    static class GroupingState implements GroupingAggregatorState {
        private final BigArrays bigArrays;
        private final CircuitBreaker breaker;
        private final Double percentile;
        private final double tDigestStateCompression;
        private ObjectArray<TDigestState> digests;
        private LongArray nanCounts;
        private LongArray negInfCounts;
        private LongArray posInfCounts;

        GroupingState(CircuitBreaker breaker, BigArrays bigArrays, double percentile, double tDigestStateCompression) {
            this.breaker = breaker;
            this.bigArrays = bigArrays;
            this.digests = bigArrays.newObjectArray(1);
            this.nanCounts = bigArrays.newLongArray(1, true);
            this.negInfCounts = bigArrays.newLongArray(1, true);
            this.posInfCounts = bigArrays.newLongArray(1, true);
            this.percentile = 0 <= percentile && percentile <= 100 ? percentile : null;
            this.tDigestStateCompression = tDigestStateCompression;
        }

        private TDigestState getOrAddGroup(int groupId) {
            digests = bigArrays.grow(digests, groupId + 1);
            nanCounts = bigArrays.grow(nanCounts, groupId + 1);
            negInfCounts = bigArrays.grow(negInfCounts, groupId + 1);
            posInfCounts = bigArrays.grow(posInfCounts, groupId + 1);
            TDigestState digest = digests.get(groupId);
            if (digest == null) {
                digest = TDigestState.create(breaker, tDigestStateCompression);
                digests.set(groupId, digest);
            }
            return digest;
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // Groups without observations are rendered as null by toIntermediate and evaluatePercentile.
        }

        void add(int groupId, double v) {
            TDigestState digest = getOrAddGroup(groupId);
            if (Double.isNaN(v)) {
                nanCounts.increment(groupId, 1);
            } else if (v == Double.POSITIVE_INFINITY) {
                posInfCounts.increment(groupId, 1);
            } else if (v == Double.NEGATIVE_INFINITY) {
                negInfCounts.increment(groupId, 1);
            } else {
                digest.add(v);
            }
        }

        void add(int groupId, BytesRef other, long otherNanCount, long otherNegInfCount, long otherPosInfCount) {
            TDigestState digest = getOrAddGroup(groupId);
            try (var otherDigest = QuantileStates.deserializeDigest(breaker, other)) {
                digest.add(otherDigest);
            }
            nanCounts.increment(groupId, otherNanCount);
            negInfCounts.increment(groupId, otherNegInfCount);
            posInfCounts.increment(groupId, otherPosInfCount);
        }

        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset + 4;
            BlockFactory blockFactory = driverContext.blockFactory();
            int positions = selected.getPositionCount();
            try (
                var digestBuilder = blockFactory.newBytesRefBlockBuilder(positions);
                LongBlock.Builder nanBuilder = blockFactory.newLongBlockBuilder(positions);
                LongBlock.Builder negInfBuilder = blockFactory.newLongBlockBuilder(positions);
                LongBlock.Builder posInfBuilder = blockFactory.newLongBlockBuilder(positions);
            ) {
                for (int i = 0; i < positions; i++) {
                    int group = selected.getInt(i);
                    TDigestState digest = group < digests.size() ? digests.get(group) : null;
                    if (digest == null) {
                        try (TDigestState empty = TDigestState.create(breaker, tDigestStateCompression)) {
                            digestBuilder.appendBytesRef(QuantileStates.serializeDigest(empty));
                        }
                        nanBuilder.appendLong(0);
                        negInfBuilder.appendLong(0);
                        posInfBuilder.appendLong(0);
                    } else {
                        digestBuilder.appendBytesRef(QuantileStates.serializeDigest(digest));
                        nanBuilder.appendLong(nanCounts.get(group));
                        negInfBuilder.appendLong(negInfCounts.get(group));
                        posInfBuilder.appendLong(posInfCounts.get(group));
                    }
                }
                blocks[offset + 0] = digestBuilder.build();
                blocks[offset + 1] = nanBuilder.build();
                blocks[offset + 2] = negInfBuilder.build();
                blocks[offset + 3] = posInfBuilder.build();
            }
        }

        Block evaluatePercentile(IntVector selected, DriverContext driverContext) {
            try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int group = selected.getInt(i);
                    TDigestState digest = group < digests.size() ? digests.get(group) : null;
                    if (percentile == null || digest == null) {
                        builder.appendNull();
                        continue;
                    }
                    long nanCount = nanCounts.get(group);
                    long negInfCount = negInfCounts.get(group);
                    long posInfCount = posInfCounts.get(group);
                    if (digest.size() + nanCount + negInfCount + posInfCount == 0) {
                        builder.appendNull();
                    } else {
                        builder.appendDouble(quantile(percentile / 100, digest, nanCount, negInfCount, posInfCount));
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
                nanCounts,
                negInfCounts,
                posInfCounts
            );
        }
    }
}
