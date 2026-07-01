/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.elasticsearch.simdvec.QuantizedByteVectorValuesAccess;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public abstract sealed class Int7SQVectorScorerSupplier implements RandomVectorScorerSupplier, QuantizedByteVectorValuesAccess {

    final int dims;
    final int maxOrd;
    final float scoreCorrectionConstant;
    final IndexInput input;
    final QuantizedByteVectorValues values; // to support ordToDoc/getAcceptOrds
    final int vectorDataBytes;
    final int vectorTotalBytes;
    final FixedSizeScratch firstScratch;
    final FixedSizeScratch secondScratch;
    final AddressesScratch addrsScratch = new AddressesScratch();
    final OffsetsScratch offsetsScratch = new OffsetsScratch();

    protected Int7SQVectorScorerSupplier(IndexInput input, QuantizedByteVectorValues values, float scoreCorrectionConstant) {
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.maxOrd = values.size();
        this.scoreCorrectionConstant = scoreCorrectionConstant;
        this.vectorDataBytes = dims;
        this.vectorTotalBytes = vectorDataBytes + Float.BYTES;
        this.firstScratch = new FixedSizeScratch(vectorTotalBytes);
        this.secondScratch = new FixedSizeScratch(vectorTotalBytes);
    }

    protected final void checkOrdinal(int ord) {
        if (ord < 0 || ord >= maxOrd) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    final float bulkScoreFromOrds(int firstOrd, int[] ordinals, float[] scores, int numNodes) throws IOException {
        if (numNodes == 0) {
            return Float.NEGATIVE_INFINITY;
        }

        long queryByteOffset = (long) firstOrd * vectorTotalBytes;
        input.seek(queryByteOffset);
        return IndexInputUtils.withSlice(input, vectorTotalBytes, firstScratch::getScratch, query -> {
            float queryOffsetValue = query.get(ValueLayout.JAVA_FLOAT_UNALIGNED, dims);

            long[] offsets = offsetsScratch.get(numNodes);
            for (int i = 0; i < numNodes; i++) {
                offsets[i] = (long) ordinals[i] * vectorTotalBytes;
            }

            float[] maxScore = new float[] { Float.NEGATIVE_INFINITY };
            boolean resolved = IndexInputUtils.withSliceAddresses(
                input,
                offsets,
                vectorTotalBytes,
                numNodes,
                addrsScratch::get,
                addrs -> maxScore[0] = bulkScoreFromSegment(addrs, query, queryOffsetValue, MemorySegment.ofArray(scores), numNodes)
            );
            if (resolved == false) {
                // fallback to per-vector scorer
                for (int i = 0; i < numNodes; i++) {
                    input.seek(offsets[i]);
                    scores[i] = IndexInputUtils.withSlice(input, vectorTotalBytes, secondScratch::getScratch, vector -> {
                        float vectorOffsetValue = vector.get(ValueLayout.JAVA_FLOAT_UNALIGNED, dims);
                        var score = scoreFromSegments(query, queryOffsetValue, vector, vectorOffsetValue);
                        maxScore[0] = Math.max(maxScore[0], score);
                        return score;
                    });
                }
            }
            return maxScore[0];
        });
    }

    final float scoreFromOrds(int firstOrd, int secondOrd) throws IOException {
        long firstByteOffset = (long) firstOrd * vectorTotalBytes;
        long secondByteOffset = (long) secondOrd * vectorTotalBytes;

        input.seek(firstByteOffset);
        return IndexInputUtils.withSlice(input, vectorTotalBytes, firstScratch::getScratch, firstSeg -> {
            float firstOffsetValue = firstSeg.get(ValueLayout.JAVA_FLOAT_UNALIGNED, dims);
            input.seek(secondByteOffset);
            return IndexInputUtils.withSlice(input, vectorTotalBytes, secondScratch::getScratch, secondSeg -> {
                float secondOffsetValue = secondSeg.get(ValueLayout.JAVA_FLOAT_UNALIGNED, dims);
                return scoreFromSegments(firstSeg, firstOffsetValue, secondSeg, secondOffsetValue);
            });
        });
    }

    protected abstract float scoreFromSegments(MemorySegment a, float aOffset, MemorySegment b, float bOffset);

    protected abstract float bulkScoreFromSegment(
        MemorySegment addresses,
        MemorySegment query,
        float queryOffsetValue,
        MemorySegment scores,
        int numNodes
    );

    @Override
    public UpdateableRandomVectorScorer scorer() {
        return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
            private int ord = -1;

            @Override
            public float score(int node) throws IOException {
                checkOrdinal(node);
                return scoreFromOrds(ord, node);
            }

            @Override
            public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
                return bulkScoreFromOrds(ord, nodes, scores, numNodes);
            }

            @Override
            public void setScoringOrdinal(int node) {
                checkOrdinal(node);
                this.ord = node;
            }
        };
    }

    @Override
    public QuantizedByteVectorValues get() {
        return values;
    }

    public static final class EuclideanSupplier extends Int7SQVectorScorerSupplier {

        public EuclideanSupplier(IndexInput input, QuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant);
        }

        @Override
        protected float scoreFromSegments(MemorySegment a, float aOffset, MemorySegment b, float bOffset) {
            int squareDistance = Similarities.squareDistanceI7u(a, b, dims);
            float adjustedDistance = squareDistance * scoreCorrectionConstant;
            return 1 / (1f + adjustedDistance);
        }

        @Override
        protected float bulkScoreFromSegment(
            MemorySegment addresses,
            MemorySegment query,
            float queryOffsetValue,
            MemorySegment scores,
            int numNodes
        ) {
            Similarities.squareDistanceI7uBulkSparse(addresses, query, dims, numNodes, scores);

            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; ++i) {
                var squareDistance = scores.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                float adjustedDistance = squareDistance * scoreCorrectionConstant;
                float adjustedScore = 1 / (1f + adjustedDistance);
                scores.setAtIndex(ValueLayout.JAVA_FLOAT, i, adjustedScore);
                max = Math.max(max, adjustedScore);
            }
            return max;
        }

        @Override
        public EuclideanSupplier copy() {
            return new EuclideanSupplier(input.clone(), values, scoreCorrectionConstant);
        }
    }

    public static final class DotProductSupplier extends Int7SQVectorScorerSupplier {

        public DotProductSupplier(IndexInput input, QuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant);
        }

        @Override
        protected float scoreFromSegments(MemorySegment a, float aOffset, MemorySegment b, float bOffset) {
            int dotProduct = Similarities.dotProductI7u(a, b, dims);
            assert dotProduct >= 0;
            float adjustedDistance = dotProduct * scoreCorrectionConstant + aOffset + bOffset;
            return Math.max((1 + adjustedDistance) / 2, 0f);
        }

        @Override
        protected float bulkScoreFromSegment(
            MemorySegment addresses,
            MemorySegment query,
            float queryOffsetValue,
            MemorySegment scores,
            int numNodes
        ) {
            Similarities.dotProductI7uBulkSparse(addresses, query, dims, numNodes, scores);

            // Java-side adjustment
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; ++i) {
                var dotProduct = scores.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                MemorySegment vectorAddress = addresses.getAtIndex(ValueLayout.ADDRESS, i).reinterpret(vectorTotalBytes);
                var vectorOffsetValue = vectorAddress.get(ValueLayout.JAVA_FLOAT_UNALIGNED, dims);
                float adjustedDistance = dotProduct * scoreCorrectionConstant + queryOffsetValue + vectorOffsetValue;
                float adjustedScore = Math.max((1 + adjustedDistance) / 2, 0f);
                scores.setAtIndex(ValueLayout.JAVA_FLOAT, i, adjustedScore);
                max = Math.max(max, adjustedScore);
            }
            return max;
        }

        @Override
        public DotProductSupplier copy() {
            return new DotProductSupplier(input.clone(), values, scoreCorrectionConstant);
        }
    }

    public static final class MaxInnerProductSupplier extends Int7SQVectorScorerSupplier {

        public MaxInnerProductSupplier(IndexInput input, QuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant);
        }

        @Override
        protected float scoreFromSegments(MemorySegment a, float aOffset, MemorySegment b, float bOffset) {
            int dotProduct = Similarities.dotProductI7u(a, b, dims);
            assert dotProduct >= 0;
            float adjustedDistance = dotProduct * scoreCorrectionConstant + aOffset + bOffset;
            if (adjustedDistance < 0) {
                return 1 / (1 + -1 * adjustedDistance);
            }
            return adjustedDistance + 1;
        }

        @Override
        protected float bulkScoreFromSegment(
            MemorySegment addresses,
            MemorySegment query,
            float queryOffsetValue,
            MemorySegment scores,
            int numNodes
        ) {
            Similarities.dotProductI7uBulkSparse(addresses, query, dims, numNodes, scores);

            // Java-side adjustment
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; ++i) {
                var dotProduct = scores.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                MemorySegment vectorAddress = addresses.getAtIndex(ValueLayout.ADDRESS, i).reinterpret(vectorTotalBytes);
                var vectorOffsetValue = vectorAddress.get(ValueLayout.JAVA_FLOAT_UNALIGNED, dims);
                float adjustedDistance = dotProduct * scoreCorrectionConstant + queryOffsetValue + vectorOffsetValue;
                adjustedDistance = adjustedDistance < 0 ? 1 / (1 + -1 * adjustedDistance) : adjustedDistance + 1;
                scores.setAtIndex(ValueLayout.JAVA_FLOAT, i, adjustedDistance);
                max = Math.max(max, adjustedDistance);
            }
            return max;
        }

        @Override
        public MaxInnerProductSupplier copy() {
            return new MaxInnerProductSupplier(input.clone(), values, scoreCorrectionConstant);
        }
    }
}
