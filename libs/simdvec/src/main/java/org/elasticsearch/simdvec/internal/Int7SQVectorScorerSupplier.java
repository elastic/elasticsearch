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
import org.apache.lucene.util.quantization.LegacyQuantizedByteVectorValues;
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.lucene.store.SegmentBufferScratch;
import org.elasticsearch.simdvec.QuantizedByteVectorValuesAccess;
import org.elasticsearch.simdvec.SimdVecLibrary;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public abstract sealed class Int7SQVectorScorerSupplier implements RandomVectorScorerSupplier, QuantizedByteVectorValuesAccess {

    private static final SimdVecLibrary DISTANCE_FUNCS = SimdVecLibrary.instance().orElseThrow(AssertionError::new);

    final int dims;
    final float scoreCorrectionConstant;
    final IndexInput input;
    final LegacyQuantizedByteVectorValues values; // to support ordToDoc/getAcceptOrds
    final int vectorDataBytes;
    final int vectorTotalBytes;
    final FixedSizeSegmentScratch queryScratch;
    final FixedSizeSegmentScratch documentScratch;
    final AddressesScratch addrsScratch = new AddressesScratch();
    final OffsetsScratch offsetsScratch = new OffsetsScratch();
    final SegmentBufferScratch scoresScratch = new SegmentBufferScratch();
    final float[] maxScore = new float[] { Float.NEGATIVE_INFINITY };

    protected Int7SQVectorScorerSupplier(IndexInput input, LegacyQuantizedByteVectorValues values, float scoreCorrectionConstant) {
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.scoreCorrectionConstant = scoreCorrectionConstant;
        this.vectorDataBytes = dims;
        this.vectorTotalBytes = vectorDataBytes + Float.BYTES;
        this.queryScratch = new FixedSizeSegmentScratch(vectorTotalBytes);
        this.documentScratch = new FixedSizeSegmentScratch(vectorTotalBytes);
    }

    protected final void checkOrdinal(int ord) {
        if (ord < 0 || ord >= values.size()) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    final float bulkScoreFromOrds(int queryOrd, int[] ordinals, float[] scores, int numNodes) throws IOException {
        if (numNodes == 0) {
            return Float.NEGATIVE_INFINITY;
        }

        long queryByteOffset = (long) queryOrd * vectorTotalBytes;
        input.seek(queryByteOffset);
        return IndexInputUtils.withFloatSlice(input, vectorTotalBytes, queryScratch, query -> {
            float queryOffsetValue = query.get(ValueLayout.JAVA_FLOAT_UNALIGNED, dims);

            long[] offsets = offsetsScratch.get(numNodes);
            for (int i = 0; i < numNodes; i++) {
                offsets[i] = (long) ordinals[i] * vectorTotalBytes;
            }

            maxScore[0] = Float.NEGATIVE_INFINITY;
            boolean resolved = IndexInputUtils.withSliceAddresses(
                input,
                offsets,
                vectorTotalBytes,
                numNodes,
                addrsScratch,
                addrs -> maxScore[0] = bulkScoreFromSegment(addrs, query, queryOffsetValue, scores, numNodes)
            );
            if (resolved == false) {
                maxScore[0] = scorePerVectorFallback(scores, numNodes, query, offsets, queryOffsetValue);
            }
            return maxScore[0];
        });
    }

    private float scorePerVectorFallback(float[] scores, int numNodes, MemorySegment query, long[] offsets, float queryOffsetValue)
        throws IOException {
        float maxScore = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < numNodes; i++) {
            final int idx = i;
            input.seek(offsets[idx]);
            IndexInputUtils.withVoidSlice(input, vectorTotalBytes, documentScratch, vector -> {
                float vectorOffsetValue = vector.get(ValueLayout.JAVA_FLOAT_UNALIGNED, dims);
                scores[idx] = scoreFromSegments(vector, vectorOffsetValue, query, queryOffsetValue);
            });
            maxScore = Math.max(maxScore, scores[idx]);
        }
        return maxScore;
    }

    final float scoreFromOrds(int queryOrd, int documentOrd) throws IOException {
        long queryByteOffset = (long) queryOrd * vectorTotalBytes;
        long documentByteOffset = (long) documentOrd * vectorTotalBytes;

        input.seek(queryByteOffset);
        return IndexInputUtils.withFloatSlice(input, vectorTotalBytes, queryScratch, querySeg -> {
            float queryOffsetValue = querySeg.get(ValueLayout.JAVA_FLOAT_UNALIGNED, dims);
            input.seek(documentByteOffset);
            return IndexInputUtils.withFloatSlice(input, vectorTotalBytes, documentScratch, documentSeg -> {
                float documentOffsetValue = documentSeg.get(ValueLayout.JAVA_FLOAT_UNALIGNED, dims);
                return scoreFromSegments(documentSeg, documentOffsetValue, querySeg, queryOffsetValue);
            });
        });
    }

    protected abstract float scoreFromSegments(MemorySegment document, float documentOffset, MemorySegment query, float queryOffset);

    protected abstract float bulkScoreFromSegment(
        MemorySegment addresses,
        MemorySegment query,
        float queryOffsetValue,
        float[] scores,
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
    public LegacyQuantizedByteVectorValues get() {
        return values;
    }

    public static final class EuclideanSupplier extends Int7SQVectorScorerSupplier {

        public EuclideanSupplier(IndexInput input, LegacyQuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant);
        }

        @Override
        protected float scoreFromSegments(MemorySegment document, float documentOffset, MemorySegment query, float queryOffset) {
            int squareDistance = DISTANCE_FUNCS.squareDistanceI7u(document, query, dims);
            float adjustedDistance = squareDistance * scoreCorrectionConstant;
            return 1 / (1f + adjustedDistance);
        }

        @Override
        protected float bulkScoreFromSegment(
            MemorySegment addresses,
            MemorySegment query,
            float queryOffsetValue,
            float[] scores,
            int numNodes
        ) {
            MemorySegment segment = scoresScratch.apply(numNodes * Float.BYTES);
            DISTANCE_FUNCS.squareDistanceI7uBulkSparse(addresses, query, dims, numNodes, segment);

            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; ++i) {
                var squareDistance = segment.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                float adjustedDistance = squareDistance * scoreCorrectionConstant;
                float adjustedScore = 1 / (1f + adjustedDistance);
                scores[i] = adjustedScore;
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

        public DotProductSupplier(IndexInput input, LegacyQuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant);
        }

        @Override
        protected float scoreFromSegments(MemorySegment document, float documentOffset, MemorySegment query, float queryOffset) {
            int dotProduct = DISTANCE_FUNCS.dotProductI7u(document, query, dims);
            assert dotProduct >= 0;
            float adjustedDistance = dotProduct * scoreCorrectionConstant + queryOffset + documentOffset;
            return Math.max((1 + adjustedDistance) / 2, 0f);
        }

        @Override
        protected float bulkScoreFromSegment(
            MemorySegment addresses,
            MemorySegment query,
            float queryOffsetValue,
            float[] scores,
            int numNodes
        ) {
            MemorySegment segment = scoresScratch.apply(numNodes * Float.BYTES);
            DISTANCE_FUNCS.dotProductI7uBulkSparse(addresses, query, dims, numNodes, segment);

            // Java-side adjustment
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; ++i) {
                var dotProduct = segment.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                MemorySegment vectorAddress = addresses.getAtIndex(ValueLayout.ADDRESS, i).reinterpret(vectorTotalBytes);
                var vectorOffsetValue = vectorAddress.get(ValueLayout.JAVA_FLOAT_UNALIGNED, dims);
                float adjustedDistance = dotProduct * scoreCorrectionConstant + queryOffsetValue + vectorOffsetValue;
                float adjustedScore = Math.max((1 + adjustedDistance) / 2, 0f);
                scores[i] = adjustedScore;
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

        public MaxInnerProductSupplier(IndexInput input, LegacyQuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant);
        }

        @Override
        protected float scoreFromSegments(MemorySegment document, float documentOffset, MemorySegment query, float queryOffset) {
            int dotProduct = DISTANCE_FUNCS.dotProductI7u(document, query, dims);
            assert dotProduct >= 0;
            float adjustedDistance = dotProduct * scoreCorrectionConstant + queryOffset + documentOffset;
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
            float[] scores,
            int numNodes
        ) {
            MemorySegment segment = scoresScratch.apply(numNodes * Float.BYTES);
            DISTANCE_FUNCS.dotProductI7uBulkSparse(addresses, query, dims, numNodes, segment);

            // Java-side adjustment
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; ++i) {
                var dotProduct = segment.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                MemorySegment vectorAddress = addresses.getAtIndex(ValueLayout.ADDRESS, i).reinterpret(vectorTotalBytes);
                var vectorOffsetValue = vectorAddress.get(ValueLayout.JAVA_FLOAT_UNALIGNED, dims);
                float adjustedDistance = dotProduct * scoreCorrectionConstant + queryOffsetValue + vectorOffsetValue;
                adjustedDistance = adjustedDistance < 0 ? 1 / (1 + -1 * adjustedDistance) : adjustedDistance + 1;
                scores[i] = adjustedDistance;
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
