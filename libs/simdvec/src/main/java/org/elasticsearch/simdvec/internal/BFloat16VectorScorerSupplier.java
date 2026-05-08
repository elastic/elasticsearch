/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public abstract class BFloat16VectorScorerSupplier implements RandomVectorScorerSupplier {

    final int dims;
    final int vectorByteSize;
    final int maxOrd;
    final IndexInput input;
    final FloatVectorValues values;
    final FixedSizeScratch firstScratch;
    final FixedSizeScratch secondScratch;
    final AddressesScratch addrsScratch = new AddressesScratch();
    final OffsetsScratch offsetsScratch = new OffsetsScratch();

    protected BFloat16VectorScorerSupplier(IndexInput input, FloatVectorValues values) {
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.vectorByteSize = values.getVectorByteLength();
        this.maxOrd = values.size();
        this.firstScratch = new FixedSizeScratch(vectorByteSize);
        this.secondScratch = new FixedSizeScratch(vectorByteSize);
    }

    protected final void checkOrdinal(int ord) {
        if (ord < 0 || ord > maxOrd) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    final float bulkScoreFromOrds(int firstOrd, int[] ordinals, float[] scores, int numNodes) throws IOException {
        if (numNodes == 0) {
            return Float.NEGATIVE_INFINITY;
        }

        long queryByteOffset = (long) firstOrd * vectorByteSize;
        input.seek(queryByteOffset);
        return IndexInputUtils.withSlice(input, vectorByteSize, firstScratch::getScratch, query -> {
            long[] offsets = offsetsScratch.get(numNodes);
            for (int i = 0; i < numNodes; i++) {
                offsets[i] = (long) ordinals[i] * vectorByteSize;
            }

            float[] maxScore = new float[] { Float.NEGATIVE_INFINITY };
            boolean resolved = IndexInputUtils.withSliceAddresses(
                input,
                offsets,
                vectorByteSize,
                numNodes,
                addrsScratch::get,
                addrs -> maxScore[0] = bulkScoreFromSegment(addrs, query, MemorySegment.ofArray(scores), numNodes)
            );
            if (resolved == false) {
                // fallback to per-vector scorer
                for (int i = 0; i < numNodes; i++) {
                    input.seek(offsets[i]);
                    scores[i] = IndexInputUtils.withSlice(input, vectorByteSize, secondScratch::getScratch, vector -> {
                        var score = scoreFromSegments(query, vector);
                        maxScore[0] = Math.max(maxScore[0], score);
                        return score;
                    });
                }
            }
            return maxScore[0];
        });
    }

    final float scoreFromOrds(int firstOrd, int secondOrd) throws IOException {
        long firstByteOffset = (long) firstOrd * vectorByteSize;
        long secondByteOffset = (long) secondOrd * vectorByteSize;

        input.seek(firstByteOffset);
        return IndexInputUtils.withSlice(input, vectorByteSize, firstScratch::getScratch, firstSeg -> {
            input.seek(secondByteOffset);
            return IndexInputUtils.withSlice(
                input,
                vectorByteSize,
                secondScratch::getScratch,
                secondSeg -> scoreFromSegments(firstSeg, secondSeg)
            );
        });
    }

    abstract float scoreFromSegments(MemorySegment a, MemorySegment b);

    abstract float bulkScoreFromSegment(MemorySegment addresses, MemorySegment query, MemorySegment scores, int numNodes);

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

    public static final class EuclideanSupplier extends BFloat16VectorScorerSupplier {

        public EuclideanSupplier(IndexInput input, FloatVectorValues values) {
            super(input, values);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return VectorUtil.normalizeDistanceToUnitInterval(Similarities.squareDistanceDBF16QBF16(a, b, dims));
        }

        @Override
        protected float bulkScoreFromSegment(MemorySegment addresses, MemorySegment query, MemorySegment scores, int numNodes) {
            Similarities.squareDistanceDBF16QBF16BulkSparse(addresses, query, dims, numNodes, scores);

            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; ++i) {
                float squareDistance = scores.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                float normalizedScore = VectorUtil.normalizeDistanceToUnitInterval(squareDistance);
                scores.setAtIndex(ValueLayout.JAVA_FLOAT, i, normalizedScore);
                max = Math.max(max, normalizedScore);
            }
            return max;
        }

        @Override
        public EuclideanSupplier copy() {
            return new EuclideanSupplier(input.clone(), values);
        }
    }

    public static final class DotProductSupplier extends BFloat16VectorScorerSupplier {

        public DotProductSupplier(IndexInput input, FloatVectorValues values) {
            super(input, values);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return VectorUtil.normalizeToUnitInterval(Similarities.dotProductDBF16QBF16(a, b, dims));
        }

        @Override
        protected float bulkScoreFromSegment(MemorySegment addresses, MemorySegment query, MemorySegment scores, int numNodes) {
            Similarities.dotProductDBF16QBF16BulkSparse(addresses, query, dims, numNodes, scores);

            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; ++i) {
                float dotProduct = scores.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                float normalizedScore = VectorUtil.normalizeToUnitInterval(dotProduct);
                scores.setAtIndex(ValueLayout.JAVA_FLOAT, i, normalizedScore);
                max = Math.max(max, normalizedScore);
            }
            return max;
        }

        @Override
        public DotProductSupplier copy() {
            return new DotProductSupplier(input.clone(), values);
        }
    }

    public static final class MaxInnerProductSupplier extends BFloat16VectorScorerSupplier {

        public MaxInnerProductSupplier(IndexInput input, FloatVectorValues values) {
            super(input, values);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return VectorUtil.scaleMaxInnerProductScore(Similarities.dotProductDBF16QBF16(a, b, dims));
        }

        @Override
        protected float bulkScoreFromSegment(MemorySegment addresses, MemorySegment query, MemorySegment scores, int numNodes) {
            Similarities.dotProductDBF16QBF16BulkSparse(addresses, query, dims, numNodes, scores);

            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; ++i) {
                float dotProduct = scores.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                float scaledScore = VectorUtil.scaleMaxInnerProductScore(dotProduct);
                scores.setAtIndex(ValueLayout.JAVA_FLOAT, i, scaledScore);
                max = Math.max(max, scaledScore);
            }
            return max;
        }

        @Override
        public MaxInnerProductSupplier copy() {
            return new MaxInnerProductSupplier(input.clone(), values);
        }
    }
}
