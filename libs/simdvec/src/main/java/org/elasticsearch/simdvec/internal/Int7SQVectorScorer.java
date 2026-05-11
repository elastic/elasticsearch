/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.codecs.hnsw.ScalarQuantizedVectorScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.elasticsearch.simdvec.MemorySegmentAccessInputAccess;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

import static org.elasticsearch.simdvec.internal.Similarities.dotProductI7u;
import static org.elasticsearch.simdvec.internal.Similarities.squareDistanceI7u;
import static org.elasticsearch.simdvec.internal.vectorization.JdkFeatures.SUPPORTS_HEAP_SEGMENTS;

public abstract sealed class Int7SQVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {

    final int vectorByteSize;
    final int vectorPitch;
    final IndexInput input;
    final MemorySegment query;
    final float scoreCorrectionConstant;
    final float queryCorrection;
    final FixedSizeScratch scratch;
    final AddressesScratch addrsScratch = new AddressesScratch();
    final OffsetsScratch offsetsScratch = new OffsetsScratch();

    /** Return an optional whose value, if present, is the scorer. Otherwise, an empty optional is returned. */
    public static Optional<RandomVectorScorer> create(VectorSimilarityFunction sim, QuantizedByteVectorValues values, float[] queryVector) {
        if (SUPPORTS_HEAP_SEGMENTS == false) {
            return Optional.empty();
        }
        checkDimensions(queryVector.length, values.dimension());
        var input = values.getSlice();
        if (input == null) {
            return Optional.empty();
        }
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        IndexInputUtils.checkInputType(input);
        checkInvariants(values.size(), values.dimension(), input);

        ScalarQuantizer scalarQuantizer = values.getScalarQuantizer();
        byte[] quantizedQuery = new byte[queryVector.length];
        float queryCorrection = ScalarQuantizedVectorScorer.quantizeQuery(queryVector, quantizedQuery, sim, scalarQuantizer);
        return switch (sim) {
            case COSINE, DOT_PRODUCT -> Optional.of(new DotProductScorer(input, values, quantizedQuery, queryCorrection));
            case EUCLIDEAN -> Optional.of(new EuclideanScorer(input, values, quantizedQuery, queryCorrection));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductScorer(input, values, quantizedQuery, queryCorrection));
        };
    }

    Int7SQVectorScorer(IndexInput input, QuantizedByteVectorValues values, byte[] queryVector, float queryCorrection) {
        super(values);
        this.input = input;
        assert queryVector.length == values.getVectorByteLength();
        this.vectorByteSize = values.getVectorByteLength();
        this.vectorPitch = vectorByteSize + Float.BYTES;
        this.query = MemorySegment.ofArray(queryVector);
        this.queryCorrection = queryCorrection;
        this.scoreCorrectionConstant = values.getScalarQuantizer().getConstantMultiplier();
        this.scratch = new FixedSizeScratch(vectorByteSize);
    }

    static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
        if (input.length() < (long) vectorByteLength * maxOrd) {
            throw new IllegalArgumentException("input length is less than expected vector data");
        }
    }

    final void checkOrdinal(int ord) {
        if (ord < 0 || ord >= maxOrd()) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    /**
     * Resolves native memory addresses for the given node ordinals and calls
     * the sparse scoring function. Returns true if addresses were resolved
     * (via mmap or DirectAccessInput), false if fallback scoring is needed.
     */
    final boolean bulkScoreWithSparse(int[] nodes, float[] scores, int numNodes, SparseScorer sparseScorer) throws IOException {
        if (numNodes == 0) {
            return false;
        }
        long[] offsets = offsetsScratch.get(numNodes);
        for (int i = 0; i < numNodes; i++) {
            offsets[i] = (long) nodes[i] * vectorPitch;
        }
        return IndexInputUtils.withSliceAddresses(
            input,
            offsets,
            vectorByteSize,
            numNodes,
            addrsScratch::get,
            a -> sparseScorer.score(a, query, vectorByteSize, numNodes, MemorySegment.ofArray(scores))
        );
    }

    public static final class DotProductScorer extends Int7SQVectorScorer {
        public DotProductScorer(IndexInput in, QuantizedByteVectorValues values, byte[] query, float correction) {
            super(in, values, query, correction);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            long byteOffset = (long) node * vectorPitch;
            input.seek(byteOffset);
            int dotProduct = IndexInputUtils.withSlice(
                input,
                vectorByteSize,
                scratch::getScratch,
                seg -> dotProductI7u(query, seg, vectorByteSize)
            );
            assert dotProduct >= 0;
            float nodeCorrection = Float.intBitsToFloat(input.readInt());
            float adjustedDistance = dotProduct * scoreCorrectionConstant + queryCorrection + nodeCorrection;
            return VectorUtil.normalizeToUnitInterval(adjustedDistance);
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (bulkScoreWithSparse(nodes, scores, numNodes, Similarities::dotProductI7uBulkSparse)) {
                float max = Float.NEGATIVE_INFINITY;
                for (int i = 0; i < numNodes; ++i) {
                    var dotProduct = scores[i];
                    long secondByteOffset = (long) nodes[i] * vectorPitch;
                    input.seek(secondByteOffset + vectorByteSize);
                    var nodeCorrection = Float.intBitsToFloat(input.readInt());
                    float adjustedDistance = dotProduct * scoreCorrectionConstant + queryCorrection + nodeCorrection;
                    scores[i] = VectorUtil.normalizeToUnitInterval(adjustedDistance);
                    max = Math.max(max, scores[i]);
                }
                return max;
            } else {
                return super.bulkScore(nodes, scores, numNodes);
            }
        }
    }

    public static final class EuclideanScorer extends Int7SQVectorScorer {
        public EuclideanScorer(IndexInput in, QuantizedByteVectorValues values, byte[] query, float correction) {
            super(in, values, query, correction);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            long byteOffset = (long) node * vectorPitch;
            input.seek(byteOffset);
            int sqDist = IndexInputUtils.withSlice(
                input,
                vectorByteSize,
                scratch::getScratch,
                seg -> squareDistanceI7u(query, seg, vectorByteSize)
            );
            float adjustedDistance = sqDist * scoreCorrectionConstant;
            return VectorUtil.normalizeDistanceToUnitInterval(adjustedDistance);
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (bulkScoreWithSparse(nodes, scores, numNodes, Similarities::squareDistanceI7uBulkSparse)) {
                float max = Float.NEGATIVE_INFINITY;
                for (int i = 0; i < numNodes; ++i) {
                    var squareDistance = scores[i];
                    float adjustedDistance = squareDistance * scoreCorrectionConstant;
                    scores[i] = VectorUtil.normalizeDistanceToUnitInterval(adjustedDistance);
                    max = Math.max(max, scores[i]);
                }
                return max;
            } else {
                return super.bulkScore(nodes, scores, numNodes);
            }
        }
    }

    public static final class MaxInnerProductScorer extends Int7SQVectorScorer {
        public MaxInnerProductScorer(IndexInput in, QuantizedByteVectorValues values, byte[] query, float corr) {
            super(in, values, query, corr);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            long byteOffset = (long) node * vectorPitch;
            input.seek(byteOffset);
            int dotProduct = IndexInputUtils.withSlice(
                input,
                vectorByteSize,
                scratch::getScratch,
                seg -> dotProductI7u(query, seg, vectorByteSize)
            );
            assert dotProduct >= 0;
            float nodeCorrection = Float.intBitsToFloat(input.readInt());
            float adjustedDistance = dotProduct * scoreCorrectionConstant + queryCorrection + nodeCorrection;
            return VectorUtil.scaleMaxInnerProductScore(adjustedDistance);
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (bulkScoreWithSparse(nodes, scores, numNodes, Similarities::dotProductI7uBulkSparse)) {
                float max = Float.NEGATIVE_INFINITY;
                for (int i = 0; i < numNodes; ++i) {
                    var dotProduct = scores[i];
                    long secondByteOffset = (long) nodes[i] * vectorPitch;
                    input.seek(secondByteOffset + vectorByteSize);
                    var nodeCorrection = Float.intBitsToFloat(input.readInt());
                    float adjustedDistance = dotProduct * scoreCorrectionConstant + queryCorrection + nodeCorrection;
                    scores[i] = VectorUtil.scaleMaxInnerProductScore(adjustedDistance);
                    max = Math.max(max, scores[i]);
                }
                return max;
            } else {
                return super.bulkScore(nodes, scores, numNodes);
            }
        }
    }

    static void checkDimensions(int queryLen, int fieldLen) {
        if (queryLen != fieldLen) {
            throw new IllegalArgumentException("vector query dimension: " + queryLen + " differs from field dimension: " + fieldLen);
        }
    }
}
