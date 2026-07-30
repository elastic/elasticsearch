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
import org.apache.lucene.util.quantization.LegacyQuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.simdvec.IndexInputUtils;
import org.elasticsearch.simdvec.MemorySegmentAccessInputAccess;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Optional;

public abstract sealed class Int7SQVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {

    private static final VectorSimilarityFunctions DISTANCE_FUNCS = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow(AssertionError::new);

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
    public static Optional<RandomVectorScorer> create(
        VectorSimilarityFunction sim,
        LegacyQuantizedByteVectorValues values,
        float[] queryVector
    ) {
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

    Int7SQVectorScorer(IndexInput input, LegacyQuantizedByteVectorValues values, byte[] queryVector, float queryCorrection) {
        super(values);
        this.input = input;
        assert queryVector.length == values.getVectorByteLength();
        this.vectorByteSize = values.getVectorByteLength();
        this.vectorPitch = vectorByteSize + Float.BYTES;
        this.query = MemorySegment.ofArray(queryVector);
        this.queryCorrection = queryCorrection;
        this.scoreCorrectionConstant = values.getScalarQuantizer().getConstantMultiplier();
        this.scratch = new FixedSizeScratch(vectorPitch);
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

    long[] getOffsets(int[] nodes, int numNodes) {
        long[] offsets = offsetsScratch.get(numNodes);
        for (int i = 0; i < numNodes; i++) {
            offsets[i] = (long) nodes[i] * vectorPitch;
        }
        return offsets;
    }

    float getNodeCorrection(MemorySegment addrs, long i) {
        long addr = addrs.get(ValueLayout.JAVA_LONG, i * Long.BYTES);
        MemorySegment nodeSeg = MemorySegment.ofAddress(addr).reinterpret(vectorPitch);
        return Float.intBitsToFloat(nodeSeg.get(ValueLayout.JAVA_INT_UNALIGNED, vectorByteSize));
    }

    public static final class DotProductScorer extends Int7SQVectorScorer {
        public DotProductScorer(IndexInput in, LegacyQuantizedByteVectorValues values, byte[] query, float correction) {
            super(in, values, query, correction);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            long byteOffset = (long) node * vectorPitch;
            input.seek(byteOffset);
            return IndexInputUtils.withSlice(input, vectorPitch, scratch::getScratch, seg -> {
                int dotProduct = DISTANCE_FUNCS.dotProductI7u(query, seg.asSlice(0, vectorByteSize), vectorByteSize);
                assert dotProduct >= 0;
                float nodeCorrection = Float.intBitsToFloat(seg.get(ValueLayout.JAVA_INT_UNALIGNED, vectorByteSize));
                float adjustedDistance = dotProduct * scoreCorrectionConstant + queryCorrection + nodeCorrection;
                return VectorUtil.normalizeToUnitInterval(adjustedDistance);
            });
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (numNodes == 0) {
                return Float.NEGATIVE_INFINITY;
            }
            long[] offsets = getOffsets(nodes, numNodes);
            float[] maxScore = new float[] { Float.NEGATIVE_INFINITY };
            boolean resolved = IndexInputUtils.withSliceAddresses(input, offsets, vectorPitch, numNodes, addrsScratch::get, addrs -> {
                DISTANCE_FUNCS.dotProductI7uBulkSparse(addrs, query, vectorByteSize, numNodes, MemorySegment.ofArray(scores));
                for (int i = 0; i < numNodes; ++i) {
                    float adjustedDistance = scores[i] * scoreCorrectionConstant + queryCorrection + getNodeCorrection(addrs, i);
                    scores[i] = VectorUtil.normalizeToUnitInterval(adjustedDistance);
                    maxScore[0] = Math.max(maxScore[0], scores[i]);
                }
            });
            if (resolved == false) {
                return super.bulkScore(nodes, scores, numNodes);
            }
            return maxScore[0];
        }
    }

    public static final class EuclideanScorer extends Int7SQVectorScorer {
        public EuclideanScorer(IndexInput in, LegacyQuantizedByteVectorValues values, byte[] query, float correction) {
            super(in, values, query, correction);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            long byteOffset = (long) node * vectorPitch;
            input.seek(byteOffset);
            int sqDist = IndexInputUtils.withSlice(
                input,
                vectorPitch,
                scratch::getScratch,
                seg -> DISTANCE_FUNCS.squareDistanceI7u(query, seg.asSlice(0, vectorByteSize), vectorByteSize)
            );
            float adjustedDistance = sqDist * scoreCorrectionConstant;
            return VectorUtil.normalizeDistanceToUnitInterval(adjustedDistance);
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (numNodes == 0) {
                return Float.NEGATIVE_INFINITY;
            }
            long[] offsets = getOffsets(nodes, numNodes);
            float[] maxScore = new float[] { Float.NEGATIVE_INFINITY };
            boolean resolved = IndexInputUtils.withSliceAddresses(input, offsets, vectorPitch, numNodes, addrsScratch::get, addrs -> {
                DISTANCE_FUNCS.squareDistanceI7uBulkSparse(addrs, query, vectorByteSize, numNodes, MemorySegment.ofArray(scores));
                for (int i = 0; i < numNodes; ++i) {
                    var squareDistance = scores[i];
                    float adjustedDistance = squareDistance * scoreCorrectionConstant;
                    scores[i] = VectorUtil.normalizeDistanceToUnitInterval(adjustedDistance);
                    maxScore[0] = Math.max(maxScore[0], scores[i]);
                }
            });

            if (resolved == false) {
                return super.bulkScore(nodes, scores, numNodes);
            }
            return maxScore[0];
        }
    }

    public static final class MaxInnerProductScorer extends Int7SQVectorScorer {
        public MaxInnerProductScorer(IndexInput in, LegacyQuantizedByteVectorValues values, byte[] query, float corr) {
            super(in, values, query, corr);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            long byteOffset = (long) node * vectorPitch;
            input.seek(byteOffset);
            return IndexInputUtils.withSlice(input, vectorPitch, scratch::getScratch, seg -> {
                int dotProduct = DISTANCE_FUNCS.dotProductI7u(query, seg.asSlice(0, vectorByteSize), vectorByteSize);
                assert dotProduct >= 0;
                float nodeCorrection = Float.intBitsToFloat(seg.get(ValueLayout.JAVA_INT_UNALIGNED, vectorByteSize));
                float adjustedDistance = dotProduct * scoreCorrectionConstant + queryCorrection + nodeCorrection;
                return VectorUtil.scaleMaxInnerProductScore(adjustedDistance);
            });
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (numNodes == 0) {
                return Float.NEGATIVE_INFINITY;
            }
            long[] offsets = getOffsets(nodes, numNodes);
            float[] maxScore = new float[] { Float.NEGATIVE_INFINITY };
            boolean resolved = IndexInputUtils.withSliceAddresses(input, offsets, vectorPitch, numNodes, addrsScratch::get, addrs -> {
                DISTANCE_FUNCS.dotProductI7uBulkSparse(addrs, query, vectorByteSize, numNodes, MemorySegment.ofArray(scores));
                for (int i = 0; i < numNodes; ++i) {
                    float adjustedDistance = scores[i] * scoreCorrectionConstant + queryCorrection + getNodeCorrection(addrs, i);
                    scores[i] = VectorUtil.scaleMaxInnerProductScore(adjustedDistance);
                    maxScore[0] = Math.max(maxScore[0], scores[i]);
                }
            });
            if (resolved == false) {
                return super.bulkScore(nodes, scores, numNodes);
            }
            return maxScore[0];
        }
    }

    static void checkDimensions(int queryLen, int fieldLen) {
        if (queryLen != fieldLen) {
            throw new IllegalArgumentException("vector query dimension: " + queryLen + " differs from field dimension: " + fieldLen);
        }
    }
}
