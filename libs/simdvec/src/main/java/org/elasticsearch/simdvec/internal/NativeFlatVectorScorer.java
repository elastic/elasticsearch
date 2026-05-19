/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.HasKnnVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import static org.apache.lucene.util.VectorUtil.normalizeDistanceToUnitInterval;
import static org.apache.lucene.util.VectorUtil.normalizeToUnitInterval;
import static org.apache.lucene.util.VectorUtil.scaleMaxInnerProductScore;

/**
 * A FlatVectorsScorer that uses native code to compare arrays.
 */
public final class NativeFlatVectorScorer implements FlatVectorsScorer {

    /*
     * Unlike other native scorers, this does not use IndexInput slices (as it can't get one),
     * but instead uses {@code MemorySegment.ofArray} to get an address pointer for native code.
     * <p>
     * Java FFI does not handle nested MemorySegments - when a MemorySegment created from an array
     * is passed to native code, the GC pins the array to get a pointer address.
     * This is done to all MemorySegments passed to the native method handle,
     * as an intrinsic part of calling the method.
     * There is no way to explicitly pin and release an array segment to get an address.
     * So all the vectors need to be top-level arguments, not in MemorySegment
     * created from a a {@code MemorySegment[]}, as done for the 'slices' bulk variants.
     *
     * To get round this, we bulk score in groups of 8 explicit array arguments.
     * This means we don't have to do a native call for every vector,
     * and we get some amortization of the native call overhead and bookkeeping.
     */

    private static final int BULK_SIZE = 8;

    @Override
    public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
        VectorSimilarityFunction similarityFunction,
        KnnVectorValues vectorValues
    ) throws IOException {
        return switch (vectorValues.getEncoding()) {
            case FLOAT32 -> new FloatScoringSupplier((FloatVectorValues) vectorValues, similarityFunction);
            case BYTE -> new ByteScoringSupplier((ByteVectorValues) vectorValues, similarityFunction);
        };
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(
        VectorSimilarityFunction similarityFunction,
        KnnVectorValues vectorValues,
        float[] target
    ) throws IOException {
        if (target.length != vectorValues.dimension()) {
            throw new IllegalArgumentException(
                "vector query dimension: " + target.length + " differs from field dimension: " + vectorValues.dimension()
            );
        }
        return createScorer(similarityFunction, target, (FloatVectorValues) vectorValues);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(
        VectorSimilarityFunction similarityFunction,
        KnnVectorValues vectorValues,
        byte[] target
    ) throws IOException {
        if (target.length != vectorValues.dimension()) {
            throw new IllegalArgumentException(
                "vector query dimension: " + target.length + " differs from field dimension: " + vectorValues.dimension()
            );
        }
        return createScorer(similarityFunction, target, (ByteVectorValues) vectorValues);
    }

    private abstract static class AbstractNativeScorer extends RandomVectorScorer.AbstractRandomVectorScorer
        implements
            RandomVectorScorer,
            HasKnnVectorValues {

        final int dims;
        private final MemorySegment[] segments = new MemorySegment[BULK_SIZE];

        AbstractNativeScorer(KnnVectorValues vectors) {
            super(vectors);
            dims = vectors.dimension();
        }

        abstract MemorySegment queryVectorSegment() throws IOException;

        abstract MemorySegment vectorValueSegment(int ord, int bulkIndex) throws IOException;

        abstract float score(MemorySegment query, MemorySegment value);

        abstract void bulk8Score(MemorySegment[] segments, MemorySegment query, MemorySegment scores);

        abstract float correction(float score);

        @Override
        public float score(int node) throws IOException {
            return correction(score(queryVectorSegment(), vectorValueSegment(node, 0)));
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            MemorySegment querySegment = queryVectorSegment();
            MemorySegment scoreSegment = MemorySegment.ofArray(scores);

            int n = 0;
            for (; n + BULK_SIZE <= numNodes; n += BULK_SIZE) {
                for (int i = 0; i < BULK_SIZE; i++) {
                    segments[i] = vectorValueSegment(nodes[n + i], i);
                }

                bulk8Score(segments, querySegment, scoreSegment.asSlice((long) n * Float.BYTES, (long) BULK_SIZE * Float.BYTES));
            }

            for (; n < numNodes; n++) {
                scores[n] = score(vectorValueSegment(nodes[n], 0), querySegment);
            }

            float max = Float.NEGATIVE_INFINITY;
            for (int s = 0; s < numNodes; s++) {
                scores[s] = correction(scores[s]);
                max = Math.max(max, scores[s]);
            }
            return max;
        }
    }

    private abstract static class NativeFloatScorer extends AbstractNativeScorer {
        private final FloatVectorValues vectors;
        private final float[][] scratch;

        NativeFloatScorer(FloatVectorValues vectors) throws IOException {
            super(vectors);
            this.vectors = vectors;

            // check if values actually creates a copy
            // if it just returns the same instance,
            // then the arrays it returns don't need to be copied to be accessed concurrently
            this.scratch = vectors.copy() == vectors ? null : new float[BULK_SIZE][dims];
        }

        @Override
        MemorySegment vectorValueSegment(int ord, int bulkIndex) throws IOException {
            if (scratch != null) {
                float[] value = vectors.vectorValue(ord);
                System.arraycopy(value, 0, scratch[bulkIndex], 0, value.length);
                return MemorySegment.ofArray(scratch[bulkIndex]);
            } else {
                // each array is unique - just use directly
                return MemorySegment.ofArray(vectors.vectorValue(ord));
            }
        }
    }

    private abstract static class NativeUpdateableFloatScorer extends NativeFloatScorer implements UpdateableRandomVectorScorer {
        private final FloatVectorValues targetVectors;
        private final float[] vector;

        NativeUpdateableFloatScorer(FloatVectorValues vectors, FloatVectorValues targetVectors) throws IOException {
            super(vectors);
            this.targetVectors = targetVectors;
            vector = new float[targetVectors.dimension()];
        }

        @Override
        MemorySegment queryVectorSegment() throws IOException {
            return MemorySegment.ofArray(vector);
        }

        @Override
        public void setScoringOrdinal(int node) throws IOException {
            System.arraycopy(targetVectors.vectorValue(node), 0, vector, 0, vector.length);
        }
    }

    private abstract static class NativeByteScorer extends AbstractNativeScorer {
        private final ByteVectorValues vectors;
        private final byte[][] scratch;

        NativeByteScorer(ByteVectorValues vectors) throws IOException {
            super(vectors);
            this.vectors = vectors;

            // check if values actually creates a copy
            // if it just returns the same instance,
            // then the arrays it returns don't need to be copied to be accessed concurrently
            this.scratch = vectors.copy() == vectors ? null : new byte[BULK_SIZE][dims];
        }

        @Override
        MemorySegment vectorValueSegment(int ord, int bulkIndex) throws IOException {
            if (scratch != null) {
                byte[] value = vectors.vectorValue(ord);
                System.arraycopy(value, 0, scratch[bulkIndex], 0, value.length);
                return MemorySegment.ofArray(scratch[bulkIndex]);
            } else {
                // each array is unique - just use directly
                return MemorySegment.ofArray(vectors.vectorValue(ord));
            }
        }
    }

    private abstract static class NativeUpdateableByteScorer extends NativeByteScorer implements UpdateableRandomVectorScorer {
        private final ByteVectorValues targetVectors;
        private final byte[] vector;

        NativeUpdateableByteScorer(ByteVectorValues vectors, ByteVectorValues targetVectors) throws IOException {
            super(vectors);
            this.targetVectors = targetVectors;
            vector = new byte[targetVectors.dimension()];
        }

        @Override
        MemorySegment queryVectorSegment() throws IOException {
            return MemorySegment.ofArray(vector);
        }

        @Override
        public void setScoringOrdinal(int node) throws IOException {
            System.arraycopy(targetVectors.vectorValue(node), 0, vector, 0, vector.length);
        }
    }

    private static class FloatScoringSupplier implements RandomVectorScorerSupplier {
        private final FloatVectorValues vectors;
        private final FloatVectorValues targetVectors;
        private final VectorSimilarityFunction function;

        FloatScoringSupplier(FloatVectorValues vectors, VectorSimilarityFunction function) throws IOException {
            this.vectors = vectors;
            targetVectors = vectors.copy();
            this.function = function;
        }

        @Override
        public UpdateableRandomVectorScorer scorer() throws IOException {
            return switch (function) {
                case EUCLIDEAN -> new NativeUpdateableFloatScorer(vectors, targetVectors) {
                    @Override
                    float score(MemorySegment query, MemorySegment value) {
                        return Similarities.squareDistanceF32(query, value, dims);
                    }

                    @Override
                    void bulk8Score(MemorySegment[] segments, MemorySegment query, MemorySegment scores) {
                        Similarities.squareDistanceF32Bulk8(segments, query, dims, scores);
                    }

                    @Override
                    float correction(float score) {
                        return normalizeDistanceToUnitInterval(score);
                    }
                };
                case DOT_PRODUCT -> new NativeUpdateableFloatScorer(vectors, targetVectors) {
                    @Override
                    float score(MemorySegment query, MemorySegment value) {
                        return Similarities.dotProductF32(query, value, dims);
                    }

                    @Override
                    void bulk8Score(MemorySegment[] segments, MemorySegment query, MemorySegment scores) {
                        Similarities.dotProductF32Bulk8(segments, query, dims, scores);
                    }

                    @Override
                    float correction(float score) {
                        return normalizeToUnitInterval(score);
                    }
                };
                case COSINE -> DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(function, targetVectors).scorer();
                case MAXIMUM_INNER_PRODUCT -> new NativeUpdateableFloatScorer(vectors, targetVectors) {
                    @Override
                    float score(MemorySegment query, MemorySegment value) {
                        return Similarities.dotProductF32(query, value, dims);
                    }

                    @Override
                    void bulk8Score(MemorySegment[] segments, MemorySegment query, MemorySegment scores) {
                        Similarities.dotProductF32Bulk8(segments, query, dims, scores);
                    }

                    @Override
                    float correction(float score) {
                        return scaleMaxInnerProductScore(score);
                    }
                };
            };
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return new FloatScoringSupplier(vectors, function);
        }

        @Override
        public String toString() {
            return "FloatScoringSupplier(similarityFunction=" + function + ")";
        }
    }

    private static RandomVectorScorer createScorer(VectorSimilarityFunction similarityFunction, float[] target, FloatVectorValues values)
        throws IOException {
        return switch (similarityFunction) {
            case EUCLIDEAN -> new NativeFloatScorer(values) {
                @Override
                MemorySegment queryVectorSegment() {
                    return MemorySegment.ofArray(target);
                }

                @Override
                float score(MemorySegment q, MemorySegment value) {
                    return Similarities.squareDistanceF32(q, value, dims);
                }

                @Override
                void bulk8Score(MemorySegment[] segments, MemorySegment q, MemorySegment scores) {
                    Similarities.squareDistanceF32Bulk8(segments, q, dims, scores);
                }

                @Override
                float correction(float score) {
                    return normalizeDistanceToUnitInterval(score);
                }
            };
            case DOT_PRODUCT -> new NativeFloatScorer(values) {
                @Override
                MemorySegment queryVectorSegment() {
                    return MemorySegment.ofArray(target);
                }

                @Override
                float score(MemorySegment q, MemorySegment value) {
                    return Similarities.dotProductF32(q, value, dims);
                }

                @Override
                void bulk8Score(MemorySegment[] segments, MemorySegment q, MemorySegment scores) {
                    Similarities.dotProductF32Bulk8(segments, q, dims, scores);
                }

                @Override
                float correction(float score) {
                    return normalizeToUnitInterval(score);
                }
            };
            case COSINE -> DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorer(similarityFunction, values, target);
            case MAXIMUM_INNER_PRODUCT -> new NativeFloatScorer(values) {
                @Override
                MemorySegment queryVectorSegment() {
                    return MemorySegment.ofArray(target);
                }

                @Override
                float score(MemorySegment q, MemorySegment value) {
                    return Similarities.dotProductF32(q, value, dims);
                }

                @Override
                void bulk8Score(MemorySegment[] segments, MemorySegment q, MemorySegment scores) {
                    Similarities.dotProductF32Bulk8(segments, q, dims, scores);
                }

                @Override
                float correction(float score) {
                    return scaleMaxInnerProductScore(score);
                }
            };
        };
    }

    private static float dotProductByteCorrection(float score, int dims) {
        // not available as a separate method in VectorUtil
        return 0.5f + score / (float) (dims * (1 << 15));
    }

    private static class ByteScoringSupplier implements RandomVectorScorerSupplier {
        private final ByteVectorValues vectors;
        private final ByteVectorValues targetVectors;
        private final VectorSimilarityFunction function;

        ByteScoringSupplier(ByteVectorValues vectors, VectorSimilarityFunction function) throws IOException {
            this.vectors = vectors;
            targetVectors = vectors.copy();
            this.function = function;
        }

        @Override
        public UpdateableRandomVectorScorer scorer() throws IOException {
            return switch (function) {
                case EUCLIDEAN -> new NativeUpdateableByteScorer(vectors, targetVectors) {
                    @Override
                    float score(MemorySegment query, MemorySegment value) {
                        return Similarities.squareDistanceI8(query, value, dims);
                    }

                    @Override
                    void bulk8Score(MemorySegment[] segments, MemorySegment query, MemorySegment scores) {
                        Similarities.squareDistanceI8Bulk8(segments, query, dims, scores);
                    }

                    @Override
                    float correction(float score) {
                        return normalizeDistanceToUnitInterval(score);
                    }
                };
                case DOT_PRODUCT -> new NativeUpdateableByteScorer(vectors, targetVectors) {
                    @Override
                    float score(MemorySegment query, MemorySegment value) {
                        return Similarities.dotProductI8(query, value, dims);
                    }

                    @Override
                    void bulk8Score(MemorySegment[] segments, MemorySegment query, MemorySegment scores) {
                        Similarities.dotProductI8Bulk8(segments, query, dims, scores);
                    }

                    @Override
                    float correction(float score) {
                        return dotProductByteCorrection(score, dims);
                    }
                };
                case COSINE -> new NativeUpdateableByteScorer(vectors, targetVectors) {
                    @Override
                    float score(MemorySegment query, MemorySegment value) {
                        return Similarities.cosineI8(query, value, dims);
                    }

                    @Override
                    void bulk8Score(MemorySegment[] segments, MemorySegment query, MemorySegment scores) {
                        Similarities.cosineI8Bulk8(segments, query, dims, scores);
                    }

                    @Override
                    float correction(float score) {
                        return (1f + score) / 2f;
                    }
                };
                case MAXIMUM_INNER_PRODUCT -> new NativeUpdateableByteScorer(vectors, targetVectors) {
                    @Override
                    float score(MemorySegment query, MemorySegment value) {
                        return Similarities.dotProductI8(query, value, dims);
                    }

                    @Override
                    void bulk8Score(MemorySegment[] segments, MemorySegment query, MemorySegment scores) {
                        Similarities.dotProductI8Bulk8(segments, query, dims, scores);
                    }

                    @Override
                    float correction(float score) {
                        return scaleMaxInnerProductScore(score);
                    }
                };
            };
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return new ByteScoringSupplier(vectors, function);
        }

        @Override
        public String toString() {
            return "ByteScoringSupplier(similarityFunction=" + function + ")";
        }
    }

    private static RandomVectorScorer createScorer(VectorSimilarityFunction similarityFunction, byte[] target, ByteVectorValues values)
        throws IOException {
        return switch (similarityFunction) {
            case EUCLIDEAN -> new NativeByteScorer(values) {
                @Override
                MemorySegment queryVectorSegment() {
                    return MemorySegment.ofArray(target);
                }

                @Override
                float score(MemorySegment query, MemorySegment value) {
                    return Similarities.squareDistanceI8(query, value, dims);
                }

                @Override
                void bulk8Score(MemorySegment[] segments, MemorySegment query, MemorySegment scores) {
                    Similarities.squareDistanceI8Bulk8(segments, query, dims, scores);
                }

                @Override
                float correction(float score) {
                    return normalizeDistanceToUnitInterval(score);
                }
            };
            case DOT_PRODUCT -> new NativeByteScorer(values) {
                @Override
                MemorySegment queryVectorSegment() {
                    return MemorySegment.ofArray(target);
                }

                @Override
                float score(MemorySegment query, MemorySegment value) {
                    return Similarities.dotProductI8(query, value, dims);
                }

                @Override
                void bulk8Score(MemorySegment[] segments, MemorySegment query, MemorySegment scores) {
                    Similarities.dotProductI8Bulk8(segments, query, dims, scores);
                }

                @Override
                float correction(float score) {
                    return dotProductByteCorrection(score, dims);
                }
            };
            case COSINE -> new NativeByteScorer(values) {
                @Override
                MemorySegment queryVectorSegment() {
                    return MemorySegment.ofArray(target);
                }

                @Override
                float score(MemorySegment query, MemorySegment value) {
                    return Similarities.cosineI8(query, value, dims);
                }

                @Override
                void bulk8Score(MemorySegment[] segments, MemorySegment query, MemorySegment scores) {
                    Similarities.cosineI8Bulk8(segments, query, dims, scores);
                }

                @Override
                float correction(float score) {
                    return normalizeToUnitInterval(score);
                }
            };
            case MAXIMUM_INNER_PRODUCT -> new NativeByteScorer(values) {
                @Override
                MemorySegment queryVectorSegment() {
                    return MemorySegment.ofArray(target);
                }

                @Override
                float score(MemorySegment query, MemorySegment value) {
                    return Similarities.dotProductI8(query, value, dims);
                }

                @Override
                void bulk8Score(MemorySegment[] segments, MemorySegment query, MemorySegment scores) {
                    Similarities.dotProductI8Bulk8(segments, query, dims, scores);
                }

                @Override
                float correction(float score) {
                    return scaleMaxInnerProductScore(score);
                }
            };
        };
    }

    @Override
    public String toString() {
        return "NativeFlatVectorScorer";
    }
}
