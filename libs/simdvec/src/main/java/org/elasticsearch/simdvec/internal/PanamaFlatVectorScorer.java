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
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;

import static org.apache.lucene.util.VectorUtil.normalizeDistanceToUnitInterval;
import static org.apache.lucene.util.VectorUtil.normalizeToUnitInterval;
import static org.apache.lucene.util.VectorUtil.scaleMaxInnerProductScore;

/**
 * A FlatVectorsScorer that uses SIMD code to compare arrays.
 */
public final class PanamaFlatVectorScorer implements FlatVectorsScorer {

    private static final int BULK_SIZE = 4;

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

    private abstract static class AbstractNativeScorer<V> extends RandomVectorScorer.AbstractRandomVectorScorer
        implements
            RandomVectorScorer,
            HasKnnVectorValues {

        final int dims;

        AbstractNativeScorer(KnnVectorValues vectors) {
            super(vectors);
            dims = vectors.dimension();
        }

        abstract V queryVector();

        abstract V vectorValue(int ord, int bulkIndex) throws IOException;

        abstract float score(V query, V value);

        abstract void bulkScore(V query, V v0, V v1, V v2, V v3, int scoreOffset, float[] scores);

        abstract float correction(float score);

        @Override
        public float score(int node) throws IOException {
            return correction(score(queryVector(), vectorValue(node, 0)));
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            V query = queryVector();

            int n = 0;
            for (; n + BULK_SIZE <= numNodes; n += BULK_SIZE) {
                bulkScore(
                    query,
                    vectorValue(nodes[n], 0),
                    vectorValue(nodes[n + 1], 1),
                    vectorValue(nodes[n + 2], 2),
                    vectorValue(nodes[n + 3], 3),
                    n,
                    scores
                );
            }

            for (; n < numNodes; n++) {
                scores[n] = score(vectorValue(nodes[n], 0), query);
            }

            float max = Float.NEGATIVE_INFINITY;
            for (int s = 0; s < numNodes; s++) {
                scores[s] = correction(scores[s]);
                max = Math.max(max, scores[s]);
            }
            return max;
        }
    }

    private abstract static class NativeFloatScorer extends AbstractNativeScorer<float[]> {
        private final FloatVectorValues vectors;
        private final float[][] scratch;

        NativeFloatScorer(FloatVectorValues vectors) {
            super(vectors);
            this.vectors = vectors;

            this.scratch = new float[BULK_SIZE][dims];
        }

        @Override
        float[] vectorValue(int ord, int bulkIndex) throws IOException {
            float[] value = vectors.vectorValue(ord);
            System.arraycopy(value, 0, scratch[bulkIndex], 0, value.length);
            return scratch[bulkIndex];
        }
    }

    private abstract static class NativeUpdateableFloatScorer extends NativeFloatScorer implements UpdateableRandomVectorScorer {
        private final FloatVectorValues targetVectors;
        private final float[] vector;

        NativeUpdateableFloatScorer(FloatVectorValues vectors, FloatVectorValues targetVectors) {
            super(vectors);
            this.targetVectors = targetVectors;
            vector = new float[targetVectors.dimension()];
        }

        @Override
        float[] queryVector() {
            return vector;
        }

        @Override
        public void setScoringOrdinal(int node) throws IOException {
            System.arraycopy(targetVectors.vectorValue(node), 0, vector, 0, vector.length);
        }
    }

    private abstract static class NativeByteScorer extends AbstractNativeScorer<byte[]> {
        private final ByteVectorValues vectors;
        private final byte[][] scratch;

        NativeByteScorer(ByteVectorValues vectors) {
            super(vectors);
            this.vectors = vectors;

            this.scratch = new byte[BULK_SIZE][dims];
        }

        @Override
        byte[] vectorValue(int ord, int bulkIndex) throws IOException {
            byte[] value = vectors.vectorValue(ord);
            System.arraycopy(value, 0, scratch[bulkIndex], 0, value.length);
            return scratch[bulkIndex];
        }
    }

    private abstract static class NativeUpdateableByteScorer extends NativeByteScorer implements UpdateableRandomVectorScorer {
        private final ByteVectorValues targetVectors;
        private final byte[] vector;

        NativeUpdateableByteScorer(ByteVectorValues vectors, ByteVectorValues targetVectors) {
            super(vectors);
            this.targetVectors = targetVectors;
            vector = new byte[targetVectors.dimension()];
        }

        @Override
        byte[] queryVector() {
            return vector;
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
                    float score(float[] query, float[] value) {
                        return ESVectorUtil.squareDistance(query, value);
                    }

                    @Override
                    void bulkScore(float[] query, float[] v0, float[] v1, float[] v2, float[] v3, int scoreOffset, float[] scores) {
                        ESVectorUtil.squareDistanceBulk(query, v0, v1, v2, v3, scoreOffset, scores);
                    }

                    @Override
                    float correction(float score) {
                        return normalizeDistanceToUnitInterval(score);
                    }
                };
                case DOT_PRODUCT -> new NativeUpdateableFloatScorer(vectors, targetVectors) {
                    @Override
                    float score(float[] query, float[] value) {
                        return ESVectorUtil.dotProduct(query, value);
                    }

                    @Override
                    void bulkScore(float[] query, float[] v0, float[] v1, float[] v2, float[] v3, int scoreOffset, float[] scores) {
                        ESVectorUtil.dotProductBulk(query, v0, v1, v2, v3, scoreOffset, scores);
                    }

                    @Override
                    float correction(float score) {
                        return normalizeToUnitInterval(score);
                    }
                };
                case COSINE -> DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(function, targetVectors).scorer();
                case MAXIMUM_INNER_PRODUCT -> new NativeUpdateableFloatScorer(vectors, targetVectors) {
                    @Override
                    float score(float[] query, float[] value) {
                        return ESVectorUtil.dotProduct(query, value);
                    }

                    @Override
                    void bulkScore(float[] query, float[] v0, float[] v1, float[] v2, float[] v3, int scoreOffset, float[] scores) {
                        ESVectorUtil.dotProductBulk(query, v0, v1, v2, v3, scoreOffset, scores);
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
                float[] queryVector() {
                    return target;
                }

                @Override
                float score(float[] query, float[] value) {
                    return ESVectorUtil.squareDistance(query, value);
                }

                @Override
                void bulkScore(float[] query, float[] v0, float[] v1, float[] v2, float[] v3, int scoreOffset, float[] scores) {
                    ESVectorUtil.squareDistanceBulk(query, v0, v1, v2, v3, scoreOffset, scores);
                }

                @Override
                float correction(float score) {
                    return normalizeDistanceToUnitInterval(score);
                }
            };
            case DOT_PRODUCT -> new NativeFloatScorer(values) {
                @Override
                float[] queryVector() {
                    return target;
                }

                @Override
                float score(float[] query, float[] value) {
                    return ESVectorUtil.dotProduct(query, value);
                }

                @Override
                void bulkScore(float[] query, float[] v0, float[] v1, float[] v2, float[] v3, int scoreOffset, float[] scores) {
                    ESVectorUtil.dotProductBulk(query, v0, v1, v2, v3, scoreOffset, scores);
                }

                @Override
                float correction(float score) {
                    return normalizeToUnitInterval(score);
                }
            };
            case COSINE -> DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorer(similarityFunction, values, target);
            case MAXIMUM_INNER_PRODUCT -> new NativeFloatScorer(values) {
                @Override
                float[] queryVector() {
                    return target;
                }

                @Override
                float score(float[] query, float[] value) {
                    return ESVectorUtil.dotProduct(query, value);
                }

                @Override
                void bulkScore(float[] query, float[] v0, float[] v1, float[] v2, float[] v3, int scoreOffset, float[] scores) {
                    ESVectorUtil.dotProductBulk(query, v0, v1, v2, v3, scoreOffset, scores);
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
                    float score(byte[] query, byte[] value) {
                        return ESVectorUtil.squareDistance(query, value);
                    }

                    @Override
                    void bulkScore(byte[] query, byte[] v0, byte[] v1, byte[] v2, byte[] v3, int scoreOffset, float[] scores) {
                        ESVectorUtil.squareDistanceBulk(query, v0, v1, v2, v3, scoreOffset, scores);
                    }

                    @Override
                    float correction(float score) {
                        return normalizeDistanceToUnitInterval(score);
                    }
                };
                case DOT_PRODUCT -> new NativeUpdateableByteScorer(vectors, targetVectors) {
                    @Override
                    float score(byte[] query, byte[] value) {
                        return ESVectorUtil.dotProduct(query, value);
                    }

                    @Override
                    void bulkScore(byte[] query, byte[] v0, byte[] v1, byte[] v2, byte[] v3, int scoreOffset, float[] scores) {
                        ESVectorUtil.dotProductBulk(query, v0, v1, v2, v3, scoreOffset, scores);
                    }

                    @Override
                    float correction(float score) {
                        return dotProductByteCorrection(score, dims);
                    }
                };
                case COSINE -> new NativeUpdateableByteScorer(vectors, targetVectors) {
                    @Override
                    float score(byte[] query, byte[] value) {
                        return ESVectorUtil.cosine(query, value);
                    }

                    @Override
                    void bulkScore(byte[] query, byte[] v0, byte[] v1, byte[] v2, byte[] v3, int scoreOffset, float[] scores) {
                        ESVectorUtil.cosineBulk(query, v0, v1, v2, v3, scoreOffset, scores);
                    }

                    @Override
                    float correction(float score) {
                        return (1f + score) / 2f;
                    }
                };
                case MAXIMUM_INNER_PRODUCT -> new NativeUpdateableByteScorer(vectors, targetVectors) {
                    @Override
                    float score(byte[] query, byte[] value) {
                        return ESVectorUtil.dotProduct(query, value);
                    }

                    @Override
                    void bulkScore(byte[] query, byte[] v0, byte[] v1, byte[] v2, byte[] v3, int scoreOffset, float[] scores) {
                        ESVectorUtil.dotProductBulk(query, v0, v1, v2, v3, scoreOffset, scores);
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
                byte[] queryVector() {
                    return target;
                }

                @Override
                float score(byte[] query, byte[] value) {
                    return ESVectorUtil.squareDistance(query, value);
                }

                @Override
                void bulkScore(byte[] query, byte[] v0, byte[] v1, byte[] v2, byte[] v3, int scoreOffset, float[] scores) {
                    ESVectorUtil.squareDistanceBulk(query, v0, v1, v2, v3, scoreOffset, scores);
                }

                @Override
                float correction(float score) {
                    return normalizeDistanceToUnitInterval(score);
                }
            };
            case DOT_PRODUCT -> new NativeByteScorer(values) {
                @Override
                byte[] queryVector() {
                    return target;
                }

                @Override
                float score(byte[] query, byte[] value) {
                    return ESVectorUtil.dotProduct(query, value);
                }

                @Override
                void bulkScore(byte[] query, byte[] v0, byte[] v1, byte[] v2, byte[] v3, int scoreOffset, float[] scores) {
                    ESVectorUtil.dotProductBulk(query, v0, v1, v2, v3, scoreOffset, scores);
                }

                @Override
                float correction(float score) {
                    return dotProductByteCorrection(score, dims);
                }
            };
            case COSINE -> new NativeByteScorer(values) {
                @Override
                byte[] queryVector() {
                    return target;
                }

                @Override
                float score(byte[] query, byte[] value) {
                    return ESVectorUtil.cosine(query, value);
                }

                @Override
                void bulkScore(byte[] query, byte[] v0, byte[] v1, byte[] v2, byte[] v3, int scoreOffset, float[] scores) {
                    ESVectorUtil.cosineBulk(query, v0, v1, v2, v3, scoreOffset, scores);
                }

                @Override
                float correction(float score) {
                    return normalizeToUnitInterval(score);
                }
            };
            case MAXIMUM_INNER_PRODUCT -> new NativeByteScorer(values) {
                @Override
                byte[] queryVector() {
                    return target;
                }

                @Override
                float score(byte[] query, byte[] value) {
                    return ESVectorUtil.dotProduct(query, value);
                }

                @Override
                void bulkScore(byte[] query, byte[] v0, byte[] v1, byte[] v2, byte[] v3, int scoreOffset, float[] scores) {
                    ESVectorUtil.dotProductBulk(query, v0, v1, v2, v3, scoreOffset, scores);
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
        return "PanamaFlatVectorScorer()";
    }
}
