/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */
package org.elasticsearch.simdvec;

import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

import java.io.IOException;

import static org.apache.lucene.util.VectorUtil.normalizeDistanceToUnitInterval;
import static org.apache.lucene.util.VectorUtil.normalizeToUnitInterval;
import static org.apache.lucene.util.VectorUtil.scaleMaxInnerProductScore;

public class DefaultNativeFlatVectorScorer implements FlatVectorsScorer {

    public static final DefaultNativeFlatVectorScorer INSTANCE = new DefaultNativeFlatVectorScorer();

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
        assert vectorValues instanceof FloatVectorValues;
        if (target.length != vectorValues.dimension()) {
            throw new IllegalArgumentException(
                "vector query dimension: " + target.length + " differs from field dimension: " + vectorValues.dimension()
            );
        }
        return FloatVectorScorer.create((FloatVectorValues) vectorValues, target, similarityFunction);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(
        VectorSimilarityFunction similarityFunction,
        KnnVectorValues vectorValues,
        byte[] target
    ) throws IOException {
        assert vectorValues instanceof ByteVectorValues;
        if (target.length != vectorValues.dimension()) {
            throw new IllegalArgumentException(
                "vector query dimension: " + target.length + " differs from field dimension: " + vectorValues.dimension()
            );
        }
        return ByteVectorScorer.create((ByteVectorValues) vectorValues, target, similarityFunction);
    }

    @Override
    public String toString() {
        return "DefaultNativeFlatVectorScorer()";
    }

    private static float byteEuclidean(byte[] v1, byte[] v2) {
        return 1 / (1f + ESVectorUtil.squareDistance(v1, v2));
    }

    private static float byteDotProduct(byte[] v1, byte[] v2) {
        float denom = (float) (v1.length * (1 << 15));
        return 0.5f + ESVectorUtil.dotProduct(v1, v2) / denom;
    }

    private static float byteCosine(byte[] v1, byte[] v2) {
        return (1 + ESVectorUtil.cosine(v1, v2)) / 2;
    }

    private static float byteMaximumInnerProduct(byte[] v1, byte[] v2) {
        return scaleMaxInnerProductScore(ESVectorUtil.dotProduct(v1, v2));
    }

    private static float floatEuclidean(float[] v1, float[] v2) {
        return normalizeDistanceToUnitInterval(ESVectorUtil.squareDistance(v1, v2));
    }

    private static float floatDotProduct(float[] v1, float[] v2) {
        return normalizeToUnitInterval(ESVectorUtil.dotProduct(v1, v2));
    }

    private static float floatCosine(float[] v1, float[] v2) {
        // no ES version, we expect all float vectors to be normalized
        return normalizeToUnitInterval(VectorUtil.cosine(v1, v2));
    }

    private static float floatMaximumInnerProduct(float[] v1, float[] v2) {
        return scaleMaxInnerProductScore(ESVectorUtil.dotProduct(v1, v2));
    }

    public static float compare(VectorSimilarityFunction function, float[] a, float[] b) {
        return switch (function) {
            case EUCLIDEAN -> floatEuclidean(a, b);
            case DOT_PRODUCT -> floatDotProduct(a, b);
            case COSINE -> floatCosine(a, b);
            case MAXIMUM_INNER_PRODUCT -> floatMaximumInnerProduct(a, b);
        };
    }

    public static float compare(VectorSimilarityFunction function, byte[] a, byte[] b) {
        return switch (function) {
            case EUCLIDEAN -> byteEuclidean(a, b);
            case DOT_PRODUCT -> byteDotProduct(a, b);
            case COSINE -> byteCosine(a, b);
            case MAXIMUM_INNER_PRODUCT -> byteDotProduct(a, b);
        };
    }

    private static class FloatScoringSupplier implements RandomVectorScorerSupplier {
        private final FloatVectorValues vectors;
        private final FloatVectorValues targetVectors;
        private final VectorSimilarityFunction function;

        private FloatScoringSupplier(FloatVectorValues vectors, VectorSimilarityFunction function) throws IOException {
            this.vectors = vectors;
            targetVectors = vectors.copy();
            this.function = function;
        }

        private abstract static class FloatUpdateableScorer extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
            protected final float[] vector;
            protected final FloatVectorValues targetVectors;

            private FloatUpdateableScorer(FloatVectorValues vectors, FloatVectorValues targetVectors) {
                super(vectors);
                this.vector = new float[vectors.dimension()];
                this.targetVectors = targetVectors;
            }

            @Override
            public void setScoringOrdinal(int node) throws IOException {
                System.arraycopy(targetVectors.vectorValue(node), 0, vector, 0, vector.length);
            }
        }

        @Override
        public UpdateableRandomVectorScorer scorer() throws IOException {
            return switch (function) {
                case EUCLIDEAN -> new FloatUpdateableScorer(vectors, targetVectors) {
                    @Override
                    public float score(int node) throws IOException {
                        return floatEuclidean(vector, targetVectors.vectorValue(node));
                    }
                };
                case DOT_PRODUCT -> new FloatUpdateableScorer(vectors, targetVectors) {
                    @Override
                    public float score(int node) throws IOException {
                        return floatDotProduct(vector, targetVectors.vectorValue(node));
                    }
                };
                case COSINE -> new FloatUpdateableScorer(vectors, targetVectors) {
                    @Override
                    public float score(int node) throws IOException {
                        return floatCosine(vector, targetVectors.vectorValue(node));
                    }
                };
                case MAXIMUM_INNER_PRODUCT -> new FloatUpdateableScorer(vectors, targetVectors) {
                    @Override
                    public float score(int node) throws IOException {
                        return floatMaximumInnerProduct(vector, targetVectors.vectorValue(node));
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

    private static class ByteScoringSupplier implements RandomVectorScorerSupplier {
        private final ByteVectorValues vectors;
        private final ByteVectorValues targetVectors;
        private final VectorSimilarityFunction function;

        private ByteScoringSupplier(ByteVectorValues vectors, VectorSimilarityFunction function) throws IOException {
            this.vectors = vectors;
            targetVectors = vectors.copy();
            this.function = function;
        }

        private abstract static class ByteUpdateableScorer extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
            protected final byte[] vector;
            protected final ByteVectorValues targetVectors;

            private ByteUpdateableScorer(ByteVectorValues vectors, ByteVectorValues targetVectors) {
                super(vectors);
                this.vector = new byte[vectors.dimension()];
                this.targetVectors = targetVectors;
            }

            @Override
            public void setScoringOrdinal(int node) throws IOException {
                System.arraycopy(targetVectors.vectorValue(node), 0, vector, 0, vector.length);
            }
        }

        @Override
        public UpdateableRandomVectorScorer scorer() throws IOException {
            return switch (function) {
                case EUCLIDEAN -> new ByteUpdateableScorer(vectors, targetVectors) {
                    @Override
                    public float score(int node) throws IOException {
                        return byteEuclidean(vector, targetVectors.vectorValue(node));
                    }
                };
                case DOT_PRODUCT -> new ByteUpdateableScorer(vectors, targetVectors) {
                    @Override
                    public float score(int node) throws IOException {
                        return byteDotProduct(vector, targetVectors.vectorValue(node));
                    }
                };
                case COSINE -> new ByteUpdateableScorer(vectors, targetVectors) {
                    @Override
                    public float score(int node) throws IOException {
                        return byteCosine(vector, targetVectors.vectorValue(node));
                    }
                };
                case MAXIMUM_INNER_PRODUCT -> new ByteUpdateableScorer(vectors, targetVectors) {
                    @Override
                    public float score(int node) throws IOException {
                        return byteMaximumInnerProduct(vector, targetVectors.vectorValue(node));
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

    private abstract static class FloatVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
        protected final FloatVectorValues values;
        protected final float[] query;

        private FloatVectorScorer(FloatVectorValues values, float[] query) {
            super(values);
            this.values = values;
            this.query = query;
        }

        static FloatVectorScorer create(FloatVectorValues values, float[] query, VectorSimilarityFunction similarityFunction) {
            return switch (similarityFunction) {
                case EUCLIDEAN -> new FloatVectorScorer(values, query) {
                    @Override
                    public float score(int node) throws IOException {
                        return floatEuclidean(query, values.vectorValue(node));
                    }
                };
                case DOT_PRODUCT -> new FloatVectorScorer(values, query) {
                    @Override
                    public float score(int node) throws IOException {
                        return floatDotProduct(query, values.vectorValue(node));
                    }
                };
                case COSINE -> new FloatVectorScorer(values, query) {
                    @Override
                    public float score(int node) throws IOException {
                        return floatCosine(query, values.vectorValue(node));
                    }
                };
                case MAXIMUM_INNER_PRODUCT -> new FloatVectorScorer(values, query) {
                    @Override
                    public float score(int node) throws IOException {
                        return floatMaximumInnerProduct(query, values.vectorValue(node));
                    }
                };
            };
        }
    }

    private abstract static class ByteVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
        protected final ByteVectorValues values;
        protected final byte[] query;

        private ByteVectorScorer(ByteVectorValues values, byte[] query) {
            super(values);
            this.values = values;
            this.query = query;
        }

        static ByteVectorScorer create(ByteVectorValues values, byte[] query, VectorSimilarityFunction similarityFunction) {
            return switch (similarityFunction) {
                case EUCLIDEAN -> new ByteVectorScorer(values, query) {
                    @Override
                    public float score(int node) throws IOException {
                        return byteEuclidean(query, values.vectorValue(node));
                    }
                };
                case DOT_PRODUCT -> new ByteVectorScorer(values, query) {
                    @Override
                    public float score(int node) throws IOException {
                        return byteDotProduct(query, values.vectorValue(node));
                    }
                };
                case COSINE -> new ByteVectorScorer(values, query) {
                    @Override
                    public float score(int node) throws IOException {
                        return byteCosine(query, values.vectorValue(node));
                    }
                };
                case MAXIMUM_INNER_PRODUCT -> new ByteVectorScorer(values, query) {
                    @Override
                    public float score(int node) throws IOException {
                        return byteMaximumInnerProduct(query, values.vectorValue(node));
                    }
                };
            };
        }
    }
}
