/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.backward_codecs.lucene99.OffHeapQuantizedByteVectorValues;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorScorer;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.codec.vectors.es93.OffHeapBFloat16VectorValues;
import org.elasticsearch.simdvec.VectorScorerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class BenchmarkUtils {

    // Unsigned int7 byte vectors have values in the range of 0 to 127 (inclusive).
    static final byte MIN_INT7_VALUE = 0;
    static final byte MAX_INT7_VALUE = 127;

    static void randomInt7BytesBetween(byte[] bytes) {
        var random = ThreadLocalRandom.current();
        for (int i = 0, len = bytes.length; i < len;) {
            bytes[i++] = (byte) random.nextInt(MIN_INT7_VALUE, MAX_INT7_VALUE + 1);
        }
    }

    static void writeInt7VectorData(Directory dir, byte[][] vectors, float[] offsets) throws IOException {
        try (IndexOutput out = dir.createOutput("vector.data", IOContext.DEFAULT)) {
            for (int v = 0; v < vectors.length; v++) {
                out.writeBytes(vectors[v], vectors[v].length);
                out.writeInt(Float.floatToIntBits(offsets[v]));
            }
        }
    }

    static void writeFloatVectorData(Directory dir, float[][] vectors) throws IOException {
        try (IndexOutput out = dir.createOutput("vector.data", IOContext.DEFAULT)) {
            ByteBuffer buffer = ByteBuffer.allocate(vectors[0].length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            for (float[] vector : vectors) {
                buffer.asFloatBuffer().put(vector);
                out.writeBytes(buffer.array(), buffer.capacity());
            }
        }
    }

    static void writeBFloat16VectorData(Directory dir, float[][] vectors) throws IOException {
        try (IndexOutput out = dir.createOutput("vector.data", IOContext.DEFAULT)) {
            byte[] buffer = new byte[vectors[0].length * BFloat16.BYTES];
            for (float[] vector : vectors) {
                BFloat16.floatToBFloat16(vector, buffer);
                out.writeBytes(buffer, buffer.length);
            }
        }
    }

    static void writeByteVectorData(Directory dir, byte[][] vectors) throws IOException {
        try (IndexOutput out = dir.createOutput("vector.data", IOContext.DEFAULT)) {
            for (byte[] vector : vectors) {
                out.writeBytes(vector, vector.length);
            }
        }
    }

    static VectorScorerFactory getScorerFactoryOrDie() {
        var optionalVectorScorerFactory = VectorScorerFactory.instance();
        if (optionalVectorScorerFactory.isEmpty()) {
            String msg = "JDK=["
                + Runtime.version()
                + "], os.name=["
                + System.getProperty("os.name")
                + "], os.arch=["
                + System.getProperty("os.arch")
                + "]";
            throw new AssertionError("Vector scorer factory not present. Cannot run the benchmark. " + msg);
        }
        return optionalVectorScorerFactory.get();
    }

    static boolean supportsHeapSegments() {
        return Runtime.version().feature() >= 22;
    }

    static FloatVectorValues floatVectorValues(int dims, int size, IndexInput in, VectorSimilarityFunction sim) throws IOException {
        var slice = in.slice("values", 0, in.length());
        return new OffHeapFloatVectorValues.DenseOffHeapVectorValues(dims, size, slice, dims * Float.BYTES, null, sim);
    }

    static FloatVectorValues bfloat16VectorValues(int dims, int size, IndexInput in, VectorSimilarityFunction sim) throws IOException {
        var slice = in.slice("values", 0, in.length());
        return new OffHeapBFloat16VectorValues.DenseOffHeapVectorValues(dims, size, slice, dims * BFloat16.BYTES, null, sim);
    }

    static ByteVectorValues byteVectorValues(int dims, int size, IndexInput in, VectorSimilarityFunction sim) throws IOException {
        var slice = in.slice("values", 0, in.length());
        return new OffHeapByteVectorValues.DenseOffHeapVectorValues(dims, size, slice, dims, null, sim);
    }

    static QuantizedByteVectorValues quantizedVectorValues(int dims, int size, IndexInput in, VectorSimilarityFunction sim)
        throws IOException {
        var sq = new ScalarQuantizer(0.1f, 0.9f, (byte) 7);
        var slice = in.slice("values", 0, in.length());
        return new OffHeapQuantizedByteVectorValues.DenseOffHeapVectorValues(dims, size, sq, false, sim, null, slice);
    }

    static RandomVectorScorerSupplier luceneScoreSupplier(FloatVectorValues values, VectorSimilarityFunction sim) throws IOException {
        return FlatVectorScorerUtil.getLucene99FlatVectorsScorer().getRandomVectorScorerSupplier(sim, values);
    }

    static RandomVectorScorerSupplier arrayScoreSupplier(FloatVectorValues values, VectorSimilarityFunction sim) throws IOException {
        return DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(sim, values);
    }

    static RandomVectorScorerSupplier luceneScoreSupplier(ByteVectorValues values, VectorSimilarityFunction sim) throws IOException {
        return FlatVectorScorerUtil.getLucene99FlatVectorsScorer().getRandomVectorScorerSupplier(sim, values);
    }

    static RandomVectorScorerSupplier luceneScoreSupplier(QuantizedByteVectorValues values, VectorSimilarityFunction sim)
        throws IOException {
        return new Lucene99ScalarQuantizedVectorScorer(null).getRandomVectorScorerSupplier(sim, values);
    }

    static RandomVectorScorer luceneScorer(FloatVectorValues values, VectorSimilarityFunction sim, float[] queryVec) throws IOException {
        return FlatVectorScorerUtil.getLucene99FlatVectorsScorer().getRandomVectorScorer(sim, values, queryVec);
    }

    static RandomVectorScorer arrayScorer(FloatVectorValues values, VectorSimilarityFunction sim, float[] queryVec) throws IOException {
        return DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorer(sim, values, queryVec);
    }

    static RandomVectorScorer luceneScorer(ByteVectorValues values, VectorSimilarityFunction sim, byte[] queryVec) throws IOException {
        return FlatVectorScorerUtil.getLucene99FlatVectorsScorer().getRandomVectorScorer(sim, values, queryVec);
    }

    static RandomVectorScorer luceneScorer(QuantizedByteVectorValues values, VectorSimilarityFunction sim, float[] queryVec)
        throws IOException {
        return new Lucene99ScalarQuantizedVectorScorer(null).getRandomVectorScorer(sim, values, queryVec);
    }

    static RandomVectorScorerSupplier lucene104ScoreSupplier(
        org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues values,
        VectorSimilarityFunction sim
    ) throws IOException {
        return new Lucene104ScalarQuantizedVectorScorer(null).getRandomVectorScorerSupplier(sim, values);
    }

    static RandomVectorScorer lucene104Scorer(
        org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues values,
        VectorSimilarityFunction sim,
        float[] queryVec
    ) throws IOException {
        return new Lucene104ScalarQuantizedVectorScorer(null).getRandomVectorScorer(sim, values, queryVec);
    }

    static RuntimeException rethrow(Throwable t) {
        if (t instanceof Error err) {
            throw err;
        }
        return t instanceof RuntimeException re ? re : new RuntimeException(t);
    }

    static int[] generateRandomOrdinals(int numVectors, int numVectorsToScore, Random random) {
        List<Integer> list = IntStream.range(0, numVectors).boxed().collect(Collectors.toList());
        Collections.shuffle(list, random);
        return list.stream().limit(numVectorsToScore).mapToInt(Integer::intValue).toArray();
    }
}
