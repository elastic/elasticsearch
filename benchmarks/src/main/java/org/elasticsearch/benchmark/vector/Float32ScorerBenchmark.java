/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.simdvec.VectorScorerFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.simdvec.VectorSimilarityType.DOT_PRODUCT;
import static org.elasticsearch.simdvec.VectorSimilarityType.EUCLIDEAN;

@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
/**
 * Benchmark that compares various float32 vector similarity function
 * implementations;: scalar, lucene's panama-ized, and Elasticsearch's native.
 * Run with ./gradlew -p benchmarks run --args 'Float32ScorerBenchmark'
 */
public class Float32ScorerBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Param({ "96", "768", "1024" })
    public int dims;
    final int size = 3; // there are only two vectors to compare against

    Directory dir;
    IndexInput in;
    VectorScorerFactory factory;

    float[] vec1, vec2, vec3;

    UpdateableRandomVectorScorer luceneDotScorer;
    UpdateableRandomVectorScorer luceneSqrScorer;
    UpdateableRandomVectorScorer nativeDotScorer;
    UpdateableRandomVectorScorer nativeSqrScorer;

    RandomVectorScorer luceneDotScorerQuery;
    RandomVectorScorer nativeDotScorerQuery;
    RandomVectorScorer luceneSqrScorerQuery;
    RandomVectorScorer nativeSqrScorerQuery;

    @Setup
    public void setup() throws IOException {
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
        factory = optionalVectorScorerFactory.get();
        vec1 = randomFloatArray(dims);
        vec2 = randomFloatArray(dims);
        vec3 = randomFloatArray(dims);

        dir = new MMapDirectory(Files.createTempDirectory("nativeFloat32Bench"));
        try (IndexOutput out = dir.createOutput("vector32.data", IOContext.DEFAULT)) {
            writeFloat32Vectors(out, vec1, vec2, vec3);
        }
        in = dir.openInput("vector32.data", IOContext.DEFAULT);
        var values = vectorValues(dims, 3, in, VectorSimilarityFunction.DOT_PRODUCT);
        luceneDotScorer = luceneScoreSupplier(values, VectorSimilarityFunction.DOT_PRODUCT).scorer();
        luceneDotScorer.setScoringOrdinal(0);
        values = vectorValues(dims, 3, in, VectorSimilarityFunction.EUCLIDEAN);
        luceneSqrScorer = luceneScoreSupplier(values, VectorSimilarityFunction.EUCLIDEAN).scorer();
        luceneSqrScorer.setScoringOrdinal(0);

        nativeDotScorer = factory.getFloat32VectorScorerSupplier(DOT_PRODUCT, in, values).get().scorer();
        nativeDotScorer.setScoringOrdinal(0);
        nativeSqrScorer = factory.getFloat32VectorScorerSupplier(EUCLIDEAN, in, values).get().scorer();
        nativeSqrScorer.setScoringOrdinal(0);

        // setup for getFloat32VectorScorer / query vector scoring
        float[] queryVec = new float[dims];
        for (int i = 0; i < dims; i++) {
            queryVec[i] = ThreadLocalRandom.current().nextFloat();
        }
        luceneDotScorerQuery = luceneScorer(values, VectorSimilarityFunction.DOT_PRODUCT, queryVec);
        nativeDotScorerQuery = factory.getFloat32VectorScorer(VectorSimilarityFunction.DOT_PRODUCT, values, queryVec).get();
        luceneSqrScorerQuery = luceneScorer(values, VectorSimilarityFunction.EUCLIDEAN, queryVec);
        nativeSqrScorerQuery = factory.getFloat32VectorScorer(VectorSimilarityFunction.EUCLIDEAN, values, queryVec).get();
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(dir, in);
    }

    // we score against two different ords to avoid the lastOrd cache in vector values
    @Benchmark
    public float dotProductLucene() throws IOException {
        return luceneDotScorer.score(1) + luceneDotScorer.score(2);
    }

    @Benchmark
    public float dotProductNative() throws IOException {
        return nativeDotScorer.score(1) + nativeDotScorer.score(2);
    }

    @Benchmark
    public float dotProductScalar() {
        return dotProductScalarImpl(vec1, vec2) + dotProductScalarImpl(vec1, vec3);
    }

    @Benchmark
    public float dotProductLuceneQuery() throws IOException {
        return luceneDotScorerQuery.score(1) + luceneDotScorerQuery.score(2);
    }

    @Benchmark
    public float dotProductNativeQuery() throws IOException {
        return nativeDotScorerQuery.score(1) + nativeDotScorerQuery.score(2);
    }

    // -- square distance

    @Benchmark
    public float squareDistanceLucene() throws IOException {
        return luceneSqrScorer.score(1) + luceneSqrScorer.score(2);
    }

    @Benchmark
    public float squareDistanceNative() throws IOException {
        return nativeSqrScorer.score(1) + nativeSqrScorer.score(2);
    }

    @Benchmark
    public float squareDistanceScalar() {
        return squareDistanceScalarImpl(vec1, vec2) + squareDistanceScalarImpl(vec1, vec3);
    }

    @Benchmark
    public float squareDistanceLuceneQuery() throws IOException {
        return luceneSqrScorerQuery.score(1) + luceneSqrScorerQuery.score(2);
    }

    @Benchmark
    public float squareDistanceNativeQuery() throws IOException {
        return nativeSqrScorerQuery.score(1) + nativeSqrScorerQuery.score(2);
    }

    static float dotProductScalarImpl(float[] vec1, float[] vec2) {
        float dot = 0;
        for (int i = 0; i < vec1.length; i++) {
            dot += vec1[i] * vec2[i];
        }
        return Math.max((1 + dot) / 2, 0);
    }

    static float squareDistanceScalarImpl(float[] vec1, float[] vec2) {
        float dst = 0;
        for (int i = 0; i < vec1.length; i++) {
            float diff = vec1[i] - vec2[i];
            dst += diff * diff;
        }
        return 1 / (1f + dst);
    }

    FloatVectorValues vectorValues(int dims, int size, IndexInput in, VectorSimilarityFunction sim) throws IOException {
        var slice = in.slice("values", 0, in.length());
        var byteSize = dims * Float.BYTES;
        return new OffHeapFloatVectorValues.DenseOffHeapVectorValues(dims, size, slice, byteSize, DefaultFlatVectorScorer.INSTANCE, sim);
    }

    RandomVectorScorerSupplier luceneScoreSupplier(FloatVectorValues values, VectorSimilarityFunction sim) throws IOException {
        return DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(sim, values);
    }

    RandomVectorScorer luceneScorer(FloatVectorValues values, VectorSimilarityFunction sim, float[] queryVec) throws IOException {
        return DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorer(sim, values, queryVec);
    }

    static void writeFloat32Vectors(IndexOutput out, float[]... vectors) throws IOException {
        var buffer = ByteBuffer.allocate(vectors[0].length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (var v : vectors) {
            buffer.asFloatBuffer().put(v);
            out.writeBytes(buffer.array(), buffer.array().length);
        }
    }

    static float[] randomFloatArray(int length) {
        var random = ThreadLocalRandom.current();
        float[] fa = new float[length];
        for (int i = 0; i < length; i++) {
            fa[i] = random.nextFloat();
        }
        return fa;
    }
}
