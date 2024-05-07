/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.vector;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.quantization.ScalarQuantizedVectorSimilarity;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.vec.VectorScorer;
import org.elasticsearch.vec.VectorScorerFactory;
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
import java.nio.file.Files;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.vec.VectorSimilarityType.DOT_PRODUCT;
import static org.elasticsearch.vec.VectorSimilarityType.EUCLIDEAN;

@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
/**
 * Benchmark that compares various scalar quantized vector similarity function
 * implementations;: scalar, lucene's panama-ized, and Elasticsearch's native.
 * Run with ./gradlew -p benchmarks run --args 'VectorScorerBenchmark'
 */
public class VectorScorerBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Param({ "96", "768", "1024" })
    int dims;
    int size = 2; // there are only two vectors to compare

    Directory dir;
    IndexInput in;
    VectorScorerFactory factory;

    byte[] vec1;
    byte[] vec2;
    float vec1Offset;
    float vec2Offset;
    float scoreCorrectionConstant;

    ScalarQuantizedVectorSimilarity luceneDotScorer;
    ScalarQuantizedVectorSimilarity luceneSqrScorer;
    VectorScorer nativeDotScorer;
    VectorScorer nativeSqrScorer;

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
        scoreCorrectionConstant = 1f;
        vec1 = new byte[dims];
        vec2 = new byte[dims];

        randomInt7BytesBetween(vec1);
        randomInt7BytesBetween(vec2);
        vec1Offset = ThreadLocalRandom.current().nextFloat();
        vec2Offset = ThreadLocalRandom.current().nextFloat();

        dir = new MMapDirectory(Files.createTempDirectory("nativeScalarQuantBench"));
        try (IndexOutput out = dir.createOutput("vector.data", IOContext.DEFAULT)) {
            out.writeBytes(vec1, 0, vec1.length);
            out.writeInt(Float.floatToIntBits(vec1Offset));
            out.writeBytes(vec2, 0, vec2.length);
            out.writeInt(Float.floatToIntBits(vec2Offset));
        }
        in = dir.openInput("vector.data", IOContext.DEFAULT);

        luceneDotScorer = ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
            VectorSimilarityFunction.DOT_PRODUCT,
            scoreCorrectionConstant
        );
        luceneSqrScorer = ScalarQuantizedVectorSimilarity.fromVectorSimilarity(VectorSimilarityFunction.EUCLIDEAN, scoreCorrectionConstant);
        nativeDotScorer = factory.getInt7ScalarQuantizedVectorScorer(dims, size, scoreCorrectionConstant, DOT_PRODUCT, in).get();
        nativeSqrScorer = factory.getInt7ScalarQuantizedVectorScorer(dims, size, scoreCorrectionConstant, EUCLIDEAN, in).get();

        // sanity
        var f1 = dotProductLucene();
        var f2 = dotProductNative();
        var f3 = dotProductScalar();
        if (f1 != f2) {
            throw new AssertionError("lucene[" + f1 + "] != " + "native[" + f2 + "]");
        }
        if (f1 != f3) {
            throw new AssertionError("lucene[" + f1 + "] != " + "scalar[" + f3 + "]");
        }
        // square distance
        f1 = squareDistanceLucene();
        f2 = squareDistanceNative();
        f3 = squareDistanceScalar();
        if (f1 != f2) {
            throw new AssertionError("lucene[" + f1 + "] != " + "native[" + f2 + "]");
        }
        if (f1 != f3) {
            throw new AssertionError("lucene[" + f1 + "] != " + "scalar[" + f3 + "]");
        }
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(dir, in);
    }

    @Benchmark
    public float dotProductLucene() {
        return luceneDotScorer.score(vec1, vec1Offset, vec2, vec2Offset);
    }

    @Benchmark
    public float dotProductNative() throws IOException {
        return nativeDotScorer.score(0, 1);
    }

    @Benchmark
    public float dotProductScalar() {
        int dotProduct = 0;
        for (int i = 0; i < vec1.length; i++) {
            dotProduct += vec1[i] * vec2[i];
        }
        float adjustedDistance = dotProduct * scoreCorrectionConstant + vec1Offset + vec2Offset;
        return (1 + adjustedDistance) / 2;
    }

    // -- square distance

    @Benchmark
    public float squareDistanceLucene() {
        return luceneSqrScorer.score(vec1, vec1Offset, vec2, vec2Offset);
    }

    @Benchmark
    public float squareDistanceNative() throws IOException {
        return nativeSqrScorer.score(0, 1);
    }

    @Benchmark
    public float squareDistanceScalar() {
        int squareDistance = 0;
        for (int i = 0; i < vec1.length; i++) {
            int diff = vec1[i] - vec2[i];
            squareDistance += diff * diff;
        }
        float adjustedDistance = squareDistance * scoreCorrectionConstant;
        return 1 / (1f + adjustedDistance);
    }

    // Unsigned int7 byte vectors have values in the range of 0 to 127 (inclusive).
    static final byte MIN_INT7_VALUE = 0;
    static final byte MAX_INT7_VALUE = 127;

    static void randomInt7BytesBetween(byte[] bytes) {
        var random = ThreadLocalRandom.current();
        for (int i = 0, len = bytes.length; i < len;) {
            bytes[i++] = (byte) random.nextInt(MIN_INT7_VALUE, MAX_INT7_VALUE + 1);
        }
    }
}
