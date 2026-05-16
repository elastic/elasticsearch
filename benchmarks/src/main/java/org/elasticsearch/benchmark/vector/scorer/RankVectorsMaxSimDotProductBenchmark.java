/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.script.field.vectors.BFloat16RankVectors;
import org.elasticsearch.script.field.vectors.ByteRankVectors;
import org.elasticsearch.script.field.vectors.FloatRankVectors;
import org.elasticsearch.script.field.vectors.VectorIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(value = 1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class RankVectorsMaxSimDotProductBenchmark {
    static {
        Utils.configureBenchmarkLogging();
    }

    public enum RankVectorType {
        FLOAT,
        BYTE,
        BFLOAT16
    }

    @Param({ "FLOAT", "BYTE", "BFLOAT16" })
    public RankVectorType type;

    @Param({ "128" })
    public int dims;

    @Param({ "256" })
    public int numDocVectors;

    @Param({ "16" })
    public int numQueryVectors;

    private float[][] floatDocVectors;
    private float[][] bfloat16DocVectors;
    private float[][] floatQueryVectors;
    private byte[][] byteDocVectors;
    private byte[][] byteQueryVectors;

    private FloatRankVectors floatRankVectors;
    private ByteRankVectors byteRankVectors;
    private BFloat16RankVectors bfloat16RankVectors;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(0x5EEDL);
        floatDocVectors = new float[numDocVectors][dims];
        bfloat16DocVectors = new float[numDocVectors][dims];
        floatQueryVectors = new float[numQueryVectors][dims];
        byteDocVectors = new byte[numDocVectors][dims];
        byteQueryVectors = new byte[numQueryVectors][dims];
        for (int i = 0; i < numDocVectors; i++) {
            for (int d = 0; d < dims; d++) {
                floatDocVectors[i][d] = random.nextFloat() * 2f - 1f;
                bfloat16DocVectors[i][d] = BFloat16.truncateToBFloat16(floatDocVectors[i][d]);
            }
            random.nextBytes(byteDocVectors[i]);
        }
        for (int i = 0; i < numQueryVectors; i++) {
            for (int d = 0; d < dims; d++) {
                floatQueryVectors[i][d] = random.nextFloat() * 2f - 1f;
            }
            random.nextBytes(byteQueryVectors[i]);
        }

        BytesRef magnitudes = new BytesRef(new byte[numDocVectors * Float.BYTES]);
        floatRankVectors = new FloatRankVectors(
            VectorIterator.from(floatDocVectors),
            magnitudes,
            numDocVectors,
            dims,
            encodeFloatDocVectors(floatDocVectors)
        );
        byteRankVectors = new ByteRankVectors(
            VectorIterator.from(byteDocVectors),
            magnitudes,
            numDocVectors,
            dims,
            encodeByteDocVectors(byteDocVectors)
        );
        bfloat16RankVectors = new BFloat16RankVectors(
            VectorIterator.from(bfloat16DocVectors),
            magnitudes,
            numDocVectors,
            dims,
            encodeBFloat16DocVectors(bfloat16DocVectors)
        );
    }

    @Benchmark
    public float defaultPath() {
        return switch (type) {
            case FLOAT -> floatRankVectors.maxSimDotProduct(floatQueryVectors);
            case BYTE -> byteRankVectors.maxSimDotProduct(byteQueryVectors);
            case BFLOAT16 -> bfloat16RankVectors.maxSimDotProduct(floatQueryVectors);
        };
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public float panamaPath() {
        return switch (type) {
            case FLOAT -> floatRankVectors.maxSimDotProduct(floatQueryVectors);
            case BYTE -> byteRankVectors.maxSimDotProduct(byteQueryVectors);
            case BFLOAT16 -> bfloat16RankVectors.maxSimDotProduct(floatQueryVectors);
        };
    }

    private static BytesRef encodeFloatDocVectors(float[][] vectors) {
        int dims = vectors[0].length;
        ByteBuffer buffer = ByteBuffer.allocate(vectors.length * dims * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        var floatBuffer = buffer.asFloatBuffer();
        for (float[] vector : vectors) {
            floatBuffer.put(vector);
        }
        return new BytesRef(buffer.array());
    }

    private static BytesRef encodeByteDocVectors(byte[][] vectors) {
        int dims = vectors[0].length;
        byte[] bytes = new byte[vectors.length * dims];
        int offset = 0;
        for (byte[] vector : vectors) {
            System.arraycopy(vector, 0, bytes, offset, dims);
            offset += dims;
        }
        return new BytesRef(bytes);
    }

    private static BytesRef encodeBFloat16DocVectors(float[][] vectors) {
        int dims = vectors[0].length;
        byte[] buffer = new byte[vectors.length * dims * BFloat16.BYTES];
        for (int i = 0; i < vectors.length; i++) {
            BFloat16.floatToBFloat16(vectors[i], 0, buffer, i * dims * BFloat16.BYTES, dims, ByteOrder.LITTLE_ENDIAN);
        }
        return new BytesRef(buffer);
    }
}
