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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.script.field.vectors.ByteRankVectors;
import org.elasticsearch.script.field.vectors.FloatRankVectors;
import org.elasticsearch.script.field.vectors.VectorIterator;
import org.elasticsearch.simdvec.ESVectorUtil;
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
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
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
        BYTE
    }

    @Param({ "FLOAT", "BYTE" })
    public RankVectorType type;

    @Param({ "128" })
    public int dims;

    @Param({ "256" })
    public int numDocVectors;

    @Param({ "16" })
    public int numQueryVectors;

    private float[][] floatDocVectors;
    private float[][] floatQueryVectors;
    private byte[][] byteDocVectors;
    private byte[][] byteQueryVectors;

    private FloatRankVectors floatRankVectorsPanama;
    private FloatRankVectors floatRankVectorsDefault;
    private ByteRankVectors byteRankVectorsPanama;
    private ByteRankVectors byteRankVectorsDefault;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(0x5EEDL);
        floatDocVectors = new float[numDocVectors][dims];
        floatQueryVectors = new float[numQueryVectors][dims];
        byteDocVectors = new byte[numDocVectors][dims];
        byteQueryVectors = new byte[numQueryVectors][dims];
        for (int i = 0; i < numDocVectors; i++) {
            for (int d = 0; d < dims; d++) {
                floatDocVectors[i][d] = random.nextFloat() * 2f - 1f;
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
        floatRankVectorsPanama = new FloatRankVectors(
            VectorIterator.from(floatDocVectors),
            magnitudes,
            numDocVectors,
            dims,
            encodeFloatDocVectors(floatDocVectors),
            ElementType.FLOAT
        );
        floatRankVectorsDefault = new FloatRankVectors(VectorIterator.from(floatDocVectors), magnitudes, numDocVectors, dims);
        byteRankVectorsPanama = new ByteRankVectors(
            VectorIterator.from(byteDocVectors),
            magnitudes,
            numDocVectors,
            dims,
            encodeByteDocVectors(byteDocVectors),
            ElementType.BYTE
        );
        byteRankVectorsDefault = new ByteRankVectors(VectorIterator.from(byteDocVectors), magnitudes, numDocVectors, dims);

        verifyCorrectness();
    }

    @Benchmark
    public float defaultPath() {
        return switch (type) {
            case FLOAT -> floatRankVectorsDefault.maxSimDotProduct(floatQueryVectors);
            case BYTE -> byteRankVectorsDefault.maxSimDotProduct(byteQueryVectors);
        };
    }

    @Benchmark
    public float panamaPath() {
        return switch (type) {
            case FLOAT -> floatRankVectorsPanama.maxSimDotProduct(floatQueryVectors);
            case BYTE -> byteRankVectorsPanama.maxSimDotProduct(byteQueryVectors);
        };
    }

    private float oldWayFloat() {
        float[] maxes = new float[floatQueryVectors.length];
        Arrays.fill(maxes, Float.NEGATIVE_INFINITY);
        for (float[] doc : floatDocVectors) {
            for (int i = 0; i < floatQueryVectors.length; i++) {
                maxes[i] = Math.max(maxes[i], ESVectorUtil.dotProduct(floatQueryVectors[i], doc));
            }
        }
        return sum(maxes);
    }

    private float oldWayByte() {
        float[] maxes = new float[byteQueryVectors.length];
        Arrays.fill(maxes, Float.NEGATIVE_INFINITY);
        for (byte[] doc : byteDocVectors) {
            for (int i = 0; i < byteQueryVectors.length; i++) {
                maxes[i] = Math.max(maxes[i], ESVectorUtil.dotProduct(byteQueryVectors[i], doc));
            }
        }
        return sum(maxes);
    }

    private void verifyCorrectness() {
        float scalarScore = switch (type) {
            case FLOAT -> oldWayFloat();
            case BYTE -> oldWayByte();
        };
        float defaultScore = defaultPath();
        float panamaScore = panamaPath();
        if (Math.abs(scalarScore - defaultScore) > 1e-3f) {
            throw new IllegalStateException("defaultPath and scalar differ: scalar=" + scalarScore + ", defaultPath=" + defaultScore);
        }
        if (Math.abs(scalarScore - panamaScore) > 1e-3f) {
            throw new IllegalStateException("panamaPath and scalar differ: scalar=" + scalarScore + ", panamaPath=" + panamaScore);
        }
    }

    private static float sum(float[] values) {
        float sum = 0f;
        for (float value : values) {
            sum += value;
        }
        return sum;
    }

    private static BytesRef encodeFloatDocVectors(float[][] vectors) {
        int dims = vectors[0].length;
        ByteBuffer buffer = ByteBuffer.allocate(vectors.length * dims * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (float[] vector : vectors) {
            for (float value : vector) {
                buffer.putFloat(value);
            }
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
}
