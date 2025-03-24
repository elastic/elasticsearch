/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.script.field.vectors.BinaryDenseVector;
import org.elasticsearch.script.field.vectors.BitBinaryDenseVector;
import org.elasticsearch.script.field.vectors.BitKnnDenseVector;
import org.elasticsearch.script.field.vectors.ByteBinaryDenseVector;
import org.elasticsearch.script.field.vectors.ByteKnnDenseVector;
import org.elasticsearch.script.field.vectors.DenseVector;
import org.elasticsearch.script.field.vectors.KnnDenseVector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

/**
 * Various benchmarks for the distance functions used by indexed and non-indexed vectors.
 * Parameters include doc and query type, dims, function, and implementation.
 * For individual local tests it may be useful to increase
 * fork, measurement, and operations per invocation.
 */
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(DistanceFunctionBenchmark.OPERATIONS)
@State(Scope.Benchmark)
public class DistanceFunctionBenchmark {

    public static final int OPERATIONS = 25000;

    static {
        LogConfigurator.configureESLogging();
    }

    public enum VectorType {
        FLOAT,
        BYTE,
        BIT
    }

    public enum Function {
        DOT,
        COSINE,
        L1,
        L2,
        HAMMING
    }

    public enum Implementation {
        KNN,
        BINARY
    }

    @Param
    private VectorType docType;

    @Param
    private VectorType queryType;

    @Param({ "1024" })
    private int dims;

    @Param
    private Function function;

    @Param
    private Implementation type;

    private DoubleSupplier benchmarkImpl;

    private static float calculateMag(float[] vector) {
        float mag = 0;
        for (float f : vector) {
            mag += f * f;
        }
        return (float) Math.sqrt(mag);
    }

    private static float calculateMag(byte[] vector) {
        float mag = 0;
        for (byte b : vector) {
            mag += b * b;
        }
        return (float) Math.sqrt(mag);
    }

    private static float normalizeVector(float[] vector) {
        float mag = calculateMag(vector);
        for (int i = 0; i < vector.length; i++) {
            vector[i] /= mag;
        }
        return mag;
    }

    private static BytesRef generateVectorData(float[] vector) {
        return generateVectorData(vector, calculateMag(vector));
    }

    private static BytesRef generateVectorData(float[] vector, float mag) {
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * Float.BYTES + Float.BYTES);
        for (float f : vector) {
            buffer.putFloat(f);
        }
        buffer.putFloat(mag);
        return new BytesRef(buffer.array());
    }

    private static BytesRef generateVectorData(byte[] vector) {
        float mag = calculateMag(vector);

        ByteBuffer buffer = ByteBuffer.allocate(vector.length + Float.BYTES);
        buffer.put(vector);
        buffer.putFloat(mag);
        return new BytesRef(buffer.array());
    }

    @Setup
    public void findBenchmarkImpl() {
        if (dims % 8 != 0) throw new IllegalArgumentException("Dims must be a multiple of 8");
        Random r = new Random();

        float[] floatDocVector = new float[dims];
        byte[] byteDocVector = new byte[dims];
        byte[] bitDocVector = new byte[dims / 8];

        float[] floatQueryVector = new float[dims];
        byte[] byteQueryVector = new byte[dims];
        byte[] bitQueryVector = new byte[dims / 8];

        r.nextBytes(byteDocVector);
        r.nextBytes(bitDocVector);
        r.nextBytes(byteQueryVector);
        r.nextBytes(bitQueryVector);
        for (int i = 0; i < dims; i++) {
            floatDocVector[i] = r.nextFloat();
            floatQueryVector[i] = r.nextFloat();
        }

        DenseVector vectorImpl = switch (docType) {
            case FLOAT -> switch (type) {
                case KNN -> {
                    if (function == Function.COSINE) {
                        normalizeVector(floatDocVector);
                        normalizeVector(floatQueryVector);
                    }
                    yield new KnnDenseVector(floatDocVector);
                }
                case BINARY -> {
                    BytesRef vectorData;
                    if (function == Function.COSINE || function == Function.L1 || function == Function.L2) {
                        float mag = normalizeVector(floatDocVector);
                        vectorData = generateVectorData(floatDocVector, mag);
                        normalizeVector(floatQueryVector);
                    } else {
                        vectorData = generateVectorData(floatDocVector);
                    }
                    yield new BinaryDenseVector(floatDocVector, vectorData, dims, IndexVersion.current());
                }
            };
            case BYTE -> switch (type) {
                case KNN -> new ByteKnnDenseVector(byteDocVector);
                case BINARY -> new ByteBinaryDenseVector(byteDocVector, generateVectorData(byteDocVector), dims);
            };
            case BIT -> switch (type) {
                case KNN -> new BitKnnDenseVector(bitDocVector);
                case BINARY -> new BitBinaryDenseVector(bitDocVector, new BytesRef(bitDocVector), bitDocVector.length);
            };
        };

        benchmarkImpl = switch (queryType) {
            case FLOAT -> switch (function) {
                case DOT -> () -> vectorImpl.dotProduct(floatQueryVector);
                case COSINE -> () -> vectorImpl.cosineSimilarity(floatQueryVector, false);
                case L1 -> () -> vectorImpl.l1Norm(floatQueryVector);
                case L2 -> () -> vectorImpl.l2Norm(floatQueryVector);
                case HAMMING -> throw new UnsupportedOperationException("Unsupported function " + function);
            };
            case BYTE -> switch (function) {
                case DOT -> () -> vectorImpl.dotProduct(byteQueryVector);
                case COSINE -> {
                    float mag = calculateMag(byteQueryVector);
                    yield () -> vectorImpl.cosineSimilarity(byteQueryVector, mag);
                }
                case L1 -> () -> vectorImpl.l1Norm(byteQueryVector);
                case L2 -> () -> vectorImpl.l2Norm(byteQueryVector);
                case HAMMING -> () -> vectorImpl.hamming(byteQueryVector);
            };
            case BIT -> switch (function) {
                case DOT -> () -> vectorImpl.dotProduct(bitQueryVector);
                case COSINE -> throw new UnsupportedOperationException("Unsupported function " + function);
                case L1 -> () -> vectorImpl.l1Norm(bitQueryVector);
                case L2 -> () -> vectorImpl.l2Norm(bitQueryVector);
                case HAMMING -> () -> vectorImpl.hamming(bitQueryVector);
            };
        };
    }

    @Fork(1)
    @Benchmark
    public void benchmark(Blackhole blackhole) {
        for (int i = 0; i < OPERATIONS; ++i) {
            blackhole.consume(benchmarkImpl.getAsDouble());
        }
    }

    @Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    @Benchmark
    public void vectorBenchmark(Blackhole blackhole) {
        for (int i = 0; i < OPERATIONS; ++i) {
            blackhole.consume(benchmarkImpl.getAsDouble());
        }
    }
}
