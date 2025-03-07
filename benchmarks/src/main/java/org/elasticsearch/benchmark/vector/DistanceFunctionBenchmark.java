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

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Various benchmarks for the distance functions
 * used by indexed and non-indexed vectors.
 * Parameters include element, dims, function, and type.
 * For individual local tests it may be useful to increase
 * fork, measurement, and operations per invocation. (Note
 * to also update the benchmark loop if operations per invocation
 * is increased.)
 */
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(25000)
@State(Scope.Benchmark)
public class DistanceFunctionBenchmark {

    static {
        LogConfigurator.configureESLogging();
    }

    @Param({ "float", "byte" })
    private String docType;

    @Param({ "float", "byte" })
    private String queryType;

    @Param({ "96" })
    private int dims;

    @Param({ "dot", "cosine", "l1", "l2", "hamming" })
    private String function;

    @Param({ "knn", "binary" })
    private String type;

    private Runnable benchmarkImpl;

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
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * 4 + 4);
        for (float f : vector) {
            buffer.putFloat(f);
        }
        buffer.putFloat(mag);
        return new BytesRef(buffer.array());
    }

    private static BytesRef generateVectorData(byte[] vector) {
        return new BytesRef(vector);
    }

    @Setup
    public void findBenchmarkImpl() {
        Random r = new Random();

        float[] floatDocVector = new float[dims];
        byte[] byteDocVector = new byte[dims];

        float[] floatQueryVector = new float[dims];
        byte[] byteQueryVector = new byte[dims];

        r.nextBytes(byteDocVector);
        r.nextBytes(byteQueryVector);
        for (int i = 0; i < dims; i++) {
            floatDocVector[i] = r.nextFloat();
            floatQueryVector[i] = r.nextFloat();
        }

        DenseVector vectorImpl = switch (docType) {
            case "float" -> switch (type) {
                case "knn" -> {
                    if (function.equals("cosine")) {
                        normalizeVector(floatDocVector);
                        normalizeVector(floatQueryVector);
                    }
                    yield new KnnDenseVector(floatDocVector);
                }
                case "binary" -> {
                    BytesRef vectorData;
                    if (function.equals("cosine") || function.equals("l1") || function.equals("l2")) {
                        float mag = normalizeVector(floatDocVector);
                        vectorData = generateVectorData(floatDocVector, mag);
                        normalizeVector(floatQueryVector);
                    } else {
                        vectorData = generateVectorData(floatDocVector);
                    }
                    yield new BinaryDenseVector(floatDocVector, vectorData, dims, IndexVersion.current());
                }
                default -> throw new UnsupportedOperationException("Unexpected type " + type);
            };
            case "byte" -> switch (type) {
                case "knn" -> new ByteKnnDenseVector(byteDocVector);
                case "binary" -> {
                    BytesRef vectorData = generateVectorData(byteDocVector);
                    yield new ByteBinaryDenseVector(byteDocVector, vectorData, dims);
                }
                default -> throw new UnsupportedOperationException("Unexpected type " + type);
            };
            default -> throw new UnsupportedOperationException("Unexpected docType " + docType);
        };

        benchmarkImpl = switch (queryType) {
            case "float" -> switch (function) {
                case "dot" -> () -> vectorImpl.dotProduct(floatQueryVector);
                case "cosine" -> () -> vectorImpl.cosineSimilarity(floatQueryVector, false);
                case "l1" -> () -> vectorImpl.l1Norm(floatQueryVector);
                case "l2" -> () -> vectorImpl.l2Norm(floatQueryVector);
                default -> throw new UnsupportedOperationException("Unexpected function " + function);
            };
            case "byte" -> switch (function) {
                case "dot" -> () -> vectorImpl.dotProduct(byteQueryVector);
                case "cosine" -> {
                    float mag = calculateMag(byteQueryVector);
                    yield () -> vectorImpl.cosineSimilarity(byteQueryVector, mag);
                }
                case "l1" -> () -> vectorImpl.l1Norm(byteQueryVector);
                case "l2" -> () -> vectorImpl.l2Norm(byteQueryVector);
                case "hamming" -> () -> vectorImpl.hamming(byteQueryVector);
                default -> throw new UnsupportedOperationException("Unexpected function " + function);
            };
            default -> throw new UnsupportedOperationException("Unexpected queryType " + queryType);
        };
    }

    @Benchmark
    public void benchmark() {
        for (int i = 0; i < 25000; ++i) {
            benchmarkImpl.run();
        }
    }
}
