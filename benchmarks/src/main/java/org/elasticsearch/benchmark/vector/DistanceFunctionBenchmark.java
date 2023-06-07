/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.vector;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.script.field.vectors.BinaryDenseVector;
import org.elasticsearch.script.field.vectors.ByteBinaryDenseVector;
import org.elasticsearch.script.field.vectors.ByteKnnDenseVector;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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

    @Param({ "float", "byte" })
    private String element;

    @Param({ "96" })
    private int dims;

    @Param({ "dot", "cosine", "l1", "l2" })
    private String function;

    @Param({ "knn", "binary" })
    private String type;

    private abstract static class BenchmarkFunction {

        final int dims;

        private BenchmarkFunction(int dims) {
            this.dims = dims;
        }

        abstract void execute(Consumer<Object> consumer);
    }

    private abstract static class KnnFloatBenchmarkFunction extends BenchmarkFunction {

        final float[] docVector;
        final float[] queryVector;

        private KnnFloatBenchmarkFunction(int dims, boolean normalize) {
            super(dims);

            docVector = new float[dims];
            queryVector = new float[dims];

            float docMagnitude = 0f;
            float queryMagnitude = 0f;

            for (int i = 0; i < dims; ++i) {
                docVector[i] = (float) (dims - i);
                queryVector[i] = (float) i;

                docMagnitude += (float) (dims - i);
                queryMagnitude += (float) i;
            }

            docMagnitude /= dims;
            queryMagnitude /= dims;

            if (normalize) {
                for (int i = 0; i < dims; ++i) {
                    docVector[i] /= docMagnitude;
                    queryVector[i] /= queryMagnitude;
                }
            }
        }
    }

    private abstract static class BinaryFloatBenchmarkFunction extends BenchmarkFunction {

        final BytesRef docVector;
        final float[] docFloatVector;
        final float[] queryVector;

        private BinaryFloatBenchmarkFunction(int dims, boolean normalize) {
            super(dims);

            docFloatVector = new float[dims];
            queryVector = new float[dims];

            float docMagnitude = 0f;
            float queryMagnitude = 0f;

            for (int i = 0; i < dims; ++i) {
                docFloatVector[i] = (float) (dims - i);
                queryVector[i] = (float) i;

                docMagnitude += (float) (dims - i);
                queryMagnitude += (float) i;
            }

            docMagnitude /= dims;
            queryMagnitude /= dims;

            ByteBuffer byteBuffer = ByteBuffer.allocate(dims * 4 + 4);

            for (int i = 0; i < dims; ++i) {
                if (normalize) {
                    docFloatVector[i] /= docMagnitude;
                    queryVector[i] /= queryMagnitude;
                }

                byteBuffer.putFloat(docFloatVector[i]);
            }

            byteBuffer.putFloat(docMagnitude);
            this.docVector = new BytesRef(byteBuffer.array());
        }
    }

    private abstract static class KnnByteBenchmarkFunction extends BenchmarkFunction {

        final byte[] docVector;
        final byte[] queryVector;

        final float queryMagnitude;

        private KnnByteBenchmarkFunction(int dims) {
            super(dims);

            ByteBuffer docVector = ByteBuffer.allocate(dims);
            queryVector = new byte[dims];

            float queryMagnitude = 0f;

            for (int i = 0; i < dims; ++i) {
                docVector.put((byte) (dims - i));
                queryVector[i] = (byte) i;

                queryMagnitude += (float) i;
            }

            this.docVector = docVector.array();
            this.queryMagnitude = queryMagnitude / dims;
        }
    }

    private abstract static class BinaryByteBenchmarkFunction extends BenchmarkFunction {

        final BytesRef docVector;
        final byte[] vectorValue;
        final byte[] queryVector;

        final float queryMagnitude;

        private BinaryByteBenchmarkFunction(int dims) {
            super(dims);

            ByteBuffer docVector = ByteBuffer.allocate(dims + 4);
            queryVector = new byte[dims];
            vectorValue = new byte[dims];

            float docMagnitude = 0f;
            float queryMagnitude = 0f;

            for (int i = 0; i < dims; ++i) {
                docVector.put((byte) (dims - i));
                vectorValue[i] = (byte) (dims - i);
                queryVector[i] = (byte) i;

                docMagnitude += (float) (dims - i);
                queryMagnitude += (float) i;
            }

            docVector.putFloat(docMagnitude / dims);
            this.docVector = new BytesRef(docVector.array());
            this.queryMagnitude = queryMagnitude / dims;

        }
    }

    private static class DotKnnFloatBenchmarkFunction extends KnnFloatBenchmarkFunction {

        private DotKnnFloatBenchmarkFunction(int dims) {
            super(dims, false);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new KnnDenseVector(docVector).dotProduct(queryVector);
        }
    }

    private static class DotKnnByteBenchmarkFunction extends KnnByteBenchmarkFunction {

        private DotKnnByteBenchmarkFunction(int dims) {
            super(dims);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new ByteKnnDenseVector(docVector).dotProduct(queryVector);
        }
    }

    private static class DotBinaryFloatBenchmarkFunction extends BinaryFloatBenchmarkFunction {

        private DotBinaryFloatBenchmarkFunction(int dims) {
            super(dims, false);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new BinaryDenseVector(docFloatVector, docVector, dims, Version.CURRENT).dotProduct(queryVector);
        }
    }

    private static class DotBinaryByteBenchmarkFunction extends BinaryByteBenchmarkFunction {

        private DotBinaryByteBenchmarkFunction(int dims) {
            super(dims);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new ByteBinaryDenseVector(vectorValue, docVector, dims).dotProduct(queryVector);
        }
    }

    private static class CosineKnnFloatBenchmarkFunction extends KnnFloatBenchmarkFunction {

        private CosineKnnFloatBenchmarkFunction(int dims) {
            super(dims, true);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new KnnDenseVector(docVector).cosineSimilarity(queryVector, false);
        }
    }

    private static class CosineKnnByteBenchmarkFunction extends KnnByteBenchmarkFunction {

        private CosineKnnByteBenchmarkFunction(int dims) {
            super(dims);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new ByteKnnDenseVector(docVector).cosineSimilarity(queryVector, queryMagnitude);
        }
    }

    private static class CosineBinaryFloatBenchmarkFunction extends BinaryFloatBenchmarkFunction {

        private CosineBinaryFloatBenchmarkFunction(int dims) {
            super(dims, true);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new BinaryDenseVector(docFloatVector, docVector, dims, Version.CURRENT).cosineSimilarity(queryVector, false);
        }
    }

    private static class CosineBinaryByteBenchmarkFunction extends BinaryByteBenchmarkFunction {

        private CosineBinaryByteBenchmarkFunction(int dims) {
            super(dims);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new ByteBinaryDenseVector(vectorValue, docVector, dims).cosineSimilarity(queryVector, queryMagnitude);
        }
    }

    private static class L1KnnFloatBenchmarkFunction extends KnnFloatBenchmarkFunction {

        private L1KnnFloatBenchmarkFunction(int dims) {
            super(dims, false);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new KnnDenseVector(docVector).l1Norm(queryVector);
        }
    }

    private static class L1KnnByteBenchmarkFunction extends KnnByteBenchmarkFunction {

        private L1KnnByteBenchmarkFunction(int dims) {
            super(dims);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new ByteKnnDenseVector(docVector).l1Norm(queryVector);
        }
    }

    private static class L1BinaryFloatBenchmarkFunction extends BinaryFloatBenchmarkFunction {

        private L1BinaryFloatBenchmarkFunction(int dims) {
            super(dims, true);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new BinaryDenseVector(docFloatVector, docVector, dims, Version.CURRENT).l1Norm(queryVector);
        }
    }

    private static class L1BinaryByteBenchmarkFunction extends BinaryByteBenchmarkFunction {

        private L1BinaryByteBenchmarkFunction(int dims) {
            super(dims);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new ByteBinaryDenseVector(vectorValue, docVector, dims).l1Norm(queryVector);
        }
    }

    private static class L2KnnFloatBenchmarkFunction extends KnnFloatBenchmarkFunction {

        private L2KnnFloatBenchmarkFunction(int dims) {
            super(dims, false);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new KnnDenseVector(docVector).l2Norm(queryVector);
        }
    }

    private static class L2KnnByteBenchmarkFunction extends KnnByteBenchmarkFunction {

        private L2KnnByteBenchmarkFunction(int dims) {
            super(dims);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new ByteKnnDenseVector(docVector).l2Norm(queryVector);
        }
    }

    private static class L2BinaryFloatBenchmarkFunction extends BinaryFloatBenchmarkFunction {

        private L2BinaryFloatBenchmarkFunction(int dims) {
            super(dims, true);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            new BinaryDenseVector(docFloatVector, docVector, dims, Version.CURRENT).l1Norm(queryVector);
        }
    }

    private static class L2BinaryByteBenchmarkFunction extends BinaryByteBenchmarkFunction {

        private L2BinaryByteBenchmarkFunction(int dims) {
            super(dims);
        }

        @Override
        public void execute(Consumer<Object> consumer) {
            consumer.accept(new ByteBinaryDenseVector(vectorValue, docVector, dims).l2Norm(queryVector));
        }
    }

    private BenchmarkFunction benchmarkFunction;

    @Setup
    public void setBenchmarkFunction() {
        switch (element) {
            case "float" -> {
                switch (function) {
                    case "dot" -> benchmarkFunction = switch (type) {
                        case "knn" -> new DotKnnFloatBenchmarkFunction(dims);
                        case "binary" -> new DotBinaryFloatBenchmarkFunction(dims);
                        default -> throw new UnsupportedOperationException("unexpected type [" + type + "]");
                    };
                    case "cosine" -> benchmarkFunction = switch (type) {
                        case "knn" -> new CosineKnnFloatBenchmarkFunction(dims);
                        case "binary" -> new CosineBinaryFloatBenchmarkFunction(dims);
                        default -> throw new UnsupportedOperationException("unexpected type [" + type + "]");
                    };
                    case "l1" -> benchmarkFunction = switch (type) {
                        case "knn" -> new L1KnnFloatBenchmarkFunction(dims);
                        case "binary" -> new L1BinaryFloatBenchmarkFunction(dims);
                        default -> throw new UnsupportedOperationException("unexpected type [" + type + "]");
                    };
                    case "l2" -> benchmarkFunction = switch (type) {
                        case "knn" -> new L2KnnFloatBenchmarkFunction(dims);
                        case "binary" -> new L2BinaryFloatBenchmarkFunction(dims);
                        default -> throw new UnsupportedOperationException("unexpected type [" + type + "]");
                    };
                    default -> throw new UnsupportedOperationException("unexpected function [" + function + "]");
                }
            }
            case "byte" -> {
                switch (function) {
                    case "dot" -> benchmarkFunction = switch (type) {
                        case "knn" -> new DotKnnByteBenchmarkFunction(dims);
                        case "binary" -> new DotBinaryByteBenchmarkFunction(dims);
                        default -> throw new UnsupportedOperationException("unexpected type [" + type + "]");
                    };
                    case "cosine" -> benchmarkFunction = switch (type) {
                        case "knn" -> new CosineKnnByteBenchmarkFunction(dims);
                        case "binary" -> new CosineBinaryByteBenchmarkFunction(dims);
                        default -> throw new UnsupportedOperationException("unexpected type [" + type + "]");
                    };
                    case "l1" -> benchmarkFunction = switch (type) {
                        case "knn" -> new L1KnnByteBenchmarkFunction(dims);
                        case "binary" -> new L1BinaryByteBenchmarkFunction(dims);
                        default -> throw new UnsupportedOperationException("unexpected type [" + type + "]");
                    };
                    case "l2" -> benchmarkFunction = switch (type) {
                        case "knn" -> new L2KnnByteBenchmarkFunction(dims);
                        case "binary" -> new L2BinaryByteBenchmarkFunction(dims);
                        default -> throw new UnsupportedOperationException("unexpected type [" + type + "]");
                    };
                    default -> throw new UnsupportedOperationException("unexpected function [" + function + "]");
                }
            }
            default -> throw new UnsupportedOperationException("unexpected element [" + element + "]");
        }
        ;
    }

    @Benchmark
    public void benchmark() throws IOException {
        for (int i = 0; i < 25000; ++i) {
            benchmarkFunction.execute(Object::toString);
        }
    }
}
