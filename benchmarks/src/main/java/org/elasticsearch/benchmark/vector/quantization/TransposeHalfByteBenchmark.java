/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.quantization;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.simdvec.ESVectorUtil;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
// first iteration is complete garbage, so make sure we really warmup
@Warmup(iterations = 4, time = 1)
// real iterations. not useful to spend tons of time here, better to fork more
@Measurement(iterations = 5, time = 1)
// engage some noise reduction
@Fork(value = 1)
public class TransposeHalfByteBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Param({ "384", "782", "1024" })
    int dims;

    int length;

    int numVectors = 1000;

    int[][] qVectors;
    byte[] packed;

    @Setup
    public void setup() throws IOException {
        Random random = new Random(123);

        this.length = 4 * BQVectorUtils.discretize(dims, 64) / 8;
        this.packed = new byte[length];

        qVectors = new int[numVectors][dims];
        for (int[] qVector : qVectors) {
            for (int i = 0; i < dims; i++) {
                qVector[i] = random.nextInt(16);
            }
        }
    }

    @Benchmark
    public void transposeHalfByte(Blackhole bh) {
        for (int i = 0; i < numVectors; i++) {
            ESVectorUtil.transposeHalfByte(qVectors[i], packed);
            bh.consume(packed);
        }
    }

    @Benchmark
    public void transposeHalfByteLegacy(Blackhole bh) {
        for (int i = 0; i < numVectors; i++) {
            transposeHalfByteLegacy(qVectors[i], packed);
            bh.consume(packed);
        }
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public void transposeHalfBytePanama(Blackhole bh) {
        for (int i = 0; i < numVectors; i++) {
            ESVectorUtil.transposeHalfByte(qVectors[i], packed);
            bh.consume(packed);
        }
    }

    public static void transposeHalfByteLegacy(int[] q, byte[] quantQueryByte) {
        for (int i = 0; i < q.length;) {
            assert q[i] >= 0 && q[i] <= 15;
            int lowerByte = 0;
            int lowerMiddleByte = 0;
            int upperMiddleByte = 0;
            int upperByte = 0;
            for (int j = 7; j >= 0 && i < q.length; j--) {
                lowerByte |= (q[i] & 1) << j;
                lowerMiddleByte |= ((q[i] >> 1) & 1) << j;
                upperMiddleByte |= ((q[i] >> 2) & 1) << j;
                upperByte |= ((q[i] >> 3) & 1) << j;
                i++;
            }
            int index = ((i + 7) / 8) - 1;
            quantQueryByte[index] = (byte) lowerByte;
            quantQueryByte[index + quantQueryByte.length / 4] = (byte) lowerMiddleByte;
            quantQueryByte[index + quantQueryByte.length / 2] = (byte) upperMiddleByte;
            quantQueryByte[index + 3 * quantQueryByte.length / 4] = (byte) upperByte;
        }
    }
}
