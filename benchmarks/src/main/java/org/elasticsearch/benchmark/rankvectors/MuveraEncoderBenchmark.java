/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.rankvectors;

import org.elasticsearch.xpack.rank.vectors.fde.MuveraEncoder;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class MuveraEncoderBenchmark {

    private static final int TOKEN_DIMS = 128;

    @Param({ "1", "10", "25", "50" })
    public int tokenCount;

    private MuveraEncoder encoder;
    private float[][] tokens;

    @Setup
    public void setup() {
        encoder = new MuveraEncoder(TOKEN_DIMS, 3, 8, 64, 42L);
        tokens = unitVectors(tokenCount);
    }

    @Benchmark
    public float[] encodeDocument() {
        return encoder.encodeDocument(tokens);
    }

    @Benchmark
    public float[] encodeQuery() {
        return encoder.encodeQuery(tokens);
    }

    private static float[][] unitVectors(int count) {
        Random random = new Random(7L);
        float[][] vectors = new float[count][TOKEN_DIMS];
        for (float[] vector : vectors) {
            double squaredMagnitude = 0d;
            for (int d = 0; d < vector.length; d++) {
                vector[d] = (float) random.nextGaussian();
                squaredMagnitude += (double) vector[d] * vector[d];
            }
            float inverseMagnitude = (float) (1d / Math.sqrt(squaredMagnitude));
            for (int d = 0; d < vector.length; d++) {
                vector[d] *= inverseMagnitude;
            }
        }
        return vectors;
    }
}
