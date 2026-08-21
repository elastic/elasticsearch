/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class AshSphericalScalarQuantizerBenchmark {

    // TODO: move to org.elasticsearch.benchmark.vector.quantization when the methods move out of SphericalQuantizer for panama-isation
    static {
        Utils.configureBenchmarkLogging();
    }

    public enum Distribution {
        GAUSSIAN,
        UNIFORM,
        /**
         * Magnitudes drawn from a small discrete set, producing many exactly-equal |z| values.
         * Tests degraded case of the quantization sweeps.
         */
        TIED
    }

    @Param({ "1000" })
    int numVectors;

    /** Projected dims: originalDim / 2 by default, so these cover 384/768/1024/1536-dim input vectors. */
    @Param({ "192", "384", "512", "768" })
    int dims;

    @Param({ "GAUSSIAN", "UNIFORM", "TIED" })
    Distribution distribution;

    private float[][] vectors;
    private float[] out;

    @Setup(Level.Trial)
    public void init() {
        Random random = new Random();
        out = new float[dims];
        vectors = new float[numVectors][];
        for (int i = 0; i < numVectors; i++) {
            vectors[i] = switch (distribution) {
                case GAUSSIAN -> SvdUtil.randomGaussians(random, dims);
                case UNIFORM -> VectorTestUtils.randomFloatVector(random, dims);
                case TIED -> tied(random, dims);
            };
        }
    }

    @Benchmark
    public void oneBit(Blackhole bh) {
        for (int i = 0; i < numVectors; i++) {
            float val = AshSphericalScalarQuantizer.quantizeExact1Bit(vectors[i], 0, out, 0, dims);
            bh.consume(val);
        }
    }

    @Benchmark
    public void twoBit(Blackhole bh) {
        for (int i = 0; i < numVectors; i++) {
            float val = AshSphericalScalarQuantizer.quantizeExact2Bit(vectors[i], 0, out, 0, dims);
            bh.consume(val);
        }
    }

    @Benchmark
    public void threeBit(Blackhole bh) {
        final int bits = 3;
        for (int i = 0; i < numVectors; i++) {
            float val = AshSphericalScalarQuantizer.quantizeExactGeneral(
                vectors[i],
                0,
                out,
                0,
                dims,
                1 << (bits - 1),
                (1 << (bits - 1)) - 1
            );
            bh.consume(val);
        }
    }

    @Benchmark
    public void fourBit(Blackhole bh) {
        final int bits = 4;
        for (int i = 0; i < numVectors; i++) {
            float val = AshSphericalScalarQuantizer.quantizeExactGeneral(
                vectors[i],
                0,
                out,
                0,
                dims,
                1 << (bits - 1),
                (1 << (bits - 1)) - 1
            );
            bh.consume(val);
        }
    }

    /**
     * Produces a vector whose absolute values are drawn from a small discrete set (10 distinct
     * magnitudes), creating long runs of exactly-equal |z| values in sorted order.
     */
    private static float[] tied(Random random, int dims) {
        float[] v = new float[dims];
        for (int j = 0; j < dims; j++) {
            float mag = (float) (random.nextInt(10) + 1) / 10f;
            v[j] = random.nextBoolean() ? mag : -mag;
        }
        return v;
    }
}
