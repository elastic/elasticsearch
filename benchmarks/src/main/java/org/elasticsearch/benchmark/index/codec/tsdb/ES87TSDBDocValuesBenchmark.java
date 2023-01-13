/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.elasticsearch.benchmark.index.codec.tsdb.internal.AbstractDocValuesForUtilBenchmark;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.DecodeConstantIntegerBenchmark;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.DecodeDecreasingIntegerBenchmark;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.DecodeFloatingPointBenchmark;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.DecodeIncreasingIntegerBenchmark;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.EncodeConstantIntegerBenchmark;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.EncodeDecreasingIntegerBenchmark;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.EncodeFloatingPointBenchmark;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.EncodeIncreasingIntegerBenchmark;
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@BenchmarkMode(value = Mode.AverageTime)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@State(value = Scope.Benchmark)
public class ES87TSDBDocValuesBenchmark {
    @Param({ "4", "8", "12", "16", "24", "28", "32", "36", "40", "44", "48", "52", "56", "64" })
    private int bitsPerValue;
    @Param(
        {
            "decode constant integer",
            "decode increasing integer",
            "decode decreasing integer",
            "decode floating point",
            "encode constant integer",
            "encode increasing integer",
            "encode decreasing integer",
            "encode floating point" }
    )
    private String mode;

    private final AbstractDocValuesForUtilBenchmark decodeConstantInteger;
    private final AbstractDocValuesForUtilBenchmark decodeIncreasingInteger;
    private final AbstractDocValuesForUtilBenchmark decodeDecreasingInteger;
    private final AbstractDocValuesForUtilBenchmark decodeFloatingPoint;
    private final AbstractDocValuesForUtilBenchmark encodeConstantInteger;
    private final AbstractDocValuesForUtilBenchmark encodeIncreasingInteger;
    private final AbstractDocValuesForUtilBenchmark encodeDecreasingInteger;
    private final AbstractDocValuesForUtilBenchmark encodeFloatingPoint;

    public ES87TSDBDocValuesBenchmark() {
        this.decodeConstantInteger = new DecodeConstantIntegerBenchmark();
        this.decodeIncreasingInteger = new DecodeIncreasingIntegerBenchmark();
        this.decodeDecreasingInteger = new DecodeDecreasingIntegerBenchmark();
        this.decodeFloatingPoint = new DecodeFloatingPointBenchmark();
        this.encodeConstantInteger = new EncodeConstantIntegerBenchmark();
        this.encodeIncreasingInteger = new EncodeIncreasingIntegerBenchmark();
        this.encodeDecreasingInteger = new EncodeDecreasingIntegerBenchmark();
        this.encodeFloatingPoint = new EncodeFloatingPointBenchmark();
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws IOException {
        switch (mode) {
            case "decode constant integer" -> decodeConstantInteger.setupInvocation(bitsPerValue);
            case "decode increasing integer" -> decodeIncreasingInteger.setupInvocation(bitsPerValue);
            case "decode decreasing integer" -> decodeDecreasingInteger.setupInvocation(bitsPerValue);
            case "decode floating point" -> decodeFloatingPoint.setupInvocation(bitsPerValue);
            case "encode constant integer" -> encodeConstantInteger.setupInvocation(bitsPerValue);
            case "encode increasing integer" -> encodeIncreasingInteger.setupInvocation(bitsPerValue);
            case "encode decreasing integer" -> encodeDecreasingInteger.setupInvocation(bitsPerValue);
            case "encode floating point" -> encodeFloatingPoint.setupInvocation(bitsPerValue);
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        switch (mode) {
            case "decode constant integer" -> decodeConstantInteger.setupIteration(bitsPerValue);
            case "decode increasing integer" -> decodeIncreasingInteger.setupIteration(bitsPerValue);
            case "decode decreasing integer" -> decodeDecreasingInteger.setupIteration(bitsPerValue);
            case "decode floating point" -> decodeFloatingPoint.setupIteration(bitsPerValue);
            case "encode constant integer" -> encodeConstantInteger.setupIteration(bitsPerValue);
            case "encode increasing integer" -> encodeIncreasingInteger.setupIteration(bitsPerValue);
            case "encode decreasing integer" -> encodeDecreasingInteger.setupIteration(bitsPerValue);
            case "encode floating point" -> encodeFloatingPoint.setupIteration(bitsPerValue);
        }
    }

    @Benchmark
    public void benchmark() throws IOException {
        switch (mode) {
            case "decode constant integer" -> decodeConstantInteger.benchmark(bitsPerValue);
            case "decode increasing integer" -> decodeIncreasingInteger.benchmark(bitsPerValue);
            case "decode decreasing integer" -> decodeDecreasingInteger.benchmark(bitsPerValue);
            case "decode floating point" -> decodeFloatingPoint.benchmark(bitsPerValue);
            case "encode constant integer" -> encodeConstantInteger.benchmark(bitsPerValue);
            case "encode increasing integer" -> encodeIncreasingInteger.benchmark(bitsPerValue);
            case "encode decreasing integer" -> encodeDecreasingInteger.benchmark(bitsPerValue);
            case "encode floating point" -> encodeFloatingPoint.benchmark(bitsPerValue);
        }
    }

}
