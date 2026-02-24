/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.compute.runtime.RuntimeEvaluatorGenerator;
import org.elasticsearch.xpack.esql.functions.test.Abs2;
import org.elasticsearch.xpack.esql.functions.test.Add2;
import org.elasticsearch.xpack.esql.functions.test.Greatest2;
import org.elasticsearch.xpack.esql.functions.test.MvSum2;
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

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark measuring evaluator creation time (one-time cost).
 * <p>
 * This measures the cost of generating bytecode for a new evaluator class.
 * In practice, this happens once per function type and is cached.
 * <p>
 * Run with: ./gradlew :benchmarks:run --args 'RuntimeEvaluatorCreationBenchmark'
 */
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(1)
public class RuntimeEvaluatorCreationBenchmark {

    static {
        LogConfigurator.configureESLogging();
    }

    @Param(
        {
            "abs_long",         // Unary scalar
            "add_long",         // Binary scalar
            "greatest_long",    // Variadic
            "mvsum_long"        // Multi-value
        }
    )
    public String operation;

    private Method processMethod;
    private RuntimeEvaluatorGenerator generator;
    private boolean isMv;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        generator = RuntimeEvaluatorGenerator.getInstance(RuntimeEvaluatorCreationBenchmark.class.getClassLoader());

        switch (operation) {
            case "abs_long" -> {
                processMethod = Abs2.class.getMethod("processLong", long.class);
                isMv = false;
            }
            case "add_long" -> {
                processMethod = Add2.class.getMethod("processLong", long.class, long.class);
                isMv = false;
            }
            case "greatest_long" -> {
                processMethod = Greatest2.class.getMethod("processLong", long[].class);
                isMv = false;
            }
            case "mvsum_long" -> {
                processMethod = MvSum2.class.getMethod("processLong", long.class, long.class);
                isMv = true;
            }
            default -> throw new IllegalArgumentException("Unknown operation: " + operation);
        }
    }

    /**
     * Measures cached lookup time (after first generation).
     * This is the common case - evaluator classes are reused.
     */
    @Benchmark
    public Class<?> cachedLookup() {
        if (isMv) {
            return generator.getOrGenerateMvEvaluator(processMethod);
        } else {
            return generator.getOrGenerateEvaluator(processMethod);
        }
    }

    /**
     * Measures fresh generation time by using a new generator instance.
     * This simulates first-time generation for a new function.
     */
    @Benchmark
    public Class<?> freshGeneration() {
        RuntimeEvaluatorGenerator freshGenerator = new RuntimeEvaluatorGenerator(RuntimeEvaluatorCreationBenchmark.class.getClassLoader());
        if (isMv) {
            return freshGenerator.getOrGenerateMvEvaluator(processMethod);
        } else {
            return freshGenerator.getOrGenerateEvaluator(processMethod);
        }
    }
}
