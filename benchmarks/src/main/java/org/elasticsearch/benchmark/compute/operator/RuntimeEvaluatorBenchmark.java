/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.runtime.RuntimeEvaluatorGenerator;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.GreatestLongEvaluator;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.AbsLongEvaluator;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMaxLongEvaluator;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSumLongEvaluator;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.AddLongsEvaluator;
import org.elasticsearch.xpack.esql.functions.test.Abs2;
import org.elasticsearch.xpack.esql.functions.test.Add2;
import org.elasticsearch.xpack.esql.functions.test.Greatest2;
import org.elasticsearch.xpack.esql.functions.test.MvMax2;
import org.elasticsearch.xpack.esql.functions.test.MvSum2;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark comparing compile-time generated evaluators vs runtime-generated evaluators.
 * <p>
 * This benchmark measures evaluation throughput using DIRECT instantiation for both
 * compile-time and runtime evaluators (no EvalMapper overhead).
 * <p>
 * Functions compared:
 * <ul>
 *   <li>Unary: Abs vs Abs2</li>
 *   <li>Binary: Add vs Add2</li>
 *   <li>Variadic: Greatest vs Greatest2</li>
 *   <li>Multi-value: MvSum vs MvSum2</li>
 *   <li>Multi-value with ascending: MvMax vs MvMax2</li>
 * </ul>
 * <p>
 * Run with: ./gradlew :benchmarks:run --args 'RuntimeEvaluatorBenchmark'
 */
@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class RuntimeEvaluatorBenchmark {

    private static final BlockFactory blockFactory = BlockFactory.getInstance(
        new NoopCircuitBreaker("noop"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    private static final int BLOCK_LENGTH = 8 * 1024;

    static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    static {
        LogConfigurator.configureESLogging();
    }

    @Param({
        // Unary - Abs
        "compile_time_abs_long",
        "runtime_abs_long",
        // Binary - Add
        "compile_time_add_long",
        "runtime_add_long",
        // Variadic - Greatest (3 args)
        "compile_time_greatest_long",
        "runtime_greatest_long",
        // Multi-value - MvSum (with warnExceptions)
        "compile_time_mvsum_long",
        "runtime_mvsum_long",
        // Multi-value - MvMax (with ascending optimization)
        "compile_time_mvmax_long",
        "runtime_mvmax_long"
    })
    public String operation;

    private EvalOperator.ExpressionEvaluator evaluator;
    private Page page;

    @Setup(Level.Trial)
    public void setup() {
        evaluator = createEvaluator(operation);
        page = createPage(operation);

        Block result = evaluator.eval(page);
        validateResult(operation, result);
        result.close();
    }

    private static EvalOperator.ExpressionEvaluator createEvaluator(String operation) {
        return switch (operation) {
            // Unary - Abs (direct instantiation)
            case "compile_time_abs_long" -> new AbsLongEvaluator(
                Source.EMPTY,
                new FieldEvaluator(0),
                driverContext
            );
            case "runtime_abs_long" -> createRuntimeEvaluator(Abs2.class, "processLong", long.class, 1);

            // Binary - Add (direct instantiation)
            case "compile_time_add_long" -> new AddLongsEvaluator(
                Source.EMPTY,
                new FieldEvaluator(0),
                new FieldEvaluator(1),
                driverContext
            );
            case "runtime_add_long" -> createRuntimeEvaluator(Add2.class, "processLong", long.class, 2);

            // Variadic - Greatest (direct instantiation)
            case "compile_time_greatest_long" -> new GreatestLongEvaluator(
                Source.EMPTY,
                new EvalOperator.ExpressionEvaluator[] {
                    new FieldEvaluator(0),
                    new FieldEvaluator(1),
                    new FieldEvaluator(2)
                },
                driverContext
            );
            case "runtime_greatest_long" -> createRuntimeVariadicEvaluator(Greatest2.class, "processLong", long[].class, 3);

            // Multi-value - MvSum (direct instantiation)
            case "compile_time_mvsum_long" -> new MvSumLongEvaluator(
                Source.EMPTY,
                new FieldEvaluator(0),
                driverContext
            );
            case "runtime_mvsum_long" -> createRuntimeMvEvaluator(MvSum2.class, "processLong", long.class);

            // Multi-value - MvMax with ascending optimization (direct instantiation)
            case "compile_time_mvmax_long" -> new MvMaxLongEvaluator(
                new FieldEvaluator(0),
                driverContext
            );
            case "runtime_mvmax_long" -> createRuntimeMvEvaluator(MvMax2.class, "processLong", long.class);

            default -> throw new UnsupportedOperationException("Unknown operation: " + operation);
        };
    }

    private static EvalOperator.ExpressionEvaluator createRuntimeEvaluator(
        Class<?> functionClass,
        String methodName,
        Class<?> paramType,
        int paramCount
    ) {
        try {
            Class<?>[] paramTypes = new Class<?>[paramCount];
            Arrays.fill(paramTypes, paramType);
            Method processMethod = functionClass.getMethod(methodName, paramTypes);

            RuntimeEvaluatorGenerator generator = RuntimeEvaluatorGenerator.getInstance(RuntimeEvaluatorBenchmark.class.getClassLoader());
            Class<?> evaluatorClass = generator.getOrGenerateEvaluator(processMethod);

            Class<?>[] constructorParamTypes = new Class<?>[paramCount + 2];
            constructorParamTypes[0] = Class.forName("org.elasticsearch.xpack.esql.core.tree.Source");
            for (int i = 0; i < paramCount; i++) {
                constructorParamTypes[i + 1] = EvalOperator.ExpressionEvaluator.class;
            }
            constructorParamTypes[paramCount + 1] = DriverContext.class;

            var constructor = evaluatorClass.getConstructor(constructorParamTypes);

            Object[] constructorArgs = new Object[paramCount + 2];
            constructorArgs[0] = Source.EMPTY;
            for (int i = 0; i < paramCount; i++) {
                constructorArgs[i + 1] = new FieldEvaluator(i);
            }
            constructorArgs[paramCount + 1] = driverContext;

            return (EvalOperator.ExpressionEvaluator) constructor.newInstance(constructorArgs);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create runtime evaluator for " + methodName, e);
        }
    }

    private static EvalOperator.ExpressionEvaluator createRuntimeVariadicEvaluator(
        Class<?> functionClass,
        String methodName,
        Class<?> arrayType,
        int paramCount
    ) {
        try {
            Method processMethod = functionClass.getMethod(methodName, arrayType);

            RuntimeEvaluatorGenerator generator = RuntimeEvaluatorGenerator.getInstance(RuntimeEvaluatorBenchmark.class.getClassLoader());
            Class<?> evaluatorClass = generator.getOrGenerateEvaluator(processMethod);

            // Variadic evaluators take (Source, ExpressionEvaluator[], DriverContext)
            var constructor = evaluatorClass.getConstructor(
                Class.forName("org.elasticsearch.xpack.esql.core.tree.Source"),
                EvalOperator.ExpressionEvaluator[].class,
                DriverContext.class
            );

            EvalOperator.ExpressionEvaluator[] fieldEvaluators = new EvalOperator.ExpressionEvaluator[paramCount];
            for (int i = 0; i < paramCount; i++) {
                fieldEvaluators[i] = new FieldEvaluator(i);
            }

            return (EvalOperator.ExpressionEvaluator) constructor.newInstance(Source.EMPTY, fieldEvaluators, driverContext);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create runtime variadic evaluator for " + methodName, e);
        }
    }

    private static EvalOperator.ExpressionEvaluator createRuntimeMvEvaluator(
        Class<?> functionClass,
        String methodName,
        Class<?> paramType
    ) {
        try {
            Method processMethod = functionClass.getMethod(methodName, paramType, paramType);

            RuntimeEvaluatorGenerator generator = RuntimeEvaluatorGenerator.getInstance(RuntimeEvaluatorBenchmark.class.getClassLoader());
            Class<?> evaluatorClass = generator.getOrGenerateMvEvaluator(processMethod);

            // Try different constructor signatures
            // MvMax2 (no warnExceptions) uses: (ExpressionEvaluator, DriverContext)
            // MvSum2 (with warnExceptions) uses: (Source, ExpressionEvaluator, DriverContext)
            try {
                var constructor = evaluatorClass.getConstructor(
                    EvalOperator.ExpressionEvaluator.class,
                    DriverContext.class
                );
                return (EvalOperator.ExpressionEvaluator) constructor.newInstance(new FieldEvaluator(0), driverContext);
            } catch (NoSuchMethodException e) {
                var constructor = evaluatorClass.getConstructor(
                    Class.forName("org.elasticsearch.xpack.esql.core.tree.Source"),
                    EvalOperator.ExpressionEvaluator.class,
                    DriverContext.class
                );
                return (EvalOperator.ExpressionEvaluator) constructor.newInstance(Source.EMPTY, new FieldEvaluator(0), driverContext);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create runtime MV evaluator for " + methodName, e);
        }
    }

    private static Page createPage(String operation) {
        return switch (operation) {
            case "compile_time_abs_long", "runtime_abs_long" -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendLong(i % 2 == 0 ? i * 1000L : -i * 1000L);
                }
                yield new Page(builder.build());
            }
            case "compile_time_add_long", "runtime_add_long" -> {
                var builder1 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var builder2 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder1.appendLong(i * 1000L);
                    builder2.appendLong(i * 100L);
                }
                yield new Page(builder1.build(), builder2.build());
            }
            case "compile_time_greatest_long", "runtime_greatest_long" -> {
                var builder1 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var builder2 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var builder3 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder1.appendLong(i * 100L);
                    builder2.appendLong(i * 1000L);
                    builder3.appendLong(i * 10L);
                }
                yield new Page(builder1.build(), builder2.build(), builder3.build());
            }
            case "compile_time_mvsum_long", "runtime_mvsum_long",
                 "compile_time_mvmax_long", "runtime_mvmax_long" -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.beginPositionEntry();
                    builder.appendLong(i);
                    builder.appendLong(i + 1);
                    builder.appendLong(i + 2);
                    builder.endPositionEntry();
                }
                yield new Page(builder.build());
            }
            default -> throw new UnsupportedOperationException("Unknown operation: " + operation);
        };
    }

    private static void validateResult(String operation, Block result) {
        switch (operation) {
            case "compile_time_abs_long", "runtime_abs_long" -> {
                LongVector v = ((LongBlock) result).asVector();
                for (int i = 0; i < Math.min(10, BLOCK_LENGTH); i++) {
                    long expected = Math.abs(i % 2 == 0 ? i * 1000L : -i * 1000L);
                    if (v.getLong(i) != expected) {
                        throw new AssertionError("Expected " + expected + " but got " + v.getLong(i));
                    }
                }
            }
            case "compile_time_add_long", "runtime_add_long" -> {
                LongVector v = ((LongBlock) result).asVector();
                for (int i = 0; i < Math.min(10, BLOCK_LENGTH); i++) {
                    long expected = i * 1000L + i * 100L;
                    if (v.getLong(i) != expected) {
                        throw new AssertionError("Expected " + expected + " but got " + v.getLong(i));
                    }
                }
            }
            case "compile_time_greatest_long", "runtime_greatest_long" -> {
                LongVector v = ((LongBlock) result).asVector();
                for (int i = 0; i < Math.min(10, BLOCK_LENGTH); i++) {
                    long expected = Math.max(Math.max(i * 100L, i * 1000L), i * 10L);
                    if (v.getLong(i) != expected) {
                        throw new AssertionError("Expected " + expected + " but got " + v.getLong(i));
                    }
                }
            }
            case "compile_time_mvsum_long", "runtime_mvsum_long" -> {
                LongBlock block = (LongBlock) result;
                for (int i = 0; i < Math.min(10, BLOCK_LENGTH); i++) {
                    long expected = i + (i + 1) + (i + 2);
                    long actual = block.getLong(block.getFirstValueIndex(i));
                    if (actual != expected) {
                        throw new AssertionError("Expected " + expected + " but got " + actual);
                    }
                }
            }
            case "compile_time_mvmax_long", "runtime_mvmax_long" -> {
                LongBlock block = (LongBlock) result;
                for (int i = 0; i < Math.min(10, BLOCK_LENGTH); i++) {
                    long expected = i + 2; // max of [i, i+1, i+2]
                    long actual = block.getLong(block.getFirstValueIndex(i));
                    if (actual != expected) {
                        throw new AssertionError("Expected " + expected + " but got " + actual);
                    }
                }
            }
            default -> throw new UnsupportedOperationException("Unknown operation: " + operation);
        }
    }

    @Benchmark
    @OperationsPerInvocation(BLOCK_LENGTH)
    public Block eval() {
        return evaluator.eval(page);
    }

    static class FieldEvaluator implements EvalOperator.ExpressionEvaluator {
        private final int channel;

        FieldEvaluator(int channel) {
            this.channel = channel;
        }

        @Override
        public Block eval(Page page) {
            Block block = page.getBlock(channel);
            block.incRef();
            return block;
        }

        @Override
        public void close() {}

        @Override
        public long baseRamBytesUsed() {
            return 0;
        }
    }
}
