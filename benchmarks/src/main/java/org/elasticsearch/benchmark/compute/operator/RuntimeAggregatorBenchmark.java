/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.MaxBytesRefAggregatorFunction;
import org.elasticsearch.compute.aggregation.MaxBytesRefGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.aggregation.SumDoubleAggregatorFunction;
import org.elasticsearch.compute.aggregation.SumDoubleGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunction;
import org.elasticsearch.compute.aggregation.SumLongGroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.runtime.AggregatorSpec;
import org.elasticsearch.compute.runtime.RuntimeAggregatorGenerator;
import org.elasticsearch.xpack.esql.functions.test.DoubleSum2Aggregator;
import org.elasticsearch.xpack.esql.functions.test.MaxBytes2Aggregator;
import org.elasticsearch.xpack.esql.functions.test.SumLong2Aggregator;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark comparing compile-time generated aggregators vs runtime-generated aggregators.
 * <p>
 * This benchmark measures aggregation throughput for both non-grouping and grouping aggregators.
 * <p>
 * Aggregators compared:
 * <ul>
 *   <li>sum_long: SumLongAggregator vs Sum2Aggregator</li>
 *   <li>sum_double: SumDoubleAggregator vs SumDouble2Aggregator</li>
 *   <li>max_bytes: MaxBytesRefAggregator vs MaxBytes2Aggregator</li>
 * </ul>
 * <p>
 * Run with: ./gradlew :benchmarks:run --args 'RuntimeAggregatorBenchmark'
 */
@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class RuntimeAggregatorBenchmark {

    private static final BlockFactory blockFactory = BlockFactory.getInstance(
        new NoopCircuitBreaker("noop"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    private static final int BLOCK_LENGTH = 8 * 1024;
    private static final int GROUPS = 100;

    static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    static {
        LogConfigurator.configureESLogging();
    }

    @Param({
        // Non-grouping sum long
        "compile_time_sum_long",
        "runtime_sum_long",
        // Non-grouping sum double
        "compile_time_sum_double",
        "runtime_sum_double",
        // Non-grouping max bytes
        "compile_time_max_bytes",
        "runtime_max_bytes",
        // Grouping sum long
        "compile_time_grouping_sum_long",
        "runtime_grouping_sum_long",
        // Grouping sum double
        "compile_time_grouping_sum_double",
        "runtime_grouping_sum_double",
        // Grouping max bytes
        "compile_time_grouping_max_bytes",
        "runtime_grouping_max_bytes"
    })
    public String operation;

    private AggregatorFunction aggregator;
    private GroupingAggregatorFunction groupingAggregator;
    private Page page;
    private BooleanVector mask;
    private IntVector groupIds;
    private boolean isGrouping;

    @Setup(Level.Trial)
    public void setup() {
        isGrouping = operation.contains("grouping");
        page = createPage(operation);
        mask = blockFactory.newConstantBooleanVector(true, BLOCK_LENGTH);

        if (isGrouping) {
            groupingAggregator = createGroupingAggregator(operation);
            groupIds = createGroupIds();
        } else {
            aggregator = createAggregator(operation);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (aggregator != null) {
            aggregator.close();
        }
        if (groupingAggregator != null) {
            groupingAggregator.close();
        }
        if (mask != null) {
            mask.close();
        }
        if (groupIds != null) {
            groupIds.close();
        }
        if (page != null) {
            page.releaseBlocks();
        }
    }

    private static AggregatorFunction createAggregator(String operation) {
        List<Integer> channels = List.of(0);
        return switch (operation) {
            case "compile_time_sum_long" -> SumLongAggregatorFunction.create(driverContext, channels);
            case "runtime_sum_long" -> createRuntimeAggregator(SumLong2Aggregator.class, channels);
            case "compile_time_sum_double" -> SumDoubleAggregatorFunction.create(driverContext, channels);
            case "runtime_sum_double" -> createRuntimeAggregator(DoubleSum2Aggregator.class, channels);
            case "compile_time_max_bytes" -> MaxBytesRefAggregatorFunction.create(driverContext, channels);
            case "runtime_max_bytes" -> createRuntimeAggregator(MaxBytes2Aggregator.class, channels);
            default -> throw new UnsupportedOperationException("Unknown operation: " + operation);
        };
    }

    private static GroupingAggregatorFunction createGroupingAggregator(String operation) {
        List<Integer> channels = List.of(0);
        return switch (operation) {
            case "compile_time_grouping_sum_long" -> SumLongGroupingAggregatorFunction.create(channels, driverContext);
            case "runtime_grouping_sum_long" -> createRuntimeGroupingAggregator(SumLong2Aggregator.class, channels);
            case "compile_time_grouping_sum_double" -> SumDoubleGroupingAggregatorFunction.create(channels, driverContext);
            case "runtime_grouping_sum_double" -> createRuntimeGroupingAggregator(DoubleSum2Aggregator.class, channels);
            case "compile_time_grouping_max_bytes" -> MaxBytesRefGroupingAggregatorFunction.create(channels, driverContext);
            case "runtime_grouping_max_bytes" -> createRuntimeGroupingAggregator(MaxBytes2Aggregator.class, channels);
            default -> throw new UnsupportedOperationException("Unknown operation: " + operation);
        };
    }

    private static AggregatorFunction createRuntimeAggregator(Class<?> aggregatorClass, List<Integer> channels) {
        try {
            AggregatorSpec spec = AggregatorSpec.from(aggregatorClass);
            RuntimeAggregatorGenerator generator = RuntimeAggregatorGenerator.getInstance(
                RuntimeAggregatorBenchmark.class.getClassLoader()
            );
            Class<? extends AggregatorFunction> generatedClass = generator.getOrGenerateAggregator(spec);

            Method createMethod = generatedClass.getMethod("create", DriverContext.class, List.class);
            return (AggregatorFunction) createMethod.invoke(null, driverContext, channels);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create runtime aggregator for " + aggregatorClass.getSimpleName(), e);
        }
    }

    private static GroupingAggregatorFunction createRuntimeGroupingAggregator(Class<?> aggregatorClass, List<Integer> channels) {
        try {
            AggregatorSpec spec = AggregatorSpec.from(aggregatorClass);
            RuntimeAggregatorGenerator generator = RuntimeAggregatorGenerator.getInstance(
                RuntimeAggregatorBenchmark.class.getClassLoader()
            );
            Class<? extends GroupingAggregatorFunction> generatedClass = generator.getOrGenerateGroupingAggregator(spec);

            Method createMethod = generatedClass.getMethod("create", List.class, DriverContext.class);
            return (GroupingAggregatorFunction) createMethod.invoke(null, channels, driverContext);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create runtime grouping aggregator for " + aggregatorClass.getSimpleName(), e);
        }
    }

    private static Page createPage(String operation) {
        if (operation.contains("sum_long")) {
            LongBlock.Builder builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
            for (int i = 0; i < BLOCK_LENGTH; i++) {
                builder.appendLong(i * 1000L);
            }
            return new Page(builder.build());
        } else if (operation.contains("sum_double")) {
            DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(BLOCK_LENGTH);
            for (int i = 0; i < BLOCK_LENGTH; i++) {
                builder.appendDouble(i * 1.5);
            }
            return new Page(builder.build());
        } else if (operation.contains("max_bytes") || operation.contains("grouping_max_bytes")) {
            BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(BLOCK_LENGTH);
            for (int i = 0; i < BLOCK_LENGTH; i++) {
                builder.appendBytesRef(new BytesRef("value_" + String.format("%08d", i)));
            }
            return new Page(builder.build());
        }
        throw new UnsupportedOperationException("Unknown operation: " + operation);
    }

    private IntVector createGroupIds() {
        IntVector.Builder builder = blockFactory.newIntVectorBuilder(BLOCK_LENGTH);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            builder.appendInt(i % GROUPS);
        }
        return builder.build();
    }

    @Benchmark
    @OperationsPerInvocation(BLOCK_LENGTH)
    public void aggregate() {
        if (isGrouping) {
            runGroupingAggregation();
        } else {
            runNonGroupingAggregation();
        }
    }

    private void runNonGroupingAggregation() {
        aggregator.addRawInput(page, mask);
        Block[] results = new Block[1];
        aggregator.evaluateFinal(results, 0, driverContext);
        results[0].close();

        aggregator.close();
        aggregator = createAggregator(operation);
    }

    private void runGroupingAggregation() {
        SeenGroupIds seenGroupIds = new SeenGroupIds.Empty();
        GroupingAggregatorFunction.AddInput addInput = groupingAggregator.prepareProcessRawInputPage(seenGroupIds, page);
        addInput.add(0, groupIds);
        addInput.close();

        IntVector.Builder selectedBuilder = blockFactory.newIntVectorBuilder(GROUPS);
        for (int i = 0; i < GROUPS; i++) {
            selectedBuilder.appendInt(i);
        }
        IntVector selected = selectedBuilder.build();

        Block[] results = new Block[1];
        GroupingAggregatorEvaluationContext ctx = new GroupingAggregatorEvaluationContext(driverContext);
        groupingAggregator.evaluateFinal(results, 0, selected, ctx);
        results[0].close();
        selected.close();

        groupingAggregator.close();
        groupingAggregator = createGroupingAggregator(operation);
    }
}
