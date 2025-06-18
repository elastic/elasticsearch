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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.ValuesBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * Benchmark for the {@code VALUES} aggregator that supports grouping by many many
 * many values.
 */
@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(1)
public class ValuesAggregatorBenchmark {
    static final int MIN_BLOCK_LENGTH = 8 * 1024;
    private static final int OP_COUNT = 1024;
    private static final int UNIQUE_VALUES = 6;
    private static final BytesRef[] KEYWORDS = new BytesRef[] {
        new BytesRef("Tokyo"),
        new BytesRef("Delhi"),
        new BytesRef("Shanghai"),
        new BytesRef("São Paulo"),
        new BytesRef("Mexico City"),
        new BytesRef("Cairo") };
    static {
        assert KEYWORDS.length == UNIQUE_VALUES;
    }

    private static final BlockFactory blockFactory = BlockFactory.getInstance(
        new NoopCircuitBreaker("noop"),
        BigArrays.NON_RECYCLING_INSTANCE  // TODO real big arrays?
    );

    static {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            // Smoke test all the expected values and force loading subclasses more like prod
            selfTest();
        }
    }

    static void selfTest() {
        // Smoke test all the expected values and force loading subclasses more like prod
        try {
            for (String groups : ValuesAggregatorBenchmark.class.getField("groups").getAnnotationsByType(Param.class)[0].value()) {
                for (String dataType : ValuesAggregatorBenchmark.class.getField("dataType").getAnnotationsByType(Param.class)[0].value()) {
                    run(Integer.parseInt(groups), dataType, 10);
                }
            }
        } catch (NoSuchFieldException e) {
            throw new AssertionError();
        }
    }

    private static final String BYTES_REF = "BytesRef";
    private static final String INT = "int";
    private static final String LONG = "long";

    @Param({ "1", "1000", /*"1000000"*/ })
    public int groups;

    @Param({ BYTES_REF, INT, LONG })
    public String dataType;

    private static Operator operator(DriverContext driverContext, int groups, String dataType) {
        if (groups == 1) {
            return new AggregationOperator(
                List.of(supplier(dataType).aggregatorFactory(AggregatorMode.SINGLE, List.of(0)).apply(driverContext)),
                driverContext
            );
        }
        List<BlockHash.GroupSpec> groupSpec = List.of(new BlockHash.GroupSpec(0, ElementType.LONG));
        return new HashAggregationOperator(
            List.of(supplier(dataType).groupingAggregatorFactory(AggregatorMode.SINGLE, List.of(1))),
            () -> BlockHash.build(groupSpec, driverContext.blockFactory(), 16 * 1024, false),
            driverContext
        );
    }

    private static AggregatorFunctionSupplier supplier(String dataType) {
        return switch (dataType) {
            case BYTES_REF -> new ValuesBytesRefAggregatorFunctionSupplier();
            case INT -> new ValuesIntAggregatorFunctionSupplier();
            case LONG -> new ValuesLongAggregatorFunctionSupplier();
            default -> throw new IllegalArgumentException("unsupported data type [" + dataType + "]");
        };
    }

    private static void checkExpected(int groups, String dataType, Page page) {
        String prefix = String.format("[%s][%s]", groups, dataType);
        int positions = page.getPositionCount();
        if (positions != groups) {
            throw new IllegalArgumentException(prefix + " expected " + groups + " positions, got " + positions);
        }
        if (groups == 1) {
            checkUngrouped(prefix, dataType, page);
            return;
        }
        checkGrouped(prefix, groups, dataType, page);
    }

    private static void checkGrouped(String prefix, int groups, String dataType, Page page) {
        LongVector groupsVector = page.<LongBlock>getBlock(0).asVector();
        for (int p = 0; p < groups; p++) {
            long group = groupsVector.getLong(p);
            if (group != p) {
                throw new IllegalArgumentException(prefix + "[" + p + "] expected group " + p + " but was " + groups);
            }
        }
        switch (dataType) {
            case BYTES_REF -> {
                // Build the expected values
                List<Set<BytesRef>> expected = new ArrayList<>(groups);
                for (int g = 0; g < groups; g++) {
                    expected.add(new HashSet<>(KEYWORDS.length));
                }
                int blockLength = blockLength(groups);
                for (int p = 0; p < blockLength; p++) {
                    expected.get(p % groups).add(KEYWORDS[p % KEYWORDS.length]);
                }

                // Check them
                BytesRefBlock values = page.getBlock(1);
                for (int p = 0; p < groups; p++) {
                    checkExpectedBytesRef(prefix, values, p, expected.get(p));
                }
            }
            case INT -> {
                // Build the expected values
                List<Set<Integer>> expected = new ArrayList<>(groups);
                for (int g = 0; g < groups; g++) {
                    expected.add(new HashSet<>(UNIQUE_VALUES));
                }
                int blockLength = blockLength(groups);
                for (int p = 0; p < blockLength; p++) {
                    expected.get(p % groups).add(p % KEYWORDS.length);
                }

                // Check them
                IntBlock values = page.getBlock(1);
                for (int p = 0; p < groups; p++) {
                    checkExpectedInt(prefix, values, p, expected.get(p));
                }
            }
            case LONG -> {
                // Build the expected values
                List<Set<Long>> expected = new ArrayList<>(groups);
                for (int g = 0; g < groups; g++) {
                    expected.add(new HashSet<>(UNIQUE_VALUES));
                }
                int blockLength = blockLength(groups);
                for (int p = 0; p < blockLength; p++) {
                    expected.get(p % groups).add((long) p % KEYWORDS.length);
                }

                // Check them
                LongBlock values = page.getBlock(1);
                for (int p = 0; p < groups; p++) {
                    checkExpectedLong(prefix, values, p, expected.get(p));
                }
            }
            default -> throw new IllegalArgumentException(prefix + " unsupported data type " + dataType);
        }
    }

    private static void checkUngrouped(String prefix, String dataType, Page page) {
        switch (dataType) {
            case BYTES_REF -> {
                BytesRefBlock values = page.getBlock(0);
                checkExpectedBytesRef(prefix, values, 0, Set.of(KEYWORDS));
            }
            case INT -> {
                IntBlock values = page.getBlock(0);
                checkExpectedInt(prefix, values, 0, IntStream.range(0, UNIQUE_VALUES).boxed().collect(Collectors.toSet()));
            }
            case LONG -> {
                LongBlock values = page.getBlock(0);
                checkExpectedLong(prefix, values, 0, LongStream.range(0, UNIQUE_VALUES).boxed().collect(Collectors.toSet()));
            }
            default -> throw new IllegalArgumentException(prefix + " unsupported data type " + dataType);
        }
    }

    private static int checkExpectedBlock(String prefix, Block values, int position, Set<?> expected) {
        int valueCount = values.getValueCount(position);
        if (valueCount != expected.size()) {
            throw new IllegalArgumentException(
                prefix + "[" + position + "] expected " + expected.size() + " values but count was " + valueCount
            );
        }
        return valueCount;
    }

    private static void checkExpectedBytesRef(String prefix, BytesRefBlock values, int position, Set<BytesRef> expected) {
        int valueCount = checkExpectedBlock(prefix, values, position, expected);
        BytesRef scratch = new BytesRef();
        for (int i = values.getFirstValueIndex(position); i < valueCount; i++) {
            BytesRef v = values.getBytesRef(i, scratch);
            if (expected.contains(v) == false) {
                throw new IllegalArgumentException(prefix + "[" + position + "] expected " + v + " to be in " + expected);
            }
        }
    }

    private static void checkExpectedInt(String prefix, IntBlock values, int position, Set<Integer> expected) {
        int valueCount = checkExpectedBlock(prefix, values, position, expected);
        for (int i = values.getFirstValueIndex(position); i < valueCount; i++) {
            Integer v = values.getInt(i);
            if (expected.contains(v) == false) {
                throw new IllegalArgumentException(prefix + "[" + position + "] expected " + v + " to be in " + expected);
            }
        }
    }

    private static void checkExpectedLong(String prefix, LongBlock values, int position, Set<Long> expected) {
        int valueCount = checkExpectedBlock(prefix, values, position, expected);
        for (int i = values.getFirstValueIndex(position); i < valueCount; i++) {
            Long v = values.getLong(i);
            if (expected.contains(v) == false) {
                throw new IllegalArgumentException(prefix + "[" + position + "] expected " + v + " to be in " + expected);
            }
        }
    }

    private static Page page(int groups, String dataType) {
        Block dataBlock = dataBlock(groups, dataType);
        if (groups == 1) {
            return new Page(dataBlock);
        }
        return new Page(groupingBlock(groups), dataBlock);
    }

    private static Block dataBlock(int groups, String dataType) {
        int blockLength = blockLength(groups);
        return switch (dataType) {
            case BYTES_REF -> {
                try (
                    BytesRefVector.Builder dict = blockFactory.newBytesRefVectorBuilder(blockLength);
                    IntVector.Builder ords = blockFactory.newIntVectorBuilder(blockLength)
                ) {
                    final int dictLength = Math.min(blockLength, KEYWORDS.length);
                    for (int i = 0; i < dictLength; i++) {
                        dict.appendBytesRef(KEYWORDS[i]);
                    }
                    for (int i = 0; i < blockLength; i++) {
                        ords.appendInt(i % dictLength);
                    }
                    yield new OrdinalBytesRefVector(ords.build(), dict.build()).asBlock();
                }
            }
            case INT -> {
                try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(blockLength)) {
                    for (int i = 0; i < blockLength; i++) {
                        builder.appendInt(i % UNIQUE_VALUES);
                    }
                    yield builder.build();
                }
            }
            case LONG -> {
                try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(blockLength)) {
                    for (int i = 0; i < blockLength; i++) {
                        builder.appendLong(i % UNIQUE_VALUES);
                    }
                    yield builder.build();
                }
            }
            default -> throw new IllegalArgumentException("unsupported data type " + dataType);
        };
    }

    private static Block groupingBlock(int groups) {
        int blockLength = blockLength(groups);
        try (LongVector.Builder builder = blockFactory.newLongVectorBuilder(blockLength)) {
            for (int i = 0; i < blockLength; i++) {
                builder.appendLong(i % groups);
            }
            return builder.build().asBlock();
        }
    }

    @Benchmark
    public void run() {
        run(groups, dataType, OP_COUNT);
    }

    private static void run(int groups, String dataType, int opCount) {
        DriverContext driverContext = driverContext();
        try (Operator operator = operator(driverContext, groups, dataType)) {
            Page page = page(groups, dataType);
            for (int i = 0; i < opCount; i++) {
                operator.addInput(page.shallowCopy());
            }
            operator.finish();
            checkExpected(groups, dataType, operator.getOutput());
        }
    }

    static DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory);
    }

    static int blockLength(int groups) {
        return Math.max(MIN_BLOCK_LENGTH, groups);
    }
}
