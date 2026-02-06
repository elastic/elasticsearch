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
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.CountAggregatorFunction;
import org.elasticsearch.compute.aggregation.CountDistinctDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountDistinctLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.FilteredAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Benchmark for many different kinds of aggregator and groupings.
 */
@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class AggregatorBenchmark {
    static final int BLOCK_LENGTH = 8 * 1024;
    private static final int OP_COUNT = 1024;
    private static final int TOP_N_LIMIT = 3;

    private static final BlockFactory blockFactory = BlockFactory.getInstance(
        new NoopCircuitBreaker("noop"),
        BigArrays.NON_RECYCLING_INSTANCE  // TODO real big arrays?
    );

    private static final String LONGS = "longs";
    private static final String INTS = "ints";
    private static final String DOUBLES = "doubles";
    private static final String BOOLEANS = "booleans";
    private static final String BYTES_REFS = "bytes_refs";
    private static final String ORDINALS = "ordinals";
    private static final String TWO_LONGS = "two_" + LONGS;
    private static final String TWO_BYTES_REFS = "two_" + BYTES_REFS;
    private static final String TWO_ORDINALS = "two_" + ORDINALS;
    private static final String LONGS_AND_BYTES_REFS = LONGS + "_and_" + BYTES_REFS;
    private static final String TWO_LONGS_AND_BYTES_REFS = "two_" + LONGS + "_and_" + BYTES_REFS;
    private static final String TOP_N_LONGS = "top_n_" + LONGS;

    private static final String VECTOR_DOUBLES = "vector_doubles";
    private static final String HALF_NULL_DOUBLES = "half_null_doubles";
    private static final String VECTOR_LONGS = "vector_" + LONGS;
    private static final String HALF_NULL_LONGS = "half_null_" + LONGS;
    private static final String MULTIVALUED_LONGS = "multivalued";

    private static final String AVG = "avg";
    private static final String COUNT = "count";
    private static final String COUNT_DISTINCT = "count_distinct";
    private static final String MIN = "min";
    private static final String MAX = "max";
    private static final String SUM = "sum";

    private static final String NONE = "none";

    private static final String CONSTANT_TRUE = "constant_true";
    private static final String ALL_TRUE = "all_true";
    private static final String HALF_TRUE = "half_true";
    private static final String CONSTANT_FALSE = "constant_false";

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized

        // Smoke test all the expected values and force loading subclasses more like prod
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    static void selfTest() {
        try {
            for (String grouping : AggregatorBenchmark.class.getField("grouping").getAnnotationsByType(Param.class)[0].value()) {
                for (String op : AggregatorBenchmark.class.getField("op").getAnnotationsByType(Param.class)[0].value()) {
                    for (String blockType : AggregatorBenchmark.class.getField("blockType").getAnnotationsByType(Param.class)[0].value()) {
                        for (String filter : AggregatorBenchmark.class.getField("filter").getAnnotationsByType(Param.class)[0].value()) {
                            for (String groups : AggregatorBenchmark.class.getField("groups").getAnnotationsByType(Param.class)[0].value()) {
                                for (String distinctPerGroup : AggregatorBenchmark.class.getField("distinctPerGroup")
                                    .getAnnotationsByType(Param.class)[0].value()) {
                                    run(
                                        grouping,
                                        op,
                                        blockType,
                                        filter,
                                        Integer.parseInt(groups),
                                        Integer.parseInt(distinctPerGroup),
                                        10
                                    );
                                }
                            }
                        }
                    }
                }
            }
        } catch (NoSuchFieldException e) {
            throw new AssertionError();
        }
    }

    @Param(
        {
            NONE,
            LONGS,
            INTS,
            DOUBLES,
            BOOLEANS,
            BYTES_REFS,
            ORDINALS,
            TWO_LONGS,
            TWO_BYTES_REFS,
            TWO_ORDINALS,
            LONGS_AND_BYTES_REFS,
            TWO_LONGS_AND_BYTES_REFS,
            TOP_N_LONGS }
    )
    public String grouping;

    @Param({ COUNT, COUNT_DISTINCT, MIN, MAX, SUM })
    public String op;

    @Param({ VECTOR_LONGS, HALF_NULL_LONGS, VECTOR_DOUBLES, HALF_NULL_DOUBLES })
    public String blockType;

    @Param({ NONE, CONSTANT_TRUE, ALL_TRUE, HALF_TRUE, CONSTANT_FALSE })
    public String filter;

    @Param({ "5" })
    public int groups;

    // Only used for count_distinct; 0 keeps the legacy full-distinct pattern.
    @Param({ "0" })
    public int distinctPerGroup;

    private static Operator operator(DriverContext driverContext, String grouping, String op, String dataType, String filter) {
        if (grouping.equals(NONE)) {
            return new AggregationOperator(
                List.of(supplier(op, dataType, filter).aggregatorFactory(AggregatorMode.SINGLE, List.of(0)).apply(driverContext)),
                driverContext
            );
        }
        List<BlockHash.GroupSpec> groups = switch (grouping) {
            case LONGS -> List.of(new BlockHash.GroupSpec(0, ElementType.LONG));
            case INTS -> List.of(new BlockHash.GroupSpec(0, ElementType.INT));
            case DOUBLES -> List.of(new BlockHash.GroupSpec(0, ElementType.DOUBLE));
            case BOOLEANS -> List.of(new BlockHash.GroupSpec(0, ElementType.BOOLEAN));
            case BYTES_REFS, ORDINALS -> List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF));
            case TWO_LONGS -> List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.LONG));
            case TWO_BYTES_REFS, TWO_ORDINALS -> List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF),
                new BlockHash.GroupSpec(1, ElementType.BYTES_REF)
            );
            case LONGS_AND_BYTES_REFS -> List.of(
                new BlockHash.GroupSpec(0, ElementType.LONG),
                new BlockHash.GroupSpec(1, ElementType.BYTES_REF)
            );
            case TWO_LONGS_AND_BYTES_REFS -> List.of(
                new BlockHash.GroupSpec(0, ElementType.LONG),
                new BlockHash.GroupSpec(1, ElementType.LONG),
                new BlockHash.GroupSpec(2, ElementType.BYTES_REF)
            );
            case TOP_N_LONGS -> List.of(
                new BlockHash.GroupSpec(0, ElementType.LONG, null, new BlockHash.TopNDef(0, true, true, TOP_N_LIMIT))
            );
            default -> throw new IllegalArgumentException("unsupported grouping [" + grouping + "]");
        };
        return new HashAggregationOperator(
            AggregatorMode.SINGLE,
            List.of(supplier(op, dataType, filter).groupingAggregatorFactory(AggregatorMode.SINGLE, List.of(groups.size()))),
            () -> BlockHash.build(groups, driverContext.blockFactory(), 16 * 1024, false),
            Integer.MAX_VALUE,
            1.0,
            driverContext
        );
    }

    private static AggregatorFunctionSupplier supplier(String op, String dataType, String filter) {
        return filtered(switch (op) {
            case COUNT -> CountAggregatorFunction.supplier();
            case COUNT_DISTINCT -> switch (dataType) {
                case LONGS -> new CountDistinctLongAggregatorFunctionSupplier(3000);
                case DOUBLES -> new CountDistinctDoubleAggregatorFunctionSupplier(3000);
                default -> throw new IllegalArgumentException("unsupported data type [" + dataType + "]");
            };
            case MAX -> switch (dataType) {
                case LONGS -> new MaxLongAggregatorFunctionSupplier();
                case DOUBLES -> new MaxDoubleAggregatorFunctionSupplier();
                default -> throw new IllegalArgumentException("unsupported data type [" + dataType + "]");
            };
            case MIN -> switch (dataType) {
                case LONGS -> new MinLongAggregatorFunctionSupplier();
                case DOUBLES -> new MinDoubleAggregatorFunctionSupplier();
                default -> throw new IllegalArgumentException("unsupported data type [" + dataType + "]");
            };
            case SUM -> switch (dataType) {
                case LONGS -> new SumLongAggregatorFunctionSupplier();
                case DOUBLES -> new SumDoubleAggregatorFunctionSupplier();
                default -> throw new IllegalArgumentException("unsupported data type [" + dataType + "]");
            };
            default -> throw new IllegalArgumentException("unsupported op [" + op + "]");
        }, filter);
    }

    private static void checkExpected(
        String grouping,
        String op,
        String blockType,
        String filter,
        String dataType,
        Page page,
        int opCount,
        int dataGroupCount,
        int distinctPerGroup
    ) {
        if (filter.equals(CONSTANT_FALSE) || filter.equals(HALF_TRUE)) {
            // We don't verify these because it's hard to get the right answer.
            return;
        }
        String prefix = String.format("[%s][%s][%s] ", grouping, op, blockType);
        if (grouping.equals(NONE)) {
            checkUngrouped(prefix, op, dataType, page, opCount, distinctPerGroup);
            return;
        }
        checkGrouped(prefix, grouping, op, dataType, page, opCount, dataGroupCount, distinctPerGroup);
    }

    private static void checkGrouped(
        String prefix,
        String grouping,
        String op,
        String dataType,
        Page page,
        int opCount,
        int dataGroupCount,
        int distinctPerGroup
    ) {
        switch (grouping) {
            case TWO_LONGS -> {
                checkGroupingBlock(prefix, LONGS, page.getBlock(0), dataGroupCount);
                checkGroupingBlock(prefix, LONGS, page.getBlock(1), dataGroupCount);
            }
            case TWO_BYTES_REFS, TWO_ORDINALS -> {
                checkGroupingBlock(prefix, BYTES_REFS, page.getBlock(0), dataGroupCount);
                checkGroupingBlock(prefix, BYTES_REFS, page.getBlock(1), dataGroupCount);
            }
            case LONGS_AND_BYTES_REFS -> {
                checkGroupingBlock(prefix, LONGS, page.getBlock(0), dataGroupCount);
                checkGroupingBlock(prefix, BYTES_REFS, page.getBlock(1), dataGroupCount);
            }
            case TWO_LONGS_AND_BYTES_REFS -> {
                checkGroupingBlock(prefix, LONGS, page.getBlock(0), dataGroupCount);
                checkGroupingBlock(prefix, LONGS, page.getBlock(1), dataGroupCount);
                checkGroupingBlock(prefix, BYTES_REFS, page.getBlock(2), dataGroupCount);
            }
            default -> checkGroupingBlock(prefix, grouping, page.getBlock(0), dataGroupCount);
        }
        Block values = page.getBlock(page.getBlockCount() - 1);
        int groups = dataGroupCount;
        int availableGroups = availableGroups(grouping, dataGroupCount);
        switch (op) {
            case AVG -> {
                DoubleBlock dValues = (DoubleBlock) values;
                for (int g = 0; g < availableGroups; g++) {
                    long group = g;
                    long sum = LongStream.range(0, BLOCK_LENGTH).filter(l -> l % groups == group).sum();
                    long count = LongStream.range(0, BLOCK_LENGTH).filter(l -> l % groups == group).count();
                    double expected = (double) sum / count;
                    if (dValues.getDouble(g) != expected) {
                        throw new AssertionError(prefix + "expected [" + expected + "] but was [" + dValues.getDouble(g) + "]");
                    }
                }
            }
            case COUNT -> {
                LongBlock lValues = (LongBlock) values;
                for (int g = 0; g < availableGroups; g++) {
                    long group = g;
                    long expected = LongStream.range(0, BLOCK_LENGTH).filter(l -> l % groups == group).count() * opCount;
                    if (lValues.getLong(g) != expected) {
                        throw new AssertionError(prefix + "expected [" + expected + "] but was [" + lValues.getLong(g) + "]");
                    }
                }
            }
            case COUNT_DISTINCT -> {
                LongBlock lValues = (LongBlock) values;
                for (int g = 0; g < availableGroups; g++) {
                    long expected = expectedDistinctCount(g, groups, distinctPerGroup);
                    long count = lValues.getLong(g);
                    // count should be within 10% from the expected value
                    if (count < expected * 0.9 || count > expected * 1.1) {
                        throw new AssertionError(prefix + "expected [" + expected + "] but was [" + count + "]");
                    }
                }
            }
            case MIN -> {
                switch (dataType) {
                    case LONGS -> {
                        LongBlock lValues = (LongBlock) values;
                        for (int g = 0; g < availableGroups; g++) {
                            if (lValues.getLong(g) != (long) g) {
                                throw new AssertionError(prefix + "expected [" + g + "] but was [" + lValues.getLong(g) + "]");
                            }
                        }
                    }
                    case DOUBLES -> {
                        DoubleBlock dValues = (DoubleBlock) values;
                        for (int g = 0; g < availableGroups; g++) {
                            if (dValues.getDouble(g) != (long) g) {
                                throw new AssertionError(prefix + "expected [" + g + "] but was [" + dValues.getDouble(g) + "]");
                            }
                        }
                    }
                    default -> throw new IllegalArgumentException("bad data type " + dataType);
                }
            }
            case MAX -> {
                switch (dataType) {
                    case LONGS -> {
                        LongBlock lValues = (LongBlock) values;
                        for (int g = 0; g < availableGroups; g++) {
                            long group = g;
                            long expected = LongStream.range(0, BLOCK_LENGTH).filter(l -> l % groups == group).max().getAsLong();
                            if (lValues.getLong(g) != expected) {
                                throw new AssertionError(prefix + "expected [" + expected + "] but was [" + lValues.getLong(g) + "]");
                            }
                        }
                    }
                    case DOUBLES -> {
                        DoubleBlock dValues = (DoubleBlock) values;
                        for (int g = 0; g < availableGroups; g++) {
                            long group = g;
                            long expected = LongStream.range(0, BLOCK_LENGTH).filter(l -> l % groups == group).max().getAsLong();
                            if (dValues.getDouble(g) != expected) {
                                throw new AssertionError(prefix + "expected [" + expected + "] but was [" + dValues.getDouble(g) + "]");
                            }
                        }
                    }
                    default -> throw new IllegalArgumentException("bad data type " + dataType);
                }
            }
            case SUM -> {
                switch (dataType) {
                    case LONGS -> {
                        LongBlock lValues = (LongBlock) values;
                        for (int g = 0; g < availableGroups; g++) {
                            long group = g;
                            long expected = LongStream.range(0, BLOCK_LENGTH).filter(l -> l % groups == group).sum() * opCount;
                            if (lValues.getLong(g) != expected) {
                                throw new AssertionError(prefix + "expected [" + expected + "] but was [" + lValues.getLong(g) + "]");
                            }
                        }
                    }
                    case DOUBLES -> {
                        DoubleBlock dValues = (DoubleBlock) values;
                        for (int g = 0; g < availableGroups; g++) {
                            long group = g;
                            long expected = LongStream.range(0, BLOCK_LENGTH).filter(l -> l % groups == group).sum() * opCount;
                            if (dValues.getDouble(g) != expected) {
                                throw new AssertionError(prefix + "expected [" + expected + "] but was [" + dValues.getDouble(g) + "]");
                            }
                        }
                    }
                    default -> throw new IllegalArgumentException("bad data type " + dataType);
                }
            }
            default -> throw new IllegalArgumentException("bad op " + op);
        }
    }

    private static int dataGroupCount(String grouping, int groupCount) {
        int normalized = Math.max(1, groupCount);
        return switch (grouping) {
            case NONE -> 1;
            case BOOLEANS -> 2;
            default -> normalized;
        };
    }

    private static int availableGroups(String grouping, int groupCount) {
        int normalized = Math.max(1, groupCount);
        int populatedGroups = Math.min(normalized, BLOCK_LENGTH);
        return switch (grouping) {
            case TOP_N_LONGS -> Math.min(TOP_N_LIMIT, populatedGroups);
            default -> populatedGroups;
        };
    }

    private static long expectedDistinctCount(int group, int groupCount, int distinctPerGroup) {
        int values = valuesForGroup(group, groupCount);
        if (distinctPerGroup > 0) {
            return Math.min(distinctPerGroup, values);
        }
        return values;
    }

    private static int valuesForGroup(int group, int groupCount) {
        int base = BLOCK_LENGTH / groupCount;
        int remainder = BLOCK_LENGTH % groupCount;
        return base + (group < remainder ? 1 : 0);
    }

    private static boolean useDistinctPerGroup(String op, int distinctPerGroup) {
        return op.equals(COUNT_DISTINCT) && distinctPerGroup > 0;
    }

    private static long distinctValue(int index, int groupCount, int distinctPerGroup) {
        if (distinctPerGroup <= 0) {
            return index;
        }
        int normalized = Math.max(1, groupCount);
        int ordinalInGroup = index / normalized;
        return ordinalInGroup % distinctPerGroup;
    }

    private static void checkGroupingBlock(String prefix, String grouping, Block block, int groupCount) {
        int availableGroups = availableGroups(grouping, groupCount);
        switch (grouping) {
            case LONGS -> {
                LongBlock groups = (LongBlock) block;
                for (int g = 0; g < availableGroups; g++) {
                    if (groups.getLong(g) != (long) g) {
                        throw new AssertionError(prefix + "bad group expected [" + g + "] but was [" + groups.getLong(g) + "]");
                    }
                }
            }
            case TOP_N_LONGS -> {
                LongBlock groups = (LongBlock) block;
                for (int g = 0; g < availableGroups; g++) {
                    if (groups.getLong(g) != (long) g) {
                        throw new AssertionError(prefix + "bad group expected [" + g + "] but was [" + groups.getLong(g) + "]");
                    }
                }
            }
            case INTS -> {
                IntBlock groups = (IntBlock) block;
                for (int g = 0; g < availableGroups; g++) {
                    if (groups.getInt(g) != g) {
                        throw new AssertionError(prefix + "bad group expected [" + g + "] but was [" + groups.getInt(g) + "]");
                    }
                }
            }
            case DOUBLES -> {
                DoubleBlock groups = (DoubleBlock) block;
                for (int g = 0; g < availableGroups; g++) {
                    if (groups.getDouble(g) != (double) g) {
                        throw new AssertionError(prefix + "bad group expected [" + (double) g + "] but was [" + groups.getDouble(g) + "]");
                    }
                }
            }
            case BOOLEANS -> {
                BooleanBlock groups = (BooleanBlock) block;
                if (groups.getBoolean(0) != false) {
                    throw new AssertionError(prefix + "bad group expected [false] but was [" + groups.getBoolean(0) + "]");
                }
                if (groups.getBoolean(1) != true) {
                    throw new AssertionError(prefix + "bad group expected [true] but was [" + groups.getBoolean(1) + "]");
                }
            }
            case BYTES_REFS, ORDINALS -> {
                BytesRefBlock groups = (BytesRefBlock) block;
                for (int g = 0; g < availableGroups; g++) {
                    if (false == groups.getBytesRef(g, new BytesRef()).equals(bytesGroup(g))) {
                        throw new AssertionError(
                            prefix + "bad group expected [" + bytesGroup(g) + "] but was [" + groups.getBytesRef(g, new BytesRef()) + "]"
                        );
                    }
                }
            }
            default -> throw new IllegalArgumentException("bad grouping [" + grouping + "]");
        }
    }

    private static void checkUngrouped(String prefix, String op, String dataType, Page page, int opCount, int distinctPerGroup) {
        Block block = page.getBlock(0);
        switch (op) {
            case AVG -> {
                DoubleBlock dBlock = (DoubleBlock) block;
                if (dBlock.getDouble(0) != (BLOCK_LENGTH - 1) / 2.0) {
                    throw new AssertionError(
                        prefix + "expected [" + ((BLOCK_LENGTH - 1) / 2.0) + "] but was [" + dBlock.getDouble(0) + "]"
                    );
                }
            }
            case COUNT -> {
                LongBlock lBlock = (LongBlock) block;
                if (lBlock.getLong(0) != (long) BLOCK_LENGTH * opCount) {
                    throw new AssertionError(prefix + "expected [" + (BLOCK_LENGTH * opCount) + "] but was [" + lBlock.getLong(0) + "]");
                }
            }
            case COUNT_DISTINCT -> {
                LongBlock lBlock = (LongBlock) block;
                long expected = expectedDistinctCount(0, 1, distinctPerGroup);
                long count = lBlock.getLong(0);
                // count should be within 10% from the expected value
                if (count < expected * 0.9 || count > expected * 1.1) {
                    throw new AssertionError(prefix + "expected [" + expected + "] but was [" + count + "]");
                }
            }
            case MIN -> {
                long expected = 0L;
                var val = switch (dataType) {
                    case LONGS -> ((LongBlock) block).getLong(0);
                    case DOUBLES -> ((DoubleBlock) block).getDouble(0);
                    default -> throw new IllegalStateException("Unexpected aggregation type: " + dataType);
                };
                if (val != expected) {
                    throw new AssertionError(prefix + "expected [" + expected + "] but was [" + val + "]");
                }
            }
            case MAX -> {
                long expected = BLOCK_LENGTH - 1;
                var val = switch (dataType) {
                    case LONGS -> ((LongBlock) block).getLong(0);
                    case DOUBLES -> ((DoubleBlock) block).getDouble(0);
                    default -> throw new IllegalStateException("Unexpected aggregation type: " + dataType);
                };
                if (val != expected) {
                    throw new AssertionError(prefix + "expected [" + expected + "] but was [" + val + "]");
                }
            }
            case SUM -> {
                long expected = (BLOCK_LENGTH * (BLOCK_LENGTH - 1L)) * ((long) opCount) / 2;
                var val = switch (dataType) {
                    case LONGS -> ((LongBlock) block).getLong(0);
                    case DOUBLES -> ((DoubleBlock) block).getDouble(0);
                    default -> throw new IllegalStateException("Unexpected aggregation type: " + dataType);
                };
                if (val != expected) {
                    throw new AssertionError(prefix + "expected [" + expected + "] but was [" + val + "]");
                }
            }
            default -> throw new IllegalArgumentException("bad op " + op);
        }
    }

    private static Page page(
        BlockFactory blockFactory,
        String grouping,
        String blockType,
        String op,
        int groupCount,
        int distinctPerGroup
    ) {
        Block dataBlock = dataBlock(blockFactory, blockType, op, groupCount, distinctPerGroup);
        if (grouping.equals(NONE)) {
            return new Page(dataBlock);
        }
        List<Block> blocks = groupingBlocks(grouping, blockType, groupCount);
        return new Page(Stream.concat(blocks.stream(), Stream.of(dataBlock)).toArray(Block[]::new));
    }

    private static Block dataBlock(BlockFactory blockFactory, String blockType, String op, int groupCount, int distinctPerGroup) {
        boolean limitDistinct = useDistinctPerGroup(op, distinctPerGroup);
        return switch (blockType) {
            case VECTOR_LONGS -> {
                long[] values = new long[BLOCK_LENGTH];
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    values[i] = limitDistinct ? distinctValue(i, groupCount, distinctPerGroup) : i;
                }
                yield blockFactory.newLongArrayVector(values, BLOCK_LENGTH).asBlock();
            }
            case VECTOR_DOUBLES -> {
                double[] values = new double[BLOCK_LENGTH];
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    long value = limitDistinct ? distinctValue(i, groupCount, distinctPerGroup) : i;
                    values[i] = (double) value;
                }
                yield blockFactory.newDoubleArrayVector(values, BLOCK_LENGTH).asBlock();
            }
            case MULTIVALUED_LONGS -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                builder.beginPositionEntry();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendLong(i);
                    if (i % 5 == 0) {
                        builder.endPositionEntry();
                        builder.beginPositionEntry();
                    }
                }
                builder.endPositionEntry();
                yield builder.build();
            }
            case HALF_NULL_LONGS -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendLong(limitDistinct ? distinctValue(i, groupCount, distinctPerGroup) : i);
                    builder.appendNull();
                }
                yield builder.build();
            }
            case HALF_NULL_DOUBLES -> {
                var builder = blockFactory.newDoubleBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    double value = limitDistinct ? distinctValue(i, groupCount, distinctPerGroup) : i;
                    builder.appendDouble(value);
                    builder.appendNull();
                }
                yield builder.build();
            }
            default -> throw new IllegalArgumentException("bad blockType: " + blockType);
        };
    }

    private static List<Block> groupingBlocks(String grouping, String blockType, int groupCount) {
        return switch (grouping) {
            case TWO_LONGS -> List.of(groupingBlock(LONGS, blockType, groupCount), groupingBlock(LONGS, blockType, groupCount));
            case TWO_BYTES_REFS -> List.of(groupingBlock(BYTES_REFS, blockType, groupCount), groupingBlock(BYTES_REFS, blockType, groupCount));
            case TWO_ORDINALS -> List.of(groupingBlock(ORDINALS, blockType, groupCount), groupingBlock(ORDINALS, blockType, groupCount));
            case LONGS_AND_BYTES_REFS -> List.of(groupingBlock(LONGS, blockType, groupCount), groupingBlock(BYTES_REFS, blockType, groupCount));
            case TWO_LONGS_AND_BYTES_REFS -> List.of(
                groupingBlock(LONGS, blockType, groupCount),
                groupingBlock(LONGS, blockType, groupCount),
                groupingBlock(BYTES_REFS, blockType, groupCount)
            );
            default -> List.of(groupingBlock(grouping, blockType, groupCount));
        };
    }

    private static Block groupingBlock(String grouping, String blockType, int groupCount) {
        int valuesPerGroup = switch (blockType) {
            case VECTOR_LONGS, VECTOR_DOUBLES -> 1;
            case HALF_NULL_LONGS, HALF_NULL_DOUBLES -> 2;
            default -> throw new UnsupportedOperationException("bad grouping [" + grouping + "]");
        };
        return switch (grouping) {
            case TOP_N_LONGS, LONGS -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    for (int v = 0; v < valuesPerGroup; v++) {
                        builder.appendLong(i % groupCount);
                    }
                }
                yield builder.build();
            }
            case INTS -> {
                var builder = blockFactory.newIntBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    for (int v = 0; v < valuesPerGroup; v++) {
                        builder.appendInt(i % groupCount);
                    }
                }
                yield builder.build();
            }
            case DOUBLES -> {
                var builder = blockFactory.newDoubleBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    for (int v = 0; v < valuesPerGroup; v++) {
                        builder.appendDouble(i % groupCount);
                    }
                }
                yield builder.build();
            }
            case BOOLEANS -> {
                BooleanBlock.Builder builder = blockFactory.newBooleanBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    for (int v = 0; v < valuesPerGroup; v++) {
                        builder.appendBoolean(i % 2 == 1);
                    }
                }
                yield builder.build();
            }
            case BYTES_REFS -> {
                BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    for (int v = 0; v < valuesPerGroup; v++) {
                        builder.appendBytesRef(bytesGroup(i % groupCount));
                    }
                }
                yield builder.build();
            }
            case ORDINALS -> {
                IntVector.Builder ordinals = blockFactory.newIntVectorBuilder(BLOCK_LENGTH * valuesPerGroup);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    for (int v = 0; v < valuesPerGroup; v++) {
                        ordinals.appendInt(i % groupCount);
                    }
                }
                BytesRefVector.Builder bytes = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH * valuesPerGroup);
                for (int i = 0; i < groupCount; i++) {
                    bytes.appendBytesRef(bytesGroup(i));
                }
                yield new OrdinalBytesRefVector(ordinals.build(), bytes.build()).asBlock();
            }
            default -> throw new UnsupportedOperationException("unsupported grouping [" + grouping + "]");
        };
    }

    private static BytesRef bytesGroup(int group) {
        return new BytesRef(switch (group) {
            case 0 -> "cat";
            case 1 -> "dog";
            case 2 -> "chicken";
            case 3 -> "pig";
            case 4 -> "cow";
            default -> "group-" + group;
        });
    }

    private static AggregatorFunctionSupplier filtered(AggregatorFunctionSupplier agg, String filter) {
        if (filter.equals("none")) {
            return agg;
        }
        BooleanBlock mask = mask(filter).asBlock();
        return new FilteredAggregatorFunctionSupplier(agg, context -> new EvalOperator.ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                mask.incRef();
                return mask;
            }

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }

            @Override
            public void close() {
                mask.close();
            }
        });
    }

    private static BooleanVector mask(String filter) {
        // Usually BLOCK_LENGTH is the count of positions, but sometimes the blocks are longer
        int positionCount = BLOCK_LENGTH * 10;
        return switch (filter) {
            case CONSTANT_TRUE -> blockFactory.newConstantBooleanVector(true, positionCount);
            case ALL_TRUE -> {
                try (BooleanVector.Builder builder = blockFactory.newBooleanVectorFixedBuilder(positionCount)) {
                    for (int i = 0; i < positionCount; i++) {
                        builder.appendBoolean(true);
                    }
                    yield builder.build();
                }
            }
            case HALF_TRUE -> {
                try (BooleanVector.Builder builder = blockFactory.newBooleanVectorFixedBuilder(positionCount)) {
                    for (int i = 0; i < positionCount; i++) {
                        builder.appendBoolean(i % 2 == 0);
                    }
                    yield builder.build();
                }
            }
            case CONSTANT_FALSE -> blockFactory.newConstantBooleanVector(false, positionCount);
            default -> throw new IllegalArgumentException("unsupported filter [" + filter + "]");
        };
    }

    @Benchmark
    @OperationsPerInvocation(OP_COUNT * BLOCK_LENGTH)
    public void run() {
        run(grouping, op, blockType, filter, groups, distinctPerGroup, OP_COUNT);
    }

    private static void run(String grouping, String op, String blockType, String filter, int groups, int distinctPerGroup, int opCount) {
        // System.err.printf("[%s][%s][%s][%s][%s]\n", grouping, op, blockType, filter, opCount);
        String dataType = switch (blockType) {
            case VECTOR_LONGS, HALF_NULL_LONGS -> LONGS;
            case VECTOR_DOUBLES, HALF_NULL_DOUBLES -> DOUBLES;
            default -> throw new IllegalArgumentException();
        };

        DriverContext driverContext = driverContext();
        int dataGroupCount = dataGroupCount(grouping, groups);
        try (Operator operator = operator(driverContext, grouping, op, dataType, filter)) {
            Page page = page(driverContext.blockFactory(), grouping, blockType, op, dataGroupCount, distinctPerGroup);
            for (int i = 0; i < opCount; i++) {
                operator.addInput(page.shallowCopy());
            }
            operator.finish();
            checkExpected(grouping, op, blockType, filter, dataType, operator.getOutput(), opCount, dataGroupCount, distinctPerGroup);
        }
    }

    static DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
    }
}
