/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.CountAggregatorFunction;
import org.elasticsearch.compute.aggregation.CountDistinctDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountDistinctLongAggregatorFunctionSupplier;
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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
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

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class AggregatorBenchmark {
    static final int BLOCK_LENGTH = 8 * 1024;
    private static final int OP_COUNT = 1024;
    private static final int GROUPS = 5;

    private static final BlockFactory blockFactory = BlockFactory.getInstance(
        new NoopCircuitBreaker("noop"),
        BigArrays.NON_RECYCLING_INSTANCE  // TODO real big arrays?
    );

    private static final String LONGS = "longs";
    private static final String INTS = "ints";
    private static final String DOUBLES = "doubles";
    private static final String BOOLEANS = "booleans";
    private static final String BYTES_REFS = "bytes_refs";
    private static final String TWO_LONGS = "two_" + LONGS;
    private static final String LONGS_AND_BYTES_REFS = LONGS + "_and_" + BYTES_REFS;
    private static final String TWO_LONGS_AND_BYTES_REFS = "two_" + LONGS + "_and_" + BYTES_REFS;

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

    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        try {
            for (String grouping : AggregatorBenchmark.class.getField("grouping").getAnnotationsByType(Param.class)[0].value()) {
                for (String op : AggregatorBenchmark.class.getField("op").getAnnotationsByType(Param.class)[0].value()) {
                    for (String blockType : AggregatorBenchmark.class.getField("blockType").getAnnotationsByType(Param.class)[0].value()) {
                        run(grouping, op, blockType, 50);
                    }
                }
            }
        } catch (NoSuchFieldException e) {
            throw new AssertionError();
        }
    }

    @Param({ NONE, LONGS, INTS, DOUBLES, BOOLEANS, BYTES_REFS, TWO_LONGS, LONGS_AND_BYTES_REFS, TWO_LONGS_AND_BYTES_REFS })
    public String grouping;

    @Param({ COUNT, COUNT_DISTINCT, MIN, MAX, SUM })
    public String op;

    @Param({ VECTOR_LONGS, HALF_NULL_LONGS, VECTOR_DOUBLES, HALF_NULL_DOUBLES })
    public String blockType;

    private static Operator operator(DriverContext driverContext, String grouping, String op, String dataType) {
        if (grouping.equals("none")) {
            return new AggregationOperator(
                List.of(supplier(op, dataType, 0).aggregatorFactory(AggregatorMode.SINGLE).apply(driverContext)),
                driverContext
            );
        }
        List<HashAggregationOperator.GroupSpec> groups = switch (grouping) {
            case LONGS -> List.of(new HashAggregationOperator.GroupSpec(0, ElementType.LONG));
            case INTS -> List.of(new HashAggregationOperator.GroupSpec(0, ElementType.INT));
            case DOUBLES -> List.of(new HashAggregationOperator.GroupSpec(0, ElementType.DOUBLE));
            case BOOLEANS -> List.of(new HashAggregationOperator.GroupSpec(0, ElementType.BOOLEAN));
            case BYTES_REFS -> List.of(new HashAggregationOperator.GroupSpec(0, ElementType.BYTES_REF));
            case TWO_LONGS -> List.of(
                new HashAggregationOperator.GroupSpec(0, ElementType.LONG),
                new HashAggregationOperator.GroupSpec(1, ElementType.LONG)
            );
            case LONGS_AND_BYTES_REFS -> List.of(
                new HashAggregationOperator.GroupSpec(0, ElementType.LONG),
                new HashAggregationOperator.GroupSpec(1, ElementType.BYTES_REF)
            );
            case TWO_LONGS_AND_BYTES_REFS -> List.of(
                new HashAggregationOperator.GroupSpec(0, ElementType.LONG),
                new HashAggregationOperator.GroupSpec(1, ElementType.LONG),
                new HashAggregationOperator.GroupSpec(2, ElementType.BYTES_REF)
            );
            default -> throw new IllegalArgumentException("unsupported grouping [" + grouping + "]");
        };
        return new HashAggregationOperator(
            List.of(supplier(op, dataType, groups.size()).groupingAggregatorFactory(AggregatorMode.SINGLE)),
            () -> BlockHash.build(groups, driverContext.blockFactory(), 16 * 1024, false),
            driverContext
        );
    }

    private static AggregatorFunctionSupplier supplier(String op, String dataType, int dataChannel) {
        return switch (op) {
            case COUNT -> CountAggregatorFunction.supplier(List.of(dataChannel));
            case COUNT_DISTINCT -> switch (dataType) {
                case LONGS -> new CountDistinctLongAggregatorFunctionSupplier(List.of(dataChannel), 3000);
                case DOUBLES -> new CountDistinctDoubleAggregatorFunctionSupplier(List.of(dataChannel), 3000);
                default -> throw new IllegalArgumentException("unsupported data type [" + dataType + "]");
            };
            case MAX -> switch (dataType) {
                case LONGS -> new MaxLongAggregatorFunctionSupplier(List.of(dataChannel));
                case DOUBLES -> new MaxDoubleAggregatorFunctionSupplier(List.of(dataChannel));
                default -> throw new IllegalArgumentException("unsupported data type [" + dataType + "]");
            };
            case MIN -> switch (dataType) {
                case LONGS -> new MinLongAggregatorFunctionSupplier(List.of(dataChannel));
                case DOUBLES -> new MinDoubleAggregatorFunctionSupplier(List.of(dataChannel));
                default -> throw new IllegalArgumentException("unsupported data type [" + dataType + "]");
            };
            case SUM -> switch (dataType) {
                case LONGS -> new SumLongAggregatorFunctionSupplier(List.of(dataChannel));
                case DOUBLES -> new SumDoubleAggregatorFunctionSupplier(List.of(dataChannel));
                default -> throw new IllegalArgumentException("unsupported data type [" + dataType + "]");
            };
            default -> throw new IllegalArgumentException("unsupported op [" + op + "]");
        };
    }

    private static void checkExpected(String grouping, String op, String blockType, String dataType, Page page, int opCount) {
        String prefix = String.format("[%s][%s][%s] ", grouping, op, blockType);
        if (grouping.equals("none")) {
            checkUngrouped(prefix, op, dataType, page, opCount);
            return;
        }
        checkGrouped(prefix, grouping, op, dataType, page, opCount);
    }

    private static void checkGrouped(String prefix, String grouping, String op, String dataType, Page page, int opCount) {
        switch (grouping) {
            case TWO_LONGS -> {
                checkGroupingBlock(prefix, LONGS, page.getBlock(0));
                checkGroupingBlock(prefix, LONGS, page.getBlock(1));
            }
            case LONGS_AND_BYTES_REFS -> {
                checkGroupingBlock(prefix, LONGS, page.getBlock(0));
                checkGroupingBlock(prefix, BYTES_REFS, page.getBlock(1));
            }
            case TWO_LONGS_AND_BYTES_REFS -> {
                checkGroupingBlock(prefix, LONGS, page.getBlock(0));
                checkGroupingBlock(prefix, LONGS, page.getBlock(1));
                checkGroupingBlock(prefix, BYTES_REFS, page.getBlock(2));
            }
            default -> checkGroupingBlock(prefix, grouping, page.getBlock(0));
        }
        Block values = page.getBlock(page.getBlockCount() - 1);
        int groups = switch (grouping) {
            case BOOLEANS -> 2;
            default -> GROUPS;
        };
        switch (op) {
            case AVG -> {
                DoubleBlock dValues = (DoubleBlock) values;
                for (int g = 0; g < groups; g++) {
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
                for (int g = 0; g < groups; g++) {
                    long group = g;
                    long expected = LongStream.range(0, BLOCK_LENGTH).filter(l -> l % groups == group).count() * opCount;
                    if (lValues.getLong(g) != expected) {
                        throw new AssertionError(prefix + "expected [" + expected + "] but was [" + lValues.getLong(g) + "]");
                    }
                }
            }
            case COUNT_DISTINCT -> {
                LongBlock lValues = (LongBlock) values;
                for (int g = 0; g < groups; g++) {
                    long group = g;
                    long expected = LongStream.range(0, BLOCK_LENGTH).filter(l -> l % groups == group).distinct().count();
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
                        for (int g = 0; g < groups; g++) {
                            if (lValues.getLong(g) != (long) g) {
                                throw new AssertionError(prefix + "expected [" + g + "] but was [" + lValues.getLong(g) + "]");
                            }
                        }
                    }
                    case DOUBLES -> {
                        DoubleBlock dValues = (DoubleBlock) values;
                        for (int g = 0; g < groups; g++) {
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
                        for (int g = 0; g < groups; g++) {
                            long group = g;
                            long expected = LongStream.range(0, BLOCK_LENGTH).filter(l -> l % groups == group).max().getAsLong();
                            if (lValues.getLong(g) != expected) {
                                throw new AssertionError(prefix + "expected [" + expected + "] but was [" + lValues.getLong(g) + "]");
                            }
                        }
                    }
                    case DOUBLES -> {
                        DoubleBlock dValues = (DoubleBlock) values;
                        for (int g = 0; g < groups; g++) {
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
                        for (int g = 0; g < groups; g++) {
                            long group = g;
                            long expected = LongStream.range(0, BLOCK_LENGTH).filter(l -> l % groups == group).sum() * opCount;
                            if (lValues.getLong(g) != expected) {
                                throw new AssertionError(prefix + "expected [" + expected + "] but was [" + lValues.getLong(g) + "]");
                            }
                        }
                    }
                    case DOUBLES -> {
                        DoubleBlock dValues = (DoubleBlock) values;
                        for (int g = 0; g < groups; g++) {
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

    private static void checkGroupingBlock(String prefix, String grouping, Block block) {
        switch (grouping) {
            case LONGS -> {
                LongBlock groups = (LongBlock) block;
                for (int g = 0; g < GROUPS; g++) {
                    if (groups.getLong(g) != (long) g) {
                        throw new AssertionError(prefix + "bad group expected [" + g + "] but was [" + groups.getLong(g) + "]");
                    }
                }
            }
            case INTS -> {
                IntBlock groups = (IntBlock) block;
                for (int g = 0; g < GROUPS; g++) {
                    if (groups.getInt(g) != g) {
                        throw new AssertionError(prefix + "bad group expected [" + g + "] but was [" + groups.getInt(g) + "]");
                    }
                }
            }
            case DOUBLES -> {
                DoubleBlock groups = (DoubleBlock) block;
                for (int g = 0; g < GROUPS; g++) {
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
            case BYTES_REFS -> {
                BytesRefBlock groups = (BytesRefBlock) block;
                for (int g = 0; g < GROUPS; g++) {
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

    private static void checkUngrouped(String prefix, String op, String dataType, Page page, int opCount) {
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
                long expected = BLOCK_LENGTH;
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

    private static Page page(BlockFactory blockFactory, String grouping, String blockType) {
        Block dataBlock = dataBlock(blockFactory, blockType);
        if (grouping.equals("none")) {
            return new Page(dataBlock);
        }
        List<Block> blocks = groupingBlocks(grouping, blockType);
        return new Page(Stream.concat(blocks.stream(), Stream.of(dataBlock)).toArray(Block[]::new));
    }

    private static Block dataBlock(BlockFactory blockFactory, String blockType) {
        return switch (blockType) {
            case VECTOR_LONGS -> blockFactory.newLongArrayVector(LongStream.range(0, BLOCK_LENGTH).toArray(), BLOCK_LENGTH).asBlock();
            case VECTOR_DOUBLES -> blockFactory.newDoubleArrayVector(
                LongStream.range(0, BLOCK_LENGTH).mapToDouble(l -> Long.valueOf(l).doubleValue()).toArray(),
                BLOCK_LENGTH
            ).asBlock();
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
                    builder.appendLong(i);
                    builder.appendNull();
                }
                yield builder.build();
            }
            case HALF_NULL_DOUBLES -> {
                var builder = blockFactory.newDoubleBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendDouble(i);
                    builder.appendNull();
                }
                yield builder.build();
            }
            default -> throw new IllegalArgumentException("bad blockType: " + blockType);
        };
    }

    private static List<Block> groupingBlocks(String grouping, String blockType) {
        return switch (grouping) {
            case TWO_LONGS -> List.of(groupingBlock(LONGS, blockType), groupingBlock(LONGS, blockType));
            case LONGS_AND_BYTES_REFS -> List.of(groupingBlock(LONGS, blockType), groupingBlock(BYTES_REFS, blockType));
            case TWO_LONGS_AND_BYTES_REFS -> List.of(
                groupingBlock(LONGS, blockType),
                groupingBlock(LONGS, blockType),
                groupingBlock(BYTES_REFS, blockType)
            );
            default -> List.of(groupingBlock(grouping, blockType));
        };
    }

    private static Block groupingBlock(String grouping, String blockType) {
        int valuesPerGroup = switch (blockType) {
            case VECTOR_LONGS, VECTOR_DOUBLES -> 1;
            case HALF_NULL_LONGS, HALF_NULL_DOUBLES -> 2;
            default -> throw new UnsupportedOperationException("bad grouping [" + grouping + "]");
        };
        return switch (grouping) {
            case LONGS -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    for (int v = 0; v < valuesPerGroup; v++) {
                        builder.appendLong(i % GROUPS);
                    }
                }
                yield builder.build();
            }
            case INTS -> {
                var builder = blockFactory.newIntBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    for (int v = 0; v < valuesPerGroup; v++) {
                        builder.appendInt(i % GROUPS);
                    }
                }
                yield builder.build();
            }
            case DOUBLES -> {
                var builder = blockFactory.newDoubleBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    for (int v = 0; v < valuesPerGroup; v++) {
                        builder.appendDouble(i % GROUPS);
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
                        builder.appendBytesRef(bytesGroup(i % GROUPS));
                    }
                }
                yield builder.build();
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
            default -> throw new UnsupportedOperationException("can't handle [" + group + "]");
        });
    }

    @Benchmark
    @OperationsPerInvocation(OP_COUNT * BLOCK_LENGTH)
    public void run() {
        run(grouping, op, blockType, OP_COUNT);
    }

    private static void run(String grouping, String op, String blockType, int opCount) {
        String dataType = switch (blockType) {
            case VECTOR_LONGS, HALF_NULL_LONGS -> LONGS;
            case VECTOR_DOUBLES, HALF_NULL_DOUBLES -> DOUBLES;
            default -> throw new IllegalArgumentException();
        };

        DriverContext driverContext = driverContext();
        Operator operator = operator(driverContext, grouping, op, dataType);
        Page page = page(driverContext.blockFactory(), grouping, blockType);
        for (int i = 0; i < opCount; i++) {
            operator.addInput(page);
        }
        operator.finish();
        checkExpected(grouping, op, blockType, dataType, operator.getOutput(), opCount);
    }

    static DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory);
    }
}
