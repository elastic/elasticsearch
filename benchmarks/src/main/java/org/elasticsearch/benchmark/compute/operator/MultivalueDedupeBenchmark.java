/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.MultivalueDedupe;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class MultivalueDedupeBenchmark {
    @Param({ "BOOLEAN", "BYTES_REF", "DOUBLE", "INT", "LONG" })
    private ElementType elementType;

    @Param({ "3", "5", "10", "50", "100", "1000" })
    private int size;

    @Param({ "0", "2", "10", "100", "1000" })
    private int repeats;

    private Block block;

    @Setup
    public void setup() {
        this.block = switch (elementType) {
            case BOOLEAN -> {
                BooleanBlock.Builder builder = BooleanBlock.newBlockBuilder(AggregatorBenchmark.BLOCK_LENGTH * (size + repeats));
                for (int p = 0; p < AggregatorBenchmark.BLOCK_LENGTH; p++) {
                    List<Boolean> values = new ArrayList<>();
                    for (int i = 0; i < size; i++) {
                        values.add(i % 2 == 0);
                    }
                    for (int r = 0; r < repeats; r++) {
                        values.add(r < size ? r % 2 == 0 : false);
                    }
                    Randomness.shuffle(values);
                    builder.beginPositionEntry();
                    for (Boolean v : values) {
                        builder.appendBoolean(v);
                    }
                    builder.endPositionEntry();
                }
                yield builder.build();
            }
            case BYTES_REF -> {
                BytesRefBlock.Builder builder = BytesRefBlock.newBlockBuilder(AggregatorBenchmark.BLOCK_LENGTH * (size + repeats));
                for (int p = 0; p < AggregatorBenchmark.BLOCK_LENGTH; p++) {
                    List<BytesRef> values = new ArrayList<>();
                    for (int i = 0; i < size; i++) {
                        values.add(new BytesRef("SAFADFASDFSADFDAFS" + i));
                    }
                    for (int r = 0; r < repeats; r++) {
                        values.add(new BytesRef("SAFADFASDFSADFDAFS" + ((r < size ? r : 0))));
                    }
                    Randomness.shuffle(values);
                    builder.beginPositionEntry();
                    for (BytesRef v : values) {
                        builder.appendBytesRef(v);
                    }
                    builder.endPositionEntry();
                }
                yield builder.build();
            }
            case DOUBLE -> {
                DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(AggregatorBenchmark.BLOCK_LENGTH * (size + repeats));
                for (int p = 0; p < AggregatorBenchmark.BLOCK_LENGTH; p++) {
                    List<Double> values = new ArrayList<>();
                    for (int i = 0; i < size; i++) {
                        values.add((double) i);
                    }
                    for (int r = 0; r < repeats; r++) {
                        values.add(r < size ? (double) r : 0.0);
                    }
                    Randomness.shuffle(values);
                    builder.beginPositionEntry();
                    for (Double v : values) {
                        builder.appendDouble(v);
                    }
                    builder.endPositionEntry();
                }
                yield builder.build();
            }
            case INT -> {
                IntBlock.Builder builder = IntBlock.newBlockBuilder(AggregatorBenchmark.BLOCK_LENGTH * (size + repeats));
                for (int p = 0; p < AggregatorBenchmark.BLOCK_LENGTH; p++) {
                    List<Integer> values = new ArrayList<>();
                    for (int i = 0; i < size; i++) {
                        values.add(i);
                    }
                    for (int r = 0; r < repeats; r++) {
                        values.add(r < size ? r : 0);
                    }
                    Randomness.shuffle(values);
                    builder.beginPositionEntry();
                    for (Integer v : values) {
                        builder.appendInt(v);
                    }
                    builder.endPositionEntry();
                }
                yield builder.build();
            }
            case LONG -> {
                LongBlock.Builder builder = LongBlock.newBlockBuilder(AggregatorBenchmark.BLOCK_LENGTH * (size + repeats));
                for (int p = 0; p < AggregatorBenchmark.BLOCK_LENGTH; p++) {
                    List<Long> values = new ArrayList<>();
                    for (long i = 0; i < size; i++) {
                        values.add(i);
                    }
                    for (int r = 0; r < repeats; r++) {
                        values.add(r < size ? r : 0L);
                    }
                    Randomness.shuffle(values);
                    builder.beginPositionEntry();
                    for (Long v : values) {
                        builder.appendLong(v);
                    }
                    builder.endPositionEntry();
                }
                yield builder.build();
            }
            default -> throw new IllegalArgumentException();
        };
    }

    @Benchmark
    @OperationsPerInvocation(AggregatorBenchmark.BLOCK_LENGTH)
    public Block adaptive() {
        return MultivalueDedupe.dedupeToBlockAdaptive(block);
    }

    @Benchmark
    @OperationsPerInvocation(AggregatorBenchmark.BLOCK_LENGTH)
    public Block copyAndSort() {
        return MultivalueDedupe.dedupeToBlockUsingCopyAndSort(block);
    }

    @Benchmark
    @OperationsPerInvocation(AggregatorBenchmark.BLOCK_LENGTH)
    public Block copyMissing() {
        return MultivalueDedupe.dedupeToBlockUsingCopyMissing(block);
    }
}
