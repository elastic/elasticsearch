/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark._nightly.esql;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.topn.NumericTopNOperator;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.core.Releasables;
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
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Go/no-go benchmark for {@link NumericTopNOperator}: at K ∈ {100, 1000, 10000} on a single
 * LONG sort key with a 2-channel {@code [sortKey, _rowPosition]} page, the new operator must
 * be measurably faster than the generic {@link TopNOperator}. If it isn't, Stage 1 stops and
 * the plan reconsiders the buffer-based variant.
 *
 * <p>A separate benchmark class (rather than a new flag on {@code TopNBenchmark}) keeps the
 * self-test domain pure: this class's parameter combinations are all valid for both operators,
 * so the mandatory self-test exercises every cell without skips.
 */
@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class NumericTopNBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final int BLOCK_LENGTH = 4 * 1024;
    private static final int RUNS_PER_INVOCATION = 1024;

    static {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    static void selfTest() {
        for (String impl : Utils.possibleValues(NumericTopNBenchmark.class, "impl")) {
            for (String topCount : Utils.possibleValues(NumericTopNBenchmark.class, "topCount")) {
                for (String direction : Utils.possibleValues(NumericTopNBenchmark.class, "direction")) {
                    NumericTopNBenchmark bench = new NumericTopNBenchmark();
                    bench.impl = impl;
                    bench.topCount = Integer.parseInt(topCount);
                    bench.direction = direction;
                    bench.setup();
                    bench.run();
                }
            }
        }
    }

    @Param({ "generic", "numericTernaryHeap" })
    public String impl;

    @Param({ "100", "1000", "10000" })
    public int topCount;

    @Param({ "asc", "desc" })
    public String direction;

    private Page page;

    @Setup
    public void setup() {
        page = buildPage();
    }

    private static Page buildPage() {
        // Random distinct longs over a wide range so the threshold tightens monotonically and
        // we exercise the {@code updateTop} path heavily once the heap fills.
        Random rng = new Random(42);
        LongBlock sortBlock;
        LongVector rowPositionVector;
        try (
            LongBlock.Builder sortBuilder = BLOCK_FACTORY.newLongBlockBuilder(BLOCK_LENGTH);
            LongVector.FixedBuilder rpBuilder = BLOCK_FACTORY.newLongVectorFixedBuilder(BLOCK_LENGTH)
        ) {
            for (int i = 0; i < BLOCK_LENGTH; i++) {
                sortBuilder.appendLong(rng.nextLong());
                rpBuilder.appendLong(i);
            }
            sortBlock = sortBuilder.build();
            rowPositionVector = rpBuilder.build();
        }
        return new Page(sortBlock, rowPositionVector.asBlock());
    }

    private boolean asc() {
        return "asc".equals(direction);
    }

    private Operator operator() {
        if ("generic".equals(impl)) {
            SharedMinCompetitive.Supplier noThreshold = null;
            return new TopNOperator(
                BLOCK_FACTORY,
                BLOCK_FACTORY.breaker(),
                topCount,
                List.of(ElementType.LONG, ElementType.LONG),
                List.of(TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.DEFAULT_SORTABLE),
                List.of(new TopNOperator.SortOrder(NumericTopNOperator.SORT_KEY_CHANNEL, asc(), false)),
                8 * 1024,
                Long.MAX_VALUE,
                TopNOperator.InputOrdering.NOT_SORTED,
                noThreshold
            );
        }
        if ("numericTernaryHeap".equals(impl)) {
            return new NumericTopNOperator.NumericTopNOperatorFactory(topCount, ElementType.LONG, asc(), false).get(
                new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, BLOCK_FACTORY, null)
            );
        }
        throw new IllegalArgumentException("unknown impl [" + impl + "]");
    }

    @Benchmark
    @OperationsPerInvocation(RUNS_PER_INVOCATION * BLOCK_LENGTH)
    public void run() {
        try (Operator op = operator()) {
            for (int i = 0; i < RUNS_PER_INVOCATION; i++) {
                op.addInput(page.shallowCopy());
            }
            op.finish();
            List<Page> out = new ArrayList<>();
            try {
                Page p;
                while ((p = op.getOutput()) != null) {
                    out.add(p);
                }
                long emitted = out.stream().mapToLong(Page::getPositionCount).sum();
                if (emitted != topCount) {
                    throw new AssertionError("expected [" + topCount + "] but got [" + emitted + "]");
                }
            } finally {
                Releasables.close(out);
            }
        }
    }
}
