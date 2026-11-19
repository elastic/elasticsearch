/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.esql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.planner.Layout;
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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for MATCH at runtime.
 * Uses Pages and evaluators to match production execution paths.
 * Suitable for before/after comparison of runtime lexical search.
 *
 * <p><b>The score is MiB of text a second, per thread</b>: one invocation evaluates exactly
 * {@value #TEXT_PER_INVOCATION} bytes of values, so the throughput JMH reports reads directly as MiB/s. Pass
 * {@code -t N} to measure several threads, where the score becomes their total.
 *
 * <p>It measures the row-by-row evaluation of values <b>already in memory</b>: nothing here decompresses a column
 * or reads a disk, which are their own throughputs.
 *
 * <p>{@code holding} is the selectivity: how many of the values spell the term the query asks for.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class MatchAtRuntimeBenchmark {
    static {
        BenchmarkLogging.configure();
    }

    /** Bytes of text one invocation evaluates, which makes the reported throughput MiB/s. */
    static final int TEXT_PER_INVOCATION = 1 << 20;
    /** A term the generated values never spell, so a value holds it only where it is put there on purpose. */
    private static final String ABSENT = "qqzzx";

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(NoopCircuitBreaker.INSTANCE)
        .build();

    private static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    private static final FoldContext FOLD_CONTEXT = FoldContext.small();

    @Param({ "NONE", "FEW", "ALL" })
    public String holding;

    @Param({ "512", "4096" })
    public int valueLength;

    private ExpressionEvaluator evaluator;
    private Page page;

    @Setup(Level.Trial)
    public void setup() {
        final int rows = TEXT_PER_INVOCATION / valueLength;
        final int holders = switch (holding) {
            case "NONE" -> 0;
            case "FEW" -> Math.max(1, rows / 100);
            default -> rows;
        };

        Attribute field = new ReferenceAttribute(Source.EMPTY, "field", DataType.TEXT);

        Expression expr = new Match(Source.EMPTY, field, Literal.text(Source.EMPTY, ABSENT), null);

        // Build evaluator through the standard eval pipeline
        Layout.Builder layoutBuilder = new Layout.Builder();
        layoutBuilder.append(List.of(field));
        Layout layout = layoutBuilder.build();
        evaluator = EvalMapper.toEvaluator(FOLD_CONTEXT, expr, layout).get(driverContext);

        var builder = blockFactory.newBytesRefVectorBuilder(rows);
        for (int row = 0; row < rows; row++) {
            builder.appendBytesRef(new BytesRef(generateMultiTermAsciiString(valueLength, row < holders)));
        }
        page = new Page(builder.build().asBlock());
    }

    @Benchmark
    public Block run() {
        return evaluator.eval(page);
    }

    /** A value of about {@code length} bytes, spelling the term the query asks for only where told to. */
    public static String generateMultiTermAsciiString(int length, boolean spellsTheTerm) {
        StringBuilder sb = new StringBuilder(length + 8);
        ThreadLocalRandom random = ThreadLocalRandom.current();

        while (sb.length() < length) {
            for (int j = 0; j < random.nextInt(4, 6); j++) {
                int ascii = random.nextInt((int) 'a', (int) 'z' + 1);
                sb.append((char) ascii);
            }
            sb.append(' ');
        }
        if (spellsTheTerm) {
            sb.append(ABSENT);
        }

        return sb.toString();
    }
}
