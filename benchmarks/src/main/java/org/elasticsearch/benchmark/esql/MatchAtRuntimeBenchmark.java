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
import org.elasticsearch.benchmark.Utils;
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
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
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
 */
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class MatchAtRuntimeBenchmark {
    static {
        Utils.configureBenchmarkLogging();
    }

    private static final int BLOCK_LENGTH = 1024;
    private static final int NUMBER_OF_TERMS = 1024;

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    private static final FoldContext FOLD_CONTEXT = FoldContext.small();

    private ExpressionEvaluator evaluator;
    private Page page;

    @Setup(Level.Trial)
    public void setup() {
        BytesRef value = new BytesRef(generateMultiTermAsciiString());

        Attribute field = new ReferenceAttribute(Source.EMPTY, "field", DataType.TEXT);

        Expression expr = new Match(Source.EMPTY, field, Literal.text(Source.EMPTY, "abc"), null);

        // Build evaluator through the standard eval pipeline
        Layout.Builder layoutBuilder = new Layout.Builder();
        layoutBuilder.append(List.of(field));
        Layout layout = layoutBuilder.build();
        evaluator = EvalMapper.toEvaluator(FOLD_CONTEXT, expr, layout).get(driverContext);

        // Build page with BLOCK_LENGTH identical rows
        var builder = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            builder.appendBytesRef(value);
        }
        page = new Page(builder.build().asBlock());
    }

    @Benchmark
    @OperationsPerInvocation(BLOCK_LENGTH)
    public Block run() {
        return evaluator.eval(page);
    }

    public static String generateMultiTermAsciiString() {
        StringBuilder sb = new StringBuilder();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        for (int i = 0; i < NUMBER_OF_TERMS; i++) {
            for (int j = 0; j < random.nextInt(4, 6); j++) {
                int ascii = random.nextInt((int) 'a', (int) 'z' + 1);
                sb.append((char) ascii);
            }
            sb.append(' ');
        }

        return sb.toString();
    }
}
