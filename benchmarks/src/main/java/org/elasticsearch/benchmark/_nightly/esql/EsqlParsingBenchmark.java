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
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * Measures ES|QL query parsing performance as a function of the number of arithmetic
 * EVAL expressions and the operator used.
 *
 * <p>This benchmark was introduced to track the regression reported in
 * https://github.com/elastic/elasticsearch/pull/153694, where adding the {@code ->}
 * (ARROW) lexer token with a semantic predicate ({@code isDevVersion()}) caused a
 * 10–15x slowdown in parsing queries that contain {@code -} characters. The predicate
 * prevents ANTLR from caching the DFA state for {@code -}, forcing ATN simulation on
 * every subtraction operator in the input.
 *
 * <p>Use the {@code operator} parameter to compare {@code minus} (subtraction,
 * affected by the regression) against {@code plus} (addition, unaffected).
 */
@Fork(1)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class EsqlParsingBenchmark {
    static {
        Utils.configureBenchmarkLogging();
    }

    /**
     * Operator used in each EVAL expression. {@code minus} triggered the regression
     * because every {@code -} character forced ANTLR's predicated DFA path.
     * {@code plus} serves as the unaffected baseline.
     */
    @Param({ "minus", "plus" })
    public String operator;

    /**
     * Number of arithmetic EVAL expressions in the query. Higher counts amplify
     * the per-operator cost difference.
     */
    @Param({ "10", "50" })
    public int expressionCount;

    private EsqlParser parser;
    private String queryText;

    @Setup(Level.Trial)
    public void setup() {
        EsqlFunctionRegistry functionRegistry = new EsqlFunctionRegistry();
        parser = new EsqlParser(new EsqlConfig(false, functionRegistry));
        queryText = buildQuery(operator, expressionCount);
    }

    @Benchmark
    public void parseQuery(Blackhole bh) {
        bh.consume(parser.parseQuery(queryText));
    }

    static String buildQuery(String operator, int expressionCount) {
        String op = operator.equals("minus") ? "-" : "+";
        StringBuilder sb = new StringBuilder("FROM index | EVAL ");
        for (int i = 0; i < expressionCount; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("e").append(i).append(" = f").append(i).append(op).append("g").append(i);
        }
        sb.append(" | LIMIT 1000");
        return sb.toString();
    }
}
