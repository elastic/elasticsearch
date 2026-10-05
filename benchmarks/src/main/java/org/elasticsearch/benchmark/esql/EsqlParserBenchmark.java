/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.esql;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.EsqlBaseLexer;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
 * Measure the ESQL query parser and lexer
 */
@Fork(1)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class EsqlParserBenchmark {
    static {
        BenchmarkLogging.configure();
    }

    @Param({ "wiki", "clickbench", "many_commands", "many_expressions" })
    public String queryType;

    @Param({ "true", "false" })
    public boolean dev;

    private String query;

    private EsqlParser parser;
    private InferenceSettings inferenceSettings;

    @Setup
    public void setup() {
        query = switch (queryType) {
            case "wiki" ->
                """
                    FROM wikipedia METADATA _id, _score, _source| WHERE KQL("montserrat")| KEEP _id, _score, _source| SORT _score DESC| LIMIT 20""";
            case "clickbench" -> """
                FROM hits | WHERE URL LIKE "*google*" | STATS count = COUNT(*)""";
            case "many_commands" -> "FROM logs-* | WHERE host.name == \"a\" | EVAL b = bytes * 2 | WHERE b > 10 | EVAL c = b + 1 "
                + "| WHERE c > 20 | EVAL d = c - 1 | WHERE d > 30 | EVAL e = d * 3 | WHERE e > 40 | EVAL f = e / 2 | WHERE f > 50 "
                + "| EVAL g = f + 7 | KEEP host.name, g | STATS m = MAX(g) BY host.name | SORT m DESC | LIMIT 10";
            case "many_expressions" -> "FROM logs-* | WHERE (a > 1 AND b < 2) OR (c == 3 AND d != 4) OR (e >= 5 AND f <= 6) "
                + "OR (g IN (1, 2, 3, 4, 5) AND h LIKE \"x*\") OR (i IS NULL AND j IS NOT NULL) OR (k + l * m - n / o > 7) "
                + "| EVAL p = CONCAT(TO_STRING(a), \"-\", TO_STRING(b), \"-\", TO_STRING(c)), q = DATE_TRUNC(1 hour, @timestamp) "
                + "| STATS COUNT(*), SUM(k), AVG(l), MIN(m), MAX(n), MEDIAN(o) BY q, p | LIMIT 100";
            default -> throw new IllegalArgumentException("unknown query type [" + queryType + "]");
        };

        parser = new EsqlParser(new EsqlConfig(dev, new EsqlFunctionRegistry()));
        inferenceSettings = new InferenceSettings(Settings.EMPTY);
    }

    @Benchmark
    public void lex(Blackhole bh) {
        EsqlBaseLexer lexer = new EsqlBaseLexer(CharStreams.fromString(query));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        bh.consume(tokens.getTokens());
    }

    @Benchmark
    public void parse(Blackhole bh) {
        bh.consume(parser.parse(query, new QueryParams(), inferenceSettings));
    }
}
