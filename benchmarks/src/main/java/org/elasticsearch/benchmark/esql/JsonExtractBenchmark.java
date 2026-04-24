/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark.esql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonExtract;
import org.elasticsearch.xpack.esql.planner.Layout;
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
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for JSON_EXTRACT through the full eval pipeline.
 * Uses Pages and evaluators to match production execution paths.
 * Suitable for before/after comparison of byte-slicing optimization.
 */
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class JsonExtractBenchmark {

    private static final int BLOCK_LENGTH = 1024;

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    private static final FoldContext FOLD_CONTEXT = FoldContext.small();

    @Param(
        {
            "small_object",
            "medium_object",
            "large_object",
            "large_nested_extract",
            "array_of_objects",
            "nested_scalar",
            "deep_nesting",
            "number",
            "boolean",
            "string" }
    )
    public String scenario;

    private ExpressionEvaluator evaluator;
    private Page page;

    @Setup(Level.Trial)
    public void setup() {
        BytesRef json;
        String path;

        switch (scenario) {
            case "small_object" -> {
                json = new BytesRef("{\"user\":{\"name\":\"John\",\"age\":30},\"active\":true}");
                path = "user";
            }
            case "medium_object" -> {
                StringBuilder sb = new StringBuilder();
                sb.append("{\"header\":\"v1\",\"payload\":{");
                for (int i = 0; i < 20; i++) {
                    if (i > 0) sb.append(",");
                    sb.append("\"f").append(i).append("\":\"value_").append(i).append("\"");
                }
                sb.append("},\"footer\":\"end\"}");
                json = new BytesRef(sb.toString());
                path = "payload";
            }
            case "large_object" -> {
                StringBuilder sb = new StringBuilder();
                sb.append("{\"data\":{");
                for (int i = 0; i < 100; i++) {
                    if (i > 0) sb.append(",");
                    sb.append("\"field_").append(i).append("\":");
                    sb.append("{\"value\":").append(i);
                    sb.append(",\"label\":\"item_").append(i).append("\"");
                    sb.append(",\"tags\":[\"a\",\"b\",\"c\"]");
                    sb.append("}");
                }
                sb.append("},\"meta\":\"info\"}");
                json = new BytesRef(sb.toString());
                path = "data";
            }
            case "large_nested_extract" -> {
                StringBuilder sb = new StringBuilder();
                sb.append("{\"level1\":{\"level2\":{\"target\":{");
                for (int i = 0; i < 200; i++) {
                    if (i > 0) sb.append(",");
                    sb.append("\"k").append(i).append("\":\"").append("x".repeat(20)).append("\"");
                }
                sb.append("},\"other\":\"skip\"},\"more\":");
                sb.append("{");
                for (int i = 0; i < 100; i++) {
                    if (i > 0) sb.append(",");
                    sb.append("\"pad").append(i).append("\":").append(i);
                }
                sb.append("}}}");
                json = new BytesRef(sb.toString());
                path = "level1.level2.target";
            }
            case "array_of_objects" -> {
                StringBuilder sb = new StringBuilder();
                sb.append("{\"items\":[");
                for (int i = 0; i < 50; i++) {
                    if (i > 0) sb.append(",");
                    sb.append("{\"id\":").append(i);
                    sb.append(",\"name\":\"item_").append(i).append("\"");
                    sb.append(",\"details\":{\"weight\":").append(i * 1.5);
                    sb.append(",\"tags\":[\"tag_a\",\"tag_b\"]}}");
                }
                sb.append("]}");
                json = new BytesRef(sb.toString());
                path = "items[25]";
            }
            case "nested_scalar" -> {
                json = new BytesRef("{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":\"deeply_nested_value\"}}}}}");
                path = "a.b.c.d.e";
            }
            case "deep_nesting" -> {
                StringBuilder sb = new StringBuilder();
                String[] keys = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j" };
                for (String key : keys) {
                    sb.append("{\"").append(key).append("\":");
                }
                sb.append("{\"result\":42,\"items\":[1,2,3]}");
                for (int i = 0; i < keys.length; i++) {
                    sb.append("}");
                }
                json = new BytesRef(sb.toString());
                path = "a.b.c.d.e.f.g.h.i.j";
            }
            case "number" -> {
                json = new BytesRef("{\"metrics\":{\"cpu\":98.6,\"memory\":1073741824,\"disk\":42},\"host\":\"server1\"}");
                path = "metrics.memory";
            }
            case "boolean" -> {
                json = new BytesRef("{\"config\":{\"enabled\":true,\"debug\":false},\"version\":2}");
                path = "config.enabled";
            }
            case "string" -> {
                json = new BytesRef("{\"user\":{\"name\":\"John Doe\",\"email\":\"john@example.com\"},\"role\":\"admin\"}");
                path = "user.name";
            }
            default -> throw new UnsupportedOperationException("unknown scenario: " + scenario);
        }

        // Build expression: JSON_EXTRACT(field, "path")
        FieldAttribute field = new FieldAttribute(
            Source.EMPTY,
            "json",
            new EsField("json", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Expression expr = new JsonExtract(Source.EMPTY, field, new Literal(Source.EMPTY, new BytesRef(path), DataType.KEYWORD));

        // Build evaluator through the standard eval pipeline
        Layout.Builder layoutBuilder = new Layout.Builder();
        layoutBuilder.append(List.of(field));
        Layout layout = layoutBuilder.build();
        evaluator = EvalMapper.toEvaluator(FOLD_CONTEXT, expr, layout).get(driverContext);

        // Build page with BLOCK_LENGTH identical JSON rows
        var builder = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            builder.appendBytesRef(json);
        }
        page = new Page(builder.build().asBlock());
    }

    @Benchmark
    @OperationsPerInvocation(BLOCK_LENGTH)
    public Block extract() {
        return evaluator.eval(page);
    }
}
