/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.escf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.escf.EscfColumnBuilder;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentString;
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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Compares {@link EscfColumnBuilder#repeatLastRow} against writing the same value once per row, for a column
 * of {@code rows} identical values.
 *
 * <p>Both benchmarks build and finish a complete column, because the per-row path pays more than the payload
 * write: it also goes through the type dispatch in the positional surface and records an offset slot per row.
 * Finishing is included since it is identical work for both and leaving the builder unfinished would measure
 * {@code discard} instead of the serialization both callers really pay for.
 *
 * <p>Pages come from a real {@link PageCacheRecycler}, as in the batch indexing path, so that page allocation
 * does not dominate the small-{@code rows} parameters.
 *
 * <h2>Ready to run</h2>
 *
 * <pre>{@code
 * # Full matrix
 * ./gradlew :benchmarks:run --args="EscfColumnBuilderRepeatBenchmark"
 *
 * # One kind across all row counts
 * ./gradlew :benchmarks:run --args="EscfColumnBuilderRepeatBenchmark -p kind=string"
 *
 * # Allocation rate
 * ./gradlew :benchmarks:run --args="EscfColumnBuilderRepeatBenchmark -p kind=long -p rows=1000 -prof gc"
 *
 * # Quick smoke
 * ./gradlew :benchmarks:run --args="EscfColumnBuilderRepeatBenchmark -wi 1 -i 1 -f 1 -w 1 -r 1 -p kind=long -p rows=100"
 * }</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@Warmup(time = 1)
@Measurement(time = 1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(1)
public class EscfColumnBuilderRepeatBenchmark {

    @Param({ "long", "string", "longArray" })
    public String kind;

    @Param({ "2", "10", "100", "1000" })
    public int rows;

    private Recycler<BytesRef> recycler;
    private long longValue;
    private XContentString.UTF8Bytes stringValue;
    private long[] arrayValue;

    @Setup(Level.Trial)
    public void setUp() {
        recycler = new BytesRefRecycler(new PageCacheRecycler(Settings.EMPTY));
        longValue = 0x0123_4567_89ab_cdefL;
        byte[] utf8 = "a-moderately-sized-keyword".getBytes(StandardCharsets.UTF_8);
        stringValue = new XContentString.UTF8Bytes(utf8, 0, utf8.length);
        arrayValue = new long[] { 1, 2, 3, 4 };
    }

    @Benchmark
    public void repeatLastRow(Blackhole bh) {
        try (EscfColumnBuilder builder = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT, recycler)) {
            writeRow(builder);
            builder.repeatLastRow(rows - 1);
            finish(builder, bh);
        }
    }

    @Benchmark
    public void writeOncePerRow(Blackhole bh) {
        try (EscfColumnBuilder builder = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT, recycler)) {
            // The switch picks a loop rather than sitting inside one: a shared per-row writeRow would charge this
            // benchmark a string switch per row, which is not free next to a long append and would show up as a
            // speedup that belongs to the harness rather than to the builder.
            switch (kind) {
                case "long" -> {
                    for (int row = 0; row < rows; row++) {
                        builder.addLong(longValue);
                    }
                }
                case "string" -> {
                    for (int row = 0; row < rows; row++) {
                        builder.addString(stringValue);
                    }
                }
                case "longArray" -> {
                    for (int row = 0; row < rows; row++) {
                        builder.addLongArray(arrayValue, arrayValue.length);
                    }
                }
                default -> throw new IllegalArgumentException("unknown column kind [" + kind + "]");
            }
            finish(builder, bh);
        }
    }

    private void writeRow(EscfColumnBuilder builder) {
        switch (kind) {
            case "long" -> builder.addLong(longValue);
            case "string" -> builder.addString(stringValue);
            case "longArray" -> builder.addLongArray(arrayValue, arrayValue.length);
            default -> throw new IllegalArgumentException("unknown column kind [" + kind + "]");
        }
    }

    private void finish(EscfColumnBuilder builder, Blackhole bh) {
        EscfColumnData data = builder.finish(rows);
        bh.consume(data);
        data.close();
    }
}
