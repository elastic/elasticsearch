/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.xpack.prometheus.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQueryResponseListener.QueryMode;
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

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

/**
 * Benchmarks the conversion of a collapsed PromQL ES|QL result into the Prometheus JSON response, see
 * {@link PrometheusQueryResponseListener#convertToPrometheusJson}. It lives in the listener's package because the listener is
 * package-private.
 *
 * <p>The cost of the conversion is split between the per-series label work and the per-sample value work, and which dominates depends
 * on the shape of the result, so the parameters cover both ends: many series with few steps is label-bound, few series with many steps
 * is sample-bound. {@code labelMode} switches between the single {@code _timeseries} JSON column and individual label columns.
 *
 * <p>Run with {@code -prof gc} to see allocation per operation, which matters as much as time here:
 * <pre>
 * ./gradlew -p benchmarks run --args 'PrometheusQueryResponseBenchmark -prof gc'
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class PrometheusQueryResponseBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    private static final String TIMESERIES_LABEL_MODE = "timeseries";
    private static final String DIMENSIONS_LABEL_MODE = "dimensions";
    private static final String DEFAULT_OUTPUT = "default";
    private static final String CHANNEL_OUTPUT = "channel";

    private static final long FIRST_STEP_MILLIS = 1_735_689_600_000L;
    private static final long STEP_INTERVAL_MILLIS = 60_000L;

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(NoopCircuitBreaker.INSTANCE)
        .build();

    @Param({ "1000", "100000" })
    int series;

    // Capped at 60 steps: the response grows with the number of samples, at about 27 bytes of JSON each, so 100k series x 360 steps
    // would be about 1 GB per operation, which makes every iteration slow and memory-hungry.
    @Param({ "1", "12", "60" })
    int steps;

    /** Either {@code timeseries} (one {@code _timeseries} JSON column) or {@code dimensions} (one keyword column per label). */
    @Param({ TIMESERIES_LABEL_MODE, DIMENSIONS_LABEL_MODE })
    String labelMode;

    @Param({ "RANGE", "INSTANT" })
    String queryMode;

    /**
     * What the response is written to: {@code default} is the builder's own buffer, a single array that doubles as it grows and is
     * copied once more into the response, and {@code channel} is what a REST channel provides, pages from the node's recycler.
     */
    @Param({ DEFAULT_OUTPUT, CHANNEL_OUTPUT })
    String outputKind;

    private Recycler<BytesRef> recycler;

    private List<Page> pages;
    private List<ColumnInfoImpl> columns;
    private QueryMode mode;

    @Setup(Level.Trial)
    public void setup() {
        mode = QueryMode.valueOf(queryMode);
        // the recycler a node's HTTP layer gets from its BigArrays
        recycler = new BigArrays(new PageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService(), "request").bytesRefRecycler();
        boolean useTimeseries = TIMESERIES_LABEL_MODE.equals(labelMode);
        columns = columns(useTimeseries);

        // One page, as a collapsed result: every position is one series, with its values and steps as multi-valued entries.
        List<Block> blocks = new ArrayList<>();
        blocks.add(valueBlock());
        if (useTimeseries) {
            blocks.add(timeseriesBlock());
        } else {
            blocks.add(keywordBlock(i -> "http_requests_total"));
            blocks.add(keywordBlock(i -> "host-" + i + ":9090"));
            blocks.add(keywordBlock(i -> "job-" + (i % 50)));
            blocks.add(keywordBlock(i -> "pod-" + i + "-7d9c5b6f4d"));
        }
        blocks.add(stepBlock());
        pages = List.of(new Page(blocks.toArray(Block[]::new)));
    }

    @Benchmark
    public void convert(Blackhole bh) throws IOException {
        switch (outputKind) {
            case DEFAULT_OUTPUT -> bh.consume(convertTo(XContentFactory.jsonBuilder()));
            case CHANNEL_OUTPUT -> {
                // released after use, as a channel's buffer is once the response has been sent
                try (RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler)) {
                    bh.consume(convertTo(XContentFactory.jsonBuilder(out)));
                }
            }
            default -> throw new IllegalStateException("unknown output kind [" + outputKind + "]");
        }
    }

    private int convertTo(XContentBuilder builder) throws IOException {
        PrometheusQueryResponseListener.convertToPrometheusJson(builder, pages, columns, ZoneOffset.UTC, "matrix", mode, Integer.MAX_VALUE);
        return BytesReference.bytes(builder).length();
    }

    private static List<ColumnInfoImpl> columns(boolean useTimeseries) {
        List<ColumnInfoImpl> columns = new ArrayList<>();
        columns.add(new ColumnInfoImpl("value", "double", null));
        if (useTimeseries) {
            columns.add(new ColumnInfoImpl("_timeseries", "keyword", null));
        } else {
            columns.add(new ColumnInfoImpl("__name__", "keyword", null));
            columns.add(new ColumnInfoImpl("instance", "keyword", null));
            columns.add(new ColumnInfoImpl("job", "keyword", null));
            columns.add(new ColumnInfoImpl("pod", "keyword", null));
        }
        columns.add(new ColumnInfoImpl("step", "long", null));
        return columns;
    }

    private DoubleBlock valueBlock() {
        try (DoubleBlock.Builder builder = BLOCK_FACTORY.newDoubleBlockBuilder(series * steps)) {
            for (int s = 0; s < series; s++) {
                appendDoubles(builder, s);
            }
            return builder.build();
        }
    }

    private void appendDoubles(DoubleBlock.Builder builder, int seriesIndex) {
        if (steps > 1) {
            builder.beginPositionEntry();
        }
        for (int i = 0; i < steps; i++) {
            builder.appendDouble(seriesIndex * 0.5 + i * 1.25);
        }
        if (steps > 1) {
            builder.endPositionEntry();
        }
    }

    private LongBlock stepBlock() {
        try (LongBlock.Builder builder = BLOCK_FACTORY.newLongBlockBuilder(series * steps)) {
            for (int s = 0; s < series; s++) {
                if (steps > 1) {
                    builder.beginPositionEntry();
                }
                for (int i = 0; i < steps; i++) {
                    builder.appendLong(FIRST_STEP_MILLIS + i * STEP_INTERVAL_MILLIS);
                }
                if (steps > 1) {
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    /** Labels as a {@code _timeseries} value: the {@code labels} namespace plus a nested resource-attribute namespace. */
    private BytesRefBlock timeseriesBlock() {
        return keywordBlock(
            i -> "{\"labels\":{\"__name__\":\"http_requests_total\",\"instance\":\"host-"
                + i
                + ":9090\",\"job\":\"job-"
                + (i % 50)
                + "\",\"pod\":\"pod-"
                + i
                + "-7d9c5b6f4d\"},\"attributes\":{\"resource\":{\"service.name\":\"svc-"
                + (i % 20)
                + "\"}}}"
        );
    }

    private BytesRefBlock keywordBlock(IntFunction<String> value) {
        try (BytesRefBlock.Builder builder = BLOCK_FACTORY.newBytesRefBlockBuilder(series)) {
            for (int s = 0; s < series; s++) {
                builder.appendBytesRef(new BytesRef(value.apply(s)));
            }
            return builder.build();
        }
    }
}
