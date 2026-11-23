/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.MetricFieldInfo;
import org.elasticsearch.compute.operator.MetricsInfoOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.TsInfoOperator;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Cost per input row of the {@code METRICS_INFO} and {@code TS_INFO} data-node operators, from the first page to the
 * released output. {@code TS_INFO} receives one row per time series of a shard, so its input grows with cardinality.
 * {@code METRICS_INFO} receives each distinct set of present fields once per shard, so its input grows with the
 * variety of field combinations instead.
 */
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class InfoOperatorsBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    private static final int ROWS = 100_000;
    private static final int ROWS_PER_PAGE = 1_000;

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(NoopCircuitBreaker.INSTANCE)
        .build();
    private static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    private static final MetricsInfoOperator.MetricFieldLookup LOOKUP = (indexName, fieldName) -> fieldName.startsWith("metric.")
        ? new MetricFieldInfo(fieldName, indexName, "double", "gauge", "unit-" + (fieldName.length() & 3))
        : null;

    /** The command whose data-node operator runs. */
    @Param({ "metrics_info", "ts_info" })
    public String command;

    /**
     * {@code integration}: 500 data streams with 20 dimensions per document, and a dimension that only the second backing
     * index has. Documents have 100 metrics for {@code METRICS_INFO} and 10 for {@code TS_INFO}, whose state grows with
     * metrics per series. {@code otel}: 20 data streams and 2,000 metric names, with 10 metrics and 8 of 60 attributes
     * per document.
     */
    @Param({ "integration", "otel" })
    public String shape;

    private final List<Block[]> pages = new ArrayList<>();
    private final Map<String, String> dataStreamsByIndex = new HashMap<>();

    @Setup
    public void setup() {
        for (int ds = 0; ds < 500; ds++) {
            dataStreamsByIndex.put(".ds-metrics-app" + ds + "-000001", "metrics-app" + ds);
            dataStreamsByIndex.put(".ds-metrics-app" + ds + "-000002", "metrics-app" + ds);
        }
        for (int start = 0; start < ROWS; start += ROWS_PER_PAGE) {
            try (
                BytesRefBlock.Builder first = blockFactory.newBytesRefBlockBuilder(ROWS_PER_PAGE);
                BytesRefBlock.Builder second = blockFactory.newBytesRefBlockBuilder(ROWS_PER_PAGE)
            ) {
                for (int row = start; row < start + ROWS_PER_PAGE; row++) {
                    String index = index(row);
                    List<String> metrics = metrics(row);
                    List<String> dimensions = dimensions(row);
                    if (command.equals("metrics_info")) {
                        first.appendBytesRef(new BytesRef(index));
                        second.beginPositionEntry();
                        for (String field : metrics) {
                            second.appendBytesRef(new BytesRef(field));
                        }
                        for (String field : dimensions) {
                            second.appendBytesRef(new BytesRef(field));
                        }
                        second.endPositionEntry();
                    } else {
                        first.appendBytesRef(new BytesRef(timeSeriesMetadata(row, metrics, dimensions)));
                        second.appendBytesRef(new BytesRef(index));
                    }
                }
                pages.add(new Block[] { first.build(), second.build() });
            }
        }
        if (run() == 0) {
            throw new IllegalStateException("expected output rows for [" + command + ", " + shape + "]");
        }
    }

    private String index(int row) {
        return shape.equals("integration")
            ? ".ds-metrics-app" + (row % 500) + "-00000" + (1 + (row / 500) % 2)
            : "metrics-otel" + (row % 20);
    }

    private List<String> metrics(int row) {
        List<String> metrics = new ArrayList<>();
        if (shape.equals("integration")) {
            int metricsPerDocument = command.equals("metrics_info") ? 100 : 10;
            for (int m = 0; m < metricsPerDocument; m++) {
                metrics.add("metric.app.field" + m);
            }
        } else {
            for (int m = 0; m < 10; m++) {
                metrics.add("metric.otel.m" + ((row * 7 + m * 131) % 2000));
            }
        }
        return metrics;
    }

    private List<String> dimensions(int row) {
        List<String> dimensions = new ArrayList<>();
        if (shape.equals("integration")) {
            for (int d = 0; d < 20; d++) {
                dimensions.add("resource.attributes.dim" + d);
            }
            if ((row / 500) % 2 == 1) {
                dimensions.add("resource.attributes.added");
            }
        } else {
            for (int a = 0; a < 8; a++) {
                dimensions.add("attributes.a" + ((row * 13 + a * 7) % 60));
            }
        }
        return dimensions;
    }

    /** One {@code _timeseries_metadata} source per row. The first dimension is unique, so every row is a new series. */
    private static String timeSeriesMetadata(int row, List<String> metrics, List<String> dimensions) {
        StringBuilder json = new StringBuilder("{");
        for (String metric : metrics) {
            json.append('"').append(metric).append("\":1.0,");
        }
        for (int d = 0; d < dimensions.size(); d++) {
            json.append('"')
                .append(dimensions.get(d))
                .append("\":\"")
                .append(d == 0 ? "series-" + row : "value-" + (row % 7))
                .append("\",");
        }
        json.setCharAt(json.length() - 1, '}');
        return json.toString();
    }

    @TearDown
    public void tearDown() {
        for (Block[] blocks : pages) {
            for (Block block : blocks) {
                block.close();
            }
        }
        pages.clear();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public int run() {
        try (Operator op = operator()) {
            int rows = 0;
            for (Block[] blocks : pages) {
                for (Block block : blocks) {
                    // The operator releases its input, and the pages are reused across invocations.
                    block.incRef();
                }
                op.addInput(new Page(ROWS_PER_PAGE, blocks));
                // TS_INFO stops taking input once it holds enough to emit early, so take its output as a driver would.
                rows += drainOutput(op);
            }
            op.finish();
            return rows + drainOutput(op);
        }
    }

    private static int drainOutput(Operator op) {
        int rows = 0;
        Page output;
        while ((output = op.getOutput()) != null) {
            rows += output.getPositionCount();
            output.releaseBlocks();
        }
        return rows;
    }

    private Operator operator() {
        return command.equals("metrics_info")
            ? new MetricsInfoOperator.Factory(LOOKUP, dataStreamsByIndex, 0, 1).get(driverContext)
            : new TsInfoOperator.Factory(
                LOOKUP,
                dataStreamsByIndex,
                0,
                1,
                TsInfoOperator.DEFAULT_PARTIAL_EMIT_THRESHOLD,
                ByteSizeValue.ofBytes(Operator.TARGET_PAGE_SIZE)
            ).get(driverContext);
    }
}
