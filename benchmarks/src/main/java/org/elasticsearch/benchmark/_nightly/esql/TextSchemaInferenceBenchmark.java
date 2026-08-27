/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark._nightly.esql;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatOptions;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Measures per-file schema inference on the text rails, isolated from decode.
 * <p>
 * Inference runs once per file at planning time, over a sample of up to 20,000 rows, and a glob
 * dataset pays it once per file — so a regression here is a planning-latency regression across a
 * whole dataset, not a per-row cost. This benchmark exists because teaching the inferrers to produce
 * {@code date_nanos} (elastic/esql-planning#1798) touched the value-classification step that every
 * sampled cell of every column goes through.
 * <p>
 * The {@code column} parameter names the shape of the sampled timestamp column, which is what decides
 * how much of the classification path runs:
 * <ul>
 *   <li>{@code keyword} — the control that matters most. A string column must cost one failed date
 *       parse for its first value and nothing thereafter; if the short-circuit that guarantees that
 *       is ever lost, this cell is where it shows up.</li>
 *   <li>{@code millis} — timestamps that stay {@code datetime}. The common temporal case, and the one
 *       that must not pay for the new capability.</li>
 *   <li>{@code nanos} — timestamps that become {@code date_nanos}: the new behavior's own cost.</li>
 *   <li>{@code mixed} — alternating precisions, which walks the rung transition repeatedly on the CSV
 *       ladder rather than settling after the first row.</li>
 * </ul>
 * The sibling {@code NdJsonReadBenchmark} and {@code CsvReadBenchmark} remain the end-to-end controls:
 * the former also runs inference (non-temporal fixture), the latter reads a typed header and so
 * measures decode alone.
 */
@Fork(1)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class TextSchemaInferenceBenchmark {

    static {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    /** The inferrers' default sample size, so a full sample is what gets measured. */
    @Param({ "20000" })
    public int rowCount;

    @Param({ "ndjson", "csv" })
    public String format;

    @Param({ "keyword", "millis", "nanos", "mixed" })
    public String column;

    private BlockFactory blockFactory;
    private StorageObject storageObject;

    @Setup(Level.Trial)
    public void setup() {
        Utils.configureBenchmarkLogging();
        blockFactory = DatasourceBenchmarks.newBlockFactory();
        byte[] bytes = generateFixture(format, column, rowCount);
        storageObject = DatasourceBenchmarks.inMemoryStorageObject(bytes, "memory://bench-inference." + format);
    }

    @Benchmark
    public int inferSchema() throws IOException {
        return inferredSchema().size();
    }

    private List<Attribute> inferredSchema() throws IOException {
        return switch (format) {
            case "ndjson" -> new NdJsonFormatReader(Settings.EMPTY, blockFactory, null).metadata(storageObject).schema();
            case "csv" -> new CsvFormatReader(blockFactory, CsvFormatOptions.DEFAULT, "csv", List.of(".csv")).metadata(storageObject)
                .schema();
            default -> throw new IllegalArgumentException("unknown format: " + format);
        };
    }

    /**
     * Asserts the inferred type as well as the column count, so the benchmark cannot quietly start
     * measuring a different code path than the one it is named for — a {@code nanos} cell that
     * inferred {@code datetime} would still produce plausible numbers.
     */
    static void selfTest() {
        for (String format : Utils.possibleValues(TextSchemaInferenceBenchmark.class, "format")) {
            for (String column : Utils.possibleValues(TextSchemaInferenceBenchmark.class, "column")) {
                TextSchemaInferenceBenchmark bench = new TextSchemaInferenceBenchmark();
                bench.rowCount = DatasourceBenchmarks.SELF_TEST_ROW_COUNT;
                bench.format = format;
                bench.column = column;
                String cell = format + "/" + column;
                try {
                    bench.setup();
                    List<Attribute> schema = bench.inferredSchema();
                    if (schema.size() != 2) {
                        throw new AssertionError(cell + " inferred " + schema.size() + " columns, expected 2");
                    }
                    DataType expected = switch (column) {
                        case "keyword" -> DataType.KEYWORD;
                        case "millis" -> DataType.DATETIME;
                        case "nanos", "mixed" -> DataType.DATE_NANOS;
                        default -> throw new IllegalArgumentException("unknown column: " + column);
                    };
                    DataType actual = schema.get(1).dataType();
                    if (actual != expected) {
                        throw new AssertionError(cell + " inferred " + actual + " for column ts, expected " + expected);
                    }
                } catch (IOException e) {
                    throw new AssertionError(cell + " failed", e);
                }
            }
        }
    }

    static byte[] generateFixture(String format, String column, int rowCount) {
        StringBuilder sb = new StringBuilder(rowCount * 50);
        if ("csv".equals(format)) {
            sb.append("id,ts\n");
        }
        for (int i = 0; i < rowCount; i++) {
            String ts = cellValue(column, i);
            if ("ndjson".equals(format)) {
                sb.append("{\"id\":").append(i).append(",\"ts\":\"").append(ts).append("\"}\n");
            } else {
                sb.append(i).append(',').append(ts).append('\n');
            }
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String cellValue(String column, int row) {
        // Vary the second so values are not all identical, which would let a parser cache flatter the
        // measurement in a way no real file would.
        String secondOfMinute = String.format(Locale.ROOT, "%02d", row % 60);
        return switch (column) {
            case "keyword" -> "row-" + row;
            case "millis" -> "2023-10-23T12:15:" + secondOfMinute + ".360Z";
            case "nanos" -> "2023-10-23T12:15:" + secondOfMinute + ".360103847Z";
            case "mixed" -> row % 2 == 0
                ? "2023-10-23T12:15:" + secondOfMinute + ".360Z"
                : "2023-10-23T12:15:" + secondOfMinute + ".360103847Z";
            default -> throw new IllegalArgumentException("unknown column: " + column);
        };
    }
}
