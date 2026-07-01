/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark._nightly.esql;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatOptions;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
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
import java.util.concurrent.TimeUnit;

/**
 * Measures end-to-end {@link CsvFormatReader#read} throughput over an in-memory CSV
 * or TSV fixture. Targets the line scanner + field tokenizer + block builder path.
 * Variants cover the two text-delimited formats ({@code ','} vs {@code '\t'}) and
 * column projection (all four columns vs a two-column subset).
 * <p>
 * Distinct from {@code CsvErrorPolicyBenchmark} (which measures error-policy overhead
 * on a fixed read) and {@code CsvBoundaryScanBenchmark} (which measures a single
 * boundary-finding routine in isolation) — this is the end-to-end read counterpart
 * for the CSV/TSV formats, matching the shape of {@link ParquetReadBenchmark} /
 * {@link OrcReadBenchmark} / {@link NdJsonReadBenchmark}.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class CsvReadBenchmark {

    static {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    @Param({ "10000", "100000" })
    public int rowCount;

    @Param({ "csv", "tsv" })
    public String delimiter;

    @Param({ "all", "projectedSubset" })
    public String projection;

    private BlockFactory blockFactory;
    private StorageObject storageObject;
    private long fixtureBytes;
    private List<String> projectedColumns;
    private CsvFormatOptions options;
    private String formatName;
    private List<String> extensions;

    @Setup(Level.Trial)
    public void setup() {
        Utils.configureBenchmarkLogging();
        blockFactory = DatasourceBenchmarks.newBlockFactory();
        char separator = switch (delimiter) {
            case "csv" -> ',';
            case "tsv" -> '\t';
            default -> throw new IllegalArgumentException("unknown delimiter: " + delimiter);
        };
        options = switch (delimiter) {
            case "csv" -> CsvFormatOptions.DEFAULT;
            case "tsv" -> CsvFormatOptions.TSV;
            default -> throw new IllegalArgumentException("unknown delimiter: " + delimiter);
        };
        formatName = delimiter;
        extensions = switch (delimiter) {
            case "csv" -> List.of(".csv");
            case "tsv" -> List.of(".tsv");
            default -> throw new IllegalArgumentException("unknown delimiter: " + delimiter);
        };
        byte[] textBytes = generateFixture(rowCount, separator);
        fixtureBytes = textBytes.length;
        storageObject = DatasourceBenchmarks.inMemoryStorageObject(textBytes, "memory://bench." + delimiter);
        projectedColumns = switch (projection) {
            case "all" -> List.of("id", "value", "name", "score");
            case "projectedSubset" -> List.of("id", "name");
            default -> throw new IllegalArgumentException("unknown projection: " + projection);
        };
    }

    @Benchmark
    public int readAll(ReadMetrics metrics) throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory, options, formatName, extensions);
        FormatReadContext ctx = FormatReadContext.builder().projectedColumns(projectedColumns).batchSize(1000).build();
        int totalRows = 0;
        try (CloseableIterator<Page> iter = reader.read(storageObject, ctx)) {
            while (iter.hasNext()) {
                Page page = iter.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        metrics.record(totalRows, fixtureBytes);
        return totalRows;
    }

    static void selfTest() {
        for (String delimiter : Utils.possibleValues(CsvReadBenchmark.class, "delimiter")) {
            for (String projection : Utils.possibleValues(CsvReadBenchmark.class, "projection")) {
                CsvReadBenchmark bench = new CsvReadBenchmark();
                bench.rowCount = DatasourceBenchmarks.SELF_TEST_ROW_COUNT;
                bench.delimiter = delimiter;
                bench.projection = projection;
                try {
                    bench.setup();
                    int actual = bench.readAll(new ReadMetrics());
                    if (actual != bench.rowCount) {
                        throw new AssertionError(
                            "CsvReadBenchmark[" + delimiter + "/" + projection + "] read " + actual + " rows, expected " + bench.rowCount
                        );
                    }
                } catch (IOException e) {
                    throw new AssertionError("CsvReadBenchmark[" + delimiter + "/" + projection + "] failed", e);
                }
            }
        }
    }

    /**
     * Produces a typed-header text fixture matching the four-column schema used by
     * the sibling format benches. The header line carries ESQL type annotations
     * ({@code id:long}, etc.) so the reader doesn't have to guess types.
     */
    static byte[] generateFixture(int rowCount, char separator) {
        StringBuilder sb = new StringBuilder(rowCount * 40);
        sb.append("id:long").append(separator).append("value:integer").append(separator);
        sb.append("name:keyword").append(separator).append("score:double\n");
        for (int i = 0; i < rowCount; i++) {
            sb.append(i).append(separator).append(i).append(separator);
            sb.append("row-").append(i).append(separator).append(i * 1.5).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
