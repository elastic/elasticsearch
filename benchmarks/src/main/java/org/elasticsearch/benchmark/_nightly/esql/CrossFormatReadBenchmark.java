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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatOptions;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonFormatReader;
import org.elasticsearch.xpack.esql.datasource.orc.OrcFormatReader;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Format-only comparison: drives the same row-equivalent dataset
 * (id LONG, value INT, name STRING, score DOUBLE) through all five readers
 * — CSV, TSV, Parquet, ORC, NDJSON — under identical {@link FormatReadContext}.
 * Each fixture is built once in {@link #setup()} so the per-{@code @Benchmark}
 * cost is dominated by the format decode path.
 * <p>
 * The five fixtures have different on-disk sizes (Parquet/ORC are columnar +
 * compressed dictionaries even at UNCOMPRESSED; CSV/TSV/NDJSON are textual). Each
 * {@code @Benchmark} method passes its fixture's byte length to {@link ReadMetrics}
 * so JMH reports per-format {@code rowsRead} and {@code bytesRead} rates alongside
 * the primary {@code ops/s} score — comparing the {@code bytesRead} column across
 * formats is the format-only delta on identical row data.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class CrossFormatReadBenchmark {

    private static final List<String> ALL_COLUMNS = List.of("id", "value", "name", "score");

    static {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    @Param({ "10000", "100000" })
    public int rowCount;

    private BlockFactory blockFactory;
    private StorageObject csvObject;
    private StorageObject tsvObject;
    private StorageObject parquetObject;
    private StorageObject orcObject;
    private StorageObject ndjsonObject;
    private long csvBytes;
    private long tsvBytes;
    private long parquetBytes;
    private long orcBytes;
    private long ndjsonBytes;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Utils.configureBenchmarkLogging();
        blockFactory = DatasourceBenchmarks.newBlockFactory();
        byte[] csv = CsvReadBenchmark.generateFixture(rowCount, ',');
        byte[] tsv = CsvReadBenchmark.generateFixture(rowCount, '\t');
        byte[] parquet = ParquetReadBenchmark.generateParquetFixture(rowCount);
        byte[] orc = OrcReadBenchmark.generateOrcFixture(rowCount);
        byte[] ndjson = NdJsonReadBenchmark.generateNdJsonFixture(rowCount);
        csvBytes = csv.length;
        tsvBytes = tsv.length;
        parquetBytes = parquet.length;
        orcBytes = orc.length;
        ndjsonBytes = ndjson.length;
        csvObject = DatasourceBenchmarks.inMemoryStorageObject(csv, "memory://bench.csv");
        tsvObject = DatasourceBenchmarks.inMemoryStorageObject(tsv, "memory://bench.tsv");
        parquetObject = DatasourceBenchmarks.inMemoryStorageObject(parquet, "memory://bench.parquet");
        orcObject = DatasourceBenchmarks.inMemoryStorageObject(orc, "memory://bench.orc");
        ndjsonObject = DatasourceBenchmarks.inMemoryStorageObject(ndjson, "memory://bench.ndjson");
    }

    @Benchmark
    public int csv(ReadMetrics metrics) throws IOException {
        return drain(new CsvFormatReader(blockFactory), csvObject, csvBytes, metrics);
    }

    @Benchmark
    public int tsv(ReadMetrics metrics) throws IOException {
        return drain(new CsvFormatReader(blockFactory, CsvFormatOptions.TSV, "tsv", List.of(".tsv")), tsvObject, tsvBytes, metrics);
    }

    @Benchmark
    public int parquet(ReadMetrics metrics) throws IOException {
        return drain(new ParquetFormatReader(blockFactory), parquetObject, parquetBytes, metrics);
    }

    @Benchmark
    public int orc(ReadMetrics metrics) throws IOException {
        return drain(new OrcFormatReader(blockFactory), orcObject, orcBytes, metrics);
    }

    @Benchmark
    public int ndjson(ReadMetrics metrics) throws IOException {
        return drain(new NdJsonFormatReader(Settings.EMPTY, blockFactory, null), ndjsonObject, ndjsonBytes, metrics);
    }

    private static int drain(FormatReader reader, StorageObject object, long fixtureBytes, ReadMetrics metrics) throws IOException {
        FormatReadContext ctx = FormatReadContext.builder().projectedColumns(ALL_COLUMNS).batchSize(1000).build();
        int totalRows = 0;
        try (CloseableIterator<Page> iter = reader.read(object, ctx)) {
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
        CrossFormatReadBenchmark bench = new CrossFormatReadBenchmark();
        bench.rowCount = DatasourceBenchmarks.SELF_TEST_ROW_COUNT;
        try {
            bench.setup();
        } catch (IOException e) {
            throw new AssertionError("CrossFormatReadBenchmark setup failed", e);
        }
        // Exercise every format independently so a regression in one is not hidden by an earlier failure.
        List<String> failures = new ArrayList<>();
        runFormat(failures, "csv", bench.rowCount, () -> bench.csv(new ReadMetrics()));
        runFormat(failures, "tsv", bench.rowCount, () -> bench.tsv(new ReadMetrics()));
        runFormat(failures, "parquet", bench.rowCount, () -> bench.parquet(new ReadMetrics()));
        runFormat(failures, "orc", bench.rowCount, () -> bench.orc(new ReadMetrics()));
        runFormat(failures, "ndjson", bench.rowCount, () -> bench.ndjson(new ReadMetrics()));
        if (failures.isEmpty() == false) {
            throw new AssertionError("CrossFormatReadBenchmark failures: " + String.join("; ", failures));
        }
    }

    @FunctionalInterface
    private interface FormatRun {
        int run() throws IOException;
    }

    private static void runFormat(List<String> failures, String label, int expected, FormatRun run) {
        try {
            int actual = run.run();
            if (actual != expected) {
                failures.add(label + " read " + actual + " rows, expected " + expected);
            }
        } catch (IOException e) {
            failures.add(label + " threw " + e);
        }
    }
}
