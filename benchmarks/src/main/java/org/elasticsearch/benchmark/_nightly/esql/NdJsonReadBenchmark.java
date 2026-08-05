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
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonFormatReader;
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
 * Measures end-to-end {@link NdJsonFormatReader#read} throughput over an in-memory
 * NDJSON fixture. Targets the Jackson decode + block builder path. Variants cover
 * column projection (all four columns vs a two-column subset). {@code resolvedSchema}
 * is left {@code null} so the reader runs per-file inference — the production path
 * for ad-hoc reads, and the pattern the existing CSV benchmarks also follow.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class NdJsonReadBenchmark {

    static {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    @Param({ "10000", "100000" })
    public int rowCount;

    @Param({ "all", "projectedSubset" })
    public String projection;

    private BlockFactory blockFactory;
    private StorageObject storageObject;
    private long fixtureBytes;
    private List<String> projectedColumns;

    @Setup(Level.Trial)
    public void setup() {
        Utils.configureBenchmarkLogging();
        blockFactory = DatasourceBenchmarks.newBlockFactory();
        byte[] ndjsonBytes = generateNdJsonFixture(rowCount);
        fixtureBytes = ndjsonBytes.length;
        storageObject = DatasourceBenchmarks.inMemoryStorageObject(ndjsonBytes, "memory://bench.ndjson");
        projectedColumns = switch (projection) {
            case "all" -> List.of("id", "value", "name", "score");
            case "projectedSubset" -> List.of("id", "name");
            default -> throw new IllegalArgumentException("unknown projection: " + projection);
        };
    }

    @Benchmark
    public int readAll(ReadMetrics metrics) throws IOException {
        NdJsonFormatReader reader = new NdJsonFormatReader(Settings.EMPTY, blockFactory, null);
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
        for (String projection : Utils.possibleValues(NdJsonReadBenchmark.class, "projection")) {
            NdJsonReadBenchmark bench = new NdJsonReadBenchmark();
            bench.rowCount = DatasourceBenchmarks.SELF_TEST_ROW_COUNT;
            bench.projection = projection;
            try {
                bench.setup();
                int actual = bench.readAll(new ReadMetrics());
                if (actual != bench.rowCount) {
                    throw new AssertionError(
                        "NdJsonReadBenchmark[" + projection + "] read " + actual + " rows, expected " + bench.rowCount
                    );
                }
            } catch (IOException e) {
                throw new AssertionError("NdJsonReadBenchmark[" + projection + "] failed", e);
            }
        }
    }

    static byte[] generateNdJsonFixture(int rowCount) {
        StringBuilder sb = new StringBuilder(rowCount * 60);
        for (int i = 0; i < rowCount; i++) {
            sb.append("{\"id\":")
                .append(i)
                .append(",\"value\":")
                .append(i)
                .append(",\"name\":\"row-")
                .append(i)
                .append("\",\"score\":")
                .append(i * 1.5)
                .append("}\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
