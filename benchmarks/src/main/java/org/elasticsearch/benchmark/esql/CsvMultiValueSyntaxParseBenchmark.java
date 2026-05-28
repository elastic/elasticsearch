/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark.esql;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Measures the CSV parser-path cost that the {@code multi_value_syntax} default selects, on standard
 * (bracket-free) CSV. {@code brackets} routes comma-delimited input through the hand-written
 * bracket-aware parser; {@code none} routes it through the Jackson streaming path. The data has no
 * bracket arrays, so both produce identical output — this isolates the parser cost the default flip
 * changes. Drives the real {@link CsvFormatReader#read} production path (configured via
 * {@code withConfig}, exactly as the EXTERNAL command does), not a splitter proxy.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class CsvMultiValueSyntaxParseBenchmark {

    @Param({ "100000" })
    int rowCount;

    @Param({ "none", "brackets" })
    String multiValueSyntax;

    private BlockFactory blockFactory;
    private byte[] csvData;
    private CsvFormatReader reader;

    @Setup(Level.Trial)
    public void setup() {
        Utils.configureBenchmarkLogging();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("bench")).build();
        csvData = generateStandardCsv(rowCount);
        // Reader (and its CsvMapper) constructed once per trial so the measurement loop reflects
        // only per-stream parse cost, not per-invocation mapper construction. In production the
        // mapper is shared via sharedCsvMapper, so this also matches the realistic call pattern.
        reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", multiValueSyntax));
    }

    @Benchmark
    public void readAll(Blackhole bh) throws IOException {
        StorageObject obj = createStorageObject(csvData);
        try (CloseableIterator<Page> iter = reader.read(obj, FormatReadContext.builder().batchSize(1000).build())) {
            while (iter.hasNext()) {
                Page page = iter.next();
                // Consume every block so escape analysis can't elide typed conversion / BlockBuilder
                // appends on the cheaper (none / Jackson) arm.
                for (int i = 0; i < page.getBlockCount(); i++) {
                    bh.consume(page.getBlock(i));
                }
                bh.consume(page.getPositionCount());
                page.releaseBlocks();
            }
        }
    }

    private static byte[] generateStandardCsv(int rows) {
        // Standard CSV with a URL-heavy keyword column (ClickBench-shaped): no bracket arrays, so the
        // output is identical under both settings and only the parser path differs.
        StringBuilder sb = new StringBuilder(rows * 120);
        sb.append("id:long,url:keyword,code:integer,score:double\n");
        for (int i = 1; i <= rows; i++) {
            sb.append(i)
                .append(",https://www.example.com/section/")
                .append(i % 997)
                .append("/article-")
                .append(i)
                .append("?utm_source=newsletter&utm_medium=email&ref=")
                .append(i % 31)
                .append(',')
                .append(i % 500)
                .append(',')
                .append(i * 1.5)
                .append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://bench.csv");
            }
        };
    }
}
