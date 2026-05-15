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
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Three variants of "find last record terminator in a synthetic TSV buffer":
 * the production override, the SPI default driver against the current per-record
 * scanner, and the same driver against a reimplemented pre-fix bulk-read scanner.
 * Pairwise differences attribute Layer 1 (override vs default driver) and Layer 2
 * (current scanner vs bulk-read) of the fix.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class CsvBoundaryScanBenchmark {

    @Param({ "262144", "1048576", "2621440" })
    int bufferBytes;

    @Param({ "100" })
    int approxRowBytes;

    private CsvFormatReader reader;
    private byte[] buf;
    private int length;

    @Setup(Level.Trial)
    public void setup() {
        Utils.configureBenchmarkLogging();
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("bench")).build();
        reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("delimiter", "\t"));
        buf = generateTsv(bufferBytes, approxRowBytes);
        length = buf.length;
    }

    @Benchmark
    public int overrideSinglePass() throws IOException {
        return reader.findLastRecordBoundary(buf, length);
    }

    @Benchmark
    public int inheritedDefault() throws IOException {
        return runDefaultDriver(buf, length, this::findNextRecordBoundaryViaReader);
    }

    @Benchmark
    public int defaultWithBulkReadScanner() throws IOException {
        return runDefaultDriver(buf, length, this::findNextRecordBoundaryBulkReadV1);
    }

    private long findNextRecordBoundaryViaReader(InputStream stream) throws IOException {
        return reader.findNextRecordBoundary(stream);
    }

    /** Bench-only Layer-2 baseline — fresh {@code byte[8192]} per call, bulk-read. */
    private long findNextRecordBoundaryBulkReadV1(InputStream stream) throws IOException {
        long consumed = 0;
        boolean inQuotes = false;
        byte quoteAsByte = (byte) '"';
        byte[] scratch = new byte[8192];
        int bytesRead;
        while ((bytesRead = stream.read(scratch, 0, scratch.length)) > 0) {
            for (int i = 0; i < bytesRead; i++) {
                consumed++;
                byte b = scratch[i];
                if (b == quoteAsByte) {
                    if (inQuotes) {
                        if (i + 1 < bytesRead) {
                            if (scratch[i + 1] == quoteAsByte) {
                                i++;
                                consumed++;
                                continue;
                            }
                            inQuotes = false;
                            if (scratch[i + 1] == '\n') {
                                consumed++;
                                return consumed;
                            }
                            continue;
                        }
                        int next = stream.read();
                        if (next == -1) {
                            return -1;
                        }
                        consumed++;
                        if (next == quoteAsByte) {
                            continue;
                        }
                        inQuotes = false;
                        if (next == '\n') {
                            return consumed;
                        }
                        continue;
                    } else {
                        inQuotes = true;
                    }
                } else if (b == '\n' && inQuotes == false) {
                    return consumed;
                }
            }
        }
        return -1;
    }

    private interface RecordScanner {
        long findNext(InputStream stream) throws IOException;
    }

    private static int runDefaultDriver(byte[] buf, int length, RecordScanner scanner) throws IOException {
        if (length <= 0) {
            return -1;
        }
        int lastBoundary = -1;
        int cumulative = 0;
        while (cumulative < length) {
            long consumed = scanner.findNext(new ByteArrayInputStream(buf, cumulative, length - cumulative));
            if (consumed < 0) {
                return lastBoundary;
            }
            cumulative += Math.toIntExact(consumed);
            lastBoundary = cumulative - 1;
        }
        return lastBoundary;
    }

    private static byte[] generateTsv(int bufferBytes, int approxRowBytes) {
        Random rng = new Random(0xC0DECAFEL);
        byte[] data = new byte[bufferBytes];
        int p = 0;
        while (p + approxRowBytes < bufferBytes) {
            int rowLen = approxRowBytes;
            int end = Math.min(p + rowLen - 1, bufferBytes - 2);
            for (int i = p; i < end; i++) {
                int r = rng.nextInt(63);
                data[i] = (byte) (r == 0 ? '\t' : ('a' + (r % 26)));
            }
            data[end] = '\n';
            p = end + 1;
        }
        // Tail: fill with non-newline non-quote so the last \n is the boundary answer.
        for (int i = p; i < bufferBytes; i++) {
            data[i] = 'x';
        }
        return data;
    }
}
