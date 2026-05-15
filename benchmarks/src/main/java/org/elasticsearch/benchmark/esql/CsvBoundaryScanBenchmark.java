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

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures {@link CsvFormatReader#findLastRecordBoundary} on a synthetic TSV buffer
 * representative of a streaming-segmentator chunk (2.5 MiB, ~100-byte rows, ~25K records).
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

    private CsvFormatReader tsvReader;
    private CsvFormatReader csvReader;
    private byte[] buf;
    private int length;

    @Setup(Level.Trial)
    public void setup() {
        Utils.configureBenchmarkLogging();
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("bench")).build();
        tsvReader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("delimiter", "\t"));
        csvReader = new CsvFormatReader(blockFactory);
        buf = generateTsv(bufferBytes, approxRowBytes);
        length = buf.length;
    }

    /** TSV config — uses the new {@code findLastRecordBoundary} override (Layer 1 fast path). */
    @Benchmark
    public int tsv() throws IOException {
        return tsvReader.findLastRecordBoundary(buf, length);
    }

    /** Default CSV config — falls through to the inherited SPI default in the override's dispatch. */
    @Benchmark
    public int csvDefault() throws IOException {
        return csvReader.findLastRecordBoundary(buf, length);
    }

    private static byte[] generateTsv(int bufferBytes, int approxRowBytes) {
        Random rng = new Random(0xC0DECAFEL);
        byte[] data = new byte[bufferBytes];
        int p = 0;
        while (p + approxRowBytes < bufferBytes) {
            int end = Math.min(p + approxRowBytes - 1, bufferBytes - 2);
            for (int i = p; i < end; i++) {
                int r = rng.nextInt(63);
                data[i] = (byte) (r == 0 ? '\t' : ('a' + (r % 26)));
            }
            data[end] = '\n';
            p = end + 1;
        }
        for (int i = p; i < bufferBytes; i++) {
            data[i] = 'x';
        }
        return data;
    }
}
