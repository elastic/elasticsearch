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
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatOptions;
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
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Attributes the speedups in {@code findLastRecordBoundary} on the QuotedFieldsOnly path.
 *
 * <p>Three TSV-configured measurements against the same 2.5 MiB buffer:
 * <ul>
 *   <li>{@code tsv} — production. Layer 1 single-pass override; both fixes applied.</li>
 *   <li>{@code tsvLayer1Disabled} — drives the production {@code findNextRecordBoundary}
 *       (post-Layer-2 scanner) through the SPI default's loop body, bypassing the Layer 1
 *       override. Delta vs {@code tsv} attributes Layer 1.</li>
 *   <li>{@code tsvBothLayersDisabled} — drives a subclass that overrides
 *       {@code findNextRecordBoundary} with the pre-Layer-2 {@code byte[8192]} bulk-read
 *       scanner. Delta vs {@code tsvLayer1Disabled} attributes Layer 2.</li>
 * </ul>
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
    private CsvFormatReader tsvPreLayer2ScannerReader;
    private byte[] buf;
    private int length;

    @Setup(Level.Trial)
    public void setup() {
        Utils.configureBenchmarkLogging();
        BlockFactory bf = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("bench")).build();
        List<String> exts = List.of(".csv", ".tsv");
        tsvReader = new CsvFormatReader(bf, CsvFormatOptions.TSV, "csv", exts);
        tsvPreLayer2ScannerReader = new PreLayer2Scanner(bf, CsvFormatOptions.TSV, "csv", exts);
        buf = generateTsv(bufferBytes, approxRowBytes);
        length = buf.length;
    }

    @Benchmark
    public int tsv() throws IOException {
        return tsvReader.findLastRecordBoundary(buf, length);
    }

    /** SPI default body driving the production (post-Layer-2) scanner. Same loop body the
     *  inherited {@code SegmentableFormatReader.findLastRecordBoundary} default uses. */
    @Benchmark
    public int tsvLayer1Disabled() throws IOException {
        return runSpiDefaultBody(tsvReader, buf, length);
    }

    /** SPI default body driving the pre-Layer-2 (byte[8192]) scanner. */
    @Benchmark
    public int tsvBothLayersDisabled() throws IOException {
        return runSpiDefaultBody(tsvPreLayer2ScannerReader, buf, length);
    }

    private static int runSpiDefaultBody(CsvFormatReader reader, byte[] buf, int length) throws IOException {
        if (length <= 0) {
            return -1;
        }
        int lastBoundary = -1;
        int cumulative = 0;
        while (cumulative < length) {
            long consumed = reader.findNextRecordBoundary(new ByteArrayInputStream(buf, cumulative, length - cumulative));
            if (consumed < 0) {
                return lastBoundary;
            }
            cumulative += Math.toIntExact(consumed);
            lastBoundary = cumulative - 1;
        }
        return lastBoundary;
    }

    /**
     * Subclass that replaces {@code findNextRecordBoundary} with the pre-Layer-2 byte[8192]
     * bulk-read scanner — the version that lived in production before this PR.
     */
    private static class PreLayer2Scanner extends CsvFormatReader {
        PreLayer2Scanner(BlockFactory bf, CsvFormatOptions opts, String format, List<String> extensions) {
            super(bf, opts, format, extensions);
        }

        @Override
        public long findNextRecordBoundary(InputStream stream) throws IOException {
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
