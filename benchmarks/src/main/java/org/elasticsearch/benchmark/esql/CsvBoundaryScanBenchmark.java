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
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatOptions;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
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
import java.io.InputStream;
import java.util.List;
import java.util.Map;
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
    private SegmentableFormatReader tsvLayer1Disabled;
    private SegmentableFormatReader tsvBothLayersDisabled;
    private byte[] buf;
    private int length;

    @Setup(Level.Trial)
    public void setup() {
        Utils.configureBenchmarkLogging();
        BlockFactory bf = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("bench")).build();
        List<String> exts = List.of(".csv", ".tsv");
        tsvReader = new CsvFormatReader(bf, CsvFormatOptions.TSV, "csv", exts);
        tsvLayer1Disabled = new SpiDefaultDriver(tsvReader);
        tsvBothLayersDisabled = new SpiDefaultDriver(new PreLayer2Scanner(bf, CsvFormatOptions.TSV, "csv", exts));
        buf = generateTsv(bufferBytes, approxRowBytes);
        length = buf.length;
    }

    @Benchmark
    public int tsv() throws IOException {
        return tsvReader.findLastRecordBoundary(buf, length);
    }

    @Benchmark
    public int tsvLayer1Disabled() throws IOException {
        return tsvLayer1Disabled.findLastRecordBoundary(buf, length);
    }

    @Benchmark
    public int tsvBothLayersDisabled() throws IOException {
        return tsvBothLayersDisabled.findLastRecordBoundary(buf, length);
    }

    /**
     * Thin wrapper that exposes the inherited SPI default {@code findLastRecordBoundary}.
     * Delegates the abstract surface to a wrapped {@link CsvFormatReader}; no body cloned.
     */
    private static final class SpiDefaultDriver implements SegmentableFormatReader {
        private final CsvFormatReader wrapped;

        SpiDefaultDriver(CsvFormatReader wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public long findNextRecordBoundary(InputStream stream) throws IOException {
            return wrapped.findNextRecordBoundary(stream);
        }

        @Override
        public SourceMetadata metadata(StorageObject object) throws IOException {
            return wrapped.metadata(object);
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            return wrapped.read(object, context);
        }

        @Override
        public String formatName() {
            return wrapped.formatName();
        }

        @Override
        public List<String> fileExtensions() {
            return wrapped.fileExtensions();
        }

        @Override
        public Configured<FormatReader> withConfigTrackingConsumedKeys(Map<String, Object> config) {
            return wrapped.withConfigTrackingConsumedKeys(config);
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }
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
