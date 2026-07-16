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
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Three measurements of "find the last record terminator in a 2.5 MiB TSV buffer":
 * <ul>
 *   <li>{@link #originalBefore} — original CSV reader, before Layer 2 was applied.</li>
 *   <li>{@link #originalAfter} — original CSV reader (no Layer 1 override), after Layer 2.</li>
 *   <li>{@link #tsvAfter} — current TSV reader, both fixes applied.</li>
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
    private SegmentableFormatReader originalReaderAfter;
    private SegmentableFormatReader originalReaderBefore;
    private byte[] buf;
    private int length;

    @Setup(Level.Trial)
    public void setup() {
        Utils.configureBenchmarkLogging();
        BlockFactory bf = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("bench")).build();
        List<String> exts = List.of(".csv", ".tsv");

        // Current TSV reader. Both Layer 1 and Layer 2 applied.
        tsvReader = new CsvFormatReader(bf, CsvFormatOptions.TSV, "csv", exts);

        // Original CSV reader (no Layer 1 override) after Layer 2 was applied: implements
        // SegmentableFormatReader directly so findLastRecordBoundary comes from the SPI
        // default. The per-record scanner is the production (post-Layer-2) findNextRecordBoundary.
        originalReaderAfter = new CsvReaderInheritingDefault(tsvReader);

        // Original CSV reader before either fix: same shape as above, but the per-record
        // scanner is the pre-Layer-2 byte[8192] version.
        originalReaderBefore = new CsvReaderInheritingDefault(new CsvReaderWithOriginalScanner(bf, CsvFormatOptions.TSV, "csv", exts));

        buf = generateTsv(bufferBytes, approxRowBytes);
        length = buf.length;
    }

    /** Original CSV reader before either fix. */
    @Benchmark
    public int originalBefore() throws IOException {
        return originalReaderBefore.recordSplitter().findLastRecordBoundary(buf, length);
    }

    /** Original CSV reader after Layer 2 was applied, before Layer 1. */
    @Benchmark
    public int originalAfter() throws IOException {
        return originalReaderAfter.recordSplitter().findLastRecordBoundary(buf, length);
    }

    /** Current TSV reader — both fixes applied. */
    @Benchmark
    public int tsvAfter() throws IOException {
        return tsvReader.recordSplitter().findLastRecordBoundary(buf, length);
    }

    /**
     * Implements the former SegmentableFormatReader default findLastRecordBoundary by driving the
     * per-record scanner forward through the buffer.
     */
    private static final class CsvReaderInheritingDefault implements SegmentableFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final CsvFormatReader wrapped;

        CsvReaderInheritingDefault(CsvFormatReader wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public RecordSplitter recordSplitter(int maxRecordBytes) {
            RecordSplitter wrappedSplitter = wrapped.recordSplitter(maxRecordBytes);
            return defaultFindLastSplitter(wrappedSplitter);
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
     * Subclass that replaces findNextRecordBoundary with the pre-Layer-2 byte[8192] bulk-read
     * scanner — the version that lived in production before this PR.
     */
    private static class CsvReaderWithOriginalScanner extends CsvFormatReader {
        private final CsvFormatOptions opts;

        CsvReaderWithOriginalScanner(BlockFactory bf, CsvFormatOptions opts, String format, List<String> extensions) {
            super(bf, opts, format, extensions);
            this.opts = opts;
        }

        @Override
        public RecordSplitter recordSplitter(int maxRecordBytes) {
            return defaultFindLastSplitter(new RecordSplitter() {
                @Override
                public long findNextRecordBoundary(InputStream stream) throws IOException {
                    long consumed = 0;
                    boolean inQuotes = false;
                    byte quoteAsByte = (byte) opts.quoteChar();
                    byte[] scratch = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = stream.read(scratch, 0, scratch.length)) > 0) {
                        for (int i = 0; i < bytesRead; i++) {
                            consumed++;
                            if (consumed > maxRecordBytes) {
                                return RECORD_TOO_LARGE;
                            }
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
                                            return consumed > maxRecordBytes ? RECORD_TOO_LARGE : consumed;
                                        }
                                        continue;
                                    }
                                    int next = stream.read();
                                    if (next == -1) {
                                        return -1;
                                    }
                                    consumed++;
                                    if (consumed > maxRecordBytes) {
                                        return RECORD_TOO_LARGE;
                                    }
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

                @Override
                public int findLastRecordBoundary(byte[] buf, int offset, int length) throws IOException {
                    return driveForwardToLastBoundary(this, buf, offset, length);
                }

                @Override
                public int maxRecordBytes() {
                    return maxRecordBytes;
                }
            });
        }
    }

    private static RecordSplitter defaultFindLastSplitter(RecordSplitter splitter) {
        return new RecordSplitter() {
            @Override
            public long findNextRecordBoundary(InputStream stream) throws IOException {
                return splitter.findNextRecordBoundary(stream);
            }

            @Override
            public int findLastRecordBoundary(byte[] buf, int offset, int length) throws IOException {
                return driveForwardToLastBoundary(splitter, buf, offset, length);
            }

            @Override
            public int maxRecordBytes() {
                return splitter.maxRecordBytes();
            }
        };
    }

    private static int driveForwardToLastBoundary(RecordSplitter splitter, byte[] buf, int offset, int length) throws IOException {
        int lastBoundary = -1;
        int cumulative = 0;
        while (cumulative < length) {
            long consumed = splitter.findNextRecordBoundary(new ByteArrayInputStream(buf, offset + cumulative, length - cumulative));
            if (consumed == RecordSplitter.RECORD_TOO_LARGE) {
                return lastBoundary >= 0 ? lastBoundary : (int) RecordSplitter.RECORD_TOO_LARGE;
            }
            if (consumed < 0) {
                return lastBoundary;
            }
            cumulative += Math.toIntExact(consumed);
            lastBoundary = offset + cumulative - 1;
        }
        return lastBoundary;
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
