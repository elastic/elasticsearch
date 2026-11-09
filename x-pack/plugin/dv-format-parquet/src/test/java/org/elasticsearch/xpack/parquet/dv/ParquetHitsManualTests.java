/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetFormatReader;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetPositionalReader;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.parquet.engine.ExternalParquetCodecReader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

/**
 * Manual benchmark of the doc values iterators over the ClickBench {@code hits.parquet}. It is skipped unless
 * {@code -Dtests.parquet.hits=<path>} is set, and it prints timings instead of asserting them. Run it with
 * {@code -Dtests.output=always -Dtests.heap.size=4g}; {@code -Dtests.parquet.rows} (default 10 million) limits the rows
 * that a scan reads, {@code -Dtests.parquet.window} (default 4096) sets the window size and {@code -Dtests.parquet.runs} (default 3)
 * the number of scans per column.
 */
// The benchmark reads a local file outside the paths that the test entitlements allow; it is never part of a CI run.
@ESTestCase.WithoutEntitlements
public class ParquetHitsManualTests extends ESTestCase {

    @SuppressForbidden(reason = "a manual benchmark takes its input from system properties")
    private static String property(String name, String defaultValue) {
        return System.getProperty(name, defaultValue);
    }

    public void testScanAndPointReads() throws IOException {
        String path = property("tests.parquet.hits", null);
        assumeTrue("set -Dtests.parquet.hits to run the manual benchmark", path != null);
        int windowSize = Integer.parseInt(property("tests.parquet.window", "4096"));
        int runs = Integer.parseInt(property("tests.parquet.runs", "3"));
        try (FileChannel channel = FileChannel.open(PathUtils.get(path), StandardOpenOption.READ)) {
            StorageObject file = new LocalFile(channel);
            long totalRows = ParquetPositionalReader.readFooter(file, ParquetBlocks.factory(), Settings.EMPTY)
                .getBlocks()
                .stream()
                .mapToLong(b -> b.getRowCount())
                .sum();
            int rows = (int) Math.min(totalRows, Long.parseLong(property("tests.parquet.rows", "10000000")));
            report("file rows [%d], benchmark rows [%d], window [%d]", totalRows, rows, windowSize);

            List<ColumnBinding> bindings = List.of(
                new ColumnBinding("UserID", 0, "UserID", DocValuesType.NUMERIC),
                new ColumnBinding("ResolutionWidth", 1, "ResolutionWidth", DocValuesType.NUMERIC),
                new ColumnBinding("URL", 2, "URL", DocValuesType.BINARY),
                new ColumnBinding("SearchPhrase", 3, "SearchPhrase", DocValuesType.BINARY)
            );
            ExternalFilePiece piece = new ExternalFilePiece("local", file.length(), 0, rows);
            ParquetDocValuesProducer producer = new ParquetDocValuesProducer(bindings, piece, (location, length) -> file, windowSize);
            ExternalParquetCodecReader fieldInfos = new ExternalParquetCodecReader(bindings, piece);

            for (ColumnBinding binding : bindings) {
                FieldInfo field = fieldInfos.getFieldInfos().fieldInfo(binding.fieldName());
                for (int run = 0; run < runs; run++) {
                    long start = System.nanoTime();
                    long checksum = 0;
                    int docs = 0;
                    if (binding.type() == DocValuesType.NUMERIC) {
                        NumericDocValues values = producer.getNumeric(field);
                        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                            checksum += values.longValue();
                            docs++;
                        }
                    } else {
                        BinaryDocValues values = producer.getBinary(field);
                        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                            checksum += values.binaryValue().length;
                            docs++;
                        }
                    }
                    double seconds = (System.nanoTime() - start) / 1e9;
                    report(
                        "scan  %-16s run %d: %7.2f s  %8.1f M docs/s  (docs with value %d, checksum %d)",
                        binding.fieldName(),
                        run,
                        seconds,
                        docs / seconds / 1e6,
                        docs,
                        checksum
                    );
                }
                // point reads: random access in a fresh iterator, which is the worst case for a window
                int points = 2_000;
                long start = System.nanoTime();
                long checksum = 0;
                if (binding.type() == DocValuesType.NUMERIC) {
                    NumericDocValues values = producer.getNumeric(field);
                    for (int i = 0; i < points; i++) {
                        if (values.advanceExact(random().nextInt(rows))) {
                            checksum += values.longValue();
                        }
                    }
                } else {
                    BinaryDocValues values = producer.getBinary(field);
                    for (int i = 0; i < points; i++) {
                        if (values.advanceExact(random().nextInt(rows))) {
                            checksum += values.binaryValue().length;
                        }
                    }
                }
                double micros = (System.nanoTime() - start) / 1e3 / points;
                report("point %-16s random advanceExact: %9.1f us per call (checksum %d)", binding.fieldName(), micros, checksum);
            }
        }
    }

    /** The forward path of the ES|QL reader over the same rows, one row group per range and one thread, for comparison. */
    public void testForwardScan() throws IOException {
        String path = property("tests.parquet.hits", null);
        assumeTrue("set -Dtests.parquet.hits to run the manual benchmark", path != null);
        int runs = Integer.parseInt(property("tests.parquet.runs", "3"));
        long rows = Long.parseLong(property("tests.parquet.rows", "10000000"));
        try (FileChannel channel = FileChannel.open(PathUtils.get(path), StandardOpenOption.READ)) {
            StorageObject file = new LocalFile(channel);
            ParquetFormatReader reader = new ParquetFormatReader(Settings.EMPTY, ParquetBlocks.factory());
            List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(file);
            ParquetMetadata footer = ParquetPositionalReader.readFooter(file, ParquetBlocks.factory(), Settings.EMPTY);
            // the ranges that cover the first [rows] rows: row group i is range i
            int groups = 0;
            long covered = 0;
            while (covered < rows) {
                covered += footer.getBlocks().get(groups++).getRowCount();
            }
            report("forward scan: [%d] row groups, [%d] rows", groups, covered);
            for (String column : List.of("UserID", "ResolutionWidth", "URL", "SearchPhrase")) {
                for (int run = 0; run < runs; run++) {
                    long start = System.nanoTime();
                    long checksum = 0;
                    long docs = 0;
                    for (int g = 0; g < groups; g++) {
                        RangeAwareFormatReader.SplitRange range = ranges.get(g);
                        try (
                            CloseableIterator<Page> pages = reader.readRange(
                                file,
                                new RangeReadContext(
                                    List.of(column),
                                    16384,
                                    range.offset(),
                                    range.offset() + range.length(),
                                    List.of(),
                                    ErrorPolicy.PERMISSIVE
                                )
                            )
                        ) {
                            while (pages.hasNext()) {
                                Page page = pages.next();
                                try {
                                    Block block = page.getBlock(0);
                                    docs += block.getPositionCount();
                                    if (block instanceof LongBlock longs) {
                                        for (int i = 0; i < longs.getPositionCount(); i++) {
                                            checksum += longs.getLong(longs.getFirstValueIndex(i));
                                        }
                                    } else if (block instanceof IntBlock ints) {
                                        for (int i = 0; i < ints.getPositionCount(); i++) {
                                            checksum += ints.getInt(ints.getFirstValueIndex(i));
                                        }
                                    } else if (block instanceof BytesRefBlock strings) {
                                        BytesRef scratch = new BytesRef();
                                        for (int i = 0; i < strings.getPositionCount(); i++) {
                                            checksum += strings.getBytesRef(strings.getFirstValueIndex(i), scratch).length;
                                        }
                                    }
                                } finally {
                                    page.releaseBlocks();
                                }
                            }
                        }
                    }
                    double seconds = (System.nanoTime() - start) / 1e9;
                    report(
                        "forward %-16s run %d: %7.2f s  %8.1f M docs/s  (docs %d, checksum %d)",
                        column,
                        run,
                        seconds,
                        docs / seconds / 1e6,
                        docs,
                        checksum
                    );
                }
            }
        }
    }

    @SuppressForbidden(reason = "a manual benchmark prints its results")
    private static void report(String format, Object... args) {
        System.out.println("BENCH " + String.format(Locale.ROOT, format, args));
    }

    /** A file on local disk. Reads are positional, so concurrent streams do not share a file position. */
    private static final class LocalFile extends AbstractTestStorageObject {
        private final FileChannel channel;

        LocalFile(FileChannel channel) {
            this.channel = channel;
        }

        @Override
        public InputStream newStream() throws IOException {
            return newStream(0, length());
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new InputStream() {
                private long next = position;
                private final long end = Math.min(position + length, lengthUnchecked());

                @Override
                public int read() throws IOException {
                    byte[] one = new byte[1];
                    return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    if (next >= end) {
                        return -1;
                    }
                    int read = channel.read(ByteBuffer.wrap(b, off, (int) Math.min(len, end - next)), next);
                    if (read > 0) {
                        next += read;
                    }
                    return read;
                }
            };
        }

        private long lengthUnchecked() {
            try {
                return channel.size();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public long length() throws IOException {
            return channel.size();
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("file:///hits.parquet");
        }
    }
}
