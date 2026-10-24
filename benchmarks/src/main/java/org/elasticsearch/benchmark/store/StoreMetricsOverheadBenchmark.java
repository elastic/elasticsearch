/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.index.store.StoreMetricsIndexInput;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.xpack.stateless.lucene.StatelessDirectoryFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures what it costs to account {@code store_bytes_read} on the Lucene read path, for the read shapes that
 * dominate query execution: variable-length decode ({@code readVInt}, postings), byte-at-a-time decode ({@code
 * readByte}, LZ4 tokens in stored fields) and bulk decode ({@code readLongs}, doc values).
 *
 * <p>The {@code readPath} arms are a stateful node ({@code MMAP}) and a stateless search node
 * ({@code STATELESS_SEARCH}), whose inputs are buffered and read through the shared blob cache. As in the other
 * stateless benchmarks, the file is written to disk first and the read path is then opened over it, which is also
 * what sizes the blob cache to hold it.
 *
 * <p>The {@code accounting} arms are:
 * <ul>
 *   <li>{@code NONE} — the read path's own input, no accounting. The floor.</li>
 *   <li>{@code STORE_METRICS} — accounting wired the way {@code Store} wires it: an input that accounts for itself is
 *   handed the holder, any other input is wrapped in a {@link StoreMetricsIndexInput} that counts on every read call.
 *   So this measures the accounting on a stateless search node, and the wrapper on an mmap one.</li>
 *   <li>{@code BUFFERED} and {@code BUFFERED_WRAPPED} — a {@link BlobCacheBufferedIndexInput} over the read path's
 *   input, without and with the wrapper. The pair is what the wrapper costs on a buffered input, on an arm that pays
 *   for the buffer either way.</li>
 * </ul>
 */
@Fork(value = 1, jvmArgsPrepend = { "--enable-native-access=ALL-UNNAMED", "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class StoreMetricsOverheadBenchmark {

    private static final String FILE_NAME = "reads.dat";
    private static final int VALUE_COUNT = 1 << 16;
    private static final int BULK_LONGS = 128;

    /**
     * The directory whose read path is measured. Both are opened over an index that is already on disk, so neither
     * has a write path to speak of.
     */
    public enum ReadPath {
        MMAP {
            @Override
            Directory open(Path dataPath, Path workPath) throws IOException {
                return new MMapDirectory(dataPath);
            }
        },
        STATELESS_SEARCH {
            @Override
            Directory open(Path dataPath, Path workPath) throws IOException {
                return StatelessDirectoryFactory.newSearchDirectory(dataPath, workPath);
            }
        };

        abstract Directory open(Path dataPath, Path workPath) throws IOException;
    }

    public enum Accounting {
        NONE,
        STORE_METRICS,
        BUFFERED,
        BUFFERED_WRAPPED
    }

    @Param({ "MMAP", "STATELESS_SEARCH" })
    ReadPath readPath;

    @Param({ "NONE", "STORE_METRICS", "BUFFERED", "BUFFERED_WRAPPED" })
    Accounting accounting;

    private Path dataPath;
    private Path workPath;
    private Directory dir;
    private IndexInput file;
    private IndexInput vints;
    private IndexInput bytes;
    private IndexInput longs;
    private long[] longScratch;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        BenchmarkLogging.configure();
        dataPath = Files.createTempDirectory("store-metrics-bench-data");
        workPath = Files.createTempDirectory("store-metrics-bench-work");
        longScratch = new long[BULK_LONGS];

        // One file, holding the three regions the benchmark reads. Written before the read path is opened, as a
        // search node only ever reads files written elsewhere.
        final long vintsLength;
        final long bytesOffset;
        final long longsOffset;
        Random rng = new Random(42);
        try (Directory writeDir = new MMapDirectory(dataPath); IndexOutput out = writeDir.createOutput(FILE_NAME, IOContext.DEFAULT)) {
            for (int i = 0; i < VALUE_COUNT; i++) {
                // a spread of vint widths, as in a postings list
                out.writeVInt(rng.nextInt(1 << (1 + rng.nextInt(28))));
            }
            vintsLength = out.getFilePointer();
            bytesOffset = vintsLength;
            byte[] raw = new byte[VALUE_COUNT];
            rng.nextBytes(raw);
            out.writeBytes(raw, raw.length);
            longsOffset = out.getFilePointer();
            for (int i = 0; i < VALUE_COUNT; i++) {
                out.writeLong(rng.nextLong());
            }
            CodecUtil.writeFooter(out);
        }

        dir = readPath.open(dataPath, workPath);
        file = dir.openInput(FILE_NAME, IOContext.DEFAULT);
        vints = account(file.slice("vints", 0, vintsLength));
        bytes = account(file.slice("bytes", bytesOffset, VALUE_COUNT));
        longs = account(file.slice("longs", longsOffset, (long) VALUE_COUNT * Long.BYTES));
    }

    private IndexInput account(IndexInput in) {
        return switch (accounting) {
            case NONE -> in;
            // as StoreMetricsDirectory#openInput does
            case STORE_METRICS -> StoreMetricsIndexInput.create(in.toString(), in, newHolder().singleThreaded());
            case BUFFERED -> new BufferedDelegatingIndexInput(in);
            case BUFFERED_WRAPPED -> StoreMetricsIndexInput.create(
                in.toString(),
                new BufferedDelegatingIndexInput(in),
                newHolder().singleThreaded()
            );
        };
    }

    private static ThreadLocalDirectoryMetricHolder<StoreMetrics> newHolder() {
        return new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        IOUtils.close(vints, bytes, longs, file, dir);
        IOUtils.rm(dataPath, workPath);
    }

    @Benchmark
    @OperationsPerInvocation(VALUE_COUNT)
    public long readVInt() throws IOException {
        vints.seek(0);
        long sum = 0;
        for (int i = 0; i < VALUE_COUNT; i++) {
            sum += vints.readVInt();
        }
        return sum;
    }

    @Benchmark
    @OperationsPerInvocation(VALUE_COUNT)
    public long readByte() throws IOException {
        bytes.seek(0);
        long sum = 0;
        for (int i = 0; i < VALUE_COUNT; i++) {
            sum += bytes.readByte();
        }
        return sum;
    }

    @Benchmark
    @OperationsPerInvocation(VALUE_COUNT)
    public long readLongsBulk() throws IOException {
        longs.seek(0);
        long sum = 0;
        for (int i = 0; i < VALUE_COUNT / BULK_LONGS; i++) {
            longs.readLongs(longScratch, 0, BULK_LONGS);
            sum += longScratch[0];
        }
        return sum;
    }

    /**
     * A buffered input over another input, so that a read path that is not buffered can be measured as if it were.
     */
    private static class BufferedDelegatingIndexInput extends BlobCacheBufferedIndexInput {
        private final IndexInput delegate;

        BufferedDelegatingIndexInput(IndexInput delegate) {
            super(delegate.toString(), IOContext.DEFAULT, delegate.length());
            this.delegate = delegate;
        }

        @Override
        protected void readInternal(ByteBuffer b) throws IOException {
            final int length = b.remaining();
            delegate.readBytes(b.array(), b.arrayOffset() + b.position(), length);
            b.position(b.position() + length);
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
            delegate.seek(pos);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) {
            throw new UnsupportedOperationException();
        }
    }
}
