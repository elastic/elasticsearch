/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RandomAccessInput;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.simdvec.ESVectorizationProvider;
import org.elasticsearch.simdvec.RandomAccessInputUtils;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport;
import org.elasticsearch.xpack.stateless.lucene.StatelessDirectoryFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Benchmark comparing bloom filter popcount and OR strategies when data is
 * read through a Lucene {@link Directory} — either mmap-backed or stateless
 * (blob-cache-backed).
 *
 * <p>Variants per operation (popcount / OR):
 * <ul>
 *   <li>{@code *ReadBytes} — readBytes into heap scratch, then scalar (baseline)</li>
 *   <li>{@code *ReadBytesThenSimd} — readBytes into heap scratch, then SIMD</li>
 *   <li>{@code *DirectAccess} — zero-copy via {@link RandomAccessInputUtils} + SIMD</li>
 * </ul>
 */
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class ESVectorUtilBitwiseBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    public enum DirType {
        MMAP,
        STATELESS
    }

    @Param({ "MMAP" })
    public DirType dirType;

    @Param({ "128", "4096", "16384" })
    public int pageSize;

    @Param({ "0.5" })
    public double saturation;

    @Param({ "SCALAR", "PANAMA" })
    public VectorImplementation implementation;

    private static final int NUM_PAGES = 1024;
    private static final String DATA_FILE = "bloom.dat";

    private Path dataPath;
    private Path workPath;
    private Directory directory;
    private IndexInput indexInput;
    private RandomAccessInput randomAccessInput;

    private ESVectorUtilSupport impl;
    private byte[] scratch;
    private byte[] destScratch;
    private int pageIndex;

    @Setup
    public void setup() throws IOException {
        dataPath = Files.createTempDirectory("bloom-bench-data");
        writeDataFile(dataPath);

        switch (dirType) {
            case MMAP -> directory = new MMapDirectory(dataPath);
            case STATELESS -> {
                workPath = Files.createTempDirectory("bloom-bench-work");
                directory = StatelessDirectoryFactory.create(dataPath, workPath);
            }
        }

        indexInput = directory.openInput(DATA_FILE, IOContext.DEFAULT);
        randomAccessInput = indexInput.randomAccessSlice(0, indexInput.length());

        if (dirType == DirType.STATELESS) {
            preWarm();
        }

        System.out.println(
            "[bloom-bench] dirType="
                + dirType
                + " randomAccessInput="
                + randomAccessInput.getClass().getName()
                + " directAccessInput="
                + (randomAccessInput instanceof org.elasticsearch.core.DirectAccessInput)
                + " memorySegmentAccessInput="
                + (randomAccessInput instanceof org.apache.lucene.store.MemorySegmentAccessInput)
        );

        scratch = new byte[pageSize];
        destScratch = new byte[pageSize];

        impl = switch (implementation) {
            case SCALAR -> ESVectorizationProvider.lookup(false, false).getVectorUtilSupport();
            case PANAMA -> ESVectorizationProvider.lookup(true, true).getVectorUtilSupport();
            default -> throw new IllegalArgumentException("Unsupported implementation: " + implementation);
        };
    }

    private void writeDataFile(Path dir) throws IOException {
        Random random = new Random(42);
        try (Directory fsDir = FSDirectory.open(dir); IndexOutput out = fsDir.createOutput(DATA_FILE, IOContext.DEFAULT)) {
            for (int i = 0; i < NUM_PAGES; i++) {
                byte[] page = generatePage(random, pageSize, saturation);
                out.writeBytes(page, page.length);
            }
        }
    }

    private void preWarm() throws IOException {
        byte[] buf = new byte[64 * 1024];
        try (IndexInput in = directory.openInput(DATA_FILE, IOContext.READONCE)) {
            long remaining = in.length();
            while (remaining > 0) {
                int n = (int) Math.min(buf.length, remaining);
                in.readBytes(buf, 0, n);
                remaining -= n;
            }
        }
    }

    @TearDown
    public void tearDown() throws IOException {
        IOUtils.close(indexInput, directory);
        deleteRecursively(dataPath);
        if (workPath != null) {
            deleteRecursively(workPath);
        }
    }

    private long pageOffset() {
        int idx = pageIndex++ & (NUM_PAGES - 1);
        return (long) idx * pageSize;
    }

    // --- popcount benchmarks ---

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public long popcountReadBytes() throws IOException {
        long offset = pageOffset();
        randomAccessInput.readBytes(offset, scratch, 0, pageSize);
        return impl.popcount(MemorySegment.ofArray(scratch), pageSize);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public long popcountDirectAccess() throws IOException {
        long offset = pageOffset();
        int len = pageSize;
        return RandomAccessInputUtils.withSlice(randomAccessInput, offset, len, n -> scratch, seg -> impl.popcount(seg, len));
    }

    // --- OR benchmarks ---

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public void orReadBytes() throws IOException {
        long offset = pageOffset();
        randomAccessInput.readBytes(offset, scratch, 0, pageSize);
        impl.orByteArrays(MemorySegment.ofArray(scratch), destScratch, 0, pageSize);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public void orDirectAccess() throws IOException {
        long offset = pageOffset();
        int len = pageSize;
        RandomAccessInputUtils.withSlice(randomAccessInput, offset, len, n -> scratch, seg -> {
            impl.orByteArrays(seg, destScratch, 0, len);
            return null;
        });
    }

    // --- helpers ---

    private static byte[] generatePage(Random random, int size, double saturation) {
        byte[] page = new byte[size];
        for (int i = 0; i < size; i++) {
            int bits = 0;
            for (int b = 0; b < 8; b++) {
                if (random.nextDouble() < saturation) {
                    bits |= (1 << b);
                }
            }
            page[i] = (byte) bits;
        }
        return page;
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (Files.exists(path) == false) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
