/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.zstd;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.zstd.ZstdCompressionMode;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectoryFactory;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures Zstd decompression throughput across different IndexInput backing stores.
 * All compressed blocks are written sequentially into a single file (mimicking Lucene's
 * stored fields .fdt layout). Exercises the three decompression code paths:
 * - NIOFS: heap copy via copyAndDecompress
 * - MMAP: zero-copy via MemorySegmentAccessInput
 * - SNAP: zero-copy via DirectAccessInput (blob-cache backed)
 *
 * By default uses random (incompressible) data. To benchmark with real compressible data,
 * pass -DdataFile=/path/to/file (e.g. a Project Gutenberg text file).
 */
@Fork(value = 1, jvmArgsPrepend = { "--enable-native-access=ALL-UNNAMED", "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class ZstdDecompressBenchmark {

    private static final int NUM_BLOCKS = 256;
    private static final String FILE_NAME = "blocks.dat";

    @Param({ "NIOFS", "MMAP", "SNAP" })
    String directoryType;

    @Param({ "4096", "16384" })
    int blockSize;

    @Param({ "FULL", "SLICE" })
    String decompressMode;

    Directory dir;
    IndexInput input;
    long[] blockOffsets;
    byte[][] originalBlocks;
    private Decompressor decompressor;
    private BytesRef bytes;
    private Path tempDir;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Utils.configureBenchmarkLogging();
        tempDir = Files.createTempDirectory("zstd-bench");
        ZstdCompressionMode mode = new ZstdCompressionMode(1);
        decompressor = mode.newDecompressor();
        bytes = new BytesRef();
        blockOffsets = new long[NUM_BLOCKS];

        byte[][] compressedBlocks = new byte[NUM_BLOCKS][];
        originalBlocks = new byte[NUM_BLOCKS][];
        byte[] sourceData = loadSourceData();
        int usableLength = sourceData.length - blockSize;
        for (int i = 0; i < NUM_BLOCKS; i++) {
            byte[] original = new byte[blockSize];
            int offset = (i * blockSize) % (usableLength > 0 ? usableLength : 1);
            System.arraycopy(sourceData, offset, original, 0, blockSize);
            originalBlocks[i] = original;
            compressedBlocks[i] = compress(mode, original);
        }

        dir = newDirectory();
        try (IndexOutput out = dir.createOutput(FILE_NAME, IOContext.DEFAULT)) {
            for (int i = 0; i < NUM_BLOCKS; i++) {
                blockOffsets[i] = out.getFilePointer();
                out.writeBytes(compressedBlocks[i], compressedBlocks[i].length);
            }
            CodecUtil.writeFooter(out);
        }
        input = dir.openInput(FILE_NAME, IOContext.DEFAULT);
    }

    private byte[] loadSourceData() throws IOException {
        String dataFile = System.getProperty("dataFile");
        if (dataFile != null) {
            byte[] fileData = Files.readAllBytes(Path.of(dataFile));
            if (fileData.length < blockSize) {
                throw new IllegalArgumentException("dataFile too small: " + fileData.length + " < blockSize " + blockSize);
            }
            return fileData;
        }
        // Default: random (incompressible) data
        Random rng = new Random(42);
        byte[] data = new byte[NUM_BLOCKS * blockSize];
        rng.nextBytes(data);
        return data;
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        IOUtils.close(input, dir);
        deleteRecursive(tempDir);
    }

    /** Decompresses all blocks sequentially, reporting per-block throughput. */
    @Benchmark
    @OperationsPerInvocation(NUM_BLOCKS)
    public void decompress(Blackhole bh) throws IOException {
        bh.consume(decompressAll());
    }

    /** Core loop: seeks to each block and decompresses. Extracted for testability. */
    byte[] decompressAll() throws IOException {
        int off, len;
        if (decompressMode.equals("FULL")) {
            off = 0;
            len = blockSize;
        } else {
            off = blockSize / 4;
            len = blockSize / 2;
        }
        for (int i = 0; i < NUM_BLOCKS; i++) {
            input.seek(blockOffsets[i]);
            decompressor.decompress(input, blockSize, off, len, bytes);
        }
        return bytes.bytes;
    }

    private Directory newDirectory() throws IOException {
        return switch (directoryType) {
            case "NIOFS" -> new NIOFSDirectory(Files.createDirectories(tempDir.resolve("data")));
            case "MMAP" -> new MMapDirectory(Files.createDirectories(tempDir.resolve("data")));
            case "SNAP" -> SearchableSnapshotDirectoryFactory.newDirectory(Files.createDirectories(tempDir.resolve("data")));
            default -> throw new IllegalArgumentException("Unknown directory type: " + directoryType);
        };
    }

    private static byte[] compress(ZstdCompressionMode mode, byte[] data) throws IOException {
        ByteBuffersDataOutput compressedOutput = new ByteBuffersDataOutput();
        Compressor compressor = mode.newCompressor();
        compressor.compress(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(data))), compressedOutput);
        compressor.close();
        return compressedOutput.toArrayCopy();
    }

    private static void deleteRecursive(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (var stream = Files.list(path)) {
                for (Path child : stream.toList()) {
                    deleteRecursive(child);
                }
            }
        }
        Files.deleteIfExists(path);
    }
}
