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
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.benchmark.store.DirectoryType;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.zstd.ZstdCompressionMode;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectoryFactory;
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
 * stored fields .fdt layout). Exercises the decompression code paths:
 * - NIO: heap copy via copyAndDecompress
 * - MMAP: zero-copy via MemorySegmentAccessInput
 * - SNAP: zero-copy via DirectAccessInput (blob-cache backed)
 * - STATELESS_INDEX_LOCAL: heap copy, via a ReopeningIndexInput that implements neither (opt in with -p)
 *
 * <p>Each operation reads one document. In SLICE mode {@code hitsPerBlock} consecutive documents are read from each block
 * through the same decompressor before it moves on to the next block, like search hits that share a stored fields block.
 *
 * <p>Uses random (incompressible) data by default. To benchmark with real compressible data,
 * pass -DdataFile=/path/to/file (e.g. a Project Gutenberg text file). Add {@code -prof gc} to report allocations per document.
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

    // Listed explicitly to avoid pulling STATELESS_INDEX_LOCAL: this directory type is expensive (it brings up a
    // node environment and blob cache per instance). Run it explicitly with -p directoryType=STATELESS_INDEX_LOCAL.
    @Param({ "NIO", "MMAP", "SNAP" })
    DirectoryType directoryType;

    @Param({ "4096", "16384" })
    int blockSize;

    @Param({ "FULL", "SLICE" })
    String decompressMode;

    // 1 is a scattered hit or a get; larger values are search hits that share a block
    @Param({ "1", "2", "3", "32" })
    int hitsPerBlock;

    Directory dir;
    IndexInput input;
    long[] blockOffsets;
    byte[][] originalBlocks;
    private Decompressor decompressor;
    private BytesRef bytes;
    private long docCount;
    private Path tempDir;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        BenchmarkLogging.configure();
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
        IOUtils.rm(tempDir);
    }

    /** Reads {@code NUM_BLOCKS} documents, visiting the blocks sequentially, reporting per-document throughput. */
    @Benchmark
    @OperationsPerInvocation(NUM_BLOCKS)
    public void decompress(Blackhole bh) throws IOException {
        for (int i = 0; i < NUM_BLOCKS; i++) {
            bh.consume(readNextDocument());
        }
    }

    /**
     * Seeks to the block of the next document and decompresses it. A FULL document is the whole block. In SLICE mode the
     * {@code hitsPerBlock} documents of a block split its middle half. Extracted for testability.
     */
    BytesRef readNextDocument() throws IOException {
        final int block = (int) (docCount / hitsPerBlock % NUM_BLOCKS);
        final int hit = (int) (docCount % hitsPerBlock);
        docCount++;
        final int off, len;
        if (decompressMode.equals("FULL")) {
            off = 0;
            len = blockSize;
        } else {
            len = blockSize / 2 / hitsPerBlock;
            off = blockSize / 4 + hit * len;
        }
        input.seek(blockOffsets[block]);
        decompressor.decompress(input, blockSize, off, len, bytes);
        return bytes;
    }

    private Directory newDirectory() throws IOException {
        Path path = Files.createDirectories(tempDir.resolve("data"));
        return switch (directoryType) {
            case NIO -> new NIOFSDirectory(path);
            case MMAP -> new MMapDirectory(path);
            case SNAP -> SearchableSnapshotDirectoryFactory.newDirectory(path);
            case STATELESS_INDEX_LOCAL -> StatelessDirectoryFactory.newIndexDirectory(path);
        };
    }

    private static byte[] compress(ZstdCompressionMode mode, byte[] data) throws IOException {
        ByteBuffersDataOutput compressedOutput = new ByteBuffersDataOutput();
        Compressor compressor = mode.newCompressor();
        compressor.compress(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(data))), compressedOutput);
        compressor.close();
        return compressedOutput.toArrayCopy();
    }
}
