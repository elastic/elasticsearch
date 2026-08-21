/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Regression for the direct-memory leak on the parquet decompression path
 *
 * <p>Loops {@link PrefetchedPageReader#readPage} over a few hundred small zstd-compressed pages
 * and asserts JVM-tracked direct memory stays bounded. Before the fix, every page allocated
 * a fresh {@code ByteBuffer.allocateDirect} that only the {@code Cleaner} could reclaim, and
 * the {@code Cleaner} only runs on Old/Mixed GC — which the tight loop never triggers, so
 * direct memory grew monotonically across iterations.
 *
 * <p>After the fix, decompression buffers come from a {@link BufferAllocator}-managed
 * {@link org.apache.arrow.memory.ArrowBuf} returned to {@link org.elasticsearch.compute.data.arrow.DirectBufferPool} on reader
 * close. Later iterations reuse that buffer: allocator balance after the first cycle is the
 * pooled size, not zero, and stays flat.
 */
public class PrefetchedPageReaderLeakRegressionTests extends ESTestCase {

    private static final int ITERATIONS = 500;
    private static final int PAGES_PER_ITERATION = 50;
    private static final int PAGE_PAYLOAD_BYTES = 64 * 1024;          // 64 KB decompressed
    private static final long MAX_DIRECT_GROWTH_BYTES = 64L * 1024 * 1024; // 64 MB ceiling
    private static final int CONCURRENT_READERS = 4;
    private static final int CONCURRENT_ITERS_PER_READER = 50;

    private PlainCompressionCodecFactory codecFactory;
    private BlockFactory blockFactory;
    private BufferAllocator allocator;

    @Before
    public void initCodecAndAllocator() {
        codecFactory = new PlainCompressionCodecFactory();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        allocator = blockFactory.arrowAllocator();
    }

    @After
    public void releaseCodecFactory() {
        codecFactory.release();
    }

    public void testRepeatedZstdDecompressionStaysWithinDirectMemoryBudget() throws IOException {
        ZstdPageFixture fixture = compressedZstdPages();

        long directBaseline = directMemoryUsedBytes();
        long allocBaseline = allocator.getAllocatedMemory();
        long pooledAfterFirst = -1L;

        for (int i = 0; i < ITERATIONS; i++) {
            try (
                PrefetchedPageReader reader = new PrefetchedPageReader(
                    codecFactory.getDecompressor(CompressionCodecName.ZSTD),
                    blockFactory.directBuffers(),
                    fixture.copyPages(),
                    null,
                    (long) PAGE_PAYLOAD_BYTES * PAGES_PER_ITERATION
                )
            ) {
                DataPage page;
                while ((page = reader.readPage()) != null) {
                    consume(page);
                }
            }
            if (i == 0) {
                pooledAfterFirst = allocator.getAllocatedMemory();
                assertThat("first cycle must park a decompress buffer", pooledAfterFirst, greaterThan(allocBaseline));
            } else {
                assertEquals("allocator must stay flat after cycle " + i, pooledAfterFirst, allocator.getAllocatedMemory());
            }
        }

        long after = directMemoryUsedBytes();
        long grew = after - directBaseline;
        assertThat(
            "direct-memory grew "
                + (grew >>> 20)
                + " MB across "
                + ITERATIONS
                + " iters x "
                + PAGES_PER_ITERATION
                + " pages; expected <= "
                + (MAX_DIRECT_GROWTH_BYTES >>> 20)
                + " MB. After the fix, decompressToDirectBuffer allocates from a BufferAllocator-managed"
                + " ArrowBuf returned to DirectBufferPool by PrefetchedPageReader.close().",
            grew,
            lessThanOrEqualTo(MAX_DIRECT_GROWTH_BYTES)
        );
    }

    /**
     * Concurrent zstd decompress loops must not accumulate native memory beyond one pooled
     * buffer per reader. MXBean remains a coarse JVM-wide ceiling matching the single-threaded
     * regression.
     */
    public void testDirectMemoryStableUnderConcurrentReads() throws Exception {
        ZstdPageFixture fixture = compressedZstdPages();
        long allocBaseline = allocator.getAllocatedMemory();
        long directBaseline = directMemoryUsedBytes();

        startInParallel(CONCURRENT_READERS, i -> {
            try {
                for (int iter = 0; iter < CONCURRENT_ITERS_PER_READER; iter++) {
                    try (
                        PrefetchedPageReader reader = new PrefetchedPageReader(
                            codecFactory.getDecompressor(CompressionCodecName.ZSTD),
                            blockFactory.directBuffers(),
                            fixture.copyPages(),
                            null,
                            (long) PAGE_PAYLOAD_BYTES * PAGES_PER_ITERATION
                        )
                    ) {
                        DataPage page;
                        while ((page = reader.readPage()) != null) {
                            consume(page);
                        }
                    }
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        long after = allocator.getAllocatedMemory();
        assertThat("concurrent readers park at least one decompress buffer", after, greaterThan(allocBaseline));
        assertThat(
            "concurrent readers must not exceed one buffer per reader",
            after,
            lessThanOrEqualTo(allocBaseline + (long) CONCURRENT_READERS * PAGE_PAYLOAD_BYTES)
        );
        long grew = directMemoryUsedBytes() - directBaseline;
        assertThat(
            "direct-memory grew "
                + (grew >>> 20)
                + " MB under concurrent zstd reads; expected <= "
                + (MAX_DIRECT_GROWTH_BYTES >>> 20)
                + " MB",
            grew,
            lessThanOrEqualTo(MAX_DIRECT_GROWTH_BYTES)
        );
    }

    private ZstdPageFixture compressedZstdPages() throws IOException {
        BytesInputCompressor compressor = codecFactory.getCompressor(CompressionCodecName.ZSTD);
        byte[][] compressed = new byte[PAGES_PER_ITERATION][];
        for (int p = 0; p < PAGES_PER_ITERATION; p++) {
            byte[] payload = randomByteArrayOfLength(PAGE_PAYLOAD_BYTES);
            compressed[p] = compressor.compress(BytesInput.from(payload)).toByteArray();
        }
        return new ZstdPageFixture(compressed);
    }

    /**
     * Immutable compressed payloads. {@link #copyPages()} builds a fresh {@link BytesInput} per
     * page so concurrent readers never share mutable parquet-mr page state.
     */
    private record ZstdPageFixture(byte[][] compressed) {
        List<PrefetchedPageReader.CompressedPage> copyPages() {
            List<PrefetchedPageReader.CompressedPage> pages = new ArrayList<>(compressed.length);
            for (byte[] compressedBytes : compressed) {
                DataPageV1 v1 = new DataPageV1(
                    BytesInput.from(compressedBytes),
                    PAGE_PAYLOAD_BYTES / 4,
                    PAGE_PAYLOAD_BYTES,
                    new IntStatistics(),
                    Encoding.RLE,
                    Encoding.RLE,
                    Encoding.PLAIN
                );
                pages.add(new PrefetchedPageReader.CompressedPage(v1, -1L));
            }
            return pages;
        }
    }

    private static void consume(DataPage page) throws IOException {
        // Force the BytesInput contents through to a heap copy (simulates the Block-construction
        // consumer) and let the array go out of scope. The copy must complete before the reader
        // is closed because closing the reader releases the underlying ArrowBuf.
        DataPageV1 v1 = (DataPageV1) page;
        byte[] sink = v1.getBytes().toByteArray();
        if (sink.length < 0) {
            throw new AssertionError(); // anti-DCE guard; JIT cannot fold sink.length < 0 to false
        }
    }

    private static long directMemoryUsedBytes() {
        for (BufferPoolMXBean p : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if ("direct".equals(p.getName())) {
                return p.getMemoryUsed();
            }
        }
        return 0;
    }
}
