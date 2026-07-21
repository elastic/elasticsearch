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
 * {@link org.apache.arrow.memory.ArrowBuf} that is released deterministically when the
 * reader is closed, so each iteration's peak goes back to the baseline within a small delta.
 */
public class PrefetchedPageReaderLeakRegressionTests extends ESTestCase {

    private static final int ITERATIONS = 500;
    private static final int PAGES_PER_ITERATION = 50;
    private static final int PAGE_PAYLOAD_BYTES = 64 * 1024;          // 64 KB decompressed
    private static final long MAX_DIRECT_GROWTH_BYTES = 64L * 1024 * 1024; // 64 MB ceiling

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
        BytesInputCompressor compressor = codecFactory.getCompressor(CompressionCodecName.ZSTD);
        List<PrefetchedPageReader.CompressedPage> template = new ArrayList<>(PAGES_PER_ITERATION);
        for (int p = 0; p < PAGES_PER_ITERATION; p++) {
            byte[] payload = randomBytesOfLength(PAGE_PAYLOAD_BYTES);
            // Materialize the compressed bytes to a stable heap byte[] so the same template
            // is replayable across all iterations (the compressor's BytesInput may otherwise
            // hold a reference into compressor-internal scratch state).
            byte[] compressedBytes = compressor.compress(BytesInput.from(payload)).toByteArray();
            DataPageV1 v1 = new DataPageV1(
                BytesInput.from(compressedBytes),
                PAGE_PAYLOAD_BYTES / 4,
                payload.length,
                new IntStatistics(),
                Encoding.RLE,
                Encoding.RLE,
                Encoding.PLAIN
            );
            template.add(new PrefetchedPageReader.CompressedPage(v1, -1L));
        }

        long baseline = directMemoryUsedBytes();

        for (int i = 0; i < ITERATIONS; i++) {
            try (
                PrefetchedPageReader reader = new PrefetchedPageReader(
                    codecFactory.getDecompressor(CompressionCodecName.ZSTD),
                    allocator,
                    template,
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

        long after = directMemoryUsedBytes();
        long grew = after - baseline;
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
                + " ArrowBuf released by PrefetchedPageReader.close().",
            grew,
            lessThanOrEqualTo(MAX_DIRECT_GROWTH_BYTES)
        );
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

    private byte[] randomBytesOfLength(int len) {
        byte[] bytes = new byte[len];
        random().nextBytes(bytes);
        return bytes;
    }
}
