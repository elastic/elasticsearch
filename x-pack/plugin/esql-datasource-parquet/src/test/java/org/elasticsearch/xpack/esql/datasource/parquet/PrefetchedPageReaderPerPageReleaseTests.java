/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.RootAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Pins the per-(column, reader) decompressed working-set bound on {@link PrefetchedPageReader}:
 * live native memory stays O(one page), not O(uncompressed column chunk). The reader reuses one
 * decompress buffer across pages (grown only when a later page needs more capacity) and releases
 * it on {@link PrefetchedPageReader#close()}.
 *
 * <p>Before per-page bounding, every {@code decompressToDirectBuffer} output was parked until
 * {@link PrefetchedPageReader#close()} at row-group rollover — the whole column chunk's
 * decompressed bytes were held live simultaneously and, multiplied across projected columns and
 * concurrent read streams, climbed until the OS OOM-killer took the node (invisible to every
 * heap-only circuit breaker). These tests FAIL on that accumulation: the allocator balance grows
 * by one page per {@code readPage()}. They PASS when live memory stays bounded by one page.
 *
 * <p>Both a zstd {@link DataPageV2} (the canonical bench case, direct-to-direct JNI fast path) and
 * a gzip {@link DataPageV1} (the codec-uniformity twin, heap-staged, no native-library dependency)
 * are covered — the direct decompress-output buffer is allocated by {@code decompressToDirectBuffer}
 * identically for every codec, so the fix must bound the working set uniformly.
 */
public class PrefetchedPageReaderPerPageReleaseTests extends ESTestCase {

    private static final int PAGE_PAYLOAD_BYTES = 64 * 1024; // power of two: Arrow capacity rounding is exact
    private static final int PAGES = 4;

    /**
     * Canonical case from the leak investigation: zstd {@link DataPageV2}, decompressed through the
     * direct-to-direct fast path. Asserts the live allocator balance stays bounded by one page as
     * pages are read, and returns to zero at {@code close()}.
     */
    public void testZstdV2DecompressBufferReleasedPerPageAndOnClose() throws IOException {
        assertPerPageReleaseAndZeroOnClose(buildZstdV2Pages());
    }

    /**
     * Codec-uniformity twin: gzip {@link DataPageV1}, heap-staged into the direct output buffer.
     * GZIP has no native-library dependency, so this covers the release bound on platforms without
     * native zstd and confirms the fix is codec-agnostic (same {@code decompressToDirectBuffer}
     * output buffer for every codec).
     */
    public void testGzipV1DecompressBufferReleasedPerPageAndOnClose() throws IOException {
        assertPerPageReleaseAndZeroOnClose(buildGzipV1Pages());
    }

    /**
     * Equal-sized zstd V2 pages with direct compressed input (the production S3 path) must
     * malloc the decompress buffer once, not once per page. Heap-backed compressed input is
     * not used here: that path allocates a scratch buffer per page and would hide reuse.
     */
    public void testBufferReuseReducesAllocationCount() throws IOException {
        CountingListener listener = new CountingListener();
        PagesFixture fixture = buildDirectZstdV2Pages(PAGE_PAYLOAD_BYTES, PAGE_PAYLOAD_BYTES, PAGE_PAYLOAD_BYTES, PAGE_PAYLOAD_BYTES);
        try (RootAllocator allocator = new RootAllocator(listener, Long.MAX_VALUE)) {
            PrefetchedPageReader reader = new PrefetchedPageReader(
                fixture.codecFactory.getDecompressor(fixture.codec),
                allocator,
                fixture.pages,
                null,
                (long) PAGE_PAYLOAD_BYTES * PAGES
            );
            try {
                int baseline = listener.allocations;
                for (int p = 0; p < PAGES; p++) {
                    assertNotNull(reader.readPage());
                }
                assertNull(reader.readPage());
                int allocated = listener.allocations - baseline;
                assertThat("must allocate the decompress buffer at least once", allocated, greaterThanOrEqualTo(1));
                assertThat("equal-sized pages must reuse one decompress buffer, not malloc per page", allocated, lessThanOrEqualTo(2));
                assertThat(allocated, lessThan(PAGES));
            } finally {
                reader.close();
            }
            assertEquals("close() must return the reused buffer to the allocator", 0L, allocator.getAllocatedMemory());
        } finally {
            fixture.codecFactory.release();
        }
    }

    /**
     * Reused buffer stays live after the queue is drained and is released only on {@code close()}.
     */
    public void testReusedBufferReleasedOnClose() throws IOException {
        PagesFixture fixture = buildZstdV2Pages();
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            PrefetchedPageReader reader = new PrefetchedPageReader(
                fixture.codecFactory.getDecompressor(fixture.codec),
                allocator,
                fixture.pages,
                null,
                (long) PAGE_PAYLOAD_BYTES * PAGES
            );
            try {
                for (int p = 0; p < PAGES; p++) {
                    assertNotNull(reader.readPage());
                }
                assertNull(reader.readPage());
                assertThat(
                    "reusable decompress buffer must stay live after the last readPage()",
                    allocator.getAllocatedMemory(),
                    greaterThan(0L)
                );
            } finally {
                reader.close();
            }
            assertEquals("close() must return the reused buffer to the allocator", 0L, allocator.getAllocatedMemory());
        } finally {
            fixture.codecFactory.release();
        }
    }

    /**
     * Growing pages realloc; a later smaller page reuses the large buffer. Live memory never
     * accumulates across pages.
     */
    public void testBufferReuseHandlesVaryingPageSizes() throws IOException {
        int[] sizes = { 32 * 1024, 64 * 1024, 128 * 1024, 64 * 1024 };
        CountingListener listener = new CountingListener();
        PagesFixture fixture = buildDirectZstdV2Pages(sizes);
        long totalValues = 0;
        for (int size : sizes) {
            totalValues += size / 4;
        }
        try (RootAllocator allocator = new RootAllocator(listener, Long.MAX_VALUE)) {
            PrefetchedPageReader reader = new PrefetchedPageReader(
                fixture.codecFactory.getDecompressor(fixture.codec),
                allocator,
                fixture.pages,
                null,
                totalValues
            );
            try {
                long peakLive = 0L;
                for (int i = 0; i < sizes.length; i++) {
                    long liveBefore = allocator.getAllocatedMemory();
                    int allocationsBefore = listener.allocations;
                    assertNotNull(reader.readPage());
                    if (sizes[i] > liveBefore) {
                        assertThat(
                            "page larger than the reusable buffer must allocate",
                            listener.allocations,
                            greaterThan(allocationsBefore)
                        );
                    } else {
                        assertEquals("page that fits in the reusable buffer must not allocate", allocationsBefore, listener.allocations);
                    }
                    long live = allocator.getAllocatedMemory();
                    peakLive = Math.max(peakLive, live);
                    assertEquals("live memory must not accumulate across pages", peakLive, live);
                }
                assertThat("must not malloc once per page", listener.allocations, lessThan(sizes.length));
                assertThat(allocator.getAllocatedMemory(), greaterThan(0L));
            } finally {
                reader.close();
            }
            assertEquals("close() must return the reused buffer to the allocator", 0L, allocator.getAllocatedMemory());
        } finally {
            fixture.codecFactory.release();
        }
    }

    private void assertPerPageReleaseAndZeroOnClose(PagesFixture fixture) {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            PrefetchedPageReader reader = new PrefetchedPageReader(
                fixture.codecFactory.getDecompressor(fixture.codec),
                allocator,
                fixture.pages,
                null,
                (long) PAGE_PAYLOAD_BYTES * PAGES
            );
            try {
                assertNotNull(reader.readPage());
                long oneLivePage = allocator.getAllocatedMemory();
                assertThat(
                    "one decompressed page must be live after the first readPage()",
                    oneLivePage,
                    greaterThanOrEqualTo((long) PAGE_PAYLOAD_BYTES)
                );

                for (int p = 1; p < PAGES; p++) {
                    assertNotNull(reader.readPage());
                    // Per-page bound: the previous page's decompress buffer is dead the moment the
                    // consumer asks for the next page. FAILS pre-fix — the balance grows by one page
                    // per readPage() until close(), which is the accumulation the bench OOM surfaced.
                    assertThat(
                        "live decompressed memory must stay bounded by one page after readPage() #" + (p + 1),
                        allocator.getAllocatedMemory(),
                        lessThanOrEqualTo(oneLivePage)
                    );
                }
                // Reader drained: the tail page stays live until close().
                assertNull(reader.readPage());
            } finally {
                reader.close();
            }
            assertEquals("close() must return every decompress buffer to the allocator", 0L, allocator.getAllocatedMemory());
        } finally {
            fixture.codecFactory.release();
        }
    }

    private PagesFixture buildZstdV2Pages() throws IOException {
        PlainCompressionCodecFactory codecFactory = new PlainCompressionCodecFactory();
        BytesInputCompressor compressor = codecFactory.getCompressor(CompressionCodecName.ZSTD);
        List<PrefetchedPageReader.CompressedPage> pages = new ArrayList<>(PAGES);
        for (int p = 0; p < PAGES; p++) {
            byte[] data = randomByteArrayOfLength(PAGE_PAYLOAD_BYTES);
            byte[] compressedData = compressor.compress(BytesInput.from(data)).toByteArray();
            // Empty repetition/definition levels, so uncompressedSize == the decompressed data size
            // (decompressV2 subtracts the rl/dl byte counts to derive the data-only size).
            DataPageV2 v2 = new DataPageV2(
                PAGE_PAYLOAD_BYTES / 4,
                0,
                PAGE_PAYLOAD_BYTES / 4,
                BytesInput.empty(),
                BytesInput.empty(),
                Encoding.PLAIN,
                BytesInput.from(compressedData),
                PAGE_PAYLOAD_BYTES,
                new IntStatistics(),
                true
            );
            pages.add(new PrefetchedPageReader.CompressedPage(v2, -1L));
        }
        return new PagesFixture(codecFactory, CompressionCodecName.ZSTD, pages);
    }

    private PagesFixture buildGzipV1Pages() throws IOException {
        PlainCompressionCodecFactory codecFactory = new PlainCompressionCodecFactory();
        BytesInputCompressor compressor = codecFactory.getCompressor(CompressionCodecName.GZIP);
        List<PrefetchedPageReader.CompressedPage> pages = new ArrayList<>(PAGES);
        for (int p = 0; p < PAGES; p++) {
            byte[] payload = randomByteArrayOfLength(PAGE_PAYLOAD_BYTES);
            byte[] compressed = compressor.compress(BytesInput.from(payload)).toByteArray();
            DataPageV1 v1 = new DataPageV1(
                BytesInput.from(compressed),
                PAGE_PAYLOAD_BYTES / 4,
                payload.length,
                new IntStatistics(),
                Encoding.RLE,
                Encoding.RLE,
                Encoding.PLAIN
            );
            pages.add(new PrefetchedPageReader.CompressedPage(v1, -1L));
        }
        return new PagesFixture(codecFactory, CompressionCodecName.GZIP, pages);
    }

    /**
     * Zstd V2 pages whose compressed bytes are already direct, matching the production S3 path
     * so {@code decompressToDirectBuffer} skips the heap-to-direct scratch allocation.
     */
    private PagesFixture buildDirectZstdV2Pages(int... payloadBytes) throws IOException {
        PlainCompressionCodecFactory codecFactory = new PlainCompressionCodecFactory();
        BytesInputCompressor compressor = codecFactory.getCompressor(CompressionCodecName.ZSTD);
        List<PrefetchedPageReader.CompressedPage> pages = new ArrayList<>(payloadBytes.length);
        for (int payloadSize : payloadBytes) {
            byte[] data = randomByteArrayOfLength(payloadSize);
            byte[] compressedData = compressor.compress(BytesInput.from(data)).toByteArray();
            ByteBuffer direct = ByteBuffer.allocateDirect(compressedData.length);
            direct.put(compressedData).flip();
            DataPageV2 v2 = new DataPageV2(
                payloadSize / 4,
                0,
                payloadSize / 4,
                BytesInput.empty(),
                BytesInput.empty(),
                Encoding.PLAIN,
                BytesInput.from(direct),
                payloadSize,
                new IntStatistics(),
                true
            );
            pages.add(new PrefetchedPageReader.CompressedPage(v2, -1L));
        }
        return new PagesFixture(codecFactory, CompressionCodecName.ZSTD, pages);
    }

    private record PagesFixture(
        PlainCompressionCodecFactory codecFactory,
        CompressionCodecName codec,
        List<PrefetchedPageReader.CompressedPage> pages
    ) {}

    private static final class CountingListener implements AllocationListener {
        private int allocations;

        @Override
        public void onAllocation(long size) {
            allocations++;
        }
    }
}
