/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

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
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Pins the per-page release bound on {@link PrefetchedPageReader}: the native memory backing a
 * decompressed page must be returned to the allocator no later than the next
 * {@link PrefetchedPageReader#readPage()} call, so the live decompressed working set per
 * (column, reader) is O(one page), not O(uncompressed column chunk).
 *
 * <p>Before the fix, every {@code decompressToDirectBuffer} output was parked until
 * {@link PrefetchedPageReader#close()} at row-group rollover — the whole column chunk's
 * decompressed bytes were held live simultaneously and, multiplied across projected columns and
 * concurrent read streams, climbed until the OS OOM-killer took the node (invisible to every
 * heap-only circuit breaker). These tests FAIL on that behavior: the allocator balance grows by
 * one page per {@code readPage()}. They PASS once {@code readPage()} releases the previous page's
 * buffer before decompressing the next.
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

    private record PagesFixture(
        PlainCompressionCodecFactory codecFactory,
        CompressionCodecName codec,
        List<PrefetchedPageReader.CompressedPage> pages
    ) {}
}
