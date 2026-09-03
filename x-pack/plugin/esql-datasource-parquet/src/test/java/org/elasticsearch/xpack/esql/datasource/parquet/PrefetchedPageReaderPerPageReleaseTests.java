/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Pins two contracts on {@link PrefetchedPageReader}: live breaker residency stays O(max
 * decompressed page seen) (+ dictionary, if any), not O(uncompressed column chunk); same-size
 * pages stay charged at one page. Compressed pages decompress onto a reused heap {@code byte[]}.
 *
 * <p>Both a zstd {@link DataPageV2} (the canonical bench case) and a gzip {@link DataPageV1}
 * (codec-uniformity twin, no native-library dependency) are covered for the live bound.
 */
public class PrefetchedPageReaderPerPageReleaseTests extends ESTestCase {

    private static final int PAGE_PAYLOAD_BYTES = 64 * 1024;
    private static final int PAGES = 4;

    /**
     * Canonical case from the leak investigation: zstd {@link DataPageV2}. Asserts the breaker
     * tracks one page as pages are read, and returns to zero at {@code close()}.
     */
    public void testZstdV2DecompressChargeReleasedPerPageAndOnClose() throws IOException {
        assertPerPageChargeAndZeroOnClose(buildZstdV2Pages());
    }

    /**
     * Codec-uniformity twin: gzip {@link DataPageV1}. GZIP has no native-library dependency, so
     * this covers the release bound on platforms without native zstd.
     */
    public void testGzipV1DecompressChargeReleasedPerPageAndOnClose() throws IOException {
        assertPerPageChargeAndZeroOnClose(buildGzipV1Pages());
    }

    /**
     * Dictionary charge is independent of the current data-page charge: live used bytes are
     * dict + one page, and both drop to zero on close.
     */
    public void testBreakerChargeReleasedPerPage() throws IOException {
        PagesFixture fixture = buildGzipV1Pages();
        BytesInputCompressor compressor = fixture.codecFactory.getCompressor(CompressionCodecName.GZIP);
        byte[] dictPayload = randomByteArrayOfLength(1024);
        byte[] compressedDictBytes = compressor.compress(BytesInput.from(dictPayload)).toByteArray();
        DictionaryPage compressedDict = new DictionaryPage(BytesInput.from(compressedDictBytes), dictPayload.length, 4, Encoding.PLAIN);
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        PrefetchedPageReader reader = new PrefetchedPageReader(
            fixture.codecFactory.getDecompressor(CompressionCodecName.GZIP),
            breaker,
            fixture.pages,
            compressedDict,
            (long) PAGE_PAYLOAD_BYTES * PAGES
        );
        try {
            assertNotNull(reader.readDictionaryPage());
            assertEquals(dictPayload.length, breaker.getUsed());
            for (int p = 0; p < PAGES; p++) {
                assertNotNull(reader.readPage());
                assertEquals(
                    "live charge must be dictionary plus dest capacity (one page) after readPage() #" + (p + 1),
                    dictPayload.length + PAGE_PAYLOAD_BYTES,
                    breaker.getUsed()
                );
            }
            assertNull(reader.readPage());
            assertEquals(dictPayload.length + PAGE_PAYLOAD_BYTES, breaker.getUsed());
        } finally {
            reader.close();
            fixture.codecFactory.release();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testHeapDecompressionNeverAllocatesDirect() throws IOException {
        assertHeapNotDirect(buildZstdV2Pages());
        assertHeapNotDirect(buildGzipV1Pages());
    }

    public void testBreakerChargeTracksMaxSeenDestCapacity() throws IOException {
        int[] sizes = { 32 * 1024, 64 * 1024, 128 * 1024, 64 * 1024 };
        PagesFixture fixture = buildZstdV2Pages(sizes);
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        PrefetchedPageReader reader = new PrefetchedPageReader(
            fixture.codecFactory.getDecompressor(CompressionCodecName.ZSTD),
            breaker,
            fixture.pages,
            null,
            valueCount(sizes)
        );
        try {
            int maxSeen = 0;
            for (int size : sizes) {
                DataPageV2 page = (DataPageV2) reader.readPage();
                assertNotNull(page);
                maxSeen = Math.max(maxSeen, size);
                assertEquals("breaker tracks grow-only dest capacity, not the current page size", maxSeen, breaker.getUsed());
            }
        } finally {
            reader.close();
            fixture.codecFactory.release();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testReusableDestArrayIdentityAndSliceLength() throws IOException {
        // Equal-or-smaller pages reuse dest; grow allocates. Slice remaining is page size, not capacity.
        int[] sizes = { 32 * 1024, 64 * 1024, 128 * 1024, 64 * 1024 };
        PagesFixture fixture = buildZstdV2Pages(sizes);
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        PrefetchedPageReader reader = new PrefetchedPageReader(
            fixture.codecFactory.getDecompressor(CompressionCodecName.ZSTD),
            breaker,
            fixture.pages,
            null,
            valueCount(sizes)
        );
        try {
            byte[] dest = null;
            int maxSeen = 0;
            for (int i = 0; i < sizes.length; i++) {
                int size = sizes[i];
                DataPageV2 page = (DataPageV2) reader.readPage();
                assertNotNull(page);
                var buf = page.getData().toByteBuffer();
                assertTrue(buf.hasArray());
                assertEquals("slice remaining must be the page size, not dest capacity", size, buf.remaining());
                byte[] array = buf.array();
                assertArrayEquals("decompressed prefix must match payload", fixture.payloads().get(i), Arrays.copyOf(array, size));
                if (size > maxSeen) {
                    if (dest != null) {
                        assertNotSame("grow must allocate a new dest", dest, array);
                    }
                    dest = array;
                    maxSeen = size;
                } else {
                    assertSame("equal-or-smaller page must reuse dest", dest, array);
                }
                assertEquals(maxSeen, array.length);
                assertEquals(maxSeen, breaker.getUsed());
            }
        } finally {
            reader.close();
            fixture.codecFactory.release();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testUncompressedPagesDoNotChargeBreaker() throws IOException {
        PlainCompressionCodecFactory codecFactory = new PlainCompressionCodecFactory();
        try {
            byte[] payload = randomByteArrayOfLength(PAGE_PAYLOAD_BYTES);
            DataPageV1 v1 = new DataPageV1(
                BytesInput.from(payload),
                PAGE_PAYLOAD_BYTES / 4,
                payload.length,
                new IntStatistics(),
                Encoding.RLE,
                Encoding.RLE,
                Encoding.PLAIN
            );
            LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(1));
            try (
                PrefetchedPageReader reader = new PrefetchedPageReader(
                    codecFactory.getDecompressor(CompressionCodecName.UNCOMPRESSED),
                    breaker,
                    List.of(new PrefetchedPageReader.CompressedPage(v1, -1L)),
                    null,
                    PAGE_PAYLOAD_BYTES / 4
                )
            ) {
                assertNotNull(reader.readPage());
                assertEquals(0L, breaker.getUsed());
            }
            assertEquals(0L, breaker.getUsed());
        } finally {
            codecFactory.release();
        }
    }

    public void testBreakerTripDoesNotLeaveCharge() throws IOException {
        PagesFixture fixture = buildGzipV1Pages();
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofBytes(1));
        PrefetchedPageReader reader = new PrefetchedPageReader(
            fixture.codecFactory.getDecompressor(fixture.codec),
            breaker,
            fixture.pages,
            null,
            (long) PAGE_PAYLOAD_BYTES * PAGES
        );
        try {
            expectThrows(CircuitBreakingException.class, reader::readPage);
            assertEquals(0L, breaker.getUsed());
        } finally {
            reader.close();
            fixture.codecFactory.release();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testGrowBreakerTripKeepsPreviousDestCharge() throws IOException {
        // First page charges dest. A later grow that trips must leave that charge in place.
        int small = 32 * 1024;
        int large = 128 * 1024;
        PagesFixture fixture = buildZstdV2Pages(new int[] { small, large });
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofBytes(small + large - 1L));
        PrefetchedPageReader reader = new PrefetchedPageReader(
            fixture.codecFactory.getDecompressor(CompressionCodecName.ZSTD),
            breaker,
            fixture.pages,
            null,
            valueCount(new int[] { small, large })
        );
        try {
            assertNotNull(reader.readPage());
            assertEquals(small, breaker.getUsed());
            expectThrows(CircuitBreakingException.class, reader::readPage);
            assertEquals("failed grow must leave the previous dest charged", small, breaker.getUsed());
        } finally {
            reader.close();
            fixture.codecFactory.release();
        }
        assertEquals(0L, breaker.getUsed());
    }

    private void assertPerPageChargeAndZeroOnClose(PagesFixture fixture) throws IOException {
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        PrefetchedPageReader reader = new PrefetchedPageReader(
            fixture.codecFactory.getDecompressor(fixture.codec),
            breaker,
            fixture.pages,
            null,
            (long) PAGE_PAYLOAD_BYTES * PAGES
        );
        try {
            DataPage first = reader.readPage();
            assertNotNull(first);
            byte[] dest = destArray(first);
            assertEquals(PAGE_PAYLOAD_BYTES, breaker.getUsed());
            assertEquals(PAGE_PAYLOAD_BYTES, dest.length);

            for (int p = 1; p < PAGES; p++) {
                DataPage page = reader.readPage();
                assertNotNull(page);
                assertSame("equal-size pages must reuse dest after readPage() #" + (p + 1), dest, destArray(page));
                assertEquals(
                    "live decompressed charge must stay one page after readPage() #" + (p + 1),
                    PAGE_PAYLOAD_BYTES,
                    breaker.getUsed()
                );
            }
            assertNull(reader.readPage());
            assertEquals("last page charge stays live after the queue drains", PAGE_PAYLOAD_BYTES, breaker.getUsed());
        } finally {
            reader.close();
            fixture.codecFactory.release();
        }
        assertEquals(0L, breaker.getUsed());
    }

    private void assertHeapNotDirect(PagesFixture fixture) throws IOException {
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        PrefetchedPageReader reader = new PrefetchedPageReader(
            fixture.codecFactory.getDecompressor(fixture.codec),
            breaker,
            fixture.pages,
            null,
            (long) PAGE_PAYLOAD_BYTES * PAGES
        );
        try {
            if (fixture.codec == CompressionCodecName.ZSTD) {
                DataPageV2 page = (DataPageV2) reader.readPage();
                assertFalse(page.getData().toByteBuffer().isDirect());
            } else {
                DataPageV1 page = (DataPageV1) reader.readPage();
                assertFalse(page.getBytes().toByteBuffer().isDirect());
            }
        } finally {
            reader.close();
            fixture.codecFactory.release();
        }
    }

    private PagesFixture buildZstdV2Pages() throws IOException {
        return buildZstdV2Pages(filledSizes(PAGE_PAYLOAD_BYTES));
    }

    private PagesFixture buildZstdV2Pages(int[] payloadBytes) throws IOException {
        PlainCompressionCodecFactory codecFactory = new PlainCompressionCodecFactory();
        BytesInputCompressor compressor = codecFactory.getCompressor(CompressionCodecName.ZSTD);
        List<PrefetchedPageReader.CompressedPage> pages = new ArrayList<>(payloadBytes.length);
        List<byte[]> payloads = new ArrayList<>(payloadBytes.length);
        for (int payloadSize : payloadBytes) {
            byte[] data = randomByteArrayOfLength(payloadSize);
            payloads.add(data);
            byte[] compressedData = compressor.compress(BytesInput.from(data)).toByteArray();
            DataPageV2 v2 = new DataPageV2(
                payloadSize / 4,
                0,
                payloadSize / 4,
                BytesInput.empty(),
                BytesInput.empty(),
                Encoding.PLAIN,
                BytesInput.from(compressedData),
                payloadSize,
                new IntStatistics(),
                true
            );
            pages.add(new PrefetchedPageReader.CompressedPage(v2, -1L));
        }
        return new PagesFixture(codecFactory, CompressionCodecName.ZSTD, pages, payloads);
    }

    private PagesFixture buildGzipV1Pages() throws IOException {
        PlainCompressionCodecFactory codecFactory = new PlainCompressionCodecFactory();
        BytesInputCompressor compressor = codecFactory.getCompressor(CompressionCodecName.GZIP);
        List<PrefetchedPageReader.CompressedPage> pages = new ArrayList<>(PAGES);
        List<byte[]> payloads = new ArrayList<>(PAGES);
        for (int p = 0; p < PAGES; p++) {
            byte[] payload = randomByteArrayOfLength(PAGE_PAYLOAD_BYTES);
            payloads.add(payload);
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
        return new PagesFixture(codecFactory, CompressionCodecName.GZIP, pages, payloads);
    }

    private static int[] filledSizes(int size) {
        int[] sizes = new int[PAGES];
        Arrays.fill(sizes, size);
        return sizes;
    }

    private static long valueCount(int[] payloadBytes) {
        long values = 0;
        for (int size : payloadBytes) {
            values += size / 4;
        }
        return values;
    }

    private static byte[] destArray(DataPage page) throws IOException {
        var buf = page instanceof DataPageV2 v2 ? v2.getData().toByteBuffer() : ((DataPageV1) page).getBytes().toByteBuffer();
        assertTrue(buf.hasArray());
        return buf.array();
    }

    private record PagesFixture(
        PlainCompressionCodecFactory codecFactory,
        CompressionCodecName codec,
        List<PrefetchedPageReader.CompressedPage> pages,
        List<byte[]> payloads
    ) {}
}
