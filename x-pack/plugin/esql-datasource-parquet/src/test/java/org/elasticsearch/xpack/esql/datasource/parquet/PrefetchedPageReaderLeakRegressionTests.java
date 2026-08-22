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
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Regression for decompress-buffer accounting on the parquet page-read path.
 *
 * <p>Loops {@link PrefetchedPageReader#readPage} over a few hundred small zstd-compressed pages
 * and asserts the request breaker returns to zero after every read-close cycle. Heap codecs
 * allocate {@code byte[]}s that GC owns; this test does not claim JVM-wide direct RSS is flat
 * (prefetch I/O still uses native buffers until a follow-up).
 */
public class PrefetchedPageReaderLeakRegressionTests extends ESTestCase {

    private static final int ITERATIONS = 500;
    private static final int PAGES_PER_ITERATION = 50;
    private static final int PAGE_PAYLOAD_BYTES = 64 * 1024;
    private static final int CONCURRENT_READERS = 4;
    private static final int CONCURRENT_ITERS_PER_READER = 50;

    private PlainCompressionCodecFactory codecFactory;

    @Before
    public void initCodec() {
        codecFactory = new PlainCompressionCodecFactory();
    }

    @After
    public void releaseCodecFactory() {
        codecFactory.release();
    }

    public void testRepeatedZstdDecompressionReleasesBreakerCharge() throws IOException {
        ZstdPageFixture fixture = compressedZstdPages();
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofGb(1));

        for (int i = 0; i < ITERATIONS; i++) {
            try (
                PrefetchedPageReader reader = new PrefetchedPageReader(
                    codecFactory.getDecompressor(CompressionCodecName.ZSTD),
                    breaker,
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
            assertEquals("breaker must return to zero after cycle " + i, 0L, breaker.getUsed());
        }
    }

    /**
     * Concurrent zstd decompress loops must not leave breaker charge behind. Each reader uses
     * its own breaker so threads cannot race on {@code used}.
     */
    public void testBreakerChargeStableUnderConcurrentReads() throws Exception {
        ZstdPageFixture fixture = compressedZstdPages();

        startInParallel(CONCURRENT_READERS, i -> {
            LimitedBreaker breaker = new LimitedBreaker("test-" + i, ByteSizeValue.ofGb(1));
            try {
                for (int iter = 0; iter < CONCURRENT_ITERS_PER_READER; iter++) {
                    try (
                        PrefetchedPageReader reader = new PrefetchedPageReader(
                            codecFactory.getDecompressor(CompressionCodecName.ZSTD),
                            breaker,
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
                    assertEquals(0L, breaker.getUsed());
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
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
        DataPageV1 v1 = (DataPageV1) page;
        byte[] sink = v1.getBytes().toByteArray();
        if (sink.length < 0) {
            throw new AssertionError(); // anti-DCE guard; JIT cannot fold sink.length < 0 to false
        }
    }
}
