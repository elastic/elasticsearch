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
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ParquetDecodingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PrefetchedPageReaderTests extends ESTestCase {

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

    public void testReadsV1PageUncompressed() throws IOException {
        assertV1RoundTripDecompresses(CompressionCodecName.UNCOMPRESSED);
    }

    public void testReadsV1PageSnappy() throws IOException {
        assertV1RoundTripDecompresses(CompressionCodecName.SNAPPY);
    }

    public void testReadsV1PageGzip() throws IOException {
        assertV1RoundTripDecompresses(CompressionCodecName.GZIP);
    }

    public void testReadsV1PageZstd() throws IOException {
        assertV1RoundTripDecompresses(CompressionCodecName.ZSTD);
    }

    public void testReadsV1PageLz4Raw() throws IOException {
        assertV1RoundTripDecompresses(CompressionCodecName.LZ4_RAW);
    }

    public void testReadsV2PageUncompressed() throws IOException {
        assertV2RoundTripDecompressesDataOnly(CompressionCodecName.UNCOMPRESSED);
    }

    public void testReadsV2PageSnappy() throws IOException {
        assertV2RoundTripDecompressesDataOnly(CompressionCodecName.SNAPPY);
    }

    public void testReadsV2PageGzip() throws IOException {
        assertV2RoundTripDecompressesDataOnly(CompressionCodecName.GZIP);
    }

    public void testReadsV2PageZstd() throws IOException {
        assertV2RoundTripDecompressesDataOnly(CompressionCodecName.ZSTD);
    }

    public void testReadsV2PageLz4Raw() throws IOException {
        assertV2RoundTripDecompressesDataOnly(CompressionCodecName.LZ4_RAW);
    }

    public void testReadsV2PageWithIsCompressedFalse() throws IOException {
        byte[] rl = new byte[] { 1, 2, 3 };
        byte[] dl = new byte[] { 4, 5 };
        byte[] data = randomBytesOfLength(64);
        DataPageV2 v2 = new DataPageV2(
            10,
            2,
            8,
            BytesInput.from(rl),
            BytesInput.from(dl),
            Encoding.PLAIN,
            BytesInput.from(data),
            rl.length + dl.length + data.length,
            intStats(),
            false
        );
        try (
            PrefetchedPageReader reader = new PrefetchedPageReader(
                codecFactory.getDecompressor(CompressionCodecName.GZIP), // codec must not be invoked
                allocator,
                List.of(new PrefetchedPageReader.CompressedPage(v2, -1L)),
                null,
                8
            )
        ) {
            DataPage out = reader.readPage();
            assertThat(out, notNullValue());
            DataPageV2 outV2 = (DataPageV2) out;
            assertThat(outV2.isCompressed(), equalTo(false));
            assertThat(outV2.getData().toByteArray(), equalTo(data));
            assertThat(outV2.getRepetitionLevels().toByteArray(), equalTo(rl));
            assertThat(outV2.getDefinitionLevels().toByteArray(), equalTo(dl));
            assertThat(reader.readPage(), nullValue());
        }
    }

    public void testReadsV2PageWithEmptyDataSection() throws IOException {
        // Spark's writer stores all-null V2 pages with an empty data buffer rather than a
        // compressed zero-byte payload. Decompression libraries reject empty input, so the reader
        // must short-circuit. Verify for every compressed codec.
        for (CompressionCodecName codec : List.of(
            CompressionCodecName.SNAPPY,
            CompressionCodecName.GZIP,
            CompressionCodecName.ZSTD,
            CompressionCodecName.LZ4_RAW
        )) {
            byte[] rl = new byte[] { 1, 2, 3 };
            byte[] dl = new byte[] { 4, 5 };
            DataPageV2 v2 = new DataPageV2(
                10,
                10,
                10,
                BytesInput.from(rl),
                BytesInput.from(dl),
                Encoding.PLAIN,
                BytesInput.empty(),
                rl.length + dl.length,
                intStats(),
                true
            );
            try (
                PrefetchedPageReader reader = new PrefetchedPageReader(
                    codecFactory.getDecompressor(codec),
                    allocator,
                    List.of(new PrefetchedPageReader.CompressedPage(v2, -1L)),
                    null,
                    10
                )
            ) {
                DataPage out = reader.readPage();
                assertThat("failed for codec " + codec, out, notNullValue());
                DataPageV2 outV2 = (DataPageV2) out;
                assertThat(outV2.isCompressed(), equalTo(false));
                assertThat(outV2.getData().toByteArray(), equalTo(new byte[0]));
                assertThat(outV2.getRepetitionLevels().toByteArray(), equalTo(rl));
                assertThat(outV2.getDefinitionLevels().toByteArray(), equalTo(dl));
                assertThat(outV2.getRowCount(), equalTo(10));
                assertThat(outV2.getNullCount(), equalTo(10));
                assertThat(outV2.getValueCount(), equalTo(10));
                assertThat(reader.readPage(), nullValue());
            }
        }
    }

    public void testUncompressedV1PageWithDirectInputSkipsAllocAndCopy() throws IOException {
        // Regression coverage for elastic/esql-planning#804: when the codec is UNCOMPRESSED and
        // the page slice is already direct (the prefetched path), decompressV1 must return a view
        // over the input buffer rather than allocating a fresh direct buffer and memcopying into
        // it.
        byte[] payload = randomBytesOfLength(64);
        ByteBuffer direct = ByteBuffer.allocateDirect(payload.length);
        direct.put(payload).flip();
        DataPageV1 v1 = new DataPageV1(
            BytesInput.from(direct.duplicate()),
            10,
            payload.length,
            intStats(),
            Encoding.RLE,
            Encoding.RLE,
            Encoding.PLAIN
        );
        try (
            PrefetchedPageReader reader = new PrefetchedPageReader(
                codecFactory.getDecompressor(CompressionCodecName.UNCOMPRESSED),
                allocator,
                List.of(new PrefetchedPageReader.CompressedPage(v1, -1L)),
                null,
                10
            )
        ) {
            DataPageV1 out = (DataPageV1) reader.readPage();
            assertThat(out, notNullValue());
            assertThat(out.getValueCount(), equalTo(10));
            assertThat(out.getUncompressedSize(), equalTo(payload.length));
            assertThat(out.getValueEncoding(), equalTo(Encoding.PLAIN));
            assertThat(out.getRlEncoding(), equalTo(Encoding.RLE));
            assertThat(out.getDlEncoding(), equalTo(Encoding.RLE));
            ByteBuffer decompressedBuf = out.getBytes().toByteBuffer();
            assertTrue("Uncompressed V1 page must be backed by a direct buffer", decompressedBuf.isDirect());
            assertThat(out.getBytes().toByteArray(), equalTo(payload));
            // Mutating the underlying direct buffer must show through the returned BytesInput — i.e.,
            // the page reader handed back a view rather than a copy of the input. The buffer
            // duplicate's position/limit are independent of the original, so writing through the
            // original is safe.
            byte sentinel = (byte) (payload[0] ^ 0xFF);
            direct.put(0, sentinel);
            assertEquals("Returned BytesInput must alias the direct input slice, not a copy", sentinel, out.getBytes().toByteArray()[0]);
        }
    }

    public void testUncompressedV1PageWithDirectInputRejectsSizeMismatch() {
        byte[] payload = randomBytesOfLength(64);
        ByteBuffer direct = ByteBuffer.allocateDirect(payload.length);
        direct.put(payload).flip();
        int declaredSize = payload.length - 1;
        DataPageV1 v1 = new DataPageV1(
            BytesInput.from(direct.duplicate()),
            10,
            declaredSize,
            intStats(),
            Encoding.RLE,
            Encoding.RLE,
            Encoding.PLAIN
        );
        try (
            PrefetchedPageReader reader = new PrefetchedPageReader(
                codecFactory.getDecompressor(CompressionCodecName.UNCOMPRESSED),
                allocator,
                List.of(new PrefetchedPageReader.CompressedPage(v1, -1L)),
                null,
                10
            )
        ) {
            ParquetDecodingException e = expectThrows(ParquetDecodingException.class, reader::readPage);
            assertThat(e.getMessage(), equalTo("Uncompressed page size mismatch: input has 64 bytes but page header declares 63"));
        }
    }

    public void testUncompressedDictionaryPageWithDirectInputSkipsAllocAndCopy() throws IOException {
        // Same short-circuit, exercised through the dictionary-page path.
        byte[] payload = randomBytesOfLength(48);
        ByteBuffer direct = ByteBuffer.allocateDirect(payload.length);
        direct.put(payload).flip();
        DictionaryPage compressedDict = new DictionaryPage(BytesInput.from(direct.duplicate()), payload.length, 4, Encoding.PLAIN);
        try (
            PrefetchedPageReader reader = new PrefetchedPageReader(
                codecFactory.getDecompressor(CompressionCodecName.UNCOMPRESSED),
                allocator,
                List.of(),
                compressedDict,
                0
            )
        ) {
            DictionaryPage out = reader.readDictionaryPage();
            assertThat(out, notNullValue());
            ByteBuffer decompressedBuf = out.getBytes().toByteBuffer();
            assertTrue("Uncompressed dictionary page must be backed by a direct buffer", decompressedBuf.isDirect());
            byte sentinel = (byte) (payload[0] ^ 0xFF);
            direct.put(0, sentinel);
            assertEquals(
                "Returned BytesInput must alias the direct dictionary input slice, not a copy",
                sentinel,
                out.getBytes().toByteArray()[0]
            );
        }
    }

    public void testReadDictionaryPageDecompressesLazilyAndCaches() throws IOException {
        byte[] payload = randomBytesOfLength(48);
        BytesInputCompressor compressor = codecFactory.getCompressor(CompressionCodecName.SNAPPY);
        BytesInput compressed = compressor.compress(BytesInput.from(payload));
        DictionaryPage compressedDict = new DictionaryPage(BytesInput.from(compressed.toByteArray()), payload.length, 4, Encoding.PLAIN);
        try (
            PrefetchedPageReader reader = new PrefetchedPageReader(
                codecFactory.getDecompressor(CompressionCodecName.SNAPPY),
                allocator,
                List.of(),
                compressedDict,
                0
            )
        ) {
            DictionaryPage first = reader.readDictionaryPage();
            DictionaryPage second = reader.readDictionaryPage();
            assertSame("Dictionary page should be cached after first decompression", first, second);
            assertThat(first.getBytes().toByteArray(), equalTo(payload));
            assertThat(first.getDictionarySize(), equalTo(4));
            assertThat(first.getEncoding(), equalTo(Encoding.PLAIN));
        }
    }

    public void testReadDictionaryPageReturnsNullWhenAbsent() {
        try (
            PrefetchedPageReader reader = new PrefetchedPageReader(
                codecFactory.getDecompressor(CompressionCodecName.UNCOMPRESSED),
                allocator,
                List.of(),
                null,
                0
            )
        ) {
            assertNull(reader.readDictionaryPage());
        }
    }

    public void testReadPageReturnsNullWhenQueueEmpty() {
        try (
            PrefetchedPageReader reader = new PrefetchedPageReader(
                codecFactory.getDecompressor(CompressionCodecName.UNCOMPRESSED),
                allocator,
                List.of(),
                null,
                0
            )
        ) {
            assertNull(reader.readPage());
        }
    }

    public void testGetTotalValueCount() {
        try (
            PrefetchedPageReader reader = new PrefetchedPageReader(
                codecFactory.getDecompressor(CompressionCodecName.UNCOMPRESSED),
                allocator,
                List.of(),
                null,
                12345L
            )
        ) {
            assertEquals(12345L, reader.getTotalValueCount());
        }
    }

    public void testV1FirstRowIndexAndIndexRowCountPreserved() throws IOException {
        byte[] payload = randomBytesOfLength(32);
        BytesInputCompressor compressor = codecFactory.getCompressor(CompressionCodecName.UNCOMPRESSED);
        BytesInput compressed = compressor.compress(BytesInput.from(payload));
        DataPageV1 v1 = new DataPageV1(
            BytesInput.from(compressed.toByteArray()),
            5,
            payload.length,
            42L,
            5,
            intStats(),
            Encoding.RLE,
            Encoding.RLE,
            Encoding.PLAIN
        );
        try (
            PrefetchedPageReader reader = new PrefetchedPageReader(
                codecFactory.getDecompressor(CompressionCodecName.UNCOMPRESSED),
                allocator,
                List.of(new PrefetchedPageReader.CompressedPage(v1, 42L)),
                null,
                5
            )
        ) {
            DataPageV1 out = (DataPageV1) reader.readPage();
            assertThat(out, notNullValue());
            assertThat(out.getFirstRowIndex().orElseThrow(), equalTo(42L));
            assertThat(out.getIndexRowCount().orElseThrow(), equalTo(5));
            assertThat(out.getValueCount(), equalTo(5));
            assertThat(out.getBytes().toByteArray(), equalTo(payload));
        }
    }

    private void assertV1RoundTripDecompresses(CompressionCodecName codec) throws IOException {
        byte[] payload = randomBytesOfLength(64);
        BytesInputCompressor compressor = codecFactory.getCompressor(codec);
        BytesInput compressed = compressor.compress(BytesInput.from(payload));
        DataPageV1 v1 = new DataPageV1(
            BytesInput.from(compressed.toByteArray()),
            10,
            payload.length,
            intStats(),
            Encoding.RLE,
            Encoding.RLE,
            Encoding.PLAIN
        );
        BytesInputDecompressor decompressor = codecFactory.getDecompressor(codec);
        try (
            PrefetchedPageReader reader = new PrefetchedPageReader(
                decompressor,
                allocator,
                List.of(new PrefetchedPageReader.CompressedPage(v1, -1L)),
                null,
                10
            )
        ) {
            DataPage page = reader.readPage();
            assertThat(page, notNullValue());
            DataPageV1 outV1 = (DataPageV1) page;
            assertThat(outV1.getValueCount(), equalTo(10));
            assertThat(outV1.getUncompressedSize(), equalTo(payload.length));
            assertThat(outV1.getValueEncoding(), equalTo(Encoding.PLAIN));
            assertThat(outV1.getRlEncoding(), equalTo(Encoding.RLE));
            assertThat(outV1.getDlEncoding(), equalTo(Encoding.RLE));
            assertThat(outV1.getBytes().toByteArray(), equalTo(payload));
            ByteBuffer decompressedBuf = outV1.getBytes().toByteBuffer();
            assertTrue("decompressed V1 page must be backed by a direct buffer to avoid G1GC pinning", decompressedBuf.isDirect());
            assertNull(reader.readPage());
        }
    }

    private void assertV2RoundTripDecompressesDataOnly(CompressionCodecName codec) throws IOException {
        byte[] rl = new byte[] { 1, 2, 3, 4 };
        byte[] dl = new byte[] { 5, 6 };
        byte[] data = randomBytesOfLength(64);
        BytesInputCompressor compressor = codecFactory.getCompressor(codec);
        BytesInput compressedData = compressor.compress(BytesInput.from(data));
        byte[] compressedDataBytes = compressedData.toByteArray();
        DataPageV2 v2 = new DataPageV2(
            8,
            1,
            10,
            BytesInput.from(rl),
            BytesInput.from(dl),
            Encoding.PLAIN,
            BytesInput.from(compressedDataBytes),
            rl.length + dl.length + data.length,
            intStats(),
            true
        );
        BytesInputDecompressor decompressor = codecFactory.getDecompressor(codec);
        try (
            PrefetchedPageReader reader = new PrefetchedPageReader(
                decompressor,
                allocator,
                List.of(new PrefetchedPageReader.CompressedPage(v2, -1L)),
                null,
                10
            )
        ) {
            DataPage page = reader.readPage();
            assertThat(page, notNullValue());
            DataPageV2 outV2 = (DataPageV2) page;
            assertThat(outV2.isCompressed(), equalTo(false));
            assertThat(outV2.getRowCount(), equalTo(8));
            assertThat(outV2.getNullCount(), equalTo(1));
            assertThat(outV2.getValueCount(), equalTo(10));
            assertThat(outV2.getDataEncoding(), equalTo(Encoding.PLAIN));
            assertThat(outV2.getRepetitionLevels().toByteArray(), equalTo(rl));
            assertThat(outV2.getDefinitionLevels().toByteArray(), equalTo(dl));
            assertThat(outV2.getData().toByteArray(), equalTo(data));
            ByteBuffer decompressedBuf = outV2.getData().toByteBuffer();
            assertTrue("decompressed V2 data must be backed by a direct buffer to avoid G1GC pinning", decompressedBuf.isDirect());
            assertNull(reader.readPage());
        }
    }

    private static Statistics<?> intStats() {
        return new IntStatistics();
    }

    private byte[] randomBytesOfLength(int len) {
        byte[] bytes = new byte[len];
        random().nextBytes(bytes);
        return bytes;
    }
}
