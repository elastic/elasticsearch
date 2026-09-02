/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import net.jpountz.lz4.LZ4FrameOutputStream;

import com.github.luben.zstd.ZstdOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.datasource.brotli.BrotliDecompressionCodec;
import org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2DecompressionCodec;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec;
import org.elasticsearch.xpack.esql.datasource.lz4.Lz4DecompressionCodec;
import org.elasticsearch.xpack.esql.datasource.snappy.SnappyDecompressionCodec;
import org.elasticsearch.xpack.esql.datasource.zstd.ZstdDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for {@link CompressionDelegatingFormatReader} and {@link ResolvedFormat} compression wrapping.
 */
public class CompressionDelegatingFormatReaderTests extends ESTestCase {

    private static final byte[] CSV_CONTENT = "a:keyword,b:integer\nfoo,1\nbar,2".getBytes(StandardCharsets.UTF_8);

    // printf 'a:keyword,b:integer\nfoo,1\nbar,2' | brotli -1 | base64
    private static final String CSV_CONTENT_BROTLI_BASE64 = "Dw+AYTprZXl3b3JkLGI6aW50ZWdlcgpmb28sMQpiYXIsMgM=";

    public void testDelegatesMetadataAndReadGzip() throws IOException {
        assertDelegatesMetadataAndRead(gzip(CSV_CONTENT), "file:///data.csv.gz", new GzipDecompressionCodec());
    }

    public void testDelegatesMetadataAndReadZstd() throws IOException {
        assertDelegatesMetadataAndRead(zstd(CSV_CONTENT), "file:///data.csv.zst", new ZstdDecompressionCodec());
    }

    public void testDelegatesMetadataAndReadBzip2() throws IOException {
        assertDelegatesMetadataAndRead(
            bzip2(CSV_CONTENT),
            "file:///data.csv.bz2",
            new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE)
        );
    }

    public void testDelegatesMetadataAndReadLz4() throws IOException {
        assertDelegatesMetadataAndRead(lz4(CSV_CONTENT), "file:///data.csv.lz4", new Lz4DecompressionCodec());
    }

    public void testDelegatesMetadataAndReadSnappy() throws IOException {
        assertDelegatesMetadataAndRead(snappy(CSV_CONTENT), "file:///data.csv.snappy", new SnappyDecompressionCodec());
    }

    public void testDelegatesMetadataAndReadBrotli() throws IOException {
        byte[] brotliCompressed = Base64.getDecoder().decode(CSV_CONTENT_BROTLI_BASE64);
        BrotliDecompressionCodec codec = new BrotliDecompressionCodec();
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(brotliCompressed))) {
            assertArrayEquals("pre-compressed brotli blob is stale", CSV_CONTENT, decompressed.readAllBytes());
        }
        assertDelegatesMetadataAndRead(brotliCompressed, "file:///data.csv.br", codec);
    }

    private void assertDelegatesMetadataAndRead(byte[] compressed, String path, DecompressionCodec codec) throws IOException {
        StorageObject rawObject = new BytesStorageObject(compressed, StoragePath.of(path));

        FormatReader innerReader = new CapturingFormatReader();
        FormatReader delegating = new CompressionDelegatingFormatReader(innerReader, codec);

        SourceMetadata meta = delegating.metadata(rawObject);
        assertNotNull(meta);
        assertTrue(((CapturingFormatReader) innerReader).metadataCalled);

        try (CloseableIterator<Page> it = delegating.read(rawObject, List.of("a", "b"), 100)) {
            assertFalse(it.hasNext());
        }
        assertTrue(((CapturingFormatReader) innerReader).readCalled);
    }

    public void testFormatNameDelegated() {
        FormatReaderFactory innerFactory = new RecordingFormatReaderFactory();
        DecompressionCodec codec = mockCodec();
        ResolvedFormat resolved = new ResolvedFormat(innerFactory, codec);

        assertEquals("csv", resolved.formatName());
    }

    public void testNullInnerThrows() {
        DecompressionCodec codec = mockCodec();
        expectThrows(QlIllegalArgumentException.class, () -> new CompressionDelegatingFormatReader(null, codec));
    }

    public void testNullCodecThrows() {
        FormatReader inner = new CapturingFormatReader();
        expectThrows(QlIllegalArgumentException.class, () -> new CompressionDelegatingFormatReader(inner, null));
    }

    public void testUnwrapReturnsInnerReader() {
        FormatReader inner = new CapturingFormatReader();
        DecompressionCodec codec = mockCodec();
        CompressionDelegatingFormatReader delegating = new CompressionDelegatingFormatReader(inner, codec);

        assertSame(inner, delegating.unwrap());
        assertSame(codec, delegating.codec());
    }

    public void testCreateWrapsInnerReaderWhenCodecPresent() {
        RecordingFormatReaderFactory inner = new RecordingFormatReaderFactory();
        DecompressionCodec codec = mockCodec();
        ResolvedFormat resolved = new ResolvedFormat(inner, codec);

        FormatReader created = resolved.create(Settings.EMPTY, null, Map.of(), FormatReadContext.Binding.empty());
        assertThat(created, instanceOf(CompressionDelegatingFormatReader.class));
        CompressionDelegatingFormatReader wrapped = (CompressionDelegatingFormatReader) created;
        assertSame(codec, wrapped.codec());
        assertThat(wrapped.unwrap(), instanceOf(CloseCountingFormatReader.class));
    }

    private static byte[] gzip(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(input);
        }
        return baos.toByteArray();
    }

    private static byte[] zstd(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZstdOutputStream zstdOut = new ZstdOutputStream(baos)) {
            zstdOut.write(input);
        }
        return baos.toByteArray();
    }

    private static byte[] bzip2(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (BZip2CompressorOutputStream bz2Out = new BZip2CompressorOutputStream(baos)) {
            bz2Out.write(input);
        }
        return baos.toByteArray();
    }

    private static byte[] lz4(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (LZ4FrameOutputStream lz4Out = new LZ4FrameOutputStream(baos)) {
            lz4Out.write(input);
        }
        return baos.toByteArray();
    }

    private static byte[] snappy(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (SnappyFramedOutputStream snappyOut = new SnappyFramedOutputStream(baos)) {
            snappyOut.write(input);
        }
        return baos.toByteArray();
    }

    private static CloseableIterator<Page> emptyIterator() {
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Page next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {}
        };
    }

    private static DecompressionCodec mockCodec() {
        return new DecompressionCodec() {
            @Override
            public String name() {
                return "mock";
            }

            @Override
            public List<String> extensions() {
                return List.of(".gz");
            }

            @Override
            public InputStream decompress(InputStream raw) {
                return raw;
            }
        };
    }

    private static class CapturingFormatReader implements FormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        boolean metadataCalled;
        boolean readCalled;

        @Override
        public SourceMetadata metadata(StorageObject object) throws IOException {
            metadataCalled = true;
            try (InputStream stream = object.newStream()) {
                stream.readAllBytes();
            }
            return new SimpleSourceMetadata(List.of(), "csv", object.path().toString());
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            readCalled = true;
            try (InputStream stream = object.newStream()) {
                stream.readAllBytes();
            }
            return emptyIterator();
        }

        @Override
        public void close() {}
    }

    private static class BytesStorageObject implements StorageObject {
        private final byte[] data;
        private final StoragePath path;

        BytesStorageObject(byte[] data, StoragePath path) {
            this.data = data;
            this.path = path;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(data, (int) position, (int) length);
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }

    public void testReadConfigReachesTheInnerFactory() {
        RecordingFormatReaderFactory inner = new RecordingFormatReaderFactory();
        ResolvedFormat resolved = new ResolvedFormat(inner, new BrotliDecompressionCodec());

        FormatReader created = resolved.create(
            Settings.EMPTY,
            null,
            Map.of(),
            FormatReadContext.Binding.empty().withReadConfig("config-A")
        );

        assertEquals("the inner factory must receive the read configuration", "config-A", inner.lastReadConfig);
        assertThat(
            "and the result must stay wrapped, or the decompression is lost",
            created,
            instanceOf(CompressionDelegatingFormatReader.class)
        );
    }

    private static class RecordingFormatReaderFactory implements FormatReaderFactory {
        String lastReadConfig;

        @Override
        public String formatName() {
            return "csv";
        }

        @Override
        public FormatReader create(Settings settings, BlockFactory blockFactory) {
            return create(settings, blockFactory, null, FormatReadContext.Binding.empty());
        }

        @Override
        public FormatReader create(
            Settings settings,
            BlockFactory blockFactory,
            Map<String, Object> config,
            FormatReadContext.Binding binding
        ) {
            lastReadConfig = binding == null ? null : binding.readConfig();
            return new CloseCountingFormatReader();
        }
    }

    /**
     * The wrapper holds no resources of its own, so the release the {@link FormatReader} lifecycle contract drives
     * has to reach the reader that does. A wrapper that swallowed {@code close()} would make the whole lifecycle a
     * no-op for compressed files. Compressed files are exactly the ones whose reader is most likely to be
     * holding a decompressor.
     */
    public void testCloseReachesTheInnerReader() throws IOException {
        CloseCountingFormatReader inner = new CloseCountingFormatReader();
        CompressionDelegatingFormatReader wrapper = new CompressionDelegatingFormatReader(inner, new GzipDecompressionCodec());

        wrapper.close();

        assertEquals(1, inner.closeCount);
    }

    public void testEachCreateOwnsAndClosesItsInnerReader() throws IOException {
        ResolvedFormat resolved = new ResolvedFormat(new RecordingFormatReaderFactory(), new GzipDecompressionCodec());
        FormatReader firstRaw = resolved.create(Settings.EMPTY, null, null, FormatReadContext.Binding.empty());
        FormatReader secondRaw = resolved.create(Settings.EMPTY, null, null, FormatReadContext.Binding.empty());
        CompressionDelegatingFormatReader first = (CompressionDelegatingFormatReader) firstRaw;
        CompressionDelegatingFormatReader second = (CompressionDelegatingFormatReader) secondRaw;
        CloseCountingFormatReader firstInner = (CloseCountingFormatReader) first.unwrap();
        CloseCountingFormatReader secondInner = (CloseCountingFormatReader) second.unwrap();

        assertNotSame(first, second);
        assertNotSame(firstInner, secondInner);

        first.close();

        assertEquals(1, firstInner.closeCount);
        assertEquals(0, secondInner.closeCount);
    }

    private static class CloseCountingFormatReader implements FormatReader {
        int closeCount;

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            throw new UnsupportedOperationException("not read in these tests");
        }

        @Override
        public void close() {
            closeCount++;
        }
    }
}
