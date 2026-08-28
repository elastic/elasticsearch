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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.SuppressForbidden;
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
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
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
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for {@link CompressionDelegatingFormatReader}.
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

    public void testFormatNameAndExtensionsDelegated() {
        FormatReader innerReader = new NoConfigFormatReader() {

            @Override
            public SourceMetadata metadata(StorageObject object) {
                return null;
            }

            @Override
            public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
                return emptyIterator();
            }

            @Override
            public String formatName() {
                return "csv";
            }

            @Override
            public List<String> fileExtensions() {
                return List.of(".csv", ".tsv");
            }

            @Override
            public RowPositionStrategy rowPositionStrategy() {
                return PassThroughRowPositionStrategy.INSTANCE;
            }

            @Override
            public void close() {}
        };
        DecompressionCodec codec = mockCodec();
        FormatReader delegating = new CompressionDelegatingFormatReader(innerReader, codec);

        assertEquals("csv", delegating.formatName());
        assertEquals(List.of(".csv", ".tsv"), delegating.fileExtensions());
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

    public void testUnwrapPreservedThroughWithConfig() {
        FormatReader inner = new CapturingFormatReader();
        DecompressionCodec codec = mockCodec();
        CompressionDelegatingFormatReader delegating = new CompressionDelegatingFormatReader(inner, codec);

        FormatReader configured = delegating.withConfig(Map.of());
        assertSame(delegating, configured);

        if (configured instanceof CompressionDelegatingFormatReader cdr) {
            assertSame(inner, cdr.unwrap());
            assertSame(codec, cdr.codec());
        }
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

    private static class CapturingFormatReader implements NoConfigFormatReader {
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
        public String formatName() {
            return "csv";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".csv", ".tsv");
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

    /**
     * The delegate forwards each wither explicitly, and the SPI defaults return the WRAPPER — so an unforwarded
     * wither is silent: the inner reader is never configured, nothing errors, and only warmth quietly disappears for
     * compressed files. That is exactly how the read-configuration stamp was lost for .csv.gz once. Pin the forward
     * rather than the incident.
     */
    public void testWithReadConfigReachesTheInnerReader() {
        ReadConfigRecordingFormatReader inner = new ReadConfigRecordingFormatReader();
        FormatReader delegating = new CompressionDelegatingFormatReader(inner, new BrotliDecompressionCodec());

        FormatReader configured = delegating.withReadConfig("config-A");

        assertEquals("the inner reader must receive the read configuration", "config-A", inner.lastReadConfig);
        assertThat(
            "and the result must stay wrapped, or the decompression is lost",
            configured,
            instanceOf(CompressionDelegatingFormatReader.class)
        );
        assertNotSame("a configured reader is a new instance, not the original wrapper", delegating, configured);
    }

    /** Records what the wrapper hands down, and returns a distinct instance so the wrapper must re-wrap. */
    private static class ReadConfigRecordingFormatReader implements NoConfigFormatReader {
        String lastReadConfig;

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public FormatReader withReadConfig(String readConfig) {
            ReadConfigRecordingFormatReader configured = new ReadConfigRecordingFormatReader();
            configured.lastReadConfig = readConfig;
            this.lastReadConfig = readConfig;
            return configured;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return new SimpleSourceMetadata(List.of(), "csv", object.path().toString());
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            throw new UnsupportedOperationException("not used by this test");
        }

        @Override
        public String formatName() {
            return "csv";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".csv");
        }

        @Override
        public void close() {}
    }

    /**
     * The class, not the instance. Every {@code with*} on the SPI defaults to returning the reader unchanged, and this
     * wrapper must override each one to hand the call down and re-wrap — otherwise the inner reader is never
     * configured and only warmth quietly disappears for compressed files. That has happened once already, for the
     * read-configuration stamp. A test pinning one wither would not stop the eighth from repeating it, so enumerate
     * them: every default method on {@link FormatReader} returning a {@link FormatReader} must be overridden here.
     */
    @SuppressForbidden(reason = "an OVERRIDE check needs declared methods; getMethod returns the inherited default")
    public void testEveryWitherIsForwardedByTheWrapper() {
        // withConfig is the one legitimate exception: its default is defined in terms of
        // withConfigTrackingConsumedKeys, which this wrapper DOES override, and the SPI states that implementations
        // must override the tracking variant rather than this one. Every other default stands alone.
        Set<String> defaultsDelegatingToAnOverride = Set.of("withConfig");

        List<String> unforwarded = new ArrayList<>();
        for (Method spiMethod : FormatReader.class.getMethods()) {
            if (spiMethod.isDefault() == false || spiMethod.getReturnType() != FormatReader.class) {
                continue;
            }
            if (defaultsDelegatingToAnOverride.contains(spiMethod.getName())) {
                continue;
            }
            try {
                CompressionDelegatingFormatReader.class.getDeclaredMethod(spiMethod.getName(), spiMethod.getParameterTypes());
            } catch (NoSuchMethodException absent) {
                unforwarded.add(spiMethod.getName());
            }
        }
        assertEquals(
            "the compression wrapper must forward every SPI wither and re-wrap the result; an unforwarded one is "
                + "silent — the inner reader is never configured and warmth disappears for compressed files only",
            List.of(),
            unforwarded
        );
    }
}
