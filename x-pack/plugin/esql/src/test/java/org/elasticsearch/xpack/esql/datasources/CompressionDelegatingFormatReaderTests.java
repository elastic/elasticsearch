/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.GZIPOutputStream;

/**
 * Unit tests for {@link CompressionDelegatingFormatReader}.
 */
public class CompressionDelegatingFormatReaderTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = new BlockFactory(new NoopCircuitBreaker("test"), BigArrays.NON_RECYCLING_INSTANCE);
    }

    public void testDelegatesMetadataAndRead() throws IOException {
        byte[] csvContent = "a:keyword,b:integer\nfoo,1\nbar,2".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = gzip(csvContent);

        StorageObject rawObject = new BytesStorageObject(compressed, StoragePath.of("file:///data.csv.gz"));
        DecompressionCodec codec = new org.elasticsearch.xpack.esql.datasource.gzip.GzipDecompressionCodec();

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
        FormatReader innerReader = new FormatReader() {
            @Override
            public SourceMetadata metadata(StorageObject object) {
                return null;
            }

            @Override
            public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) {
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

    private static byte[] gzip(byte[] input) throws IOException {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(input);
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
        boolean metadataCalled;
        boolean readCalled;

        @Override
        public SourceMetadata metadata(StorageObject object) throws IOException {
            metadataCalled = true;
            try (InputStream stream = object.newStream()) {
                stream.readAllBytes();
            }
            return new SimpleSourceMetadata(List.of(), "csv", "file:///data.csv.gz");
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
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
}
