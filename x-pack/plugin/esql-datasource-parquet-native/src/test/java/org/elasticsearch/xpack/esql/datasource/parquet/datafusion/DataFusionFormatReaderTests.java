/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.datafusion;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link DataFusionFormatReader} that verify end-to-end reading of Parquet files
 * via the native DataFusion library.
 * <p>
 * Uses a pre-generated Parquet test file created by {@link #createTestParquetFile(Path)}.
 * The native library must be built before running these tests.
 */
public class DataFusionFormatReaderTests extends ESTestCase {

    private BlockFactory blockFactory;
    private Path tempDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        tempDir = createTempDir();

        // Load native library directly from build output
        String nativeLibPath = System.getProperty("tests.native.lib.path");
        if (nativeLibPath != null) {
            System.load(nativeLibPath);
        }
    }

    public void testFormatName() {
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);
        assertEquals("parquet", reader.formatName());
    }

    public void testFileExtensions() {
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);
        List<String> extensions = reader.fileExtensions();
        assertTrue(extensions.contains(".parquet"));
    }

    public void testReadSchema() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        List<Attribute> attributes = metadata.schema();

        assertEquals(3, attributes.size());
        assertEquals("id", attributes.get(0).name());
        assertEquals("name", attributes.get(1).name());
        assertEquals("age", attributes.get(2).name());
    }

    public void testReadData() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        FormatReadContext context = FormatReadContext.of(null, 1024);
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = reader.read(storageObject, context)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }

        try {
            int totalRows = 0;
            for (Page page : pages) {
                totalRows += page.getPositionCount();
            }
            assertEquals(3, totalRows);

            Page firstPage = pages.get(0);
            assertEquals(3, firstPage.getBlockCount());

            LongBlock idBlock = (LongBlock) firstPage.getBlock(0);
            BytesRefBlock nameBlock = (BytesRefBlock) firstPage.getBlock(1);
            IntBlock ageBlock = (IntBlock) firstPage.getBlock(2);

            assertEquals(1L, idBlock.getLong(0));
            assertEquals(2L, idBlock.getLong(1));
            assertEquals(3L, idBlock.getLong(2));

            assertEquals(new BytesRef("Alice"), nameBlock.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("Bob"), nameBlock.getBytesRef(1, new BytesRef()));
            assertEquals(new BytesRef("Charlie"), nameBlock.getBytesRef(2, new BytesRef()));

            assertEquals(30, ageBlock.getInt(0));
            assertEquals(25, ageBlock.getInt(1));
            assertEquals(35, ageBlock.getInt(2));
        } finally {
            releasePages(pages);
        }
    }

    public void testReadWithProjection() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        FormatReadContext context = FormatReadContext.of(List.of("name", "age"), 1024);
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = reader.read(storageObject, context)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }

        try {
            Page firstPage = pages.get(0);
            assertEquals(2, firstPage.getBlockCount());
        } finally {
            releasePages(pages);
        }
    }

    public void testReadWithLimit() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        FormatReadContext context = FormatReadContext.of(null, 1024).withRowLimit(2);
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = reader.read(storageObject, context)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }

        try {
            int totalRows = 0;
            for (Page page : pages) {
                totalRows += page.getPositionCount();
            }
            assertEquals(2, totalRows);
        } finally {
            releasePages(pages);
        }
    }

    /**
     * Creates a simple 3-row Parquet file with columns: id (INT64), name (UTF8), age (INT32).
     * Uses parquet-mr (test dependency only) to write the file.
     */
    private static Path createTestParquetFile(Path dir) throws IOException {
        Path file = dir.resolve("test.parquet");
        // Write a minimal Parquet file using the Apache Parquet Java writer.
        // This dependency is only used in tests; the runtime reader is purely native.
        org.apache.parquet.schema.MessageType schema = org.apache.parquet.schema.Types.buildMessage()
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
            .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
            .named("name")
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .named("test_schema");

        org.apache.parquet.io.OutputFile outputFile = new org.apache.parquet.io.OutputFile() {
            @Override
            public org.apache.parquet.io.PositionOutputStream create(long blockSizeHint) throws IOException {
                java.io.OutputStream os = Files.newOutputStream(file);
                return new org.apache.parquet.io.PositionOutputStream() {
                    long pos = 0;

                    @Override
                    public long getPos() {
                        return pos;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        os.write(b);
                        pos++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        os.write(b, off, len);
                        pos += len;
                    }

                    @Override
                    public void flush() throws IOException {
                        os.flush();
                    }

                    @Override
                    public void close() throws IOException {
                        os.close();
                    }
                };
            }

            @Override
            public org.apache.parquet.io.PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };

        org.apache.parquet.example.data.simple.SimpleGroupFactory factory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(
            schema
        );

        try (
            org.apache.parquet.hadoop.ParquetWriter<org.apache.parquet.example.data.Group> writer =
                org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(outputFile)
                    .withType(schema)
                    .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED)
                    .build()
        ) {
            org.apache.parquet.example.data.Group g1 = factory.newGroup();
            g1.add("id", 1L);
            g1.add("name", "Alice");
            g1.add("age", 30);
            writer.write(g1);

            org.apache.parquet.example.data.Group g2 = factory.newGroup();
            g2.add("id", 2L);
            g2.add("name", "Bob");
            g2.add("age", 25);
            writer.write(g2);

            org.apache.parquet.example.data.Group g3 = factory.newGroup();
            g3.add("id", 3L);
            g3.add("name", "Charlie");
            g3.add("age", 35);
            writer.write(g3);
        }

        return file;
    }

    private static void releasePages(List<Page> pages) {
        for (Page page : pages) {
            page.releaseBlocks();
        }
    }

    private static StorageObject createFileStorageObject(Path path) {
        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                return Files.newInputStream(path);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                InputStream is = Files.newInputStream(path);
                is.skipNBytes(position);
                return is;
            }

            @Override
            public long length() throws IOException {
                return Files.size(path);
            }

            @Override
            public Instant lastModified() {
                return null;
            }

            @Override
            public boolean exists() throws IOException {
                return Files.exists(path);
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("file://" + path.toAbsolutePath());
            }
        };
    }
}
