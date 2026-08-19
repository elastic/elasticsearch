/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public class ParquetRsFormatReaderTests extends ESTestCase {

    /**
     * Regression: when an Arrow schema is imported from native code via FFI, attribute nullability must reflect Arrow's
     * per-field {@code isNullable()} flag. Defaulting to non-nullable would mislead planner rules (e.g. {@code COALESCE}
     * simplification, {@code IS NULL}/{@code IS NOT NULL} rewriting, {@code FoldNull}) into dropping legitimate null
     * rows for nullable columns.
     */
    public void testArrowSchemaToAttributesPropagatesNullability() {
        Schema schema = new Schema(
            List.of(
                new Field("req_id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("opt_name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("req_score", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                new Field("opt_active", FieldType.nullable(new ArrowType.Bool()), null)
            )
        );

        List<Attribute> attributes = ParquetRsFormatReader.arrowSchemaToAttributes(schema);
        assertEquals(4, attributes.size());

        assertEquals("req_id", attributes.get(0).name());
        assertEquals(Nullability.FALSE, attributes.get(0).nullable());

        assertEquals("opt_name", attributes.get(1).name());
        assertEquals(Nullability.TRUE, attributes.get(1).nullable());

        assertEquals("req_score", attributes.get(2).name());
        assertEquals(Nullability.FALSE, attributes.get(2).nullable());

        assertEquals("opt_active", attributes.get(3).name());
        assertEquals(Nullability.TRUE, attributes.get(3).nullable());
    }

    /**
     * Early-close leak/safety regression for the native path. {@code ParquetRsBatchIterator} buffers one
     * look-ahead {@link Page} in {@code hasNext()} whose Blocks zero-copy wrap native Arrow buffers; on
     * {@code close()} the base {@code BufferingPageIterator} releases that page <b>before</b> the iterator's
     * {@code closeInternal()} frees the native reader handle. This drives a real native read, materializes a
     * page with {@code hasNext()}, then closes without consuming it — the page's Arrow buffers must be
     * released and the native {@code ParquetReaderState} freed exactly once (a reversed order would be a
     * use-after-free; a double free would crash the JVM). The breaker-byte release proof lives in the
     * format-agnostic {@code BufferingPageIteratorTests}; Arrow memory is accounted by the Arrow allocator,
     * not the ES request breaker, so this test asserts native-path correctness rather than breaker bytes.
     */
    public void testCloseAfterHasNextWithoutNextReleasesNativeReaderCleanly() throws Exception {
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        Path parquetPath = createTempDir().resolve("parquet_rs_early_close.parquet");
        writeMultiRowGroupParquet(parquetPath);

        ParquetRsFormatReader reader = new ParquetRsFormatReader(blockFactory);
        // Small batch over a 600-row, multi-row-group file: hasNext() materializes a page the close abandons.
        try (CloseableIterator<Page> iterator = reader.read(localFileStorageObject(parquetPath), List.of("id"), 64)) {
            assertTrue("native reader should produce at least one page", iterator.hasNext());
            // Abandon without next(): try-with-resources close() releases the buffered Arrow page, then frees the handle.
        }
    }

    /** Same native path, mid-stream: consume one page, materialize the next, then abandon — no crash, no double free. */
    public void testCloseMidStreamReleasesNativeReaderCleanly() throws Exception {
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        Path parquetPath = createTempDir().resolve("parquet_rs_early_close_mid.parquet");
        writeMultiRowGroupParquet(parquetPath);

        ParquetRsFormatReader reader = new ParquetRsFormatReader(blockFactory);
        try (CloseableIterator<Page> iterator = reader.read(localFileStorageObject(parquetPath), List.of("id"), 64)) {
            assertTrue(iterator.hasNext());
            iterator.next().releaseBlocks();
            assertTrue(iterator.hasNext()); // buffers another native page we never consume
        }
    }

    /**
     * After close, {@code hasNext()} must short-circuit to false rather than call {@code nextBatch()} on the
     * freed native handle. Early close (LIMIT/cancel) leaves {@code exhausted==false}, so without the
     * {@code isClosed()} guard a stray {@code hasNext()} would be a native use-after-free.
     */
    public void testHasNextAfterCloseIsFalseAndDoesNotTouchFreedHandle() throws Exception {
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        Path parquetPath = createTempDir().resolve("parquet_rs_hasnext_after_close.parquet");
        writeMultiRowGroupParquet(parquetPath);

        ParquetRsFormatReader reader = new ParquetRsFormatReader(blockFactory);
        CloseableIterator<Page> iterator = reader.read(localFileStorageObject(parquetPath), List.of("id"), 64);
        assertTrue(iterator.hasNext()); // exhausted stays false: we close early, mid-stream
        iterator.close();
        assertFalse("hasNext() after close must be false, not a nextBatch() on the freed handle", iterator.hasNext());
        assertFalse("a second hasNext() must remain false", iterator.hasNext());
    }

    /** Explicit double close must be idempotent on the native handle (no second {@code closeReader} → no double free). */
    public void testDoubleCloseIsIdempotentOnNativeHandle() throws Exception {
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        Path parquetPath = createTempDir().resolve("parquet_rs_double_close.parquet");
        writeMultiRowGroupParquet(parquetPath);

        ParquetRsFormatReader reader = new ParquetRsFormatReader(blockFactory);
        CloseableIterator<Page> iterator = reader.read(localFileStorageObject(parquetPath), List.of("id"), 64);
        assertTrue(iterator.hasNext());
        iterator.close();
        iterator.close(); // second close must be a no-op; a double native free would abort the JVM
    }

    /**
     * Builds a narrow-schema, wide-row Parquet file that spans several row groups so a small batch size
     * produces multiple native pages.
     */
    private static void writeMultiRowGroupParquet(Path path) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("payload")
            .named("early_close_schema");

        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        OutputFile outputFile = outputFileOverStream(bos);
        String pad = "x".repeat(200);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(12 * 1024L)
                .withPageSize(512)
                .build()
        ) {
            for (int i = 0; i < 600; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", (long) i);
                g.add("payload", pad);
                writer.write(g);
            }
        }
        Files.write(path, bos.toByteArray());
    }

    private static OutputFile outputFileOverStream(ByteArrayOutputStream stream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        stream.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        stream.write(b, off, len);
                        position += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
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

            @Override
            public String getPath() {
                return "memory://fixture.parquet";
            }
        };
    }

    private static StorageObject localFileStorageObject(Path path) {
        String uriString = path.toUri().toString();
        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                return Files.newInputStream(path);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                InputStream base = Files.newInputStream(path);
                long skipped = 0;
                while (skipped < position) {
                    long n = base.skip(position - skipped);
                    if (n <= 0) {
                        break;
                    }
                    skipped += n;
                }
                return new InputStream() {
                    private long remaining = length;

                    @Override
                    public int read() throws IOException {
                        if (remaining <= 0) {
                            return -1;
                        }
                        int v = base.read();
                        if (v >= 0) {
                            remaining--;
                        }
                        return v;
                    }

                    @Override
                    public int read(byte[] b, int off, int len) throws IOException {
                        if (remaining <= 0) {
                            return -1;
                        }
                        int toRead = (int) Math.min(len, remaining);
                        int n = base.read(b, off, toRead);
                        if (n > 0) {
                            remaining -= n;
                        }
                        return n;
                    }

                    @Override
                    public void close() throws IOException {
                        base.close();
                    }
                };
            }

            @Override
            public long length() throws IOException {
                return Files.size(path);
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return Files.exists(path);
            }

            @Override
            public StoragePath path() {
                return StoragePath.of(uriString);
            }
        };
    }
}
