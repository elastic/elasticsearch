/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Stage 1 held-bytes cap: LIMIT clips I/O and releases compressed buffers when the
 * row budget is exhausted, without waiting for iterator close.
 */
public class ParquetHeldBytesCapTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
    }

    public void testPrefetchDepthCeilingInvertsFatRowGroups() {
        long cap = OptimizedParquetColumnIterator.DEFAULT_HELD_BYTES_CAP;
        assertThat(OptimizedParquetColumnIterator.computePrefetchDepthCeiling(cap * 2, cap), equalTo(1));
        assertThat(OptimizedParquetColumnIterator.computePrefetchDepthCeiling(cap / 2 + 1, cap), equalTo(1));
        assertThat(OptimizedParquetColumnIterator.computePrefetchDepthCeiling(1024, cap), equalTo(8));
        assertThat(OptimizedParquetColumnIterator.computePrefetchDepthCeiling(0, cap), equalTo(8));
    }

    public void testRangeReadContextDefaultsToNoLimit() {
        RangeReadContext ctx = new RangeReadContext(List.of("id"), 10, 0, 100, List.of(), ErrorPolicy.STRICT);
        assertThat(ctx.rowLimit(), equalTo(FormatReader.NO_LIMIT));
    }

    public void testReadRangeHonorsRowLimit() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(2000, 2048);
        RecordingStorageObject storage = new RecordingStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        try (
            CloseableIterator<Page> iter = reader.readRange(
                storage,
                new RangeReadContext(List.of("id"), 64, 0, parquetData.length, List.of(), ErrorPolicy.STRICT, null, 1)
            )
        ) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            assertThat(page.getPositionCount(), equalTo(1));
            page.releaseBlocks();
            assertFalse(iter.hasNext());
        }
    }

    public void testLimitDoesNotPrefetchLaterRowGroups() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(4000, 2048);
        RecordingStorageObject fullScan = new RecordingStorageObject(parquetData);
        try (
            CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory).read(fullScan, FormatReadContext.of(List.of("id"), 256))
        ) {
            while (iter.hasNext()) {
                iter.next().releaseBlocks();
            }
        }
        long fullDataGets = dataGetCount(fullScan, parquetData.length);
        assertThat("full scan should touch more than one row group's data", fullDataGets, greaterThan(1L));

        RecordingStorageObject limited = new RecordingStorageObject(parquetData);
        try (
            CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory).read(
                limited,
                FormatReadContext.builder().projectedColumns(List.of("id")).batchSize(256).rowLimit(1).build()
            )
        ) {
            assertTrue(iter.hasNext());
            iter.next().releaseBlocks();
            assertFalse(iter.hasNext());
        }
        long limitDataGets = dataGetCount(limited, parquetData.length);
        assertThat(limitDataGets, greaterThan(0L));
        assertThat(limitDataGets, lessThanOrEqualTo(fullDataGets - 1));
    }

    public void testLimitReleasesPrefetchBuffersBeforeClose() throws Exception {
        byte[] parquetData = createMultiRowGroupFile(3000, 2048);
        RecordingStorageObject storage = new RecordingStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        CloseableIterator<Page> iter = reader.read(
            storage,
            FormatReadContext.builder().projectedColumns(List.of("id")).batchSize(64).rowLimit(1).build()
        );
        try {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            page.releaseBlocks();
            assertFalse(iter.hasNext());
            assertThat("prefetch buffers must drop when LIMIT is exhausted, before close()", storage.liveAsyncBytes.get(), equalTo(0));
        } finally {
            iter.close();
        }
        assertThat(storage.liveAsyncBytes.get(), equalTo(0));
    }

    public void testLimitFirstWindowMissesTallChunkTail() throws Exception {
        byte[] parquetData = createTallFile(800, 64);
        assertThat(parquetData.length, greaterThan(16 * 1024));
        long cap = 16 * 1024;
        RecordingStorageObject storage = new RecordingStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory).withHeldBytesCap(cap);
        try (
            CloseableIterator<Page> iter = reader.read(
                storage,
                FormatReadContext.builder().projectedColumns(List.of("payload")).batchSize(32).rowLimit(1).build()
            )
        ) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            assertThat(page.getPositionCount(), equalTo(1));
            page.releaseBlocks();
            assertFalse(iter.hasNext());
        }

        long dataBytes = 0;
        for (long[] get : storage.gets) {
            if (isLikelyFooterGet(get[0], get[1], parquetData.length)) {
                continue;
            }
            assertThat("column-chunk GET exceeded held-bytes cap", get[1], lessThanOrEqualTo(cap));
            dataBytes += get[1];
        }
        assertThat(dataBytes, greaterThan(0L));
        assertThat("first-window GETs must not download the whole tall file", dataBytes, lessThanOrEqualTo(parquetData.length / 2L));
        assertThat(parquetData.length, greaterThan((int) cap * 2));
    }

    public void testLimitFirstRowMatchesFullScan() throws Exception {
        byte[] parquetData = createTallFile(80, 32);
        long fullFirst;
        try (
            CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory).read(
                new RecordingStorageObject(parquetData),
                FormatReadContext.of(List.of("id", "payload"), 64)
            )
        ) {
            Page page = iter.next();
            fullFirst = ((LongBlock) page.getBlock(0)).getLong(0);
            page.releaseBlocks();
        }
        long limitFirst;
        try (
            CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory).read(
                new RecordingStorageObject(parquetData),
                FormatReadContext.builder().projectedColumns(List.of("id", "payload")).batchSize(64).rowLimit(1).build()
            )
        ) {
            Page page = iter.next();
            limitFirst = ((LongBlock) page.getBlock(0)).getLong(0);
            page.releaseBlocks();
        }
        assertThat(limitFirst, equalTo(fullFirst));
    }

    private static long dataGetCount(RecordingStorageObject storage, int fileLength) {
        long count = 0;
        for (long[] get : storage.gets) {
            if (isLikelyFooterGet(get[0], get[1], fileLength) == false) {
                count++;
            }
        }
        return count;
    }

    private static boolean isLikelyFooterGet(long offset, long length, int fileLength) {
        return offset + length >= fileLength;
    }

    private byte[] createMultiRowGroupFile(int rowCount, int rowGroupSize) throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(outputStream))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(256)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", (long) i);
                writer.write(g);
            }
        }
        return outputStream.toByteArray();
    }

    private byte[] createTallFile(int rowCount, int payloadBytes) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("payload")
            .named("test_schema");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        String payload = "x".repeat(payloadBytes);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(outputStream))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(false)
                .withRowGroupSize(8 * 1024 * 1024L)
                .withPageSize(1024)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", (long) i);
                g.add("payload", i + payload);
                writer.write(g);
            }
        }
        return outputStream.toByteArray();
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    @Override
                    public long getPos() {
                        return outputStream.size();
                    }

                    @Override
                    public void write(int b) {
                        outputStream.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        outputStream.write(b, off, len);
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
        };
    }

    private static final class RecordingStorageObject implements StorageObject {
        private final byte[] data;
        final List<long[]> gets = new ArrayList<>();
        final AtomicInteger liveAsyncBytes = new AtomicInteger();

        RecordingStorageObject(byte[] data) {
            this.data = data;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
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
            return StoragePath.of("memory://held-bytes-cap.parquet");
        }

        @Override
        public boolean supportsNativeAsync() {
            return true;
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            gets.add(new long[] { position, length });
            executor.execute(() -> {
                try {
                    int pos = (int) position;
                    int len = (int) Math.min(length, data.length - position);
                    DirectReadBuffer allocated = factory.allocate(len);
                    ByteBuffer buffer = allocated.buffer();
                    buffer.put(data, pos, len);
                    buffer.flip();
                    liveAsyncBytes.addAndGet(len);
                    listener.onResponse(new DirectReadBuffer(buffer, () -> {
                        liveAsyncBytes.addAndGet(-len);
                        allocated.close();
                    }));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }
}
