/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalFailures;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * A self-contradicting or truncated Parquet page header is the caller's broken file, not a
 * server fault. These cases used to surface as {@link NullPointerException} or
 * {@link IllegalStateException} (HTTP 500).
 */
public class ParquetPageHeaderCorruptionTests extends ESTestCase {

    private static final byte THRIFT_DATA_PAGE_V2 = 0x06;
    private static final byte THRIFT_DICTIONARY_PAGE = 0x04;

    private final FooterByteCache footerByteCache = FooterByteCache.fromSettings(Settings.EMPTY);

    private BlockFactory blockFactory;
    private PlainCompressionCodecFactory codecFactory;

    @Before
    public void initBlockFactoryAndCodec() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
        codecFactory = new PlainCompressionCodecFactory();
    }

    @After
    public void releaseCodecFactory() {
        codecFactory.release();
    }

    public void testDeclaredDataPageV2WithV1HeaderIsRejected() throws IOException {
        assertHeaderTypeRejected(THRIFT_DATA_PAGE_V2, "DATA_PAGE_V2");
    }

    public void testDeclaredDictionaryPageWithDataHeaderIsRejected() throws IOException {
        assertHeaderTypeRejected(THRIFT_DICTIONARY_PAGE, "DICTIONARY_PAGE");
    }

    public void testCompressedPageSizePastChunkEndIsRejected() throws IOException {
        byte[] clean = writeIntFile(CompressionCodecName.UNCOMPRESSED);
        ColumnLayout layout = layout(clean);
        byte[] corrupt = withCompressedPageSize(clean, layout, layout.compressedPageSize + 1);
        RuntimeException thrown = expectThrows(RuntimeException.class, () -> buildSequential(corrupt));
        assertClient400(thrown);
        assertThat(thrown.getMessage(), containsString("id"));
    }

    public void testNegativeCompressedPageSizeIsRejected() throws IOException {
        byte[] clean = writeIntFile(CompressionCodecName.UNCOMPRESSED);
        ColumnLayout layout = layout(clean);
        byte[] corrupt = withCompressedPageSize(clean, layout, -65);
        IllegalArgumentException thrown = expectThrows(IllegalArgumentException.class, () -> buildSequential(corrupt));
        assertThat(thrown.getMessage(), containsString("column [id]"));
        assertThat(thrown.getMessage(), containsString("must not be negative"));
        assertClient400(thrown);
    }

    public void testRejectionNamesTheColumn() throws IOException {
        byte[] clean = writeIntFile(CompressionCodecName.UNCOMPRESSED);
        ColumnLayout layout = layout(clean);
        byte[] corrupt = withDeclaredPageType(clean, layout.dataPageOffset, THRIFT_DATA_PAGE_V2);
        IllegalArgumentException thrown = expectThrows(IllegalArgumentException.class, () -> buildSequential(corrupt));
        assertThat(thrown.getMessage(), containsString("column [id]"));
        assertThat(thrown.getMessage(), containsString("row group [0]"));
        assertClient400(thrown);
    }

    public void testCorruptLz4PayloadIsClient400() throws IOException {
        byte[] clean = writeIntFile(CompressionCodecName.LZ4_RAW);
        ColumnLayout layout = layout(clean);
        byte[] corrupt = corruptPayloadTail(clean, layout);
        RuntimeException thrown = expectThrows(RuntimeException.class, () -> readFirstBatch(corrupt));
        assertClient400(thrown);
    }

    private void assertHeaderTypeRejected(byte typeZigzag, String expectedTypeName) throws IOException {
        byte[] clean = writeIntFile(CompressionCodecName.UNCOMPRESSED);
        ColumnLayout layout = layout(clean);
        byte[] corrupt = withDeclaredPageType(clean, layout.dataPageOffset, typeZigzag);
        IllegalArgumentException thrown = expectThrows(IllegalArgumentException.class, () -> buildSequential(corrupt));
        assertThat(thrown, instanceOf(IllegalArgumentException.class));
        assertThat(thrown.getMessage(), containsString(expectedTypeName));
        assertThat(thrown.getMessage(), containsString("column [id]"));
        assertClient400(thrown);
    }

    private static void assertClient400(RuntimeException thrown) {
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(thrown)));
    }

    private void buildSequential(byte[] file) throws IOException {
        StorageObject storageObject = new InMemoryStorageObject(file);
        try (ParquetFileReader reader = openReader(file)) {
            BlockMetaData block = reader.getRowGroups().getFirst();
            MessageType schema = reader.getFileMetaData().getSchema();
            ColumnChunkPrefetcher.PrefetchedChunks prefetched = ColumnChunkPrefetcher.fetchSync(
                storageObject,
                block,
                Set.of("id"),
                blockFactory.breaker()
            );
            try (Releasable ignored = prefetched.release()) {
                PrefetchedRowGroupBuilder.build(
                    block,
                    0,
                    schema,
                    Set.of("id"),
                    null,
                    PreloadedRowGroupMetadata.empty(),
                    prefetched.chunks(),
                    codecFactory,
                    blockFactory.breaker()
                ).close();
            }
        }
    }

    private void readFirstBatch(byte[] file) throws IOException {
        StorageObject storageObject = new InMemoryStorageObject(file);
        try (ParquetFileReader reader = openReader(file)) {
            BlockMetaData block = reader.getRowGroups().getFirst();
            MessageType schema = reader.getFileMetaData().getSchema();
            ColumnChunkPrefetcher.PrefetchedChunks prefetched = ColumnChunkPrefetcher.fetchSync(
                storageObject,
                block,
                Set.of("id"),
                blockFactory.breaker()
            );
            try (
                Releasable ignored = prefetched.release();
                PrefetchedPageReadStore store = PrefetchedRowGroupBuilder.build(
                    block,
                    0,
                    schema,
                    Set.of("id"),
                    null,
                    PreloadedRowGroupMetadata.empty(),
                    prefetched.chunks(),
                    codecFactory,
                    blockFactory.breaker()
                )
            ) {
                ColumnDescriptor desc = schema.getColumns().getFirst();
                ColumnInfo info = new ColumnInfo(
                    desc,
                    desc.getPrimitiveType().getPrimitiveTypeName(),
                    DataType.INTEGER,
                    desc.getMaxDefinitionLevel(),
                    desc.getMaxRepetitionLevel(),
                    desc.getPrimitiveType().getLogicalTypeAnnotation()
                );
                PageColumnReader pageReader = new PageColumnReader(
                    store.getPageReader(desc),
                    desc,
                    info,
                    RowRanges.all(block.getRowCount())
                );
                pageReader.readBatch(16, blockFactory).close();
            }
        }
    }

    private ColumnLayout layout(byte[] file) throws IOException {
        try (ParquetFileReader reader = openReader(file)) {
            ColumnChunkMetaData column = reader.getRowGroups().getFirst().getColumns().getFirst();
            long dataPageOffset = column.getFirstDataPageOffset();
            int headerOff = Math.toIntExact(dataPageOffset);
            assertEquals("page header field 1 (type) i32", 0x15, file[headerOff] & 0xFF);
            assertEquals("DATA_PAGE", 0x00, file[headerOff + 1] & 0xFF);
            CompactField compressed = compressedPageSizeField(file, headerOff);
            return new ColumnLayout(dataPageOffset, compressed.offset, compressed.width, compressed.value);
        }
    }

    private static byte[] withDeclaredPageType(byte[] file, long dataPageOffset, byte typeZigzag) {
        byte[] copy = file.clone();
        copy[Math.toIntExact(dataPageOffset) + 1] = typeZigzag;
        return copy;
    }

    private static byte[] withCompressedPageSize(byte[] file, ColumnLayout layout, int newSize) {
        byte[] encoded = encodeUnsignedVarint(zigzagEncode(newSize));
        assumeTrue(
            "in-place compressed_page_size rewrite needs the same varint width, old="
                + layout.compressedVarintWidth
                + " new="
                + encoded.length,
            encoded.length == layout.compressedVarintWidth
        );
        byte[] copy = file.clone();
        System.arraycopy(encoded, 0, copy, layout.compressedVarintOffset, encoded.length);
        return copy;
    }

    private static byte[] corruptPayloadTail(byte[] file, ColumnLayout layout) throws IOException {
        int headerOff = Math.toIntExact(layout.dataPageOffset);
        int headerLen;
        try (InputStream in = new ByteArrayInputStream(file, headerOff, file.length - headerOff)) {
            int before = in.available();
            Util.readPageHeader(in);
            headerLen = before - in.available();
        }
        byte[] copy = file.clone();
        int start = headerOff + headerLen;
        int end = start + layout.compressedPageSize;
        assumeTrue("compressed payload must exist inside the file", end > start && end <= copy.length);
        for (int i = start; i < end; i++) {
            copy[i] ^= 0x5A;
        }
        return copy;
    }

    /**
     * Compact-protocol walk of a V1 data page header: field 1 type, field 2 uncompressed size,
     * field 3 compressed size. Stops at field 3.
     */
    private static CompactField compressedPageSizeField(byte[] file, int headerOff) {
        // Compact protocol encodes field ids as deltas from the previous id. After field 1
        // (type), fields 2 and 3 are each (delta=1, I32) → 0x15, then a zigzag varint.
        int pos = headerOff + 2;
        assertEquals("field 2 uncompressed_page_size", 0x15, file[pos] & 0xFF);
        pos++;
        pos = skipUnsignedVarint(file, pos);
        assertEquals("field 3 compressed_page_size", 0x15, file[pos] & 0xFF);
        pos++;
        int varintStart = pos;
        int[] cursor = new int[] { pos };
        int zigzag = readUnsignedVarint(file, cursor);
        return new CompactField(varintStart, cursor[0] - varintStart, zigzagDecode(zigzag));
    }

    private static int skipUnsignedVarint(byte[] buf, int pos) {
        int[] cursor = new int[] { pos };
        readUnsignedVarint(buf, cursor);
        return cursor[0];
    }

    private static int readUnsignedVarint(byte[] buf, int[] pos) {
        int result = 0;
        int shift = 0;
        while (true) {
            int b = buf[pos[0]++] & 0xFF;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift > 28) {
                throw new AssertionError("varint too long at " + pos[0]);
            }
        }
    }

    private static byte[] encodeUnsignedVarint(int value) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(5);
        int n = value;
        while ((n & ~0x7F) != 0) {
            out.write((n & 0x7F) | 0x80);
            n >>>= 7;
        }
        out.write(n);
        return out.toByteArray();
    }

    private static int zigzagEncode(int n) {
        return (n << 1) ^ (n >> 31);
    }

    private static int zigzagDecode(int n) {
        return (n >>> 1) ^ -(n & 1);
    }

    private byte[] writeIntFile(CompressionCodecName codec) throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("id").named("corrupt_page");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(outputStream))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withRowGroupSize(64L * 1024 * 1024)
                .withPageSize(1 << 20)
                .withDictionaryEncoding(false)
                .withWriterVersion(WriterVersion.PARQUET_1_0)
                .withCompressionCodec(codec)
                .build()
        ) {
            for (int i = 0; i < 64; i++) {
                writer.write(groups.newGroup().append("id", i));
            }
        }
        return outputStream.toByteArray();
    }

    private ParquetFileReader openReader(byte[] file) throws IOException {
        return ParquetFileReader.open(
            new ParquetStorageObjectAdapter(new InMemoryStorageObject(file), footerByteCache, blockFactory.breaker()),
            PlainParquetReadOptions.builder(codecFactory).build()
        );
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return wrap(outputStream);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return wrap(outputStream);
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

    private static PositionOutputStream wrap(ByteArrayOutputStream outputStream) {
        return new PositionOutputStream() {
            private long pos;

            @Override
            public long getPos() {
                return pos;
            }

            @Override
            public void write(int b) {
                outputStream.write(b);
                pos++;
            }

            @Override
            public void write(byte[] b, int off, int len) {
                outputStream.write(b, off, len);
                pos += len;
            }
        };
    }

    private record ColumnLayout(long dataPageOffset, int compressedVarintOffset, int compressedVarintWidth, int compressedPageSize) {}

    private record CompactField(int offset, int width, int value) {}

    private static final class InMemoryStorageObject extends AbstractTestStorageObject {
        private final byte[] data;

        InMemoryStorageObject(byte[] data) {
            this.data = data;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("memory://corrupt-page.parquet");
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(data, (int) position, (int) length);
        }
    }
}
