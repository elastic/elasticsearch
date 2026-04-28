/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class PrefetchedRowGroupBuilderParityTests extends ESTestCase {

    private static final int TOTAL_ROWS = 4096;

    private BlockFactory blockFactory;
    private PlainCompressionCodecFactory codecFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        codecFactory = new PlainCompressionCodecFactory();
    }

    @Override
    public void tearDown() throws Exception {
        codecFactory.release();
        super.tearDown();
    }

    public void testV1Uncompressed() throws IOException {
        assertParity(WriterVersion.PARQUET_1_0, CompressionCodecName.UNCOMPRESSED, true);
    }

    public void testV1Snappy() throws IOException {
        assertParity(WriterVersion.PARQUET_1_0, CompressionCodecName.SNAPPY, true);
    }

    public void testV1Gzip() throws IOException {
        assertParity(WriterVersion.PARQUET_1_0, CompressionCodecName.GZIP, true);
    }

    public void testV1Zstd() throws IOException {
        assertParity(WriterVersion.PARQUET_1_0, CompressionCodecName.ZSTD, true);
    }

    public void testV1Lz4Raw() throws IOException {
        assertParity(WriterVersion.PARQUET_1_0, CompressionCodecName.LZ4_RAW, true);
    }

    public void testV2Uncompressed() throws IOException {
        assertParity(WriterVersion.PARQUET_2_0, CompressionCodecName.UNCOMPRESSED, true);
    }

    public void testV2Snappy() throws IOException {
        assertParity(WriterVersion.PARQUET_2_0, CompressionCodecName.SNAPPY, true);
    }

    public void testV2Gzip() throws IOException {
        assertParity(WriterVersion.PARQUET_2_0, CompressionCodecName.GZIP, true);
    }

    public void testV2Zstd() throws IOException {
        assertParity(WriterVersion.PARQUET_2_0, CompressionCodecName.ZSTD, true);
    }

    public void testV2Lz4Raw() throws IOException {
        assertParity(WriterVersion.PARQUET_2_0, CompressionCodecName.LZ4_RAW, true);
    }

    public void testV1NoDictionary() throws IOException {
        assertParity(WriterVersion.PARQUET_1_0, CompressionCodecName.SNAPPY, false);
    }

    public void testV2NoDictionary() throws IOException {
        assertParity(WriterVersion.PARQUET_2_0, CompressionCodecName.SNAPPY, false);
    }

    public void testSequentialPathWithoutOffsetIndex() throws IOException {
        // Without an offset index, the builder must take the sequential path; data must still match.
        byte[] file = writeIntFile(WriterVersion.PARQUET_1_0, CompressionCodecName.UNCOMPRESSED, true, false);
        StorageObject storageObject = new InMemoryStorageObject(file);
        try (ParquetFileReader reader = openReader(file)) {
            BlockMetaData block = reader.getRowGroups().getFirst();
            MessageType schema = reader.getFileMetaData().getSchema();

            List<Integer> baseline = readBaseline(file);
            List<Integer> custom = readWithBuilder(reader, block, schema, storageObject, /* withOffsetIndex */ false);
            assertEquals(baseline, custom);
        }
    }

    private void assertParity(WriterVersion writerVersion, CompressionCodecName codec, boolean dictionary) throws IOException {
        byte[] file = writeIntFile(writerVersion, codec, dictionary, true);
        StorageObject storageObject = new InMemoryStorageObject(file);
        try (ParquetFileReader reader = openReader(file)) {
            assertEquals(1, reader.getRowGroups().size());
            BlockMetaData block = reader.getRowGroups().getFirst();
            MessageType schema = reader.getFileMetaData().getSchema();

            List<Integer> baseline = readBaseline(file);
            List<Integer> custom = readWithBuilder(reader, block, schema, storageObject, /* withOffsetIndex */ true);
            assertEquals(baseline, custom);
        }
    }

    private List<Integer> readWithBuilder(
        ParquetFileReader reader,
        BlockMetaData block,
        MessageType schema,
        StorageObject storageObject,
        boolean withOffsetIndex
    ) throws IOException {
        PreloadedRowGroupMetadata metadata = withOffsetIndex
            ? PreloadedRowGroupMetadata.preload(reader, storageObject)
            : PreloadedRowGroupMetadata.empty();
        Set<String> projected = Set.of("id");
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = withOffsetIndex
            ? prefetchChunks(storageObject, block, projected)
            : null;

        try (
            PageReadStore store = PrefetchedRowGroupBuilder.build(
                block,
                0,
                schema,
                projected,
                /* rowRanges */ null,
                metadata,
                chunks,
                storageObject,
                codecFactory
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
            PageColumnReader pcr = new PageColumnReader(store.getPageReader(desc), desc, info, RowRanges.all(block.getRowCount()));
            List<Integer> values = new ArrayList<>(TOTAL_ROWS);
            int remaining = (int) block.getRowCount();
            while (remaining > 0) {
                int batch = Math.min(1024, remaining);
                Block dataBlock = pcr.readBatch(batch, blockFactory);
                IntBlock intBlock = (IntBlock) dataBlock;
                for (int i = 0; i < intBlock.getPositionCount(); i++) {
                    values.add(intBlock.getInt(i));
                }
                remaining -= intBlock.getPositionCount();
                dataBlock.close();
            }
            return values;
        }
    }

    private List<Integer> readBaseline(byte[] file) throws IOException {
        try (ParquetFileReader reader = openReader(file)) {
            BlockMetaData block = reader.getRowGroups().getFirst();
            MessageType schema = reader.getFileMetaData().getSchema();
            ColumnDescriptor desc = schema.getColumns().getFirst();
            ColumnInfo info = new ColumnInfo(
                desc,
                desc.getPrimitiveType().getPrimitiveTypeName(),
                DataType.INTEGER,
                desc.getMaxDefinitionLevel(),
                desc.getMaxRepetitionLevel(),
                desc.getPrimitiveType().getLogicalTypeAnnotation()
            );
            PageReadStore store = reader.readNextRowGroup();
            assertNotNull(store);
            PageColumnReader pcr = new PageColumnReader(store.getPageReader(desc), desc, info, RowRanges.all(block.getRowCount()));
            List<Integer> values = new ArrayList<>(TOTAL_ROWS);
            int remaining = (int) block.getRowCount();
            while (remaining > 0) {
                int batch = Math.min(1024, remaining);
                Block dataBlock = pcr.readBatch(batch, blockFactory);
                IntBlock intBlock = (IntBlock) dataBlock;
                for (int i = 0; i < intBlock.getPositionCount(); i++) {
                    values.add(intBlock.getInt(i));
                }
                remaining -= intBlock.getPositionCount();
                dataBlock.close();
            }
            return values;
        }
    }

    private NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> prefetchChunks(
        StorageObject storageObject,
        BlockMetaData block,
        Set<String> projected
    ) {
        CompletableFuture<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> future = ColumnChunkPrefetcher.prefetch(
            storageObject,
            block,
            projected
        );
        return future.join();
    }

    private ParquetFileReader openReader(byte[] file) throws IOException {
        return ParquetFileReader.open(
            new ParquetStorageObjectAdapter(new InMemoryStorageObject(file)),
            PlainParquetReadOptions.builder(codecFactory).build()
        );
    }

    /**
     * Writes an in-memory Parquet file with sorted INT32 ids, configurable row count and
     * page boundaries to exercise multi-page row groups.
     *
     * <p>{@code dictionary=true} keeps dictionary encoding on (default for INT32 with limited
     * cardinality); {@code dictionary=false} forces plain encoding by writing each value once.
     */
    private byte[] writeIntFile(WriterVersion writerVersion, CompressionCodecName codec, boolean dictionary, boolean ignoredOffsetIndex)
        throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("id").named("parity_test");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withRowGroupSize(64L * 1024 * 1024)
                .withPageSize(512)
                .withDictionaryEncoding(dictionary)
                .withWriterVersion(writerVersion)
                .withCompressionCodec(codec)
                .build()
        ) {
            // dictionary=true uses cyclic ids to keep cardinality small (forces dict encoding);
            // dictionary=false uses unique ids so the writer falls back to plain encoding.
            for (int i = 0; i < TOTAL_ROWS; i++) {
                int id = dictionary ? (i % 32) : i;
                writer.write(factory.newGroup().append("id", id));
            }
        }
        return outputStream.toByteArray();
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
            private long pos = 0;

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

    private static final class InMemoryStorageObject implements StorageObject {
        private final byte[] data;

        InMemoryStorageObject(byte[] data) {
            this.data = data;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("memory://parity-test.parquet");
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
