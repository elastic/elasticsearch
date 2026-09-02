/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ParquetProperties;
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
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.CountingBreaker;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Byte-storage sizing behaviour of the Parquet reader's {@code BytesRef} (keyword) emission, covering
 * its three distinct paths. The reader's column {@code BytesRefArray} is allocated from
 * {@code blockFactory.bigArrays()}, while all Parquet decode/scratch buffers go through
 * {@code blockFactory.breaker()} (see {@code ParquetFormatReader} / {@code OptimizedParquetColumnIterator}).
 * By putting a counting breaker on {@code bigArrays} and a no-op breaker on the factory, the counter
 * observes only the keyword column's byte-buffer reservations.
 * <p>
 * Equal-length, distinct values are used so the column avoids constant-block detection and the
 * fixed-length offset elision in {@code BytesRefArray} keeps offsets from allocating, leaving only
 * byte-buffer reservations visible.
 * <ul>
 *   <li>{@link #testDictionaryColumnEmitsOrdinalBlock()} - dictionary-encoded columns take the ordinal
 *       fast path ({@code OrdinalBytesRefBlock}); no per-page builder. </li>
 *   <li>{@link #testBaselineReaderSizesByteStorageUpFront()} - the baseline {@code readBytesRefColumn}
 *       path already passes a byte-size hint. </li>
 *   <li>{@link #testOptimizedPlainColumnSizesByteStorageUpFront()} - the optimized {@code PageColumnReader}
 *       plain (non-dictionary) path sizes its byte storage up-front from the materialized values.</li>
 * </ul>
 */
public class ParquetByteHintTests extends ESTestCase {

    private static final int ROWS = 200;
    private static final int VALUE_BYTES = 20;
    private static final String COLUMN = "name";

    public void testDictionaryColumnEmitsOrdinalBlock() throws IOException {
        // Low-cardinality values so Parquet dictionary-encodes the column.
        List<BytesRef> distinct = equalLengthValues(8, VALUE_BYTES);
        List<BytesRef> values = new ArrayList<>(ROWS);
        for (int i = 0; i < ROWS; i++) {
            values.add(distinct.get(i % distinct.size()));
        }
        byte[] data = writeStringColumn(values, /* dictionary */ true);

        CountingBreaker breaker = new CountingBreaker();
        BlockFactory factory = factory(breaker);
        List<Page> pages = readAll(new ParquetFormatReader(factory), data);
        try {
            assertThat(pages.size(), equalTo(1));
            Block block = pages.get(0).getBlock(0);
            assertThat(
                "dictionary-encoded keyword columns should emit an OrdinalBytesRefBlock (no per-page builder)",
                block,
                instanceOf(OrdinalBytesRefBlock.class)
            );
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    public void testBaselineReaderSizesByteStorageUpFront() throws IOException {
        List<BytesRef> values = equalLengthValues(ROWS, VALUE_BYTES);
        byte[] data = writeStringColumn(values, /* dictionary */ false);

        int ideal = directHintedReservations(values);
        CountingBreaker breaker = new CountingBreaker();
        int readerReservations = readerReservations(breaker, new ParquetFormatReader(factory(breaker)).withBaselinePath(), data, values);

        assertThat("baseline reader already hints byte storage (no regrow)", readerReservations, equalTo(ideal));
    }

    public void testOptimizedPlainColumnSizesByteStorageUpFront() throws IOException {
        List<BytesRef> values = equalLengthValues(ROWS, VALUE_BYTES);
        byte[] data = writeStringColumn(values, /* dictionary */ false);

        int ideal = directHintedReservations(values);
        CountingBreaker breaker = new CountingBreaker();
        int readerReservations = readerReservations(breaker, new ParquetFormatReader(factory(breaker)), data, values);

        assertThat(
            "optimized PageColumnReader plain path must size byte storage up-front like a byte-hinted build (no regrow)",
            readerReservations,
            equalTo(ideal)
        );
    }

    /**
     * Byte-buffer reservations when the column is built directly with an exact byte hint (the ideal).
     */
    private int directHintedReservations(List<BytesRef> values) {
        long byteHint = 0;
        for (BytesRef value : values) {
            byteHint += value.length;
        }
        CountingBreaker breaker = new CountingBreaker();
        BlockFactory factory = factory(breaker);
        breaker.reset();
        try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(values.size(), byteHint)) {
            for (BytesRef value : values) {
                builder.appendBytesRef(value);
            }
            try (BytesRefBlock block = builder.build()) {
                assertThat(block.getPositionCount(), equalTo(values.size()));
            }
        }
        assertThat(breaker.used(), equalTo(0L));
        return breaker.positiveReservations();
    }

    /**
     * Byte-buffer reservations the given reader makes emitting the single keyword column.
     */
    private int readerReservations(CountingBreaker breaker, ParquetFormatReader reader, byte[] data, List<BytesRef> expected)
        throws IOException {
        breaker.reset();
        List<Page> pages = readAll(reader, data);
        try {
            assertThat(pages.size(), equalTo(1));
            BytesRefBlock block = pages.get(0).getBlock(0);
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < expected.size(); i++) {
                assertThat(block.getBytesRef(i, scratch), equalTo(expected.get(i)));
            }
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
        assertThat(breaker.used(), equalTo(0L));
        return breaker.positiveReservations();
    }

    private List<Page> readAll(ParquetFormatReader reader, byte[] data) throws IOException {
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iterator = reader.read(storageObject(data), FormatReadContext.of(List.of(COLUMN), ROWS + 10))) {
            while (iterator.hasNext()) {
                pages.add(iterator.next());
            }
        }
        return pages;
    }

    private static List<BytesRef> equalLengthValues(int count, int length) {
        List<BytesRef> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            StringBuilder sb = new StringBuilder(length);
            for (int c = 0; c < length; c++) {
                sb.append((char) ('a' + ((i * 7 + c) % 26)));
            }
            values.add(new BytesRef(sb.toString().getBytes(StandardCharsets.US_ASCII)));
        }
        return values;
    }

    private static byte[] writeStringColumn(List<BytesRef> values, boolean dictionary) throws IOException {
        MessageType schema = Types.buildMessage().required(BINARY).as(LogicalTypeAnnotation.stringType()).named(COLUMN).named("s");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(dictionary)
                .withRowGroupSize(64L * 1024 * 1024)
                .withPageSize(1024 * 1024)
                .build()
        ) {
            for (BytesRef value : values) {
                Group group = factory.newGroup();
                group.append(COLUMN, value.utf8ToString());
                writer.write(group);
            }
        }
        return out.toByteArray();
    }

    private static BlockFactory factory(CountingBreaker breaker) {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breaker.service());
        return BlockFactory.builder(bigArrays).breaker(new NoopCircuitBreaker("test-factory")).build();
    }

    private static StorageObject storageObject(byte[] data) {
        return new StorageObject() {
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
                return StoragePath.of("memory://byte_hint_test.parquet");
            }
        };
    }

    private static OutputFile outputFile(ByteArrayOutputStream out) {
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
                    public void write(int b) {
                        out.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        out.write(b, off, len);
                        position += len;
                    }

                    @Override
                    public void close() throws IOException {
                        out.close();
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
                return "memory://byte_hint_test.parquet";
            }
        };
    }

}
