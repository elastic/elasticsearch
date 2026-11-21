/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the public {@link ParquetPositionalReader} facade, which doc values formats outside ES|QL use to read
 * columns by file-global row position. The file has several row groups, so unsorted and repeated positions
 * cross row group boundaries.
 */
public class ParquetPositionalReaderTests extends ESTestCase {

    private static final int ROWS = 600;

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
    }

    public void testRowCountAndFooter() throws IOException {
        try (ParquetPositionalReader reader = ParquetPositionalReader.open(file(), blockFactory, Settings.EMPTY, null)) {
            assertThat(reader.rowCount(), equalTo((long) ROWS));
            assertTrue("expected several row groups", reader.footer().getBlocks().size() > 1);
        }
    }

    public void testExtractLongsAndStringsAtUnsortedPositions() throws IOException {
        long[] positions = { ROWS - 1, 0, 250, 250, 3, 499 };
        try (ParquetPositionalReader reader = ParquetPositionalReader.open(file(), blockFactory, Settings.EMPTY, null)) {
            Block[] blocks = reader.extract(new String[] { "id", "name" }, null, positions, blockFactory);
            try {
                LongBlock ids = (LongBlock) blocks[0];
                BytesRefBlock names = (BytesRefBlock) blocks[1];
                assertThat(ids.getPositionCount(), equalTo(positions.length));
                for (int i = 0; i < positions.length; i++) {
                    assertThat(ids.getLong(ids.getFirstValueIndex(i)), equalTo(positions[i] * 7));
                    assertThat(
                        names.getBytesRef(names.getFirstValueIndex(i), new BytesRef()).utf8ToString(),
                        equalTo("name-" + positions[i])
                    );
                }
            } finally {
                Releasables.close(blocks);
            }
        }
    }

    public void testPositionOutOfRangeFails() throws IOException {
        try (ParquetPositionalReader reader = ParquetPositionalReader.open(file(), blockFactory, Settings.EMPTY, null)) {
            expectThrows(RuntimeException.class, () -> reader.extract(new String[] { "id" }, null, new long[] { ROWS }, blockFactory));
        }
    }

    /**
     * A scan that starts at a row of the group returns that row first and the rest of the group after it, the same values as a scan
     * from the start of the group, whatever the codec and however many pages lie before the row. The group has several pages of a
     * few dozen rows, and the starts include the first row, a page boundary that the pages decide, the last row and the end.
     */
    public void testScanRowGroupFromARowReturnsTheRestOfTheGroup() throws IOException {
        for (CompressionCodecName codec : new CompressionCodecName[] { CompressionCodecName.UNCOMPRESSED, CompressionCodecName.SNAPPY }) {
            try (
                ParquetPositionalReader reader = ParquetPositionalReader.open(
                    file(codec, 1024L * 1024, 256),
                    blockFactory,
                    Settings.EMPTY,
                    null
                )
            ) {
                assertThat("one group, several pages", reader.footer().getBlocks().size(), equalTo(1));
                for (long start : new long[] { 0, 1, 37, 100, 101, 299, ROWS - 1, ROWS }) {
                    assertThat(scanIds(reader, 0, start, 25), equalTo(expectedIds(start, ROWS)));
                }
            }
        }
    }

    /** The same, in a file of several row groups: the row is relative to the group, and the ids carry the file-global row. */
    public void testScanRowGroupFromARowInLaterGroups() throws IOException {
        try (ParquetPositionalReader reader = ParquetPositionalReader.open(file(), blockFactory, Settings.EMPTY, null)) {
            long groupFirstRow = 0;
            for (int group = 0; group < reader.footer().getBlocks().size(); group++) {
                long rows = reader.footer().getBlocks().get(group).getRowCount();
                long start = randomLongBetween(0, rows - 1);
                assertThat(scanIds(reader, group, start, 16), equalTo(expectedIds(groupFirstRow + start, groupFirstRow + rows)));
                groupFirstRow += rows;
            }
            assertThat(groupFirstRow, equalTo((long) ROWS));
        }
    }

    private List<Long> scanIds(ParquetPositionalReader reader, int group, long startRow, int batchSize) throws IOException {
        List<Long> ids = new ArrayList<>();
        try (CloseableIterator<Page> pages = reader.scanRowGroup("id", group, startRow, batchSize)) {
            while (pages.hasNext()) {
                Page page = pages.next();
                try {
                    LongBlock block = page.getBlock(0);
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        ids.add(block.getLong(block.getFirstValueIndex(i)));
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        return ids;
    }

    /** The ids of the rows {@code [from, to)} of the file: the id of a row is seven times its position. */
    private static List<Long> expectedIds(long from, long to) {
        List<Long> ids = new ArrayList<>();
        for (long row = from; row < to; row++) {
            ids.add(row * 7);
        }
        return ids;
    }

    private static StorageObject file() throws IOException {
        // a tiny row group budget forces several row groups out of 600 rows
        return file(CompressionCodecName.UNCOMPRESSED, 1024L, ParquetWriter.DEFAULT_PAGE_SIZE);
    }

    private static StorageObject file(CompressionCodecName codec, long rowGroupSize, int pageSize) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .named("rows");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(codec)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(pageSize)
                .build()
        ) {
            for (long i = 0; i < ROWS; i++) {
                writer.write(factory.newGroup().append("id", i * 7).append("name", "name-" + i));
            }
        }
        byte[] data = out.toByteArray();
        return new AbstractTestStorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int pos = (int) position;
                return new ByteArrayInputStream(data, pos, (int) Math.min(length, data.length - position));
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
                return StoragePath.of("memory://test.parquet");
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
                return "memory://test.parquet";
            }
        };
    }
}
