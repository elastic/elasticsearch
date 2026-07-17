/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/**
 * Local reproduction for the two-phase page-filtered skip overshoot described in
 * esql-planning#1398.
 *
 * <p>When a projection {@link PageColumnReader} carries a survivor {@link RowRanges} that
 * EXCLUDES an early data page (zero survivors on that page) and survivors sit on a later page,
 * {@link PageColumnReader#skipRows} must advance the reader in the caller's source-row
 * coordinate space. The bug: {@code loadNextPage} skips the whole excluded page, jumping the
 * physical cursor PAST the caller's skip target; the overshoot is discarded, so the reader is
 * silently ahead of where the caller thinks it is and every later skip/read consumes real
 * survivor rows.
 *
 * <p>The invariant asserted here is coordinate-space-exact and codec/geometry independent:
 * skipping exactly the source rows of the excluded first page must leave the reader positioned
 * on the first row of the next page.
 */
public class PageColumnReaderSkipOvershootReproTests extends ESTestCase {

    private static final MessageType SCHEMA = Types.buildMessage().required(INT64).named("v").named("skip_overshoot_repro");

    private BlockFactory blockFactory;
    private PlainCompressionCodecFactory codecFactory;

    @Before
    public void init() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        codecFactory = new PlainCompressionCodecFactory();
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
    }

    @After
    public void releaseCodec() {
        codecFactory.release();
    }

    /**
     * Two skip calls that together cover exactly the excluded first page must leave the reader
     * on the first survivor row (source row == first-row-index of page 1). With the overshoot
     * bug the first skip jumps to the page boundary and discards the surplus, so the second
     * skip eats into page 1 and the subsequent read returns a survivor row too far along.
     */
    public void testSkipCoveringExcludedPageLandsAtNextPageStart() throws IOException {
        byte[] data = writeTwoPageFile();
        try (ParquetFileReader reader = openReader(data)) {
            BlockMetaData rg = reader.getRowGroups().getFirst();
            long totalRows = rg.getRowCount();
            OffsetIndex oi = reader.readOffsetIndex(rg.getColumns().getFirst());
            assertNotNull("column must have an offset index", oi);
            assertTrue("need at least two pages to reproduce", oi.getPageCount() >= 2);

            // page 0 spans [0, firstPageRows); page 1 starts at firstPageRows.
            int firstPageRows = Math.toIntExact(oi.getFirstRowIndex(1));
            assertTrue(firstPageRows >= 2);

            // Survivor RowRanges cover only page 1 onward, so page 0 is excluded (zero survivors)
            // and survivors sit strictly after it: the exact #1398 trigger.
            RowRanges survivorRanges = RowRanges.of(firstPageRows, totalRows, totalRows);

            ColumnDescriptor desc = SCHEMA.getColumns().getFirst();
            ColumnInfo info = new ColumnInfo(
                desc,
                desc.getPrimitiveType().getPrimitiveTypeName(),
                DataType.LONG,
                desc.getMaxDefinitionLevel(),
                desc.getMaxRepetitionLevel(),
                desc.getPrimitiveType().getLogicalTypeAnnotation()
            );

            PageReadStore store = reader.readNextRowGroup();
            assertNotNull(store);

            try (PageColumnReader pcr = new PageColumnReader(store.getPageReader(desc), desc, info, survivorRanges)) {
                // The two-phase caller drains the fully-filtered leading batches with skipRows in
                // source-row coordinates. Split the excluded page into two skips so the first one
                // stops short of the page boundary (count < firstPageRows) and triggers the jump.
                int firstSkip = firstPageRows / 2;
                assertTrue(firstSkip > 0 && firstSkip < firstPageRows);
                pcr.skipRows(firstSkip);
                pcr.skipRows(firstPageRows - firstSkip);

                // We have now skipped exactly the source rows of page 0. The next read must start
                // at the first survivor row, whose value equals its source-row index.
                LongBlock block = (LongBlock) pcr.readBatch(1, blockFactory);
                try {
                    assertEquals(1, block.getPositionCount());
                    assertEquals(
                        "reader must sit on the first survivor row after skipping the excluded page",
                        (long) firstPageRows,
                        block.getLong(0)
                    );
                } finally {
                    block.close();
                }
            }
        }
    }

    private byte[] writeTwoPageFile() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(codecFactory)
                .withType(SCHEMA)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                // Plain encoding + a tiny page size forces several small pages within one row group.
                .withDictionaryEncoding(false)
                .withRowGroupSize(64L * 1024 * 1024)
                .withPageSize(1024)
                .build()
        ) {
            for (int i = 0; i < 4000; i++) {
                writer.write(factory.newGroup().append("v", (long) i));
            }
        }
        return out.toByteArray();
    }

    private ParquetFileReader openReader(byte[] data) throws IOException {
        return ParquetFileReader.open(
            new ParquetStorageObjectAdapter(storageObject(data), blockFactory.arrowAllocator()),
            PlainParquetReadOptions.builder(codecFactory).build()
        );
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
                return StoragePath.of("memory://skip_overshoot_repro.parquet");
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
                    public void write(int b) throws IOException {
                        out.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
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
                return "memory://skip_overshoot_repro.parquet";
            }
        };
    }
}
