/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

/**
 * Tests that parquet-mr's page-index filtering (ColumnIndex/OffsetIndex) is active
 * and reduces the number of rows returned for selective queries on sorted data.
 * <p>
 * Page-index filtering skips individual data pages within a row group using per-page
 * min/max statistics. It is enabled by default in parquet-mr 1.12+ via
 * {@code useColumnIndexFilter=true} and activated when {@code readNextFilteredRowGroup()}
 * is called with a FilterPredicate.
 * <p>
 * This test creates a single-row-group file with many small pages of sorted data,
 * then verifies that a selective filter returns fewer rows than the full row group —
 * proving that pages were skipped within the row group.
 */
public class ParquetPageIndexFilteringTests extends ESTestCase {

    private static final int TOTAL_ROWS = 1000;

    private static final MessageType SCHEMA = Types.buildMessage().required(INT32).named("id").named("page_index_test");

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * Sorted data with small pages: an equality filter should return fewer rows than the
     * full row group because page-index filtering skips pages whose min/max exclude the value.
     * Without page-index filtering, the entire row group would be returned (row-group min=0,
     * max=999 passes the statistics check).
     */
    public void testPageIndexFilteringReducesRowsWithinRowGroup() throws IOException {
        byte[] parquetData = createSortedParquetFile();

        int allRows = readWithFilter(parquetData, null);
        assertEquals(TOTAL_ROWS, allRows);

        FilterPredicate filter = FilterApi.eq(FilterApi.intColumn("id"), 500);
        int filteredRows = readWithFilter(parquetData, filter);

        // Page-index filtering returns entire pages, not individual rows — the matching page
        // contains id=500 plus its neighbors, so filteredRows > 1. RECHECK (per-row filtering)
        // happens later in the ESQL pipeline, not at the format reader level.
        assertTrue("Expected some rows from the page containing id=500", filteredRows > 0);
        assertTrue(
            "Expected page-index filtering to skip most pages (got " + filteredRows + " of " + allRows + ")",
            filteredRows < allRows / 2
        );
    }

    /**
     * A range filter on sorted data should also benefit from page-index filtering,
     * reading only the pages that overlap with the requested range.
     */
    public void testPageIndexFilteringWithRangeFilter() throws IOException {
        byte[] parquetData = createSortedParquetFile();

        int allRows = readWithFilter(parquetData, null);

        FilterPredicate filter = FilterApi.and(
            FilterApi.gtEq(FilterApi.intColumn("id"), 100),
            FilterApi.lt(FilterApi.intColumn("id"), 200)
        );
        int filteredRows = readWithFilter(parquetData, filter);

        assertTrue("Expected rows in the [100, 200) range", filteredRows > 0);
        assertTrue(
            "Expected page-index filtering to skip most pages (got " + filteredRows + " of " + allRows + ")",
            filteredRows < allRows / 2
        );
    }

    // --- helpers ---

    /**
     * Creates an in-memory Parquet file with 1000 sorted INT32 values (0–999) in a single
     * row group with small pages to produce many pages per row group.
     */
    private byte[] createSortedParquetFile() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withType(SCHEMA)
                .withRowGroupSize(10 * 1024 * 1024) // 10 MB — keep everything in one row group
                .withPageSize(64) // Very small pages to force many pages per row group
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int i = 0; i < TOTAL_ROWS; i++) {
                writer.write(factory.newGroup().append("id", i));
            }
        }
        return outputStream.toByteArray();
    }

    private int readWithFilter(byte[] parquetData, FilterPredicate filter) throws IOException {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        if (filter != null) {
            reader = reader.withPushedFilter(FilterCompat.get(filter));
        }

        StorageObject storageObject = createStorageObject(parquetData);
        int totalRows = 0;
        try (CloseableIterator<Page> iter = reader.read(storageObject, FormatReadContext.of(List.of("id"), 1000))) {
            while (iter.hasNext()) {
                Page page = iter.next();
                totalRows += page.getPositionCount();
            }
        }
        return totalRows;
    }

    private StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.EPOCH;
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://page_index_test.parquet");
            }
        };
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
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
                        outputStream.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        outputStream.write(b, off, len);
                        position += len;
                    }

                    @Override
                    public void close() throws IOException {
                        outputStream.close();
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
                return "memory://page_index_test.parquet";
            }
        };
    }
}
