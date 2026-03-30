/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/**
 * Integration tests for Parquet bloom filter and statistics-based row-group skipping.
 * <p>
 * Creates real Parquet files with bloom filters enabled and verifies that
 * parquet-java's built-in filtering correctly skips row groups.
 */
public class ParquetBloomFilterTests extends ESTestCase {

    private static final MessageType SCHEMA = Types.buildMessage()
        .required(INT64)
        .named("id")
        .required(INT32)
        .named("value")
        .required(BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("name")
        .named("test_schema");

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * Test that equality filter on a column with bloom filter skips row groups
     * that don't contain the queried value.
     */
    public void testBloomFilterSkipsRowGroupForAbsentValue() throws IOException {
        byte[] parquetData = createParquetFileWithBloomFilter();

        // Query for a value that doesn't exist in any row group
        FilterPredicate filter = FilterApi.eq(FilterApi.longColumn("id"), 999999L);
        int totalRows = readWithFilter(parquetData, filter);

        // Bloom filter should have eliminated all row groups — no rows returned
        assertEquals(0, totalRows);
    }

    /**
     * Test that equality filter returns correct rows when value exists in some row groups.
     */
    public void testBloomFilterReturnsMatchingRows() throws IOException {
        byte[] parquetData = createParquetFileWithBloomFilter();

        // Query for value that exists
        FilterPredicate filter = FilterApi.eq(FilterApi.longColumn("id"), 150L);
        int totalRows = readWithFilter(parquetData, filter);

        // Should read at least the row group containing id=150
        assertTrue("Expected rows from the matching row group", totalRows > 0);
    }

    /**
     * Test that statistics-based skipping works for range predicates.
     * Value outside the min/max range of all row groups should return 0 rows.
     */
    public void testStatisticsSkipForOutOfRangeValue() throws IOException {
        byte[] parquetData = createParquetFileWithBloomFilter();

        // All IDs are 0-299, so id > 1000 should skip all row groups via statistics
        FilterPredicate filter = FilterApi.gt(FilterApi.longColumn("id"), 1000L);
        int totalRows = readWithFilter(parquetData, filter);

        assertEquals(0, totalRows);
    }

    /**
     * Test that IN filter with values absent from all row groups returns 0 rows.
     */
    public void testInFilterSkipsAllRowGroups() throws IOException {
        byte[] parquetData = createParquetFileWithBloomFilter();

        Set<Long> absentValues = new HashSet<>(Set.of(888888L, 999999L));
        FilterPredicate filter = FilterApi.in(FilterApi.longColumn("id"), absentValues);
        int totalRows = readWithFilter(parquetData, filter);

        assertEquals(0, totalRows);
    }

    /**
     * Test that string column bloom filter works.
     */
    public void testStringBloomFilter() throws IOException {
        byte[] parquetData = createParquetFileWithBloomFilter();

        // Query for a name that doesn't exist
        FilterPredicate filter = FilterApi.eq(FilterApi.binaryColumn("name"), Binary.fromString("nonexistent_name"));
        int totalRows = readWithFilter(parquetData, filter);

        assertEquals(0, totalRows);
    }

    /**
     * Test that reading without any filter returns all rows.
     */
    public void testNoFilterReturnsAllRows() throws IOException {
        byte[] parquetData = createParquetFileWithBloomFilter();
        int totalRows = readWithFilter(parquetData, null);
        assertEquals(300, totalRows);
    }

    /**
     * Test that the FormatReader withPushedFilter correctly integrates.
     */
    public void testFormatReaderWithPushedFilter() throws IOException {
        byte[] parquetData = createParquetFileWithBloomFilter();

        // Create filter for absent value
        FilterPredicate filter = FilterApi.eq(FilterApi.longColumn("id"), 999999L);
        FilterCompat.Filter compatFilter = FilterCompat.get(filter);

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetFormatReader filteredReader = reader.withPushedFilter(compatFilter);

        StorageObject storageObject = createStorageObject(parquetData);
        int totalRows = 0;
        FormatReadContext ctx = FormatReadContext.of(List.of("id", "value", "name"), 1000);
        try (CloseableIterator<Page> iter = filteredReader.read(storageObject, ctx)) {
            while (iter.hasNext()) {
                Page page = iter.next();
                totalRows += page.getPositionCount();
            }
        }

        assertEquals(0, totalRows);
    }

    /**
     * Test result equivalence: filtered read should be a subset of unfiltered read.
     */
    public void testResultEquivalence() throws IOException {
        byte[] parquetData = createParquetFileWithBloomFilter();

        // Read all rows
        int allRows = readWithFilter(parquetData, null);

        // Read with filter for value that exists
        FilterPredicate filter = FilterApi.eq(FilterApi.longColumn("id"), 50L);
        int filteredRows = readWithFilter(parquetData, filter);

        // Filtered should be <= all rows
        assertTrue(filteredRows <= allRows);
        assertTrue(filteredRows > 0);
    }

    /**
     * Test that withPushedFilter with null returns the same reader.
     */
    public void testWithPushedFilterNullReturnsThis() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetFormatReader result = reader.withPushedFilter(null);
        assertSame(reader, result);
    }

    /**
     * Test that withPushedFilter with non-Filter object returns the same reader.
     */
    public void testWithPushedFilterWrongTypeReturnsThis() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetFormatReader result = reader.withPushedFilter("not a filter");
        assertSame(reader, result);
    }

    /**
     * Test that filterPushdownSupport returns non-null.
     */
    public void testFilterPushdownSupport() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        assertNotNull(reader.filterPushdownSupport());
    }

    // --- helpers ---

    /**
     * Creates a Parquet file with 3+ row groups (using small row group size) with bloom filters
     * on id and name columns.
     * Row data: ids 0-299, value=id*10, name="name_N"
     */
    private byte[] createParquetFileWithBloomFilter() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withType(SCHEMA)
                .withRowGroupSize(4096) // Small row groups to force multiple groups
                .withPageSize(1024)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withBloomFilterEnabled("id", true)
                .withBloomFilterEnabled("name", true)
                .withBloomFilterNDV("id", 100L)
                .withBloomFilterNDV("name", 100L)
                .build()
        ) {
            for (int i = 0; i < 300; i++) {
                Group group = factory.newGroup().append("id", (long) i).append("value", i * 10).append("name", "name_" + i);
                writer.write(group);
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
        try (CloseableIterator<Page> iter = reader.read(storageObject, FormatReadContext.of(List.of("id", "value", "name"), 1000))) {
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
                return Instant.now();
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://bloom_test.parquet");
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
                return "memory://bloom_test.parquet";
            }
        };
    }
}
