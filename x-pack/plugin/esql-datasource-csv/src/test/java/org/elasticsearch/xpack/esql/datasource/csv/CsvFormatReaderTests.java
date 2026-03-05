/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

public class CsvFormatReaderTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
    }

    public void testSchema() throws IOException {
        String csv = """
            id:long,name:keyword,age:integer,active:boolean
            1,Alice,30,true
            2,Bob,25,false
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);

        assertEquals(4, schema.size());
        assertEquals("id", schema.get(0).name());
        assertEquals(DataType.LONG, schema.get(0).dataType());
        assertEquals("name", schema.get(1).name());
        assertEquals(DataType.KEYWORD, schema.get(1).dataType());
        assertEquals("age", schema.get(2).name());
        assertEquals(DataType.INTEGER, schema.get(2).dataType());
        assertEquals("active", schema.get(3).name());
        assertEquals(DataType.BOOLEAN, schema.get(3).dataType());
    }

    public void testSchemaWithComments() throws IOException {
        String csv = """
            // This is a comment
            // Another comment
            id:long,name:keyword
            1,Alice
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);

        assertEquals(2, schema.size());
        assertEquals("id", schema.get(0).name());
        assertEquals("name", schema.get(1).name());
    }

    public void testReadAllColumns() throws IOException {
        String csv = """
            id:long,name:keyword,score:double
            1,Alice,95.5
            2,Bob,87.3
            3,Charlie,92.1
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());
            assertEquals(3, page.getBlockCount());

            // Check first row
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(2)).getDouble(0), 0.001);

            // Check second row
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(2)).getDouble(1), 0.001);

            assertFalse(iterator.hasNext());
        }
    }

    public void testReadProjectedColumns() throws IOException {
        String csv = """
            id:long,name:keyword,score:double
            1,Alice,95.5
            2,Bob,87.3
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        // Project only name and score
        try (CloseableIterator<Page> iterator = reader.read(object, List.of("name", "score"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount()); // Only 2 projected columns

            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(1)).getDouble(0), 0.001);
        }
    }

    public void testReadWithBatching() throws IOException {
        StringBuilder csv = new StringBuilder("id:long,value:integer\n");
        for (int i = 1; i <= 25; i++) {
            csv.append(i).append(",").append(i * 10).append("\n");
        }

        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        int batchSize = 10;
        int totalRows = 0;

        try (CloseableIterator<Page> iterator = reader.read(object, null, batchSize)) {
            // First batch: 10 rows
            assertTrue(iterator.hasNext());
            Page page1 = iterator.next();
            assertEquals(10, page1.getPositionCount());
            totalRows += page1.getPositionCount();

            // Second batch: 10 rows
            assertTrue(iterator.hasNext());
            Page page2 = iterator.next();
            assertEquals(10, page2.getPositionCount());
            totalRows += page2.getPositionCount();

            // Third batch: 5 rows
            assertTrue(iterator.hasNext());
            Page page3 = iterator.next();
            assertEquals(5, page3.getPositionCount());
            totalRows += page3.getPositionCount();

            assertFalse(iterator.hasNext());
        }

        assertEquals(25, totalRows);
    }

    public void testReadWithNullValues() throws IOException {
        String csv = """
            id:long,name:keyword,score:double
            1,Alice,95.5
            2,,87.3
            3,Charlie,
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());

            // First row: all values present
            assertFalse(page.getBlock(0).isNull(0));
            assertFalse(page.getBlock(1).isNull(0));
            assertFalse(page.getBlock(2).isNull(0));

            // Second row: name is null
            assertFalse(page.getBlock(0).isNull(1));
            assertTrue(page.getBlock(1).isNull(1));
            assertFalse(page.getBlock(2).isNull(1));

            // Third row: score is null
            assertFalse(page.getBlock(0).isNull(2));
            assertFalse(page.getBlock(1).isNull(2));
            assertTrue(page.getBlock(2).isNull(2));
        }
    }

    public void testReadWithCommentsInData() throws IOException {
        String csv = """
            id:long,name:keyword
            // This is a comment
            1,Alice
            // Another comment
            2,Bob
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            // Comments should be skipped, only 2 data rows
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
        }
    }

    public void testFormatName() {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        assertEquals("csv", reader.formatName());
    }

    public void testFileExtensions() {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        List<String> extensions = reader.fileExtensions();
        assertEquals(2, extensions.size());
        assertTrue(extensions.contains(".csv"));
        assertTrue(extensions.contains(".tsv"));
    }

    public void testInvalidSchema() {
        String csv = "invalid_schema_no_colon\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        ParsingException e = expectThrows(ParsingException.class, () -> reader.schema(object));
        assertTrue(e.getMessage().contains("Invalid CSV schema format"));
    }

    public void testReadDatetimeEpochMillis() throws IOException {
        long epochMillis = 1609459200000L; // 2021-01-01T00:00:00.000Z
        String csv = "id:long,ts:datetime\n1," + epochMillis + "\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(epochMillis, ((LongBlock) page.getBlock(1)).getLong(0));
        }
    }

    public void testReadDatetimeIso8601() throws IOException {
        String csv = "id:long,ts:datetime\n1,1953-09-02T00:00:00.000Z\n2,2021-01-01T00:00:00Z\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(Instant.parse("1953-09-02T00:00:00.000Z").toEpochMilli(), ((LongBlock) page.getBlock(1)).getLong(0));
            assertEquals(Instant.parse("2021-01-01T00:00:00Z").toEpochMilli(), ((LongBlock) page.getBlock(1)).getLong(1));
        }
    }

    public void testReadDatetimeMixed() throws IOException {
        long epochMillis = 1609459200000L; // 2021-01-01T00:00:00.000Z
        String csv = "id:long,ts:datetime\n1," + epochMillis + "\n2,1953-09-02T00:00:00.000Z\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(epochMillis, ((LongBlock) page.getBlock(1)).getLong(0));
            assertEquals(Instant.parse("1953-09-02T00:00:00.000Z").toEpochMilli(), ((LongBlock) page.getBlock(1)).getLong(1));
        }
    }

    public void testUnsupportedType() {
        String csv = "id:unsupported_type\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        EsqlIllegalArgumentException e = expectThrows(EsqlIllegalArgumentException.class, () -> reader.schema(object));
        assertTrue(e.getMessage().contains("illegal data type"));
    }

    private StorageObject createStorageObject(String csvContent) {
        byte[] bytes = csvContent.getBytes(StandardCharsets.UTF_8);

        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                return new ByteArrayInputStream(bytes);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                throw new UnsupportedOperationException("Range reads not needed for CSV");
            }

            @Override
            public long length() throws IOException {
                return bytes.length;
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
                return StoragePath.of("memory://test.csv");
            }
        };
    }
}
