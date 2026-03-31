/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class CsvFormatReaderTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
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

    public void testTypedSchemaTextAndTxtAliasesMapToKeyword() throws IOException {
        String csv = """
            a:text,b:txt
            hello,world
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);

        assertEquals(2, schema.size());
        assertEquals("a", schema.get(0).name());
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals("b", schema.get(1).name());
        assertEquals(DataType.KEYWORD, schema.get(1).dataType());
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

    public void testPlainHeaderTreatedAsColumnName() throws IOException {
        String csv = "column_name\nsome_value\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(1, schema.size());
        assertEquals("column_name", schema.get(0).name());
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testQuotedColumnNameWithColon() throws IOException {
        String csv = "\"host:port\",status\n\"localhost:9200\",200\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(2, schema.size());
        assertEquals("\"host:port\"", schema.get(0).name());
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals("status", schema.get(1).name());
        assertEquals(DataType.INTEGER, schema.get(1).dataType());
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

    public void testSchemaWithIpType() throws IOException {
        String csv = """
            domain:keyword,public_ip:ip
            www.elastic.co,8.8.8.8
            discuss.elastic.co,2001:4860:4860::8888
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);

        assertEquals(2, schema.size());
        assertEquals("domain", schema.get(0).name());
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals("public_ip", schema.get(1).name());
        assertEquals(DataType.IP, schema.get(1).dataType());
    }

    public void testReadIpType() throws IOException {
        String csv = """
            domain:keyword,public_ip:ip
            www.elastic.co,8.8.8.8
            discuss.elastic.co,2001:4860:4860::8888
            files.internal,10.0.0.5
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());
            assertEquals(2, page.getBlockCount());

            BytesRef expected8 = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("8.8.8.8")));
            BytesRef expectedIpv6 = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("2001:4860:4860::8888")));
            BytesRef expected10 = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("10.0.0.5")));

            assertEquals(new BytesRef("www.elastic.co"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            assertEquals(expected8, ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));

            assertEquals(new BytesRef("discuss.elastic.co"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(1, new BytesRef()));
            assertEquals(expectedIpv6, ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));

            assertEquals(new BytesRef("files.internal"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(2, new BytesRef()));
            assertEquals(expected10, ((BytesRefBlock) page.getBlock(1)).getBytesRef(2, new BytesRef()));

            assertFalse(iterator.hasNext());
        }
    }

    public void testUnsupportedType() {
        String csv = "id:unsupported_type\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        EsqlIllegalArgumentException e = expectThrows(EsqlIllegalArgumentException.class, () -> reader.schema(object));
        assertTrue(e.getMessage().contains("illegal data type"));
    }

    public void testDefaultErrorPolicyIsStrict() {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        assertEquals(ErrorPolicy.STRICT, reader.defaultErrorPolicy());
        assertTrue(reader.defaultErrorPolicy().isStrict());
    }

    public void testStrictPolicyFailsOnMalformedRow() {
        String csv = """
            id:long,name:keyword
            1,Alice
            not_a_number,Bob
            3,Charlie
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        EsqlIllegalArgumentException e = expectThrows(EsqlIllegalArgumentException.class, () -> {
            try (
                CloseableIterator<Page> iterator = reader.read(
                    object,
                    FormatReadContext.builder().batchSize(10).errorPolicy(ErrorPolicy.STRICT).build()
                )
            ) {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }
        });
        assertTrue(e.getMessage().contains("Failed to parse CSV value"));
    }

    public void testLenientPolicySkipsMalformedRows() throws IOException {
        String csv = """
            id:long,name:keyword
            1,Alice
            not_a_number,Bob
            3,Charlie
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy lenient = new ErrorPolicy(10, true);

        try (
            CloseableIterator<Page> iterator = reader.read(object, FormatReadContext.builder().batchSize(10).errorPolicy(lenient).build())
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(3L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(new BytesRef("Charlie"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));

            assertFalse(iterator.hasNext());
        }
    }

    public void testErrorBudgetExceeded() {
        String csv = """
            id:long,name:keyword
            bad1,Alice
            bad2,Bob
            bad3,Charlie
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy limited = new ErrorPolicy(2, false);

        EsqlIllegalArgumentException e = expectThrows(EsqlIllegalArgumentException.class, () -> {
            try (
                CloseableIterator<Page> iterator = reader.read(
                    object,
                    FormatReadContext.builder().batchSize(10).errorPolicy(limited).build()
                )
            ) {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }
        });
        assertTrue(e.getMessage().contains("error budget exceeded"));
    }

    public void testLenientPolicyWithExtraColumns() throws IOException {
        String csv = """
            id:long,name:keyword
            1,Alice
            2,Bob,extra_column
            3,Charlie
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy lenient = new ErrorPolicy(10, true);

        try (
            CloseableIterator<Page> iterator = reader.read(object, FormatReadContext.builder().batchSize(10).errorPolicy(lenient).build())
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(3L, ((LongBlock) page.getBlock(0)).getLong(1));

            assertFalse(iterator.hasNext());
        }
    }

    public void testLenientPolicyAllRowsBad() throws IOException {
        String csv = """
            id:long,name:keyword
            bad1,Alice
            bad2,Bob
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy lenient = new ErrorPolicy(100, true);

        try (
            CloseableIterator<Page> iterator = reader.read(object, FormatReadContext.builder().batchSize(10).errorPolicy(lenient).build())
        ) {
            assertFalse(iterator.hasNext());
        }
    }

    public void testLenientPolicyAcrossBatches() throws IOException {
        StringBuilder csv = new StringBuilder("id:long,name:keyword\n");
        for (int i = 1; i <= 15; i++) {
            if (i % 3 == 0) {
                csv.append("bad,Name").append(i).append("\n");
            } else {
                csv.append(i).append(",Name").append(i).append("\n");
            }
        }

        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy lenient = new ErrorPolicy(100, false);

        int totalRows = 0;
        try (
            CloseableIterator<Page> iterator = reader.read(object, FormatReadContext.builder().batchSize(5).errorPolicy(lenient).build())
        ) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
            }
        }
        assertEquals(10, totalRows);
    }

    public void testErrorRatioExceeded() {
        StringBuilder csv = new StringBuilder("id:long,name:keyword\n");
        for (int i = 1; i <= 10; i++) {
            if (i <= 4) {
                csv.append("bad").append(i).append(",Name").append(i).append("\n");
            } else {
                csv.append(i).append(",Name").append(i).append("\n");
            }
        }

        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy ratioPolicy = new ErrorPolicy(Long.MAX_VALUE, 0.3, true);

        EsqlIllegalArgumentException e = expectThrows(EsqlIllegalArgumentException.class, () -> {
            try (
                CloseableIterator<Page> iterator = reader.read(
                    object,
                    FormatReadContext.builder().batchSize(10).errorPolicy(ratioPolicy).build()
                )
            ) {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }
        });
        assertTrue(e.getMessage().contains("error budget exceeded"));
    }

    public void testErrorRatioWithinBudget() throws IOException {
        StringBuilder csv = new StringBuilder("id:long,name:keyword\n");
        for (int i = 1; i <= 100; i++) {
            if (i == 50) {
                csv.append("bad,Name").append(i).append("\n");
            } else {
                csv.append(i).append(",Name").append(i).append("\n");
            }
        }

        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy ratioPolicy = new ErrorPolicy(Long.MAX_VALUE, 0.1, false);

        int totalRows = 0;
        try (
            CloseableIterator<Page> iterator = reader.read(
                object,
                FormatReadContext.builder().batchSize(200).errorPolicy(ratioPolicy).build()
            )
        ) {
            while (iterator.hasNext()) {
                totalRows += iterator.next().getPositionCount();
            }
        }
        assertEquals(99, totalRows);
    }

    // --- Delimiter and format variation tests ---

    public void testSemicolonInQuotedField() throws IOException {
        String csv = "id:long,data:keyword\n1,\"value;with;semicolons\"\n2,normal\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(new BytesRef("value;with;semicolons"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
        }
    }

    public void testPipeInQuotedField() throws IOException {
        String csv = "id:long,data:keyword\n1,\"col1|col2|col3\"\n2,simple\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(new BytesRef("col1|col2|col3"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
        }
    }

    // --- Quoted field handling tests (RFC 4180) ---

    public void testQuotedFieldsWithCommas() throws IOException {
        String csv = "id:long,name:keyword\n1,\"Smith, John\"\n2,\"Doe, Jane\"\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(new BytesRef("Smith, John"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("Doe, Jane"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
        }
    }

    public void testQuotedFieldsWithNewlines() throws IOException {
        String csv = "id:long,description:keyword\n1,\"line1\nline2\"\n2,\"simple\"\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(new BytesRef("line1\nline2"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
        }
    }

    public void testEscapedQuotesInFields() throws IOException {
        String csv = "id:long,name:keyword\n1,\"She said \\\"hello\\\"\"\n2,normal\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
        }
    }

    // --- Empty and null value handling ---

    public void testEmptyFieldsTreatedAsNull() throws IOException {
        String csv = "id:long,name:keyword,score:double\n1,,95.5\n2,Bob,\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertTrue(page.getBlock(1).isNull(0));
            assertTrue(page.getBlock(2).isNull(1));
        }
    }

    public void testExplicitNullString() throws IOException {
        String csv = "id:long,name:keyword\n1,null\n2,Bob\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertTrue(page.getBlock(1).isNull(0));
            assertFalse(page.getBlock(1).isNull(1));
        }
    }

    // --- Trailing/missing column handling ---

    public void testFewerColumnsThanSchema() throws IOException {
        String csv = "id:long,name:keyword,score:double\n1,Alice\n2,Bob,87.3\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertTrue(page.getBlock(2).isNull(0));
            assertFalse(page.getBlock(2).isNull(1));
        }
    }

    // --- Whitespace trimming ---

    public void testWhitespaceTrimming() throws IOException {
        String csv = "id:long,name:keyword\n 1 , Alice \n 2 , Bob \n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
        }
    }

    // --- All supported data types ---

    public void testAllSupportedDataTypes() throws IOException {
        String csv = "i:integer,l:long,d:double,k:keyword,t:text,b:boolean,dt:datetime,n:null\n"
            + "42,9999999999,3.14,hello,world,true,2021-01-01T00:00:00Z,\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(8, page.getBlockCount());
        }
    }

    // --- Type alias coverage ---

    public void testTypeAliases() throws IOException {
        String csv = "a:int,b:l,c:d,d:k,e:s,f:txt,g:bool,h:date,i:dt,j:n\n"
            + "1,2,3.0,kw,str,text,false,2021-01-01T00:00:00Z,2021-01-01T00:00:00Z,\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(10, page.getBlockCount());
        }
    }

    // --- Empty file handling ---

    public void testEmptyFile() throws IOException {
        StorageObject object = createStorageObject("");
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertFalse(iterator.hasNext());
        }
    }

    public void testSchemaOnlyNoData() throws IOException {
        String csv = "id:long,name:keyword\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertFalse(iterator.hasNext());
        }
    }

    // --- Mixed error types with lenient policy ---

    public void testMixedErrorTypesWithLenientPolicy() throws IOException {
        String csv = """
            id:long,score:double,active:boolean,ts:datetime
            1,95.5,true,2021-01-01T00:00:00Z
            bad_id,95.5,true,2021-01-01T00:00:00Z
            3,not_a_double,true,2021-01-01T00:00:00Z
            4,80.0,not_bool,2021-01-01T00:00:00Z
            5,90.0,false,not_a_date
            6,85.0,true,2021-06-15T12:00:00Z
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy lenient = new ErrorPolicy(100, true);

        int totalRows = 0;
        try (
            CloseableIterator<Page> iterator = reader.read(object, FormatReadContext.builder().batchSize(10).errorPolicy(lenient).build())
        ) {
            while (iterator.hasNext()) {
                totalRows += iterator.next().getPositionCount();
            }
        }
        assertEquals(2, totalRows);
    }

    // --- Large batch with sparse errors ---

    public void testLargeBatchWithSparseErrors() throws IOException {
        StringBuilder csv = new StringBuilder("id:long,value:integer\n");
        int expectedGood = 0;
        for (int i = 1; i <= 1000; i++) {
            if (i % 100 == 0) {
                csv.append("bad,").append(i).append("\n");
            } else {
                csv.append(i).append(",").append(i * 10).append("\n");
                expectedGood++;
            }
        }

        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy lenient = new ErrorPolicy(100, false);

        int totalRows = 0;
        try (
            CloseableIterator<Page> iterator = reader.read(object, FormatReadContext.builder().batchSize(200).errorPolicy(lenient).build())
        ) {
            while (iterator.hasNext()) {
                totalRows += iterator.next().getPositionCount();
            }
        }
        assertEquals(expectedGood, totalRows);
    }

    // --- Configurable Delimiter Tests (Gap 3) ---

    public void testTsvPreset() throws IOException {
        String tsv = "id:long\tname:keyword\tvalue:integer\n1\tAlice\t100\n2\tBob\t200\n";
        StorageObject object = createStorageObject(tsv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(CsvFormatOptions.TSV);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(3, page.getBlockCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(100, ((IntBlock) page.getBlock(2)).getInt(0));
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
            assertEquals(200, ((IntBlock) page.getBlock(2)).getInt(1));
        }
    }

    public void testPipeDelimited() throws IOException {
        String csv = "id:long|name:keyword|score:double\n1|Alice|95.5\n2|Bob|87.3\n";
        CsvFormatOptions options = new CsvFormatOptions(
            '|',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.NONE
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(2)).getDouble(0), 0.001);
        }
    }

    public void testSemicolonDelimited() throws IOException {
        String csv = "id:long;name:keyword;value:integer\n1;Alice;42\n2;Bob;99\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ';',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.NONE
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(42, ((IntBlock) page.getBlock(2)).getInt(0));
        }
    }

    public void testRandomizedDelimiter() throws IOException {
        char delimiter = randomFrom(',', '\t', '|', ';');
        int numCols = between(2, 5);
        int numRows = between(5, 20);
        StringBuilder schema = new StringBuilder();
        StringBuilder data = new StringBuilder();
        for (int c = 0; c < numCols; c++) {
            if (c > 0) schema.append(delimiter);
            schema.append("col").append(c).append(":long");
        }
        schema.append("\n");
        for (int r = 0; r < numRows; r++) {
            for (int c = 0; c < numCols; c++) {
                if (c > 0) data.append(delimiter);
                data.append(r * 10 + c);
            }
            data.append("\n");
        }
        CsvFormatOptions options = new CsvFormatOptions(
            delimiter,
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.NONE
        );
        StorageObject object = createStorageObject(schema.append(data).toString());
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        int totalRows = 0;
        try (CloseableIterator<Page> iterator = reader.read(object, null, between(2, 10))) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
                for (int i = 0; i < page.getPositionCount(); i++) {
                    long expected = (totalRows - page.getPositionCount() + i) * 10L;
                    assertEquals(expected, ((LongBlock) page.getBlock(0)).getLong(i));
                }
            }
        }
        assertEquals(numRows, totalRows);
    }

    // --- Configurable Quote/Escape Characters (Gap 4) ---

    public void testSingleQuoteAsQuoteChar() throws IOException {
        String csv = "id:long,name:keyword\n1,'Alice'\n2,'Bob,Smith'\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            '\'',
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.NONE
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("Bob,Smith"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
        }
    }

    public void testCustomEscapeChar() throws IOException {
        String csv = "id:long,data:keyword\n1,\"value\\\"quoted\"\n2,normal\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            '"',
            '\\',
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.NONE
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(new BytesRef("value\"quoted"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
        }
    }

    public void testRandomizedQuoteChar() throws IOException {
        char quoteChar = randomFrom('"', '\'');
        String csv = "id:long,name:keyword\n1," + quoteChar + "Alice" + quoteChar + "\n2," + quoteChar + "Bob" + quoteChar + "\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            quoteChar,
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.NONE
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
        }
    }

    // --- Custom Comment Prefix (Gap 4) ---

    public void testHashCommentPrefix() throws IOException {
        String csv = """
            # This is a comment
            id:long,name:keyword
            # Another comment
            1,Alice
            2,Bob
            """;

        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            "#",
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.NONE
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
        }
    }

    public void testCustomCommentPrefix() throws IOException {
        String csv = """
            -- SQL style comment
            id:long,name:keyword
            -- Another comment
            1,Alice
            2,Bob
            """;

        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            "--",
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.NONE
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
        }
    }

    // --- Custom Null Value (Gap 4) ---

    public void testNaAsNullValue() throws IOException {
        String csv = "id:long,name:keyword,score:double\n1,N/A,95.5\n2,Bob,N/A\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            "N/A",
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.NONE
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertTrue(page.getBlock(1).isNull(0));
            assertTrue(page.getBlock(2).isNull(1));
        }
    }

    public void testBackslashNAsNullValue() throws IOException {
        String csv = "id:long,name:keyword\n1,\\N\n2,Bob\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            '\0',
            CsvFormatOptions.DEFAULT.commentPrefix(),
            "\\N",
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.NONE
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertTrue(page.getBlock(1).isNull(0));
            assertFalse(page.getBlock(1).isNull(1));
        }
    }

    // --- PERMISSIVE Mode (Gap 1) ---

    public void testPermissiveModeNullFillsBadFields() throws IOException {
        String csv = """
            id:long,name:keyword,score:double
            1,Alice,95.5
            not_a_number,Bob,87.3
            3,Charlie,bad_double
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy permissive = new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 100, 0.0, false);

        try (
            CloseableIterator<Page> iterator = reader.read(
                object,
                FormatReadContext.builder().batchSize(10).errorPolicy(permissive).build()
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(2)).getDouble(0), 0.001);
            assertTrue(page.getBlock(0).isNull(1));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(2)).getDouble(1), 0.001);
            assertEquals(3L, ((LongBlock) page.getBlock(0)).getLong(2));
            assertEquals(new BytesRef("Charlie"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(2, new BytesRef()));
            assertTrue(page.getBlock(2).isNull(2));
        }
    }

    public void testPermissiveModeKeepsAllRows() throws IOException {
        String csv = """
            id:long,name:keyword,age:integer
            1,Alice,30
            bad_id,Bob,25
            3,Charlie,not_int
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy permissive = new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 100, 0.0, false);

        try (
            CloseableIterator<Page> iterator = reader.read(
                object,
                FormatReadContext.builder().batchSize(10).errorPolicy(permissive).build()
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertTrue(page.getBlock(0).isNull(1));
            assertEquals(3L, ((LongBlock) page.getBlock(0)).getLong(2));
            assertTrue(page.getBlock(2).isNull(2));
        }
    }

    public void testPermissiveModeErrorBudgetExceeded() {
        String csv = """
            id:long,name:keyword
            1,Alice
            bad2,Bob
            bad3,Charlie
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy permissive = new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 1, 0.0, false);

        EsqlIllegalArgumentException e = expectThrows(EsqlIllegalArgumentException.class, () -> {
            try (
                CloseableIterator<Page> iterator = reader.read(
                    object,
                    FormatReadContext.builder().batchSize(10).errorPolicy(permissive).build()
                )
            ) {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }
        });
        assertTrue(e.getMessage().contains("error budget exceeded"));
    }

    public void testPermissiveModeMultipleBadFieldsInSameRow() throws IOException {
        String csv = """
            id:long,name:keyword,age:integer,active:boolean
            1,Alice,30,true
            bad_id,valid_name,bad_age,bad_bool
            3,Charlie,25,false
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy permissive = new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 100, 0.0, false);

        try (
            CloseableIterator<Page> iterator = reader.read(
                object,
                FormatReadContext.builder().batchSize(10).errorPolicy(permissive).build()
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(30, ((IntBlock) page.getBlock(2)).getInt(0));
            assertFalse(page.getBlock(3).isNull(0));
            assertTrue(((BooleanBlock) page.getBlock(3)).getBoolean(0));
            assertTrue(page.getBlock(0).isNull(1));
            assertEquals(new BytesRef("valid_name"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
            assertTrue(page.getBlock(2).isNull(1));
            assertTrue(page.getBlock(3).isNull(1));
            assertEquals(3L, ((LongBlock) page.getBlock(0)).getLong(2));
            assertEquals(new BytesRef("Charlie"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(2, new BytesRef()));
        }
    }

    // --- Custom Datetime Format (Gap 6) ---

    public void testCustomDatetimeFormat() throws IOException {
        String csv = "id:long,ts:datetime\n1,2021-01-15 14:30:00\n2,2022-06-20 09:00:00\n";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            formatter,
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.NONE
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(Instant.parse("2021-01-15T14:30:00Z").toEpochMilli(), ((LongBlock) page.getBlock(1)).getLong(0));
            assertEquals(Instant.parse("2022-06-20T09:00:00Z").toEpochMilli(), ((LongBlock) page.getBlock(1)).getLong(1));
        }
    }

    // --- withConfig() Integration Tests ---

    public void testWithConfigDelimiter() throws IOException {
        String csv = "id:long|name:keyword\n1|Alice\n2|Bob\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader baseReader = new CsvFormatReader(blockFactory);
        FormatReader configured = baseReader.withConfig(Map.of("delimiter", "|"));

        try (CloseableIterator<Page> iterator = configured.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
        }
    }

    public void testWithConfigMultipleOptions() throws IOException {
        String csv = "id:long;name:keyword\n1;N/A\n2;Bob\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader baseReader = new CsvFormatReader(blockFactory);
        FormatReader configured = baseReader.withConfig(Map.of("delimiter", ";", "null_value", "N/A", "comment", "#"));

        try (CloseableIterator<Page> iterator = configured.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertTrue(page.getBlock(1).isNull(0));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
        }
    }

    public void testWithConfigEmptyReturnsSameReader() {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        FormatReader result = reader.withConfig(Map.of());
        assertSame(reader, result);
    }

    public void testWithConfigNullReturnsSameReader() {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        FormatReader result = reader.withConfig(null);
        assertSame(reader, result);
    }

    public void testWithConfigMultiValueSyntax() throws IOException {
        String csv = "id:integer,values:integer\n1,\"[1,2,3]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader baseReader = new CsvFormatReader(blockFactory);
        FormatReader configured = baseReader.withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = configured.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            IntBlock valuesBlock = (IntBlock) page.getBlock(1);
            assertEquals(3, valuesBlock.getValueCount(0));
            assertEquals(1, valuesBlock.getInt(valuesBlock.getFirstValueIndex(0)));
            assertEquals(2, valuesBlock.getInt(valuesBlock.getFirstValueIndex(0) + 1));
            assertEquals(3, valuesBlock.getInt(valuesBlock.getFirstValueIndex(0) + 2));
        }
    }

    public void testWithConfigMultiValueSyntaxNone() {
        String csv = "id:integer,values:integer\n1,\"[1,2]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader baseReader = new CsvFormatReader(blockFactory);
        FormatReader configured = baseReader.withConfig(Map.of("multi_value_syntax", "none"));

        EsqlIllegalArgumentException e = expectThrows(EsqlIllegalArgumentException.class, () -> {
            try (CloseableIterator<Page> iterator = configured.read(object, null, 10)) {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }
        });
        assertTrue(e.getMessage().contains("Failed to parse CSV value"));
    }

    // --- Multi-value bracket syntax tests ---

    public void testMultiValueBracketsInteger() throws IOException {
        String csv = "id:integer,values:integer\n1,\"[1,2,3]\"\n2,\"[10,20]\"\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.BRACKETS
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            IntBlock valuesBlock = (IntBlock) page.getBlock(1);
            assertEquals(3, valuesBlock.getValueCount(0));
            assertEquals(1, valuesBlock.getInt(valuesBlock.getFirstValueIndex(0)));
            assertEquals(2, valuesBlock.getInt(valuesBlock.getFirstValueIndex(0) + 1));
            assertEquals(3, valuesBlock.getInt(valuesBlock.getFirstValueIndex(0) + 2));
            assertEquals(2, valuesBlock.getValueCount(1));
            assertEquals(10, valuesBlock.getInt(valuesBlock.getFirstValueIndex(1)));
            assertEquals(20, valuesBlock.getInt(valuesBlock.getFirstValueIndex(1) + 1));
        }
    }

    public void testMultiValueBracketsKeyword() throws IOException {
        String csv = "id:integer,tags:keyword\n1,\"[hello,world]\"\n2,\"[foo,bar,baz]\"\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.BRACKETS
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            BytesRefBlock valuesBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(2, valuesBlock.getValueCount(0));
            assertEquals(new BytesRef("hello"), valuesBlock.getBytesRef(valuesBlock.getFirstValueIndex(0), new BytesRef()));
            assertEquals(new BytesRef("world"), valuesBlock.getBytesRef(valuesBlock.getFirstValueIndex(0) + 1, new BytesRef()));
            assertEquals(3, valuesBlock.getValueCount(1));
        }
    }

    public void testMultiValueBracketsBoolean() throws IOException {
        String csv = "id:integer,flags:boolean\n1,\"[true,false]\"\n2,\"[false]\"\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.BRACKETS
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            BooleanBlock valuesBlock = (BooleanBlock) page.getBlock(1);
            assertEquals(2, valuesBlock.getValueCount(0));
            assertTrue(valuesBlock.getBoolean(valuesBlock.getFirstValueIndex(0)));
            assertFalse(valuesBlock.getBoolean(valuesBlock.getFirstValueIndex(0) + 1));
            assertEquals(1, valuesBlock.getValueCount(1));
            assertFalse(valuesBlock.getBoolean(valuesBlock.getFirstValueIndex(1)));
        }
    }

    public void testMultiValueBracketsDouble() throws IOException {
        String csv = "id:integer,scores:double\n1,\"[1.5,-2.3]\"\n2,\"[0.0,3.14]\"\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.BRACKETS
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            DoubleBlock valuesBlock = (DoubleBlock) page.getBlock(1);
            assertEquals(2, valuesBlock.getValueCount(0));
            assertEquals(1.5, valuesBlock.getDouble(valuesBlock.getFirstValueIndex(0)), 0.001);
            assertEquals(-2.3, valuesBlock.getDouble(valuesBlock.getFirstValueIndex(0) + 1), 0.001);
        }
    }

    public void testMultiValueBracketsDatetime() throws IOException {
        String csv = "id:integer,ts:datetime\n1,\"[2024-01-01T00:00:00Z,2024-06-15T12:00:00Z]\"\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.BRACKETS
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            LongBlock tsBlock = (LongBlock) page.getBlock(1);
            assertEquals(2, tsBlock.getValueCount(0));
            assertEquals(Instant.parse("2024-01-01T00:00:00Z").toEpochMilli(), tsBlock.getLong(tsBlock.getFirstValueIndex(0)));
            assertEquals(Instant.parse("2024-06-15T12:00:00Z").toEpochMilli(), tsBlock.getLong(tsBlock.getFirstValueIndex(0) + 1));
        }
    }

    public void testMultiValueEmptyBrackets() throws IOException {
        String csv = "id:integer,values:integer\n1,\"[]\"\n2,\"[1,2]\"\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.BRACKETS
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            IntBlock valuesBlock = (IntBlock) page.getBlock(1);
            assertTrue(valuesBlock.isNull(0));
            assertEquals(2, valuesBlock.getValueCount(1));
        }
    }

    public void testMultiValueSingleElement() throws IOException {
        String csv = "id:integer,values:integer\n1,\"[42]\"\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.BRACKETS
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            IntBlock valuesBlock = (IntBlock) page.getBlock(1);
            assertEquals(1, valuesBlock.getValueCount(0));
            assertEquals(42, valuesBlock.getInt(valuesBlock.getFirstValueIndex(0)));
        }
    }

    public void testMultiValuePipeSeparatedElements() throws IOException {
        String csv = "id:integer,data:keyword\n1,\"[a|b,c]\"\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.BRACKETS
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            BytesRefBlock dataBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(2, dataBlock.getValueCount(0));
            assertEquals(new BytesRef("a|b"), dataBlock.getBytesRef(dataBlock.getFirstValueIndex(0), new BytesRef()));
            assertEquals(new BytesRef("c"), dataBlock.getBytesRef(dataBlock.getFirstValueIndex(0) + 1, new BytesRef()));
        }
    }

    /**
     * Bracket-aware line splitting: escaped delimiter ({@code \,}) is treated as literal,
     * so the comma does not split the column. The cell value preserves the escape sequence.
     */
    public void testEscapedDelimiterInLine() throws IOException {
        String csv = "id:long,data:keyword\n1,a\\,b\n2,normal\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("a\\,b"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(new BytesRef("normal"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
        }
    }

    /**
     * Escaped delimiter inside quoted field: {@code "a\,b"} yields literal comma in the cell.
     */
    public void testEscapedDelimiterInQuotedField() throws IOException {
        String csv = "id:long,data:keyword\n1,\"a\\,b\"\n2,normal\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(new BytesRef("a,b"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("normal"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
        }
    }

    public void testMultiValueEscapedComma() throws IOException {
        String csv = "id:integer,data:keyword\n1,\"[a\\\\,b,c]\"\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.BRACKETS
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            BytesRefBlock dataBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(2, dataBlock.getValueCount(0));
            assertEquals(new BytesRef("a,b"), dataBlock.getBytesRef(dataBlock.getFirstValueIndex(0), new BytesRef()));
            assertEquals(new BytesRef("c"), dataBlock.getBytesRef(dataBlock.getFirstValueIndex(0) + 1, new BytesRef()));
        }
    }

    public void testMultiValueEnabledByDefault() throws IOException {
        String csv = "id:integer,values:integer\n1,\"[1,2]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            IntBlock valuesBlock = (IntBlock) page.getBlock(1);
            assertEquals(2, valuesBlock.getValueCount(0));
            assertEquals(1, valuesBlock.getInt(valuesBlock.getFirstValueIndex(0)));
            assertEquals(2, valuesBlock.getInt(valuesBlock.getFirstValueIndex(0) + 1));
        }
    }

    public void testMultiValueExplicitlyDisabled() {
        String csv = "id:integer,values:integer\n1,\"[1,2]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.NONE
        );
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        EsqlIllegalArgumentException e = expectThrows(EsqlIllegalArgumentException.class, () -> {
            try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }
        });
        assertTrue(e.getMessage().contains("Failed to parse CSV value"));
    }

    public void testMultiValueBracketsQuotedStrings() throws IOException {
        String csv = "id:integer,names:keyword\n1,\"[\"\"foo\"\",\"\"bar\"\"]\"\n2,\"[\"\"hello world\"\",\"\"test\"\"]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            BytesRefBlock namesBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(2, namesBlock.getValueCount(0));
            assertEquals(new BytesRef("foo"), namesBlock.getBytesRef(namesBlock.getFirstValueIndex(0), new BytesRef()));
            assertEquals(new BytesRef("bar"), namesBlock.getBytesRef(namesBlock.getFirstValueIndex(0) + 1, new BytesRef()));
            assertEquals(2, namesBlock.getValueCount(1));
            assertEquals(new BytesRef("hello world"), namesBlock.getBytesRef(namesBlock.getFirstValueIndex(1), new BytesRef()));
            assertEquals(new BytesRef("test"), namesBlock.getBytesRef(namesBlock.getFirstValueIndex(1) + 1, new BytesRef()));
        }
    }

    /**
     * Loads employees.csv from ESQL test fixtures. Verifies 23 columns in header and each row.
     * The file has multi-value fields like {@code [Senior Python Developer,Accountant]}.
     */
    public void testEmployeesCsvWithMultiValues() throws IOException {
        String csv = new String(CsvTestsDataLoader.getResourceStream("/data/employees.csv").readAllBytes(), StandardCharsets.UTF_8);
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(23, schema.size());

        int rowCount = 0;
        try (CloseableIterator<Page> iterator = reader.read(object, null, 100)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                assertEquals(23, page.getBlockCount());
                rowCount += page.getPositionCount();
            }
        }
        assertEquals(100, rowCount);
    }

    /**
     * CSV with comma delimiter: {@code a,[hello,world],c} parses the middle cell as one column
     * whose value {@code [hello,world]} yields two multi-values: hello and world.
     */
    public void testMultiValueBracketsInMultiColumnRow() throws IOException {
        String csv = "prefix:keyword,tags:keyword,suffix:keyword\nx,[hello,world],y\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            BytesRefBlock prefixBlock = page.getBlock(0);
            BytesRefBlock tagsBlock = page.getBlock(1);
            BytesRefBlock suffixBlock = page.getBlock(2);
            assertEquals(new BytesRef("x"), prefixBlock.getBytesRef(0, new BytesRef()));
            assertEquals(2, tagsBlock.getValueCount(0));
            assertEquals(new BytesRef("hello"), tagsBlock.getBytesRef(tagsBlock.getFirstValueIndex(0), new BytesRef()));
            assertEquals(new BytesRef("world"), tagsBlock.getBytesRef(tagsBlock.getFirstValueIndex(0) + 1, new BytesRef()));
            assertEquals(new BytesRef("y"), suffixBlock.getBytesRef(0, new BytesRef()));
        }
    }

    public void testMultiValueBracketsQuotedElements() throws IOException {
        String csv = "id:integer,names:keyword\n1,\"[\"\"hello\"\",\"\"world\"\"]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            BytesRefBlock namesBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(2, namesBlock.getValueCount(0));
            assertEquals(new BytesRef("hello"), namesBlock.getBytesRef(namesBlock.getFirstValueIndex(0), new BytesRef()));
            assertEquals(new BytesRef("world"), namesBlock.getBytesRef(namesBlock.getFirstValueIndex(0) + 1, new BytesRef()));
        }
    }

    public void testMultiValueBracketsMixedQuotedUnquoted() throws IOException {
        String csv = "id:integer,data:keyword\n1,\"[hello,\"\"world,world\"\"]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            BytesRefBlock dataBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(2, dataBlock.getValueCount(0));
            assertEquals(new BytesRef("hello"), dataBlock.getBytesRef(dataBlock.getFirstValueIndex(0), new BytesRef()));
            assertEquals(new BytesRef("world,world"), dataBlock.getBytesRef(dataBlock.getFirstValueIndex(0) + 1, new BytesRef()));
        }
    }

    public void testMultiValueBracketsQuotedWithEscapedQuote() throws IOException {
        String csv = "id:integer,data:keyword\n1,\"[\"\"say \"\"\"\"hi\"\"\"\"\"\"]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            BytesRefBlock dataBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(1, dataBlock.getValueCount(0));
            assertEquals(new BytesRef("say \"hi\""), dataBlock.getBytesRef(dataBlock.getFirstValueIndex(0), new BytesRef()));
        }
    }

    public void testMultiValueBracketsLong() throws IOException {
        String csv = "id:integer,values:long\n1,\"[100000000000,200000000000]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            LongBlock valuesBlock = (LongBlock) page.getBlock(1);
            assertEquals(2, valuesBlock.getValueCount(0));
            assertEquals(100000000000L, valuesBlock.getLong(valuesBlock.getFirstValueIndex(0)));
            assertEquals(200000000000L, valuesBlock.getLong(valuesBlock.getFirstValueIndex(0) + 1));
        }
    }

    public void testMultiValueMixedWithScalar() throws IOException {
        String csv = "id:integer,values:integer\n1,\"[1,2]\"\n2,42\n3,\"[10,20,30]\"\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.BRACKETS
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());
            IntBlock valuesBlock = (IntBlock) page.getBlock(1);
            assertEquals(2, valuesBlock.getValueCount(0));
            assertEquals(1, valuesBlock.getInt(valuesBlock.getFirstValueIndex(0)));
            assertEquals(2, valuesBlock.getInt(valuesBlock.getFirstValueIndex(0) + 1));
            assertEquals(1, valuesBlock.getValueCount(1));
            assertEquals(42, valuesBlock.getInt(valuesBlock.getFirstValueIndex(1)));
            assertEquals(3, valuesBlock.getValueCount(2));
            assertEquals(10, valuesBlock.getInt(valuesBlock.getFirstValueIndex(2)));
        }
    }

    public void testMultiValueWithErrorPolicyNullField() throws IOException {
        String csv = "id:integer,values:integer\n1,\"[1,bad,3]\"\n2,\"[10,20]\"\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.BRACKETS
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);
        ErrorPolicy permissive = new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 100, 0.0, false);

        try (
            CloseableIterator<Page> iterator = reader.read(
                object,
                FormatReadContext.builder().batchSize(10).errorPolicy(permissive).build()
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            IntBlock valuesBlock = (IntBlock) page.getBlock(1);
            assertTrue(valuesBlock.isNull(0));
            assertEquals(2, valuesBlock.getValueCount(1));
        }
    }

    public void testMultiValueWithErrorPolicySkipRow() throws IOException {
        String csv = "id:integer,values:integer\n1,\"[1,bad,3]\"\n2,\"[10,20]\"\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.BRACKETS
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);
        ErrorPolicy skipRow = new ErrorPolicy(10, true);

        try (
            CloseableIterator<Page> iterator = reader.read(object, FormatReadContext.builder().batchSize(10).errorPolicy(skipRow).build())
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            IntBlock valuesBlock = (IntBlock) page.getBlock(1);
            assertEquals(2, valuesBlock.getValueCount(0));
            assertEquals(10, valuesBlock.getInt(valuesBlock.getFirstValueIndex(0)));
            assertEquals(20, valuesBlock.getInt(valuesBlock.getFirstValueIndex(0) + 1));
        }
    }

    // --- Randomized Tests ---

    public void testRandomizedDelimiterColumnRowBatchCount() throws IOException {
        char delimiter = randomFrom(',', '\t', '|', ';');
        int numCols = between(2, 10);
        int numRows = between(1, 100);
        int batchSize = between(1, 20);
        StringBuilder schema = new StringBuilder();
        StringBuilder data = new StringBuilder();
        for (int c = 0; c < numCols; c++) {
            if (c > 0) schema.append(delimiter);
            schema.append("c").append(c).append(":long");
        }
        schema.append("\n");
        for (int r = 0; r < numRows; r++) {
            for (int c = 0; c < numCols; c++) {
                if (c > 0) data.append(delimiter);
                data.append(r * 1000L + c);
            }
            data.append("\n");
        }
        CsvFormatOptions options = new CsvFormatOptions(
            delimiter,
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.NONE
        );
        StorageObject object = createStorageObject(schema.append(data).toString());
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        int totalRows = 0;
        try (CloseableIterator<Page> iterator = reader.read(object, null, batchSize)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
                for (int i = 0; i < page.getPositionCount(); i++) {
                    int rowIdx = totalRows - page.getPositionCount() + i;
                    long expected = rowIdx * 1000L;
                    assertEquals(expected, ((LongBlock) page.getBlock(0)).getLong(i));
                }
            }
        }
        assertEquals(numRows, totalRows);
    }

    public void testRandomizedErrorInjectionDropMalformed() throws IOException {
        int numRows = between(10, 50);
        int numBad = between(1, 5);
        StringBuilder csv = new StringBuilder("id:long,name:keyword\n");
        for (int i = 1; i <= numRows; i++) {
            if (i <= numBad) {
                csv.append("bad").append(i).append(",Name").append(i).append("\n");
            } else {
                csv.append(i).append(",Name").append(i).append("\n");
            }
        }
        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy dropMalformed = new ErrorPolicy(numBad + 10, false);

        int totalRows = 0;
        try (
            CloseableIterator<Page> iterator = reader.read(
                object,
                FormatReadContext.builder().batchSize(between(2, 15)).errorPolicy(dropMalformed).build()
            )
        ) {
            while (iterator.hasNext()) {
                totalRows += iterator.next().getPositionCount();
            }
        }
        assertEquals(numRows - numBad, totalRows);
    }

    public void testRandomizedErrorInjectionPermissive() throws IOException {
        int numRows = between(10, 50);
        int numBadId = between(1, 3);
        StringBuilder csv = new StringBuilder("id:long,name:keyword,score:double\n");
        for (int i = 1; i <= numRows; i++) {
            if (i <= numBadId) {
                csv.append("bad").append(i).append(",Name").append(i).append(",").append(90.0 + i).append("\n");
            } else {
                csv.append(i).append(",Name").append(i).append(",").append(90.0 + i).append("\n");
            }
        }
        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy permissive = new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 100, 0.0, false);

        int totalRows = 0;
        try (
            CloseableIterator<Page> iterator = reader.read(
                object,
                FormatReadContext.builder().batchSize(between(2, 15)).errorPolicy(permissive).build()
            )
        ) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
                for (int i = 0; i < page.getPositionCount(); i++) {
                    int rowIdx = totalRows - page.getPositionCount() + i;
                    if (rowIdx < numBadId) {
                        assertTrue("Row " + rowIdx + " with bad id should have null id", page.getBlock(0).isNull(i));
                    } else {
                        assertEquals((long) (rowIdx + 1), ((LongBlock) page.getBlock(0)).getLong(i));
                    }
                }
            }
        }
        assertEquals(numRows, totalRows);
    }

    // --- findNextRecordBoundary tests ---

    public void testFindNextRecordBoundarySimpleNewline() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "field1,field2\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length, reader.findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextRecordBoundaryCRLF() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "field1,field2\r\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.findNextRecordBoundary(new ByteArrayInputStream(data));
        assertEquals("field1,field2\r\n".indexOf('\n') + 1, boundary);
    }

    public void testFindNextRecordBoundaryNewlineInsideQuotes() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "\"field\nwith\nnewlines\",other\nreal_boundary\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.findNextRecordBoundary(new ByteArrayInputStream(data));
        int expected = "\"field\nwith\nnewlines\",other\n".length();
        assertEquals(expected, boundary);
    }

    public void testFindNextRecordBoundaryEscapedQuotes() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "\"value with \"\"escaped\"\" quotes\"\nrest\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.findNextRecordBoundary(new ByteArrayInputStream(data));
        int expected = "\"value with \"\"escaped\"\" quotes\"\n".length();
        assertEquals(expected, boundary);
    }

    public void testFindNextRecordBoundaryEofNoNewline() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "field1,field2".getBytes(StandardCharsets.UTF_8);
        assertEquals(-1, reader.findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextRecordBoundaryEmptyStream() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        assertEquals(-1, reader.findNextRecordBoundary(new ByteArrayInputStream(new byte[0])));
    }

    public void testFindNextRecordBoundaryAtBufferBoundary() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] padding = new byte[8191];
        Arrays.fill(padding, (byte) 'x');
        byte[] suffix = "\nmore\n".getBytes(StandardCharsets.UTF_8);
        byte[] data = new byte[padding.length + suffix.length];
        System.arraycopy(padding, 0, data, 0, padding.length);
        System.arraycopy(suffix, 0, data, padding.length, suffix.length);
        long boundary = reader.findNextRecordBoundary(new ByteArrayInputStream(data));
        assertEquals(8192, boundary);
    }

    public void testFindNextRecordBoundaryQuoteAtBufferEdge() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] padding = new byte[8191];
        Arrays.fill(padding, (byte) 'a');
        padding[0] = (byte) '"';
        byte[] suffix = "\"\nrest\n".getBytes(StandardCharsets.UTF_8);
        byte[] data = new byte[padding.length + suffix.length];
        System.arraycopy(padding, 0, data, 0, padding.length);
        System.arraycopy(suffix, 0, data, padding.length, suffix.length);
        long boundary = reader.findNextRecordBoundary(new ByteArrayInputStream(data));
        assertEquals(padding.length + 2, boundary);
    }

    // --- Warning tests ---

    public void testWarningsIncludeRowNumber() throws IOException {
        String csv = """
            id:long,name:keyword
            1,Alice
            bad_id,Bob
            3,Charlie
            bad_id2,Dan
            5,Eve
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy lenient = new ErrorPolicy(100, true);

        try (
            CloseableIterator<Page> iterator = reader.read(object, FormatReadContext.builder().batchSize(10).errorPolicy(lenient).build())
        ) {
            while (iterator.hasNext()) {
                iterator.next();
            }
            List<String> warnings = CsvFormatReader.getWarnings(iterator);
            assertEquals(2, warnings.size());
            assertTrue("Warning should include row number, got: " + warnings.get(0), warnings.get(0).startsWith("Row [2]"));
            assertTrue("Warning should include row number, got: " + warnings.get(1), warnings.get(1).startsWith("Row [4]"));
        }
    }

    public void testWarningsIncludeFieldNameInPermissiveMode() throws IOException {
        String csv = """
            id:long,score:double
            1,95.5
            bad_id,not_double
            3,80.0
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy permissive = new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 100, 0.0, false);

        try (
            CloseableIterator<Page> iterator = reader.read(
                object,
                FormatReadContext.builder().batchSize(10).errorPolicy(permissive).build()
            )
        ) {
            while (iterator.hasNext()) {
                iterator.next();
            }
            List<String> warnings = CsvFormatReader.getWarnings(iterator);
            assertEquals(2, warnings.size());
            assertTrue("Warning should include field name, got: " + warnings.get(0), warnings.get(0).contains("field [id]"));
            assertTrue("Warning should include field name, got: " + warnings.get(1), warnings.get(1).contains("field [score]"));
            assertTrue("Warning should include row number, got: " + warnings.get(0), warnings.get(0).contains("Row [2]"));
        }
    }

    public void testWarningsOverflowMessage() throws IOException {
        StringBuilder csv = new StringBuilder("id:long,name:keyword\n");
        for (int i = 1; i <= 30; i++) {
            csv.append("bad").append(i).append(",Name").append(i).append("\n");
        }

        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy lenient = new ErrorPolicy(100, false);

        try (
            CloseableIterator<Page> iterator = reader.read(object, FormatReadContext.builder().batchSize(50).errorPolicy(lenient).build())
        ) {
            while (iterator.hasNext()) {
                iterator.next();
            }
            List<String> warnings = CsvFormatReader.getWarnings(iterator);
            assertEquals(21, warnings.size());
            assertTrue("First warning should have row number, got: " + warnings.get(0), warnings.get(0).startsWith("Row [1]"));
            assertTrue(
                "Last warning should note suppression, got: " + warnings.get(20),
                warnings.get(20).contains("further warnings suppressed")
            );
        }
    }

    // --- Boolean case-insensitive tests (#309) ---

    public void testBooleanCapitalizedTrue() throws IOException {
        String csv = "active:boolean\nTrue\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertTrue(((BooleanBlock) page.getBlock(0)).getBoolean(0));
        }
    }

    public void testBooleanUpperCase() throws IOException {
        String csv = "active:boolean\nTRUE\nFALSE\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertTrue(((BooleanBlock) page.getBlock(0)).getBoolean(0));
            assertFalse(((BooleanBlock) page.getBlock(0)).getBoolean(1));
        }
    }

    public void testBooleanMixedCase() throws IOException {
        String csv = "active:boolean\ntrue\nFalse\nTRUE\nfalse\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(4, page.getPositionCount());
            assertTrue(((BooleanBlock) page.getBlock(0)).getBoolean(0));
            assertFalse(((BooleanBlock) page.getBlock(0)).getBoolean(1));
            assertTrue(((BooleanBlock) page.getBlock(0)).getBoolean(2));
            assertFalse(((BooleanBlock) page.getBlock(0)).getBoolean(3));
        }
    }

    // --- Date-only and zone-less datetime tests (#323) ---

    public void testDatetimeDateOnly() throws IOException {
        String csv = "ts:datetime\n2021-01-01\n2022-06-15\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(Instant.parse("2021-01-01T00:00:00Z").toEpochMilli(), ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(Instant.parse("2022-06-15T00:00:00Z").toEpochMilli(), ((LongBlock) page.getBlock(0)).getLong(1));
        }
    }

    public void testDatetimeZoneless() throws IOException {
        String csv = "ts:datetime\n2021-01-01T10:30:00\n2022-06-15T08:00:00\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(Instant.parse("2021-01-01T10:30:00Z").toEpochMilli(), ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(Instant.parse("2022-06-15T08:00:00Z").toEpochMilli(), ((LongBlock) page.getBlock(0)).getLong(1));
        }
    }

    public void testDatetimeWhitespaceSeparator() throws IOException {
        String csv = "ts:datetime\n2021-01-01 10:30:00\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(Instant.parse("2021-01-01T10:30:00Z").toEpochMilli(), ((LongBlock) page.getBlock(0)).getLong(0));
        }
    }

    public void testDatetimeMixedFormats() throws IOException {
        long epoch = 1609459200000L;
        String csv = "ts:datetime\n" + epoch + "\n2021-01-01T00:00:00Z\n2021-06-15\n2022-03-01T12:00:00\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(4, page.getPositionCount());
            assertEquals(epoch, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(Instant.parse("2021-01-01T00:00:00Z").toEpochMilli(), ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(Instant.parse("2021-06-15T00:00:00Z").toEpochMilli(), ((LongBlock) page.getBlock(0)).getLong(2));
            assertEquals(Instant.parse("2022-03-01T12:00:00Z").toEpochMilli(), ((LongBlock) page.getBlock(0)).getLong(3));
        }
    }

    // --- Numeric alias tests (#324) ---

    public void testFloatAlias() throws IOException {
        String csv = "val:float\n3.14\n2.71\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(1, schema.size());
        assertEquals(DataType.DOUBLE, schema.get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(3.14, ((DoubleBlock) page.getBlock(0)).getDouble(0), 0.001);
        }
    }

    public void testShortAlias() throws IOException {
        String csv = "val:short\n42\n-7\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(1, schema.size());
        assertEquals(DataType.INTEGER, schema.get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(42, ((IntBlock) page.getBlock(0)).getInt(0));
        }
    }

    public void testByteAlias() throws IOException {
        String csv = "val:byte\n1\n2\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(1, schema.size());
        assertEquals(DataType.INTEGER, schema.get(0).dataType());
    }

    public void testHalfFloatAlias() throws IOException {
        String csv = "val:half_float\n1.5\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(1, schema.size());
        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
    }

    public void testScaledFloatAlias() throws IOException {
        String csv = "val:scaled_float\n99.99\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(1, schema.size());
        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
    }

    public void testFloatShorthandAlias() throws IOException {
        String csv = "val:f\n1.0\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(DataType.DOUBLE, schema.get(0).dataType());
    }

    // --- Plain header / auto-inference tests (#304) ---

    public void testPlainHeaderInfersKeyword() throws IOException {
        String csv = "name,city\nAlice,London\nBob,Paris\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(2, schema.size());
        assertEquals("name", schema.get(0).name());
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals("city", schema.get(1).name());
        assertEquals(DataType.KEYWORD, schema.get(1).dataType());
    }

    public void testPlainHeaderInfersInteger() throws IOException {
        String csv = "name,age\nAlice,30\nBob,25\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(2, schema.size());
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals(DataType.INTEGER, schema.get(1).dataType());
    }

    public void testPlainHeaderInfersMultipleTypes() throws IOException {
        String csv = "name,age,score,active,created\nAlice,30,95.5,true,2021-01-01T00:00:00Z\nBob,25,87.3,false,2022-06-15T12:00:00Z\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(5, schema.size());
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals(DataType.INTEGER, schema.get(1).dataType());
        assertEquals(DataType.DOUBLE, schema.get(2).dataType());
        assertEquals(DataType.BOOLEAN, schema.get(3).dataType());
        assertEquals(DataType.DATETIME, schema.get(4).dataType());
    }

    public void testPlainHeaderReadsData() throws IOException {
        String csv = "name,age,active\nAlice,30,true\nBob,25,false\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(3, page.getBlockCount());
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            assertEquals(30, ((IntBlock) page.getBlock(1)).getInt(0));
            assertTrue(((BooleanBlock) page.getBlock(2)).getBoolean(0));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(1, new BytesRef()));
            assertEquals(25, ((IntBlock) page.getBlock(1)).getInt(1));
            assertFalse(((BooleanBlock) page.getBlock(2)).getBoolean(1));
        }
    }

    public void testPlainHeaderWithNulls() throws IOException {
        String csv = "name,age\nAlice,30\nBob,\nCharlie,25\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(DataType.INTEGER, schema.get(1).dataType());

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());
            assertFalse(page.getBlock(1).isNull(0));
            assertTrue(page.getBlock(1).isNull(1));
            assertFalse(page.getBlock(1).isNull(2));
        }
    }

    public void testPlainHeaderWidensIntegerToLong() throws IOException {
        String csv = "id,big_number\n1,9999999999\n2,42\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(DataType.INTEGER, schema.get(0).dataType());
        assertEquals(DataType.LONG, schema.get(1).dataType());
    }

    public void testPlainHeaderWidensIntegerToDouble() throws IOException {
        String csv = "id,value\n1,3.14\n2,42\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(DataType.DOUBLE, schema.get(1).dataType());
    }

    public void testPlainHeaderBooleanSkipsToKeyword() throws IOException {
        String csv = "flag\ntrue\nnot_a_bool\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testPlainHeaderDatetimeSkipsToKeyword() throws IOException {
        String csv = "ts\n2021-01-01T00:00:00Z\nnot_a_date\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testPlainHeaderAllNullsInferKeyword() throws IOException {
        String csv = "col\nnull\n\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
    }

    public void testPlainHeaderSmallFile() throws IOException {
        String csv = "x\n42\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(DataType.INTEGER, schema.get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(42, ((IntBlock) page.getBlock(0)).getInt(0));
        }
    }

    public void testPlainHeaderProjectedColumns() throws IOException {
        String csv = "name,age,active\nAlice,30,true\nBob,25,false\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("age", "active"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount());
            assertEquals(30, ((IntBlock) page.getBlock(0)).getInt(0));
            assertTrue(((BooleanBlock) page.getBlock(1)).getBoolean(0));
        }
    }

    public void testPlainHeaderLargeFile() throws IOException {
        StringBuilder csv = new StringBuilder("id,value\n");
        for (int i = 1; i <= 200; i++) {
            csv.append(i).append(",").append(i * 10).append("\n");
        }
        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(DataType.INTEGER, schema.get(0).dataType());
        assertEquals(DataType.INTEGER, schema.get(1).dataType());

        int totalRows = 0;
        try (CloseableIterator<Page> iterator = reader.read(object, null, 50)) {
            while (iterator.hasNext()) {
                totalRows += iterator.next().getPositionCount();
            }
        }
        assertEquals(200, totalRows);
    }

    public void testPlainHeaderDateOnlyInference() throws IOException {
        String csv = "event,when\nlaunch,2021-01-01\nrelease,2022-06-15\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals(DataType.DATETIME, schema.get(1).dataType());
    }

    public void testPlainHeaderCaseInsensitiveBooleanInference() throws IOException {
        String csv = "flag\nTrue\nFalse\nTRUE\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);
        assertEquals(DataType.BOOLEAN, schema.get(0).dataType());
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
