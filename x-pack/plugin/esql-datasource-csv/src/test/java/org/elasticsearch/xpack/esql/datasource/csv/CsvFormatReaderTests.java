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
import org.elasticsearch.common.logging.HeaderWarning;
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
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.hamcrest.Matchers;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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

    /**
     * Several tests below exercise non-strict {@link ErrorPolicy} paths which now emit response-header
     * warnings via {@link HeaderWarning#addWarning(String, Object[])}. Drop them here so the parent
     * {@code ensureNoWarnings} post-check passes; tests that want to assert on them call
     * {@code drainWarnings()} from inside the test method.
     */
    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            // Swap in a fresh empty context (we deliberately do not restore() - the parent
            // ESTestCase provides a fresh threadContext for the next test, so the stashed one
            // can be discarded).
            threadContext.stashContext();
        }
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

    public void testSchemaWithVersionType() throws IOException {
        String csv = """
            pkg:keyword,ver:version
            lucene,9.10.0
            jackson,2.18.2
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);

        assertEquals(2, schema.size());
        assertEquals("pkg", schema.get(0).name());
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals("ver", schema.get(1).name());
        assertEquals(DataType.VERSION, schema.get(1).dataType());
    }

    public void testSchemaWithVersionShortAlias() throws IOException {
        String csv = "pkg:keyword,ver:v\nlucene,9.10.0\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);

        assertEquals(DataType.VERSION, schema.get(1).dataType());
    }

    public void testReadVersionType() throws IOException {
        String csv = """
            pkg:keyword,ver:version
            lucene,9.10.0
            jackson,2.18.2
            esql,8.15.0-SNAPSHOT
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());
            assertEquals(2, page.getBlockCount());

            BytesRef expectedLucene = EsqlDataTypeConverter.stringToVersion("9.10.0");
            BytesRef expectedJackson = EsqlDataTypeConverter.stringToVersion("2.18.2");
            BytesRef expectedEsql = EsqlDataTypeConverter.stringToVersion("8.15.0-SNAPSHOT");

            assertEquals(new BytesRef("lucene"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            assertEquals(expectedLucene, ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));

            assertEquals(new BytesRef("jackson"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(1, new BytesRef()));
            assertEquals(expectedJackson, ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));

            assertEquals(new BytesRef("esql"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(2, new BytesRef()));
            assertEquals(expectedEsql, ((BytesRefBlock) page.getBlock(1)).getBytesRef(2, new BytesRef()));

            assertFalse(iterator.hasNext());
        }
    }

    public void testMultiValueBracketsVersion() throws IOException {
        String csv = "pkg:keyword,vers:version\n1,\"[1.0.0,2.0.0,3.5.1]\"\n";
        CsvFormatOptions options = new CsvFormatOptions(
            ',',
            CsvFormatOptions.DEFAULT.quoteChar(),
            CsvFormatOptions.DEFAULT.escapeChar(),
            CsvFormatOptions.DEFAULT.commentPrefix(),
            CsvFormatOptions.DEFAULT.nullValue(),
            CsvFormatOptions.DEFAULT.encoding(),
            CsvFormatOptions.DEFAULT.datetimeFormatter(),
            CsvFormatOptions.DEFAULT.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
        );
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            BytesRefBlock valuesBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(3, valuesBlock.getValueCount(0));
            int firstIdx = valuesBlock.getFirstValueIndex(0);
            assertEquals(EsqlDataTypeConverter.stringToVersion("1.0.0"), valuesBlock.getBytesRef(firstIdx, new BytesRef()));
            assertEquals(EsqlDataTypeConverter.stringToVersion("2.0.0"), valuesBlock.getBytesRef(firstIdx + 1, new BytesRef()));
            assertEquals(EsqlDataTypeConverter.stringToVersion("3.5.1"), valuesBlock.getBytesRef(firstIdx + 2, new BytesRef()));
        }
    }

    public void testSchemaWithDateNanosType() throws IOException {
        String csv = """
            event:keyword,ts:date_nanos
            login,2024-01-15T12:34:56.123456789Z
            logout,2024-01-15T12:35:00.000000000Z
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);

        assertEquals(2, schema.size());
        assertEquals("event", schema.get(0).name());
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals("ts", schema.get(1).name());
        assertEquals(DataType.DATE_NANOS, schema.get(1).dataType());
    }

    public void testSchemaWithDateNanosShortAlias() throws IOException {
        String csv = "event:keyword,ts:dn\nlogin,2024-01-15T12:34:56.123456789Z\n";

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        List<Attribute> schema = reader.schema(object);

        assertEquals(DataType.DATE_NANOS, schema.get(1).dataType());
    }

    public void testReadDateNanosType() throws IOException {
        String csv = """
            event:keyword,ts:date_nanos
            login,2024-01-15T12:34:56.123456789Z
            logout,2024-01-15T12:35:00.000000000Z
            raw,1737030896123456789
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());
            assertEquals(2, page.getBlockCount());

            LongBlock tsBlock = (LongBlock) page.getBlock(1);
            assertEquals(EsqlDataTypeConverter.dateNanosToLong("2024-01-15T12:34:56.123456789Z"), tsBlock.getLong(0));
            assertEquals(EsqlDataTypeConverter.dateNanosToLong("2024-01-15T12:35:00.000000000Z"), tsBlock.getLong(1));
            assertEquals(1737030896123456789L, tsBlock.getLong(2));

            assertFalse(iterator.hasNext());
        }
    }

    public void testReadDateNanosNullFieldOnBadValue() throws IOException {
        String csv = """
            event:keyword,ts:date_nanos
            good,2024-01-15T12:34:56.123456789Z
            bad,not-a-date
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
            assertEquals(2, page.getPositionCount());
            LongBlock tsBlock = (LongBlock) page.getBlock(1);
            assertEquals(EsqlDataTypeConverter.dateNanosToLong("2024-01-15T12:34:56.123456789Z"), tsBlock.getLong(0));
            assertTrue(tsBlock.isNull(1));
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

        ParsingException e = expectThrows(ParsingException.class, () -> {
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
        // Malformed user data must surface as HTTP 400, not a 500 server error.
        assertEquals(org.elasticsearch.rest.RestStatus.BAD_REQUEST, e.status());
        // Row index and a capped row excerpt must be present so the user can locate the offending input.
        assertTrue("expected row index, got: " + e.getMessage(), e.getMessage().contains("row [2]"));
        assertTrue("expected row excerpt, got: " + e.getMessage(), e.getMessage().contains("col0=[not_a_number]"));
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

        ParsingException e = expectThrows(ParsingException.class, () -> {
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
        assertEquals(org.elasticsearch.rest.RestStatus.BAD_REQUEST, e.status());
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

        ParsingException e = expectThrows(ParsingException.class, () -> {
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
        assertEquals(org.elasticsearch.rest.RestStatus.BAD_REQUEST, e.status());
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
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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

        ParsingException e = expectThrows(ParsingException.class, () -> {
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
        assertEquals(org.elasticsearch.rest.RestStatus.BAD_REQUEST, e.status());
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
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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

    public void testTsvWithConfigHeaderRowFalseKeepsTabDelimiter() throws IOException {
        String tsv = "a\tb\tc\n1\t2\t3\n";
        StorageObject object = createStorageObject(tsv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory, CsvFormatOptions.TSV, "tsv", List.of(".tsv"))
            .withConfig(Map.of("header_row", false));

        List<Attribute> schema = reader.schema(object);
        assertEquals(3, schema.size());

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(3, page.getBlockCount());
        }
    }

    public void testCsvWithConfigHeaderRowFalseKeepsCommaDelimiter() throws IOException {
        String csv = "10,20\n30,40\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false));

        List<Attribute> schema = reader.schema(object);
        assertEquals(2, schema.size());

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount());
            assertEquals(10, ((IntBlock) page.getBlock(0)).getInt(0));
            assertEquals(30, ((IntBlock) page.getBlock(0)).getInt(1));
        }
    }

    public void testQuotedCsvWithoutHeaderDoesNotSilentlyConsumeFirstDataRowOrQuotedNewLines() throws IOException {
        // Always-quoted CSV with no header. Five data rows.
        String csv = """
            "9110818468285196899","1","hello\\nworld"
            "9110818468285196900","0","world\\rhello"
            "9110818468285196901","1","foo\\r\\nbar"
            "9110818468285196902","0","bar"
            "9110818468285196903","1","baz"
            """;
        StorageObject object = createStorageObject(csv);
        var reader = new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false));

        int rowCount = 0;
        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                rowCount += page.getPositionCount();
            }
        }

        assertEquals(5, rowCount);
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

        ParsingException e = expectThrows(ParsingException.class, () -> {
            try (CloseableIterator<Page> iterator = configured.read(object, null, 10)) {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }
        });
        assertTrue(e.getMessage().contains("Failed to parse CSV value"));
    }

    public void testWithConfigSchemaSampleSizeOverride() throws IOException {
        StringBuilder csv = new StringBuilder();
        csv.append("id,value\n");
        for (int i = 0; i < 200; i++) {
            csv.append(i).append(",text_").append(i).append("\n");
        }
        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader baseReader = new CsvFormatReader(blockFactory);
        FormatReader configured = baseReader.withConfig(Map.of("schema_sample_size", "50"));
        try (CloseableIterator<Page> iterator = configured.read(object, null, 1000)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertTrue(page.getPositionCount() > 0);
        }
    }

    public void testWithConfigSchemaSampleSizeZeroIsRejected() {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        expectThrows(QlIllegalArgumentException.class, () -> reader.withConfig(Map.of("schema_sample_size", "0")));
    }

    public void testWithConfigSchemaSampleSizeNegativeIsRejected() {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        expectThrows(QlIllegalArgumentException.class, () -> reader.withConfig(Map.of("schema_sample_size", "-1")));
    }

    public void testWithConfigSchemaSampleSizeInvalidIsRejected() {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        expectThrows(IllegalArgumentException.class, () -> reader.withConfig(Map.of("schema_sample_size", "abc")));
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
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
        );
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        ParsingException e = expectThrows(ParsingException.class, () -> {
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
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
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

    public void testFindNextRecordBoundaryQuotedNewlineAcrossSplitBoundary() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        // Simulate a split boundary landing inside a quoted field with embedded newlines.
        // The boundary finder must skip past the quoted field to find the real record boundary.
        byte[] data = "partial,\"quoted\nfield\nwith\nnewlines\",end\nnext_record,b,c\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.findNextRecordBoundary(new ByteArrayInputStream(data));
        int expected = "partial,\"quoted\nfield\nwith\nnewlines\",end\n".length();
        assertEquals(expected, boundary);
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
        }
        // 1 summary + 2 details
        List<String> warnings = drainWarnings();
        assertEquals(3, warnings.size());
        assertTrue("Summary should mention skip_row, got: " + warnings.get(0), warnings.get(0).contains("policy: skip_row"));
        assertTrue("Detail should include row number, got: " + warnings.get(1), warnings.get(1).contains("Row [2]"));
        assertTrue("Detail should include row number, got: " + warnings.get(2), warnings.get(2).contains("Row [4]"));
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
        }
        // 1 summary + 2 details
        List<String> warnings = drainWarnings();
        assertEquals(3, warnings.size());
        assertTrue("Summary should mention null_field, got: " + warnings.get(0), warnings.get(0).contains("policy: null_field"));
        assertTrue("Detail should include field name, got: " + warnings.get(1), warnings.get(1).contains("field [id]"));
        assertTrue("Detail should include field name, got: " + warnings.get(2), warnings.get(2).contains("field [score]"));
        assertTrue("Detail should include row number, got: " + warnings.get(1), warnings.get(1).contains("Row [2]"));
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
        }
        // 1 summary + 20 details + 1 overflow
        List<String> warnings = drainWarnings();
        assertEquals(22, warnings.size());
        assertTrue("Summary should mention skip_row, got: " + warnings.get(0), warnings.get(0).contains("policy: skip_row"));
        assertTrue("First detail should have row number, got: " + warnings.get(1), warnings.get(1).contains("Row [1]"));
        assertTrue(
            "Last warning should note suppression, got: " + warnings.get(21),
            warnings.get(21).contains("further warnings suppressed")
        );
    }

    /**
     * Reads the response-header warnings emitted on the test thread and clears them so the
     * {@link ESTestCase#after()} no-warnings post-check passes. Returns the unwrapped warning
     * messages (without the "299 Elasticsearch-... " prefix and surrounding quotes).
     */
    private List<String> drainWarnings() {
        List<String> raw = threadContext.getResponseHeaders().getOrDefault("Warning", List.of());
        List<String> messages = raw.stream().map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, false)).toList();
        threadContext.stashContext();
        return messages;
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

    // --- Headerless CSV (header_row=false) ---

    public void testHeaderlessSchemaUsesDefaultPrefix() throws IOException {
        // No header line; first non-comment row is data with values that would otherwise collide as names.
        String csv = "9110818468285196899,0,\"\",1,2013-07-14T20:38:47Z,42.5,true\n"
            + "9110818468285196900,0,\"\",0,2013-07-14T20:38:48Z,17.0,false\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false));

        List<Attribute> schema = reader.schema(object);

        assertEquals(7, schema.size());
        for (int i = 0; i < schema.size(); i++) {
            assertEquals("col" + i, schema.get(i).name());
        }
        // Per-column inference: long, integer, keyword (empty), integer, datetime, double, boolean.
        assertEquals(DataType.LONG, schema.get(0).dataType());
        assertEquals(DataType.INTEGER, schema.get(1).dataType());
        assertEquals(DataType.KEYWORD, schema.get(2).dataType());
        assertEquals(DataType.INTEGER, schema.get(3).dataType());
        assertEquals(DataType.DATETIME, schema.get(4).dataType());
        assertEquals(DataType.DOUBLE, schema.get(5).dataType());
        assertEquals(DataType.BOOLEAN, schema.get(6).dataType());
    }

    public void testTsvFirstRowWithDatetimeFallsToInferenceNotTypedSchema() throws IOException {
        String tsv = "2021-01-01T00:00:00Z\t42\n2021-01-01T00:00:01Z\t43\n";
        StorageObject object = createStorageObject(tsv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("delimiter", "\t", "header_row", false)
        );

        List<Attribute> schema = reader.schema(object);
        assertEquals(2, schema.size());
        assertEquals("col0", schema.get(0).name());
        assertEquals("col1", schema.get(1).name());
        assertEquals(DataType.DATETIME, schema.get(0).dataType());
        assertEquals(DataType.INTEGER, schema.get(1).dataType());
    }

    public void testHeaderlessTsvWithDatetimeColonsDoesNotThrow() throws IOException {
        String tsv = """
            9110818468285196899\t1\tHello\t2013-07-14 20:38:47\t15900
            1234567890\t0\tWorld\t2013-07-15 10:47:34\t15901
            """;
        StorageObject object = createStorageObject(tsv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("delimiter", "\t", "header_row", false)
        );

        List<Attribute> schema = reader.schema(object);

        assertEquals(5, schema.size());
        assertEquals("col0", schema.get(0).name());
        assertEquals("col1", schema.get(1).name());
        assertEquals("col2", schema.get(2).name());
        assertEquals("col3", schema.get(3).name());
        assertEquals("col4", schema.get(4).name());
        assertEquals(DataType.LONG, schema.get(0).dataType());
        assertEquals(DataType.INTEGER, schema.get(1).dataType());
        assertEquals(DataType.KEYWORD, schema.get(2).dataType());
        assertEquals(DataType.DATETIME, schema.get(3).dataType());
        assertEquals(DataType.INTEGER, schema.get(4).dataType());
    }

    public void testHeaderlessSchemaUsesCustomPrefix() throws IOException {
        String csv = "1,2\n3,4\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("header_row", false, "column_prefix", "f_")
        );

        List<Attribute> schema = reader.schema(object);

        assertEquals(2, schema.size());
        assertEquals("f_0", schema.get(0).name());
        assertEquals("f_1", schema.get(1).name());
    }

    public void testHeaderlessSchemaWithEmptyPrefixYieldsDigitNames() throws IOException {
        // Documented edge case: empty prefix produces names like "0", "1" that need backtick quoting
        // in ES|QL. The reader does not reject this, so verify the names are exactly the digits.
        String csv = "1,2\n3,4\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("header_row", false, "column_prefix", "")
        );

        List<Attribute> schema = reader.schema(object);
        assertEquals(2, schema.size());
        assertEquals("0", schema.get(0).name());
        assertEquals("1", schema.get(1).name());
    }

    public void testHeaderlessReadConsumesRow0AsData() throws IOException {
        // Without header_row=false this would have lost row 0 to the header.
        String csv = "10,20\n30,40\n50,60\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false));

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());
            IntBlock col0 = (IntBlock) page.getBlock(0);
            IntBlock col1 = (IntBlock) page.getBlock(1);
            assertEquals(10, col0.getInt(0));
            assertEquals(30, col0.getInt(1));
            assertEquals(50, col0.getInt(2));
            assertEquals(20, col1.getInt(0));
            assertEquals(40, col1.getInt(1));
            assertEquals(60, col1.getInt(2));
        }
    }

    public void testHeaderlessSkipsCommentLines() throws IOException {
        String csv = "// a comment header\n1,2\n3,4\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false));

        List<Attribute> schema = reader.schema(object);
        assertEquals(2, schema.size());
        assertEquals("col0", schema.get(0).name());
        assertEquals(DataType.INTEGER, schema.get(0).dataType());
    }

    public void testHeaderlessEmptyInputThrows() {
        StorageObject object = createStorageObject("");
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false));

        IOException ex = expectThrows(IOException.class, () -> reader.schema(object));
        assertThat(ex.getMessage().toLowerCase(Locale.ROOT), Matchers.containsString("no data rows"));
    }

    public void testHeaderlessCommentsOnlyInputThrows() {
        // Filtered by comment prefix so no data rows remain — must surface the same clean IOException.
        StorageObject object = createStorageObject("// only comments\n// another\n");
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false));

        IOException ex = expectThrows(IOException.class, () -> reader.schema(object));
        assertThat(ex.getMessage().toLowerCase(Locale.ROOT), Matchers.containsString("no data rows"));
    }

    public void testInvalidHeaderRowOptionThrows() {
        // Bad value rejected at config parse time, not deferred to read.
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", "not_a_bool"))
        );
        assertThat(ex.getMessage(), Matchers.containsString("Invalid boolean value"));
        assertThat(ex.getMessage(), Matchers.containsString("header_row"));
    }

    public void testHeaderlessTreatsTypedSchemaLineAsData() throws IOException {
        // With header_row=false, a row that LOOKS like a typed header (name:type) is data, not schema.
        String csv = "id:long,age:int\n1,30\n2,25\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false));

        List<Attribute> schema = reader.schema(object);
        assertEquals(2, schema.size());
        assertEquals("col0", schema.get(0).name());
        assertEquals("col1", schema.get(1).name());
        // Cells contain "id:long" / "age:int" / "1" / "2" — mixed strings and ints widen to KEYWORD.
        assertEquals(DataType.KEYWORD, schema.get(0).dataType());
        assertEquals(DataType.KEYWORD, schema.get(1).dataType());

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());
            BytesRefBlock col0 = (BytesRefBlock) page.getBlock(0);
            assertEquals(new BytesRef("id:long"), col0.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("1"), col0.getBytesRef(1, new BytesRef()));
            assertEquals(new BytesRef("2"), col0.getBytesRef(2, new BytesRef()));
        }
    }

    public void testHeaderlessReadAcrossMultipleBatches() throws IOException {
        // Exercise the batch-reader path: enough rows to span several pages so prefetchedRows are
        // drained and additional rows are read via the csv iterator after schema inference.
        StringBuilder csv = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            csv.append(i).append(',').append(i * 10).append('\n');
        }
        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false));

        int totalRows = 0;
        try (CloseableIterator<Page> iterator = reader.read(object, null, 7)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                IntBlock col0 = (IntBlock) page.getBlock(0);
                IntBlock col1 = (IntBlock) page.getBlock(1);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    int id = col0.getInt(i);
                    assertEquals(id * 10, col1.getInt(i));
                }
                totalRows += page.getPositionCount();
            }
        }
        assertEquals(50, totalRows);
    }

    public void testHeaderlessProjectedColumns() throws IOException {
        String csv = "10,20,30\n40,50,60\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false));

        // Project col2 first then col0 to exercise reordering.
        try (CloseableIterator<Page> iterator = reader.read(object, List.of("col2", "col0"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount());
            IntBlock first = (IntBlock) page.getBlock(0);
            IntBlock second = (IntBlock) page.getBlock(1);
            assertEquals(30, first.getInt(0));
            assertEquals(60, first.getInt(1));
            assertEquals(10, second.getInt(0));
            assertEquals(40, second.getInt(1));
        }
    }

    public void testHeaderlessCustomPrefixViaReadPath() throws IOException {
        String csv = "1,2\n3,4\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("header_row", false, "column_prefix", "f_")
        );

        // Project by the synthesized name to confirm the runtime path uses the same prefix.
        try (CloseableIterator<Page> iterator = reader.read(object, List.of("f_1"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1, page.getBlockCount());
            IntBlock col = (IntBlock) page.getBlock(0);
            assertEquals(2, col.getInt(0));
            assertEquals(4, col.getInt(1));
        }
    }

    public void testHeaderlessSkipRowOnMalformedData() throws IOException {
        // Need the schema sample to lock col0=INTEGER before the malformed row is read. Cap the
        // sample size to 10 so the bad row at position 30 is encountered only at runtime.
        StringBuilder csv = new StringBuilder();
        for (int i = 1; i <= 30; i++) {
            csv.append(i).append(',').append(i * 100L).append('\n');
        }
        csv.append("not_a_number,42\n");
        csv.append("99,9900\n");
        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("header_row", false, "schema_sample_size", 10)
        );
        ErrorPolicy skipRow = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 10, 1.0, false);

        int totalRows = 0;
        try (
            CloseableIterator<Page> iterator = reader.read(object, FormatReadContext.builder().batchSize(50).errorPolicy(skipRow).build())
        ) {
            while (iterator.hasNext()) {
                totalRows += iterator.next().getPositionCount();
            }
        }
        // 30 good rows + 1 bad (skipped, not counted) + 1 good = 31 total rows returned.
        assertEquals(31, totalRows);
    }

    // --- Duplicate-name guard (header_row=true) ---

    public void testDuplicateInferredHeaderNamesThrowParsingException() {
        // Two columns both literally named "x" in the header row — would otherwise produce duplicate
        // ReferenceAttributes that the post-optimizer verifier rejects with a 500.
        String csv = "x,x,y\n1,2,3\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        ParsingException ex = expectThrows(ParsingException.class, () -> reader.schema(object));
        assertThat(ex.getMessage(), Matchers.containsString("duplicate column names"));
        assertThat(ex.getMessage(), Matchers.containsString("header_row"));
    }

    public void testBlankHeaderNamesThrowParsingException() {
        // Two adjacent commas at the start of the header line produce two empty-string column names
        // — the most common real-world trigger of the duplicate-name path. (A trailing-comma case
        // like ",a," would be String.split-stripped and not hit this guard.)
        String csv = ",,a\n1,2,3\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        ParsingException ex = expectThrows(ParsingException.class, () -> reader.schema(object));
        assertThat(ex.getMessage(), Matchers.containsString("duplicate column names"));
        assertThat(ex.getMessage(), Matchers.containsString("['']"));
    }

    public void testDuplicateTypedHeaderNamesThrowParsingException() {
        String csv = "id:long,id:long\n1,2\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        ParsingException ex = expectThrows(ParsingException.class, () -> reader.schema(object));
        assertThat(ex.getMessage(), Matchers.containsString("duplicate column names"));
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

    // -- Stage 1: structural parse error resilience --
    //
    // Jackson CSV throws on malformed rows (e.g. unclosed quotes). Before, the exception
    // propagated and killed the query. The error policy now intercepts those failures so
    // SKIP_ROW / NULL_FIELD recover and FAIL_FAST surfaces a clear error.

    public void testSkipRowOnUnclosedQuoteJacksonPath() throws IOException {
        // Trailing quote with no closing pair triggers Jackson's "Missing closing quote" error.
        String csv = """
            id:long,name:keyword
            1,Alice
            2,"Bob is broken
            3,Charlie
            """;

        StorageObject object = createStorageObject(csv);
        // Jackson path requires bracketMultiValues=false: use NONE multi-value syntax.
        Map<String, Object> config = Map.of("multi_value_syntax", "NONE");
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(config);
        ErrorPolicy lenient = new ErrorPolicy(10, true);

        List<Long> ids = new ArrayList<>();
        try (
            CloseableIterator<Page> iterator = reader.read(object, FormatReadContext.builder().batchSize(10).errorPolicy(lenient).build())
        ) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                LongBlock idBlock = (LongBlock) page.getBlock(0);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    ids.add(idBlock.getLong(i));
                }
                page.releaseBlocks();
            }
        }
        // The good row 1,Alice must survive; behaviour for rows after the malformed line is
        // defined by Jackson's resync — pin the row that precedes the malformed input so a
        // regression that drops it (e.g. removing the try/catch around csvIterator.next) fails
        // loudly. The tail behaviour is intentionally left untested as it depends on Jackson.
        assertTrue("expected id 1 (Alice) to survive the unclosed-quote row, got " + ids, ids.contains(1L));
    }

    public void testFailFastOnUnclosedQuoteJacksonPath() {
        String csv = """
            id:long,name:keyword
            1,Alice
            2,"Bob is broken
            3,Charlie
            """;

        StorageObject object = createStorageObject(csv);
        Map<String, Object> config = Map.of("multi_value_syntax", "NONE");
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(config);

        ParsingException e = expectThrows(ParsingException.class, () -> {
            try (
                CloseableIterator<Page> iterator = reader.read(
                    object,
                    FormatReadContext.builder().batchSize(10).errorPolicy(ErrorPolicy.STRICT).build()
                )
            ) {
                while (iterator.hasNext()) {
                    iterator.next().releaseBlocks();
                }
            }
        });
        assertTrue("expected CSV parse error, got: " + e.getMessage(), e.getMessage().contains("CSV parse error"));
        assertEquals(org.elasticsearch.rest.RestStatus.BAD_REQUEST, e.status());
    }

    public void testSkipRowOnBracketParseError() throws IOException {
        // Unclosed bracket in last cell is detected by splitLineBracketAware which throws
        // EsqlIllegalArgumentException; the policy must intercept it.
        String csv = """
            id:long,tags:keyword
            1,[a,b,c]
            2,[broken
            3,[x,y]
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        ErrorPolicy lenient = new ErrorPolicy(10, true);

        try (
            CloseableIterator<Page> iterator = reader.read(object, FormatReadContext.builder().batchSize(10).errorPolicy(lenient).build())
        ) {
            int totalRows = 0;
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
            // Rows 1 and 3 must survive the malformed bracketed row in the middle.
            assertEquals(2, totalRows);
        }
    }

    public void testStructuralErrorBudgetEnforced() {
        StringBuilder csv = new StringBuilder("id:long,name:keyword\n");
        for (int i = 0; i < 10; i++) {
            csv.append("1,\"unterminated\n");
        }

        StorageObject object = createStorageObject(csv.toString());
        Map<String, Object> config = Map.of("multi_value_syntax", "NONE");
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(config);
        ErrorPolicy budgetOf2 = new ErrorPolicy(2, false);

        ParsingException e = expectThrows(ParsingException.class, () -> {
            try (
                CloseableIterator<Page> iterator = reader.read(
                    object,
                    FormatReadContext.builder().batchSize(10).errorPolicy(budgetOf2).build()
                )
            ) {
                while (iterator.hasNext()) {
                    iterator.next().releaseBlocks();
                }
            }
        });
        assertTrue("expected budget message, got: " + e.getMessage(), e.getMessage().contains("error budget exceeded"));
        assertEquals(org.elasticsearch.rest.RestStatus.BAD_REQUEST, e.status());
    }

    // -- Stage 2: empty projection (COUNT(*)) fast path --
    //
    // When the optimizer prunes every column (projectedColumns=[]), the reader must emit
    // row-count-only Pages without allocating any blocks or running type conversion.

    public void testEmptyProjectionEmitsRowCountOnlyPages() throws IOException {
        String csv = """
            id:long,name:keyword,score:double
            1,Alice,1.5
            2,Bob,2.5
            3,Charlie,3.5
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (
            CloseableIterator<Page> iterator = reader.read(
                object,
                FormatReadContext.builder().projectedColumns(List.of()).batchSize(10).build()
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());
            assertEquals("COUNT(*) fast path must emit zero blocks", 0, page.getBlockCount());
            page.releaseBlocks();
            assertFalse(iterator.hasNext());
        }
    }

    public void testEmptyProjectionAcrossBatches() throws IOException {
        StringBuilder csv = new StringBuilder("id:long,name:keyword\n");
        int rowCount = 250;
        for (int i = 0; i < rowCount; i++) {
            csv.append(i).append(",Name").append(i).append('\n');
        }

        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (
            CloseableIterator<Page> iterator = reader.read(
                object,
                FormatReadContext.builder().projectedColumns(List.of()).batchSize(50).build()
            )
        ) {
            int totalRows = 0;
            while (iterator.hasNext()) {
                Page page = iterator.next();
                assertEquals("COUNT(*) fast path must emit zero blocks", 0, page.getBlockCount());
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
            assertEquals(rowCount, totalRows);
        }
    }

    public void testEmptyProjectionSkipsExtraColumnRows() throws IOException {
        // COUNT(*) still applies column-width validation; extra-column rows are routed through onRowError.
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
            CloseableIterator<Page> iterator = reader.read(
                object,
                FormatReadContext.builder().projectedColumns(List.of()).batchSize(10).errorPolicy(lenient).build()
            )
        ) {
            int totalRows = 0;
            while (iterator.hasNext()) {
                Page page = iterator.next();
                assertEquals(0, page.getBlockCount());
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
            assertEquals(2, totalRows);
        }
    }

    public void testHeaderlessEmptyProjectionSkipsWiderRowsWhenSchemaIsUnderSampled() throws IOException {
        // Force schema inference to see only the first two rows (2 columns each), then include a wider row.
        // The wider row exceeds the inferred schema width and is rejected via onRowError.
        String csv = "1,Alice\n2,Bob\n3,Charlie,extra\n4,Dina\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("header_row", false, "schema_sample_size", 2)
        );
        ErrorPolicy lenient = new ErrorPolicy(10, true);

        try (
            CloseableIterator<Page> iterator = reader.read(
                object,
                FormatReadContext.builder().projectedColumns(List.of()).batchSize(10).errorPolicy(lenient).build()
            )
        ) {
            int totalRows = 0;
            while (iterator.hasNext()) {
                Page page = iterator.next();
                assertEquals(0, page.getBlockCount());
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
            assertEquals(3, totalRows);
        }
    }

    public void testNullProjectionStillReadsAllColumns() throws IOException {
        // Backward compatibility: projectedColumns=null means "no projection info available",
        // which historically meant "read everything". Stage 2 must not regress this.
        String csv = """
            id:long,name:keyword
            1,Alice
            2,Bob
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, FormatReadContext.builder().batchSize(10).build())) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals("null projection must still produce all columns", 2, page.getBlockCount());
            // Verify column ordering and types are preserved (id:long first, name:keyword second).
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
            page.releaseBlocks();
        }
    }

    // -- Stage 3: recordAligned signal and split semantics --
    //
    // For non-first splits, the CSV reader needs to know whether the leading bytes are a
    // partial record (byte-range macro-split — drop them) or a complete record (streaming
    // chunk — keep them). The recordAligned flag carries that signal.

    public void testRecordAlignedNonFirstSplitDoesNotDropFirstLine() throws IOException {
        // Simulate a streaming-parallel chunk 1..N: bytes start exactly at a record boundary,
        // schema is pre-bound via withSchema(), and the read context is firstSplit=false +
        // recordAligned=true. The reader must NOT skip the first line.
        String csv = "1,Alice\n2,Bob\n3,Charlie\n";
        StorageObject object = createStorageObject(csv);
        List<Attribute> schema = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "id", DataType.LONG, Nullability.TRUE, null, false),
            new ReferenceAttribute(Source.EMPTY, null, "name", DataType.KEYWORD, Nullability.TRUE, null, false)
        );
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withSchema(schema);

        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).firstSplit(false).recordAligned(true).build();
        try (CloseableIterator<Page> iterator = reader.read(object, ctx)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals("recordAligned=true must preserve all 3 rows", 3, page.getPositionCount());
            // Pin the leading row so a regression that re-introduces the line skip on this path
            // (which would surface as id=2 here) fails loudly.
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(3L, ((LongBlock) page.getBlock(0)).getLong(2));
            page.releaseBlocks();
        }
    }

    public void testNonRecordAlignedNonFirstSplitDropsFirstLine() throws IOException {
        // Simulate a bzip2/zstd byte-range macro-split: bytes start mid-record. The leading
        // partial line was already emitted by the previous split, so the reader must drop it.
        String csv = "partial-line-from-previous-split\n2,Bob\n3,Charlie\n";
        StorageObject object = createStorageObject(csv);
        List<Attribute> schema = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "id", DataType.LONG, Nullability.TRUE, null, false),
            new ReferenceAttribute(Source.EMPTY, null, "name", DataType.KEYWORD, Nullability.TRUE, null, false)
        );
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withSchema(schema);

        // recordAligned defaults to false, matching legacy macro-split semantics.
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).firstSplit(false).build();
        try (CloseableIterator<Page> iterator = reader.read(object, ctx)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals("non-record-aligned non-first split must drop leading partial line", 2, page.getPositionCount());
            // Pin the rows we kept: the partial leading line is gone; 2,Bob and 3,Charlie remain.
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(3L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(new BytesRef("Charlie"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
            page.releaseBlocks();
        }
    }

    // -- Stage 4: schema-sampling resilience and chunk-0 use of pre-bound schema --

    /**
     * The streaming-parallel coordinator pre-binds the schema via {@code withSchema} before
     * dispatching chunk 0. Chunk 0 still carries the file header (with {@code header_row=true})
     * or starts at the first data row (with {@code header_row=false}). In both cases the parser
     * must use the bound schema rather than re-running schema inference on chunk-0 bytes —
     * doing so would re-read the entire {@code schema_sample_size} window of rows just to
     * rediscover the same schema, and any malformed row in that window would crash the query.
     */
    public void testFirstSplitWithBoundSchemaSkipsHeaderInsteadOfReinferring() throws IOException {
        String csv = """
            id:long,name:keyword
            1,Alice
            2,Bob
            """;
        StorageObject object = createStorageObject(csv);

        List<Attribute> bound = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "id", DataType.LONG),
            new ReferenceAttribute(Source.EMPTY, null, "name", DataType.KEYWORD)
        );
        FormatReader baseReader = new CsvFormatReader(blockFactory);
        FormatReader boundReader = baseReader.withSchema(bound);

        try (
            CloseableIterator<Page> iterator = boundReader.read(
                object,
                FormatReadContext.builder().firstSplit(true).recordAligned(true).batchSize(10).build()
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
            page.releaseBlocks();
        }
    }

    public void testFirstSplitWithBoundSchemaHeaderlessSkipsNoLine() throws IOException {
        // No header in the file; bound schema names the columns. The header-skip path must
        // be a no-op and the first line "1,Alice" must be returned as data.
        String csv = "1,Alice\n2,Bob\n";
        StorageObject object = createStorageObject(csv);

        List<Attribute> bound = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "col0", DataType.LONG),
            new ReferenceAttribute(Source.EMPTY, null, "col1", DataType.KEYWORD)
        );
        FormatReader headerless = new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false));
        FormatReader bound2 = headerless.withSchema(bound);

        try (
            CloseableIterator<Page> iterator = bound2.read(
                object,
                FormatReadContext.builder().firstSplit(true).recordAligned(true).batchSize(10).build()
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            page.releaseBlocks();
        }
    }

    /**
     * Regression for elastic#148162: a single-shot whole-file read (firstSplit=true,
     * recordAligned=false) must NOT use {@code resolvedSchema} as the iterator's positional
     * schema. The planner calls {@code withSchema(context.attributes())} at operator-factory
     * time with the projected output attributes, not the file's column layout. Treating that
     * list as the file schema would mis-align column indices and trigger spurious
     * "row has [N] columns but schema defines [M] columns" errors on every data row.
     *
     * <p>The bound-schema fast path is reserved for streaming-coordinator dispatch where
     * {@code recordAligned=true} signals that the upstream caller has bound the FULL file
     * schema. For non-streaming reads, the iterator must re-parse the header itself, exactly
     * like the legacy non-bound path.
     */
    public void testFirstSplitWithoutRecordAlignedIgnoresProjectedBoundSchema() throws IOException {
        // 4 columns in the file, projection asks for 2 of them. The planner-bound schema
        // mirrors the projected output; if read() used it as the file schema, every data row
        // (4 cols) would trip the row-shape check against schemaSize=2.
        String csv = """
            id:long,name:keyword,score:double,city:keyword
            1,Alice,10.5,NYC
            2,Bob,20.5,LON
            """;
        StorageObject object = createStorageObject(csv);

        List<Attribute> projectedAttrs = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "id", DataType.LONG),
            new ReferenceAttribute(Source.EMPTY, null, "name", DataType.KEYWORD)
        );
        FormatReader boundReader = new CsvFormatReader(blockFactory).withSchema(projectedAttrs);

        try (
            CloseableIterator<Page> iterator = boundReader.read(
                object,
                // firstSplit=true, recordAligned=false: matches AsyncExternalSourceOperatorFactory's
                // single-shot whole-file read context.
                FormatReadContext.builder()
                    .projectedColumns(List.of("id", "name"))
                    .firstSplit(true)
                    .recordAligned(false)
                    .batchSize(10)
                    .build()
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
            page.releaseBlocks();
        }
    }

    /**
     * Schema sampling honours skip_row the same way the data path does. The malformed row in
     * the middle is dropped, sampling continues, schema is inferred from the surrounding good
     * rows. This exercises the Jackson {@code RuntimeException}-wrapping path (multi_value
     * syntax is set to NONE so the bracket-aware path is bypassed).
     */
    public void testSamplingHonoursSkipRowPolicy() throws IOException {
        // Row 2 has an unclosed quote — Jackson throws on it.
        String csv = "1,Alice\n2,\"Bob\n3,Charlie\n4,Dave\n";
        StorageObject object = createStorageObject(csv);
        FormatReader reader = new CsvFormatReader(blockFactory).withConfig(
            Map.of("header_row", false, "multi_value_syntax", "NONE", "error_mode", "skip_row", "max_errors", 100)
        );

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            // Schema must be inferred from the rows that did parse — query must not 500.
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertTrue("expected at least one row to survive sampling, got " + page.getPositionCount(), page.getPositionCount() >= 1);
            page.releaseBlocks();
        }
    }

    /**
     * Sampling under FAIL_FAST (the default) surfaces the very first malformed row as a
     * client error (HTTP 400), with the row index, capped excerpt, and the hint pointing at
     * skip_row. Same exception type and message shape the data-row path uses.
     */
    public void testSamplingFailFastThrowsClientErrorWithHint() {
        // Every line is malformed (unclosed quote at end). FAIL_FAST throws on row 1.
        StringBuilder csv = new StringBuilder();
        for (int i = 0; i < CsvFormatReader.MAX_CONSECUTIVE_SAMPLING_FAILURES + 4; i++) {
            csv.append(i).append(",\"unterminated\n");
        }
        StorageObject object = createStorageObject(csv.toString());
        FormatReader reader = new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false, "multi_value_syntax", "NONE"));

        ParsingException e = expectThrows(ParsingException.class, () -> {
            try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
                while (iterator.hasNext()) {
                    iterator.next().releaseBlocks();
                }
            }
        });
        assertTrue("expected sampling error message, got: " + e.getMessage(), e.getMessage().contains("CSV schema sampling failed"));
        assertTrue("expected row index, got: " + e.getMessage(), e.getMessage().contains("row [1]"));
        assertTrue("expected skip_row hint, got: " + e.getMessage(), e.getMessage().contains("skip_row"));
        assertEquals(org.elasticsearch.rest.RestStatus.BAD_REQUEST, e.status());
        assertTrue("expected capped message, got " + e.getMessage().length() + " chars", e.getMessage().length() <= 4096);
    }

    /**
     * Sampling under SKIP_ROW with a tight budget exhausts and surfaces a capped budget error.
     * Sampling errors count against the SAME max_errors budget the data path uses, matching
     * ClickHouse's input_format_allow_errors_num semantics.
     */
    public void testSamplingSkipRowExceedsBudget() {
        StringBuilder csv = new StringBuilder();
        // 30 malformed rows, budget of 5 errors → throws after 6th error.
        for (int i = 0; i < 30; i++) {
            csv.append(i).append(",\"unterminated\n");
        }
        StorageObject object = createStorageObject(csv.toString());
        FormatReader reader = new CsvFormatReader(blockFactory).withConfig(
            Map.of("header_row", false, "multi_value_syntax", "NONE", "error_mode", "skip_row", "max_errors", 5)
        );

        ParsingException e = expectThrows(ParsingException.class, () -> {
            try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
                while (iterator.hasNext()) {
                    iterator.next().releaseBlocks();
                }
            }
        });
        assertTrue("expected budget message, got: " + e.getMessage(), e.getMessage().contains("schema sampling exceeded error budget"));
        assertEquals(org.elasticsearch.rest.RestStatus.BAD_REQUEST, e.status());
    }

    /**
     * Sampling under SKIP_ROW with a generous budget but a permanently malformed file bails
     * after MAX_CONSECUTIVE_SAMPLING_FAILURES (the Jackson-resync safety net), surfacing a
     * "no rows could be parsed" error.
     */
    public void testSamplingSkipRowConsecutiveFailureSafetyNet() {
        StringBuilder csv = new StringBuilder();
        for (int i = 0; i < CsvFormatReader.MAX_CONSECUTIVE_SAMPLING_FAILURES + 4; i++) {
            csv.append(i).append(",\"unterminated\n");
        }
        StorageObject object = createStorageObject(csv.toString());
        FormatReader reader = new CsvFormatReader(blockFactory).withConfig(
            Map.of("header_row", false, "multi_value_syntax", "NONE", "error_mode", "skip_row", "max_errors", Long.MAX_VALUE)
        );

        ParsingException e = expectThrows(ParsingException.class, () -> {
            try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
                while (iterator.hasNext()) {
                    iterator.next().releaseBlocks();
                }
            }
        });
        assertTrue("expected zero-rows message, got: " + e.getMessage(), e.getMessage().contains("schema inference failed"));
        assertEquals(org.elasticsearch.rest.RestStatus.BAD_REQUEST, e.status());
    }

    /**
     * Sanity check: the FAIL_FAST hint distinguishes structural errors (suggest skip_row) from
     * field-type errors (suggest null_field). Field-type error: skip_row drops the whole row
     * when one field is bad; null_field is the better escape hatch.
     */
    public void testFailFastHintForFieldTypeErrorMentionsNullField() {
        String csv = """
            id:long,name:keyword
            1,Alice
            not_a_number,Bob
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        ParsingException e = expectThrows(ParsingException.class, () -> {
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
        // Field-type error → the hint should point at null_field specifically.
        assertTrue("expected null_field hint, got: " + e.getMessage(), e.getMessage().contains("null_field"));
        // structural=false should NOT mention skip_row in the recommendation.
        assertFalse("did not expect skip_row hint for field error, got: " + e.getMessage(), e.getMessage().contains("skip_row"));
    }

    /**
     * Sanity check: the FAIL_FAST hint for a structural (tokeniser) error mentions skip_row.
     */
    public void testFailFastHintForStructuralErrorMentionsSkipRow() {
        String csv = """
            id:long,name:keyword
            1,"unterminated row
            """;
        StorageObject object = createStorageObject(csv);
        // Bracket parsing disabled so the Jackson tokeniser fires the structural error.
        FormatReader reader = new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "NONE"));

        ParsingException e = expectThrows(ParsingException.class, () -> {
            try (
                CloseableIterator<Page> iterator = reader.read(
                    object,
                    FormatReadContext.builder().batchSize(10).errorPolicy(ErrorPolicy.STRICT).build()
                )
            ) {
                while (iterator.hasNext()) {
                    iterator.next().releaseBlocks();
                }
            }
        });
        assertTrue("expected skip_row hint, got: " + e.getMessage(), e.getMessage().contains("skip_row"));
    }

    public void testCsvErrorMessagesSummarizeShortValuePassesThrough() {
        assertEquals("hello", CsvErrorMessages.summarize("hello"));
        assertEquals("null", CsvErrorMessages.summarize((String) null));
    }

    public void testCsvErrorMessagesSummarizeLongValueIsCapped() {
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 5_000; i++) {
            huge.append('x');
        }
        String summarized = CsvErrorMessages.summarize(huge.toString());
        assertTrue(
            "expected length <= MAX_EXCERPT_CHARS, got " + summarized.length(),
            summarized.length() <= CsvErrorMessages.MAX_EXCERPT_CHARS
        );
        assertTrue("expected truncation marker, got: " + summarized, summarized.contains("truncated"));
        assertTrue("expected total-length marker, got: " + summarized, summarized.contains("5000"));
    }

    public void testCsvErrorMessagesSummarizeRowEmptyIsSentinel() {
        assertEquals("<unparsed>", CsvErrorMessages.summarizeRow(new String[0]));
        assertEquals("<unparsed>", CsvErrorMessages.summarizeRow(null));
    }

    /**
     * The end-to-end repro for the ClickBench failure: a chunk-0 split with a malformed row
     * deep inside the schema sample window. With the previous code, the parser thread would
     * call {@code inferSchemaHeaderlessFromBatchReader} on chunk 0 even though
     * {@link FormatReader#withSchema} had already bound the schema upstream, hit the bad row,
     * and crash the query with a 500. With the fix, chunk-0 uses the bound schema and emits
     * pages even when sampling on the same bytes would fail.
     */
    public void testStreamingChunkZeroWithBoundSchemaSurvivesMidFileMalformedRow() throws IOException {
        StringBuilder csv = new StringBuilder();
        for (int i = 0; i < 1300; i++) {
            // Inject one malformed row well past 1000 to mimic ClickBench's "line 1246" case.
            if (i == 1246) {
                csv.append(i).append(",\"unterminated row that crashes Jackson\n");
            } else {
                csv.append(i).append(",\"row").append(i).append("\"\n");
            }
        }
        StorageObject object = createStorageObject(csv.toString());

        List<Attribute> bound = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "col0", DataType.LONG),
            new ReferenceAttribute(Source.EMPTY, null, "col1", DataType.KEYWORD)
        );
        FormatReader headerless = new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false, "multi_value_syntax", "NONE"));
        FormatReader bound2 = headerless.withSchema(bound);

        // SKIP_ROW so the malformed row is dropped and the rest of the file flows through.
        ErrorPolicy lenient = new ErrorPolicy(100, true);
        try (
            CloseableIterator<Page> iterator = bound2.read(
                object,
                FormatReadContext.builder().firstSplit(true).recordAligned(true).batchSize(2048).errorPolicy(lenient).build()
            )
        ) {
            int totalRows = 0;
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
            // Most of the 1300 rows must come out; we only lose the malformed one (and possibly a
            // small handful Jackson swallows during recovery). Pre-fix this would throw a 500.
            assertTrue("expected most rows to survive, got " + totalRows, totalRows >= 1295);
        }
    }

    public void testCsvErrorMessagesSummarizeRowFormatsColumns() {
        String summary = CsvErrorMessages.summarizeRow(new String[] { "1", "Alice", null });
        assertTrue("expected col0, got: " + summary, summary.contains("col0=[1]"));
        assertTrue("expected col1, got: " + summary, summary.contains("col1=[Alice]"));
        assertTrue("expected col2, got: " + summary, summary.contains("col2=[null]"));
    }
}
