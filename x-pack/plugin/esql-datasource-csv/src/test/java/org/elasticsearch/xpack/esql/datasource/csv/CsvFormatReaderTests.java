/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
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
import org.elasticsearch.xpack.esql.datasources.DrainSimulatingStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.hamcrest.Matchers;
import org.junit.After;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
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

        for (Attribute attr : schema) {
            assertEquals(Nullability.TRUE, attr.nullable());
        }
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

    /**
     * Early-close leak regression. {@code CsvFormatReader}'s batch iterator buffers one look-ahead page
     * in {@code hasNext()}; before it extended
     * {@link org.elasticsearch.xpack.esql.datasources.spi.BufferingPageIterator} a consumer that closed
     * after {@code hasNext()} but before {@code next()} (a pushed-down {@code LIMIT}, a cancellation, a
     * downstream error) left that page's blocks unreleased against the breaker. These tests drive a real
     * read on a tracking breaker and assert usage returns to zero.
     */
    private static String multiRowCsv(int rows) {
        StringBuilder sb = new StringBuilder("id:integer\n");
        for (int i = 1; i <= rows; i++) {
            sb.append(i).append('\n');
        }
        return sb.toString();
    }

    private static BlockFactory trackingBlockFactory(CircuitBreaker[] outBreaker) {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(64)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        outBreaker[0] = breaker;
        return BlockFactory.builder(bigArrays).breaker(breaker).build();
    }

    public void testCloseAfterHasNextWithoutNextDoesNotLeak() throws IOException {
        CircuitBreaker[] breaker = new CircuitBreaker[1];
        BlockFactory trackingFactory = trackingBlockFactory(breaker);

        StorageObject object = createStorageObject(multiRowCsv(50));
        CsvFormatReader reader = new CsvFormatReader(trackingFactory);
        try (CloseableIterator<Page> iterator = reader.read(object, List.of("id"), 8)) {
            assertTrue(iterator.hasNext()); // materializes (and allocates) the first look-ahead page
            assertThat("hasNext must have buffered a page", breaker[0].getUsed(), Matchers.greaterThan(0L));
            // Abandon without next(): try-with-resources close() must release the buffered page.
        }
        assertEquals("the buffered look-ahead page must be released on early close", 0L, breaker[0].getUsed());
    }

    public void testCloseMidStreamDoesNotLeak() throws IOException {
        CircuitBreaker[] breaker = new CircuitBreaker[1];
        BlockFactory trackingFactory = trackingBlockFactory(breaker);

        StorageObject object = createStorageObject(multiRowCsv(50));
        CsvFormatReader reader = new CsvFormatReader(trackingFactory);
        try (CloseableIterator<Page> iterator = reader.read(object, List.of("id"), 8)) {
            assertTrue(iterator.hasNext());
            iterator.next().releaseBlocks();
            assertTrue(iterator.hasNext());
            iterator.next().releaseBlocks();
            assertTrue(iterator.hasNext()); // buffers a third page that we never consume
            assertThat(breaker[0].getUsed(), Matchers.greaterThan(0L));
        }
        assertEquals("no page may leak when the consumer aborts mid-stream", 0L, breaker[0].getUsed());
    }

    public void testCloseAfterFullConsumptionDoesNotLeak() throws IOException {
        CircuitBreaker[] breaker = new CircuitBreaker[1];
        BlockFactory trackingFactory = trackingBlockFactory(breaker);

        StorageObject object = createStorageObject(multiRowCsv(20));
        CsvFormatReader reader = new CsvFormatReader(trackingFactory);
        int totalRows = 0;
        try (CloseableIterator<Page> iterator = reader.read(object, List.of("id"), 8)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertEquals(20, totalRows);
        assertEquals("draining to exhaustion then closing must leave the breaker at zero", 0L, breaker[0].getUsed());
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

    public void testMultiValueBracketsIp() throws IOException {
        // brackets: an [ip,ip] cell on an :ip column parses into two IP values.
        String csv = "id:integer,addrs:ip\n1,\"[1.1.1.1,8.8.8.8]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = mvcReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            BytesRefBlock addrs = (BytesRefBlock) page.getBlock(1);
            assertEquals(2, addrs.getValueCount(0));
            int idx = addrs.getFirstValueIndex(0);
            assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("1.1.1.1"))), addrs.getBytesRef(idx, new BytesRef()));
            assertEquals(
                new BytesRef(InetAddressPoint.encode(InetAddresses.forString("8.8.8.8"))),
                addrs.getBytesRef(idx + 1, new BytesRef())
            );
        }
    }

    public void testIpNoneReadsBracketsLiterallyAndFailsParse() {
        // none: the same cell is the literal string "[1.1.1.1,8.8.8.8]", which is not a valid IP, so the
        // strict policy surfaces a parse error rather than silently producing a multi-value.
        String csv = "id:integer,addrs:ip\n1,\"[1.1.1.1,8.8.8.8]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = noMvcReader(blockFactory);
        ParsingException e = expectThrows(ParsingException.class, () -> {
            try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Failed to parse CSV value") && e.getMessage().contains("[IP]"));
    }

    public void testMultiValueBracketsDateNanos() throws IOException {
        // brackets: a [date_nanos,date_nanos] cell parses into two nanosecond timestamps.
        String csv = "id:integer,ts:date_nanos\n1,\"[2024-01-15T12:34:56.123456789Z,2024-01-15T12:35:00.000000000Z]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = mvcReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            LongBlock ts = (LongBlock) page.getBlock(1);
            assertEquals(2, ts.getValueCount(0));
            int idx = ts.getFirstValueIndex(0);
            assertEquals(EsqlDataTypeConverter.dateNanosToLong("2024-01-15T12:34:56.123456789Z"), ts.getLong(idx));
            assertEquals(EsqlDataTypeConverter.dateNanosToLong("2024-01-15T12:35:00.000000000Z"), ts.getLong(idx + 1));
        }
    }

    public void testDateNanosNoneReadsBracketsLiterallyAndFailsParse() {
        // none: the same cell is the literal string, which is not a valid date_nanos, so the strict policy
        // surfaces a parse error rather than a multi-value.
        String csv = "id:integer,ts:date_nanos\n1,\"[2024-01-15T12:34:56.123456789Z,2024-01-15T12:35:00.000000000Z]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = noMvcReader(blockFactory);
        ParsingException e = expectThrows(ParsingException.class, () -> {
            try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Failed to parse CSV date_nanos value"));
    }

    public void testMultiValueBracketsManyBatchesVariedCardinality() throws IOException {
        // Varied multi-value data across many pages: int / keyword / ip / date_nanos columns, cardinality
        // cycling 1..3, read at a small batch size so multi-value cells must be assembled across page
        // boundaries. Guards against value misassembly or cardinality drift at batch edges.
        int rows = 60;
        String[] ipPool = { "1.1.1.1", "8.8.8.8", "10.0.0.1" };
        String[] dnPool = { "2024-01-15T12:34:56.123456789Z", "2024-02-20T00:00:00.000000000Z", "2024-03-30T23:59:59.999999999Z" };
        StringBuilder csv = new StringBuilder("id:integer,ints:integer,kws:keyword,addrs:ip,dns:date_nanos\n");
        for (int i = 1; i <= rows; i++) {
            int c = (i - 1) % 3 + 1; // 1, 2, 3
            StringBuilder ints = new StringBuilder();
            StringBuilder kws = new StringBuilder();
            StringBuilder addrs = new StringBuilder();
            StringBuilder dns = new StringBuilder();
            for (int k = 0; k < c; k++) {
                String sep = k == 0 ? "" : ",";
                ints.append(sep).append(i * 10 + k);
                kws.append(sep).append("kw_").append(i).append('_').append(k);
                addrs.append(sep).append(ipPool[k]);
                dns.append(sep).append(dnPool[k]);
            }
            csv.append(i)
                .append(",\"[")
                .append(ints)
                .append("]\"")
                .append(",\"[")
                .append(kws)
                .append("]\"")
                .append(",\"[")
                .append(addrs)
                .append("]\"")
                .append(",\"[")
                .append(dns)
                .append("]\"")
                .append('\n');
        }
        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = mvcReader(blockFactory);

        int pages = 0;
        int globalRow = 0;
        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                pages++;
                IntBlock idB = (IntBlock) page.getBlock(0);
                IntBlock intsB = (IntBlock) page.getBlock(1);
                BytesRefBlock kwsB = (BytesRefBlock) page.getBlock(2);
                BytesRefBlock addrsB = (BytesRefBlock) page.getBlock(3);
                LongBlock dnsB = (LongBlock) page.getBlock(4);
                for (int p = 0; p < page.getPositionCount(); p++) {
                    int i = ++globalRow;
                    int c = (i - 1) % 3 + 1;
                    assertEquals(i, idB.getInt(p));
                    assertEquals("row " + i + " ints cardinality", c, intsB.getValueCount(p));
                    assertEquals(i * 10, intsB.getInt(intsB.getFirstValueIndex(p)));
                    assertEquals("row " + i + " kws cardinality", c, kwsB.getValueCount(p));
                    assertEquals(new BytesRef("kw_" + i + "_0"), kwsB.getBytesRef(kwsB.getFirstValueIndex(p), new BytesRef()));
                    assertEquals("row " + i + " addrs cardinality", c, addrsB.getValueCount(p));
                    assertEquals("row " + i + " dns cardinality", c, dnsB.getValueCount(p));
                }
            }
        }
        assertEquals(rows, globalRow);
        assertTrue("expected multiple pages, got " + pages, pages > 1);
    }

    public void testNoneScalarAllTypesManyBatches() throws IOException {
        // none (no MV): a varied all-types scalar dataset across many pages parses without any
        // multi-value handling. Exercises long / double / boolean / datetime / date_nanos / ip / version
        // scalar reads at a scale that spans several batches.
        int rows = 50;
        String[] ipPool = { "1.1.1.1", "8.8.8.8", "10.0.0.1" };
        StringBuilder csv = new StringBuilder(
            "id:integer,n_long:long,n_dbl:double,kw:keyword,flag:boolean,ts:datetime,dn:date_nanos,addr:ip,ver:version\n"
        );
        for (int i = 1; i <= rows; i++) {
            csv.append(i)
                .append(',')
                .append(1000000000L + i)
                .append(',')
                .append(i)
                .append(".5,")
                .append("name_")
                .append(i)
                .append(',')
                .append(i % 2 == 0)
                .append(',')
                .append("2024-01-01T00:00:00Z,")
                .append("2024-01-15T12:34:56.123456789Z,")
                .append(ipPool[i % 3])
                .append(",1.0.")
                .append(i)
                .append('\n');
        }
        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = noMvcReader(blockFactory);

        int pages = 0;
        int total = 0;
        boolean checkedLatePage = false;
        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                pages++;
                IntBlock idB = (IntBlock) page.getBlock(0);
                LongBlock longB = (LongBlock) page.getBlock(1);
                BytesRefBlock kwB = (BytesRefBlock) page.getBlock(3);
                for (int p = 0; p < page.getPositionCount(); p++) {
                    int i = ++total;
                    assertEquals(i, idB.getInt(p));
                    assertEquals(1000000000L + i, longB.getLong(p));
                    assertEquals(new BytesRef("name_" + i), kwB.getBytesRef(p, new BytesRef()));
                    if (i == 35) {
                        checkedLatePage = true;
                    }
                }
            }
        }
        assertEquals(rows, total);
        assertTrue("expected multiple pages, got " + pages, pages > 1);
        assertTrue("expected to verify a row on a later page", checkedLatePage);
    }

    public void testMultiValueSyntaxDefaultsToNone() {
        // Standard CSV (RFC 4180) has no array/multi-value concept; the default must be NONE so a
        // bracketed cell is literal text, not a parsed multi-value. BRACKETS is opt-in.
        assertEquals(CsvFormatOptions.MultiValueSyntax.NONE, CsvFormatOptions.DEFAULT.multiValueSyntax());
        assertEquals(CsvFormatOptions.MultiValueSyntax.NONE, CsvFormatOptions.TSV.multiValueSyntax());
    }

    public void testMultiValueDefaultReadsBracketsAsLiteral() throws IOException {
        // With the default multi_value_syntax (NONE), a quoted "[a,b,c]" cell is a single literal
        // string — NOT a 3-element multi-value. Mirror of testMultiValueBracketsVersion, which opts
        // into BRACKETS and gets three values from the same shape.
        String csv = "pkg:keyword,tags:keyword\n1,\"[a,b,c]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory); // CsvFormatOptions.DEFAULT (multi_value=NONE)

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            BytesRefBlock tags = (BytesRefBlock) page.getBlock(1);
            assertEquals(1, tags.getValueCount(0)); // one literal value, not three
            assertEquals(new BytesRef("[a,b,c]"), tags.getBytesRef(tags.getFirstValueIndex(0), new BytesRef()));
        }
    }

    public void testTsvDefaultReadsBracketsAsLiteral() throws IOException {
        // TSV defaults to NONE like CSV, so a "[a,b,c]" cell is one literal value. The tab is the field
        // delimiter; the commas inside the cell are not separators of anything under NONE.
        String tsv = "pkg:keyword\ttags:keyword\n1\t[a,b,c]\n";
        StorageObject object = createStorageObject(tsv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(CsvFormatOptions.TSV);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            BytesRefBlock tags = (BytesRefBlock) page.getBlock(1);
            assertEquals(1, tags.getValueCount(0));
            assertEquals(new BytesRef("[a,b,c]"), tags.getBytesRef(tags.getFirstValueIndex(0), new BytesRef()));
        }
    }

    public void testTsvBracketsParsesMultiValue() throws IOException {
        // Bracket multi-values are delimiter-agnostic: the array literal "[a,b,c]" reads the same in TSV
        // (tab-delimited) as in CSV. With multi_value_syntax: brackets, a tab-delimited "[a,b,c]" cell
        // yields three values — the tab field delimiter is irrelevant inside the brackets, and elements
        // are always comma-separated. Mirror of testMultiValueBracketsVersion on the tab delimiter.
        CsvFormatOptions options = new CsvFormatOptions(
            '\t',
            CsvFormatOptions.TSV.quoteChar(),
            CsvFormatOptions.TSV.escapeChar(),
            CsvFormatOptions.TSV.commentPrefix(),
            CsvFormatOptions.TSV.nullValue(),
            CsvFormatOptions.TSV.encoding(),
            CsvFormatOptions.TSV.datetimeFormatter(),
            CsvFormatOptions.TSV.maxFieldSize(),
            CsvFormatOptions.MultiValueSyntax.BRACKETS,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
        );
        String tsv = "pkg:keyword\ttags:keyword\n1\t[a,b,c]\n";
        StorageObject object = createStorageObject(tsv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            BytesRefBlock tags = (BytesRefBlock) page.getBlock(1);
            assertEquals(3, tags.getValueCount(0));
            int firstIdx = tags.getFirstValueIndex(0);
            assertEquals(new BytesRef("a"), tags.getBytesRef(firstIdx, new BytesRef()));
            assertEquals(new BytesRef("b"), tags.getBytesRef(firstIdx + 1, new BytesRef()));
            assertEquals(new BytesRef("c"), tags.getBytesRef(firstIdx + 2, new BytesRef()));
        }
    }

    public void testTsvFusedPathWithMultiValueBrackets() throws IOException {
        // Fused fast path (splitAndConvertProjected) with a tab delimiter: projected read, typed long
        // column, multi-value keyword column. The element split inside [..] is comma-based regardless
        // of the field delimiter, so [hello,world] yields two values even though fields are tab-separated.
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("delimiter", "\t", "multi_value_syntax", "brackets")
        );
        String tsv = "id:long\ttags:keyword\n1\t[hello,world]\n2\t[foo]\n";
        StorageObject object = createStorageObject(tsv);

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("id", "tags"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            BytesRefBlock tagsBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(2, tagsBlock.getValueCount(0));
            assertEquals(1, tagsBlock.getValueCount(1));
        }
    }

    public void testTsvFindNextRecordBoundaryNewlineInsideBracketMvc() throws IOException {
        // The bracket-aware record-boundary scanner must work for tab-delimited input too: a newline
        // inside a [..] cell does not end the record. Exercises findNextRecordBoundaryBracketMvc with
        // a tab field delimiter.
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("delimiter", "\t", "multi_value_syntax", "brackets")
        );
        byte[] data = "before\t[line1\nline2\nline3]\tafter\nnext\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data));
        assertEquals("before\t[line1\nline2\nline3]\tafter\n".length(), boundary);
    }

    public void testTsvFindNextRecordBoundaryNestedBracketMvcWithEmbeddedNewlines() throws IOException {
        // TSV counterpart to testFindNextRecordBoundaryNestedBracketMvcWithEmbeddedNewlines: nested
        // brackets across a tab delimiter with newlines inside. The depth-tracking state machine
        // must not bail at the inner closing bracket.
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("delimiter", "\t", "multi_value_syntax", "brackets")
        );
        byte[] data = "a\t[[cell\ninner]]\tb\nz\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data));
        assertEquals("a\t[[cell\ninner]]\tb\n".length(), boundary);
    }

    public void testTsvFindNextRecordBoundaryMultiValueSyntaxNoneDoesNotTreatBracketsAsMvc() throws IOException {
        // TSV counterpart to the CSV version: under multi_value_syntax: none with a tab delimiter,
        // brackets are literal and a newline inside [..] terminates the record at the first newline.
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("delimiter", "\t", "multi_value_syntax", "none")
        );
        byte[] data = "before\t[not\nmvc]\tafter\nnext\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data));
        assertEquals("before\t[not\n".length(), boundary);
    }

    public void testTsvFindNextRecordBoundaryTwoRecordsStateMachineReset() throws IOException {
        // Cross-call statelessness: the same reader invoked on two separate streams must scan each
        // independently — no carry-over of bracket-depth / quoted-state from a prior call.
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("delimiter", "\t", "multi_value_syntax", "brackets")
        );
        byte[] data = "a\t[x\ny]\tb\nc\t[m\nn]\td\n".getBytes(StandardCharsets.UTF_8);
        long firstBoundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data));
        assertEquals("a\t[x\ny]\tb\n".length(), firstBoundary);
        byte[] tail = "c\t[m\nn]\td\n".getBytes(StandardCharsets.UTF_8);
        long secondBoundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(tail));
        assertEquals(tail.length, secondBoundary);
    }

    public void testTsvFusedPathWithNestedBrackets() throws IOException {
        // Fused fast path (splitAndConvertProjected) on TSV with nested-bracket content. The element
        // splitter inside [..] is a flat comma split, not a depth-tracking parser — so [a,[b,c]]
        // yields three values ("a", "[b", "c]"). The point of the test isn't to validate a parser
        // design choice; it's to confirm the fused path on TSV behaves identically to CSV on this
        // edge case (no delimiter-coupled divergence introduced by the refactor).
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("delimiter", "\t", "multi_value_syntax", "brackets")
        );
        String tsv = "id:long\ttags:keyword\n1\t[a,[b,c]]\n";
        StorageObject object = createStorageObject(tsv);

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("id", "tags"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            BytesRefBlock tagsBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(3, tagsBlock.getValueCount(0));
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
        assertTrue("expected row excerpt, got: " + e.getMessage(), e.getMessage().contains("not_a_number"));
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

    /** Mid-field literal quotes (odd counts) must not suppress the boundary — pre-fix this returned -1. */
    public void testTsvFindLastRecordBoundaryWithLiteralMidFieldQuotes() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(CsvFormatOptions.TSV);
        // one mid-field quote in row 1, three in row 2; none at field start
        String data = "1\ta\"b\tc\n2\td\"e\"f\"g\th\n";
        byte[] buf = data.getBytes(StandardCharsets.UTF_8);

        int boundary = reader.recordSplitter().findLastRecordBoundary(buf, buf.length);
        assertEquals("must find the terminating newline of the last complete record", buf.length - 1, boundary);
        assertEquals('\n', (char) buf[boundary]);
    }

    /** Same, on a 105-column ClickBench-shaped row with dense literal quotes. */
    public void testTsvFindLastRecordBoundaryClickBenchShaped() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(CsvFormatOptions.TSV);
        String row = clickBenchShapedTsvRow();
        byte[] buf = (row + row).getBytes(StandardCharsets.UTF_8);

        int boundary = reader.recordSplitter().findLastRecordBoundary(buf, buf.length);
        assertEquals(buf.length - 1, boundary);
        assertEquals('\n', (char) buf[boundary]);
    }

    /** The streaming sibling findNextRecordBoundary applies the same field-start rule. */
    public void testTsvFindNextRecordBoundaryWithLiteralMidFieldQuotes() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(CsvFormatOptions.TSV);
        String row1 = "1\ta\"b\tc\n";
        String data = row1 + "2\td\te\n";
        byte[] buf = data.getBytes(StandardCharsets.UTF_8);

        long consumed = reader.recordSplitter().findNextRecordBoundary(new BufferedInputStream(new ByteArrayInputStream(buf)));
        assertEquals(row1.length(), consumed);
    }

    /**
     * A field-leading quote opens a quoted field; an embedded {@code ""} is a literal quote and a lone
     * {@code "} closes the field. Pins the doubled-quote peek (the {@code peekByte} window) in both scanners.
     */
    public void testTsvDoubledQuoteInQuotedFieldIsLiteral() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(CsvFormatOptions.TSV);
        String row1 = "\"a\"\"b\"\tc\n"; // quoted field a"b (doubled quote), then c
        byte[] buf = (row1 + "d\te\n").getBytes(StandardCharsets.UTF_8);

        assertEquals(row1.length(), reader.recordSplitter().findNextRecordBoundary(new BufferedInputStream(new ByteArrayInputStream(buf))));
        assertEquals(buf.length - 1, reader.recordSplitter().findLastRecordBoundary(buf, buf.length));
    }

    /** Oracle: the boundary scanner and {@link CsvFormatReader#readCsvRecord} must agree on record count, CSV and TSV. */
    public void testRecordBoundaryCountAgreesWithReadCsvRecord() throws IOException {
        for (boolean tsv : new boolean[] { true, false }) {
            CsvFormatOptions options = tsv ? CsvFormatOptions.TSV : CsvFormatOptions.DEFAULT;
            CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(options);
            char delim = options.delimiter();
            boolean bracketAware = options.multiValueSyntax() == CsvFormatOptions.MultiValueSyntax.BRACKETS && delim == ',';

            int rows = randomIntBetween(5, 50);
            StringBuilder sb = new StringBuilder();
            for (int r = 0; r < rows; r++) {
                int cols = randomIntBetween(1, 8);
                for (int c = 0; c < cols; c++) {
                    if (c > 0) {
                        sb.append(delim);
                    }
                    sb.append(randomFieldWithLiteralQuotes(delim));
                }
                sb.append('\n');
            }
            String data = sb.toString();
            byte[] buf = data.getBytes(StandardCharsets.UTF_8);

            // Oracle: records produced by the real tokenizer.
            int parserRecords = 0;
            try (BufferedReader br = new BufferedReader(new StringReader(data))) {
                CsvLogicalRecordReader recordReader = new CsvLogicalRecordReader(
                    br,
                    options.quoteChar(),
                    delim,
                    SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                    options.encoding()
                );
                while (recordReader.readRecord(bracketAware) != null) {
                    parserRecords++;
                }
            }

            // Scanner: records found by driving findNextRecordBoundary forward.
            int scannerRecords = 0;
            BufferedInputStream in = new BufferedInputStream(new ByteArrayInputStream(buf));
            while (reader.recordSplitter().findNextRecordBoundary(in) >= 0) {
                scannerRecords++;
            }

            assertEquals("record count mismatch (tsv=" + tsv + ") for data:\n" + data, parserRecords, scannerRecords);
            // Buffer ends on a complete record, so the last byte is the final record boundary.
            assertEquals(buf.length - 1, reader.recordSplitter().findLastRecordBoundary(buf, buf.length));
        }
    }

    /** Fuzz: the scanner never returns -1 for a buffer ending on a complete record, at any quote count. */
    public void testTsvBoundaryNeverMinusOneWhenCompleteRecordPresent() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(CsvFormatOptions.TSV);
        for (int iter = 0; iter < 200; iter++) {
            StringBuilder sb = new StringBuilder();
            int rows = randomIntBetween(1, 6);
            for (int r = 0; r < rows; r++) {
                int cols = randomIntBetween(1, 6);
                for (int c = 0; c < cols; c++) {
                    if (c > 0) {
                        sb.append('\t');
                    }
                    // Plain field with a random number of literal mid-field quotes (no field-leading quote).
                    sb.append('x');
                    int quotes = randomIntBetween(0, 5);
                    for (int q = 0; q < quotes; q++) {
                        sb.append(randomBoolean() ? '"' : 'y');
                    }
                }
                sb.append('\n');
            }
            byte[] buf = sb.toString().getBytes(StandardCharsets.UTF_8);
            int boundary = reader.recordSplitter().findLastRecordBoundary(buf, buf.length);
            assertEquals("returned -1 for a buffer ending on a complete record:\n" + sb, buf.length - 1, boundary);
        }
    }

    /** A 105-column TSV row with literal quotes scattered through a handful of text-like columns. */
    private static String clickBenchShapedTsvRow() {
        StringBuilder row = new StringBuilder();
        for (int c = 0; c < 105; c++) {
            if (c > 0) {
                row.append('\t');
            }
            switch (c % 7) {
                case 0 -> row.append(c);                              // numeric column
                case 3 -> row.append("http://x/?q=\"a\"&p=").append(c); // URL-like, literal quotes
                case 5 -> row.append("Title \"with\" quotes ").append(c); // text, literal quotes
                default -> row.append('v').append(c);
            }
        }
        row.append('\n');
        return row.toString();
    }

    /** A field that is either plain text with random mid-field literal quotes, or a properly quoted field. */
    private String randomFieldWithLiteralQuotes(char delim) {
        if (randomInt(4) == 0) {
            // Properly quoted field: may embed the delimiter, a newline, and doubled quotes.
            StringBuilder b = new StringBuilder("\"");
            int n = randomIntBetween(0, 6);
            for (int i = 0; i < n; i++) {
                int pick = randomInt(4);
                switch (pick) {
                    case 0 -> b.append(delim);
                    case 1 -> b.append('\n');
                    case 2 -> b.append("\"\""); // escaped literal quote inside the quoted field
                    default -> b.append((char) ('a' + randomInt(25)));
                }
            }
            b.append('"');
            return b.toString();
        }
        // Plain field: starts with a letter (never a field-leading quote), then random text + literal quotes.
        StringBuilder b = new StringBuilder();
        b.append((char) ('a' + randomInt(25)));
        int n = randomIntBetween(0, 8);
        for (int i = 0; i < n; i++) {
            b.append(randomBoolean() ? '"' : (char) ('a' + randomInt(25)));
        }
        return b.toString();
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

    public void testQuotedFieldWithLoneCrPreserved() throws IOException {
        // The \r inside "b\rc" is a real 0x0D byte — it must be preserved verbatim, not converted to \n.
        String csv = "id:keyword,val:keyword,end:keyword\n\"a\",\"b\rc\",\"d\"\n\"e\",\"f\",\"g\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(3, page.getBlockCount());

            BytesRefBlock valBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(new BytesRef("b\rc"), valBlock.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("f"), valBlock.getBytesRef(1, new BytesRef()));

            BytesRefBlock endBlock = (BytesRefBlock) page.getBlock(2);
            assertEquals(new BytesRef("d"), endBlock.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("g"), endBlock.getBytesRef(1, new BytesRef()));
        }
    }

    public void testQuotedFieldWithLoneCrPreservedJacksonPath() throws IOException {
        // Same as testQuotedFieldWithLoneCrPreserved but via the Jackson (non-fused) path.
        String csv = "id:keyword,val:keyword,end:keyword\n\"a\",\"b\rc\",\"d\"\n\"e\",\"f\",\"g\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "NONE"));

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(3, page.getBlockCount());

            BytesRefBlock valBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(new BytesRef("b\rc"), valBlock.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("f"), valBlock.getBytesRef(1, new BytesRef()));

            BytesRefBlock endBlock = (BytesRefBlock) page.getBlock(2);
            assertEquals(new BytesRef("d"), endBlock.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("g"), endBlock.getBytesRef(1, new BytesRef()));
        }
    }

    public void testQuotedFieldWithCrLfInsidePreserved() throws IOException {
        // Embedded \r\n inside a quoted field must be preserved as field content, not treated as line terminator.
        String csv = "a:keyword,b:keyword\n\"hello\r\nworld\",\"x\"\n\"y\",\"z\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount());

            BytesRefBlock aBlock = (BytesRefBlock) page.getBlock(0);
            assertEquals(new BytesRef("hello\r\nworld"), aBlock.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("y"), aBlock.getBytesRef(1, new BytesRef()));

            BytesRefBlock bBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(new BytesRef("x"), bBlock.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("z"), bBlock.getBytesRef(1, new BytesRef()));
        }
    }

    public void testReadCsvRecordLoneCrOutsideQuotesTerminatesRecord() throws IOException {
        BufferedReader br = new BufferedReader(new StringReader("abc\rdef\n"));
        assertEquals("abc", CsvFormatReader.readCsvRecord(br, '"', ',', false));
        assertEquals("def", CsvFormatReader.readCsvRecord(br, '"', ',', false));
        assertNull(CsvFormatReader.readCsvRecord(br, '"', ',', false));
    }

    public void testReadCsvRecordLoneCrInsideQuotesPreserved() throws IOException {
        BufferedReader br = new BufferedReader(new StringReader("\"a\rb\",c\n"));
        assertEquals("\"a\rb\",c", CsvFormatReader.readCsvRecord(br, '"', ',', false));
        assertNull(CsvFormatReader.readCsvRecord(br, '"', ',', false));
    }

    public void testReadCsvRecordCrLfOutsideQuotesTerminates() throws IOException {
        BufferedReader br = new BufferedReader(new StringReader("abc\r\ndef\r\n"));
        assertEquals("abc", CsvFormatReader.readCsvRecord(br, '"', ',', false));
        assertEquals("def", CsvFormatReader.readCsvRecord(br, '"', ',', false));
        assertNull(CsvFormatReader.readCsvRecord(br, '"', ',', false));
    }

    public void testReadCsvRecordCrLfInsideQuotesPreserved() throws IOException {
        BufferedReader br = new BufferedReader(new StringReader("\"a\r\nb\",c\n"));
        assertEquals("\"a\r\nb\",c", CsvFormatReader.readCsvRecord(br, '"', ',', false));
        assertNull(CsvFormatReader.readCsvRecord(br, '"', ',', false));
    }

    public void testReadCsvRecordEofMidRecord() throws IOException {
        BufferedReader br = new BufferedReader(new StringReader("abc"));
        assertEquals("abc", CsvFormatReader.readCsvRecord(br, '"', ',', false));
        assertNull(CsvFormatReader.readCsvRecord(br, '"', ',', false));
    }

    public void testReadCsvRecordEofEmptyReturnsNull() throws IOException {
        BufferedReader br = new BufferedReader(new StringReader(""));
        assertNull(CsvFormatReader.readCsvRecord(br, '"', ',', false));
    }

    public void testReadCsvRecordBracketAwarePreservesCrInBrackets() throws IOException {
        BufferedReader br = new BufferedReader(new StringReader("[a\rb],c\n"));
        assertEquals("[a\rb],c", CsvFormatReader.readCsvRecord(br, '"', ',', true));
        assertNull(CsvFormatReader.readCsvRecord(br, '"', ',', true));
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
        CsvFormatReader reader = mvcReader(blockFactory);

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
        CsvFormatReader reader = mvcReader(blockFactory);

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
        CsvFormatReader reader = mvcReader(blockFactory);

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
        CsvFormatReader reader = mvcReader(blockFactory);

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

    /**
     * A cell that starts with {@code [} but has no MVC-closing {@code ]} (e.g. interval-like {@code [6:42)}) must not
     * trigger bracket-swallowing — commas in the rest of the row remain column delimiters.
     */
    public void testBracketAwareOpenBracketWithoutMvcCloseIsLiteralColumnSplit() throws IOException {
        String csv = "id:integer,title:keyword,dt:keyword\n1,[6:42) literal title text,2013-07-20\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            IntBlock idBlock = (IntBlock) page.getBlock(0);
            BytesRefBlock titleBlock = (BytesRefBlock) page.getBlock(1);
            BytesRefBlock dtBlock = (BytesRefBlock) page.getBlock(2);
            assertEquals(1, idBlock.getInt(0));
            assertEquals(new BytesRef("[6:42) literal title text"), titleBlock.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("2013-07-20"), dtBlock.getBytesRef(0, new BytesRef()));
        }
    }

    /**
     * Typed headers must use the same bracket-aware field splitting as data rows; otherwise bound schema width disagrees
     * with row parses under parallel reads.
     */
    public void testMetadataSchemaColumnCountUsesBracketAwareHeaderSplit() throws IOException {
        String csv = "id:integer,title:keyword\n1,[[37]]\n2,[hello,world]\n";
        CsvFormatReader reader = mvcReader(blockFactory);
        assertEquals(2, reader.metadata(createStorageObject(csv)).schema().size());
        try (CloseableIterator<Page> iterator = reader.read(createStorageObject(csv), null, 10)) {
            int rows = 0;
            while (iterator.hasNext()) {
                rows += iterator.next().getPositionCount();
            }
            assertEquals(2, rows);
        }
    }

    /**
     * Nested {@code [[…]]} must not close the MVC cell on the inner {@code ]} — otherwise an extra column appears.
     * With {@code multi_value_syntax=brackets}, the outer MV strips once; the inner {@code [37]} is the single keyword element.
     */
    public void testBracketAwareNestedBracketsStaySingleCell() throws IOException {
        String csv = "prefix:keyword,mid:keyword,suffix:keyword\nx,[[37]],y\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = mvcReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(3, page.getBlockCount());
            BytesRefBlock midBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(1, midBlock.getValueCount(0));
            assertEquals(new BytesRef("[37]"), midBlock.getBytesRef(midBlock.getFirstValueIndex(0), new BytesRef()));
            BytesRefBlock suffixBlock = (BytesRefBlock) page.getBlock(2);
            assertEquals(new BytesRef("y"), suffixBlock.getBytesRef(0, new BytesRef()));
        }
    }

    public void testMultiValueBracketsQuotedElements() throws IOException {
        String csv = "id:integer,names:keyword\n1,\"[\"\"hello\"\",\"\"world\"\"]\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = mvcReader(blockFactory);

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
        CsvFormatReader reader = mvcReader(blockFactory);

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
        CsvFormatReader reader = mvcReader(blockFactory);

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
        CsvFormatReader reader = mvcReader(blockFactory);

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
        assertEquals(data.length, reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextRecordBoundaryCRLF() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "field1,field2\r\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data));
        assertEquals("field1,field2\r\n".indexOf('\n') + 1, boundary);
    }

    public void testFindNextRecordBoundaryNewlineInsideQuotes() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "\"field\nwith\nnewlines\",other\nreal_boundary\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data));
        int expected = "\"field\nwith\nnewlines\",other\n".length();
        assertEquals(expected, boundary);
    }

    public void testFindNextRecordBoundaryEscapedQuotes() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "\"value with \"\"escaped\"\" quotes\"\nrest\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data));
        int expected = "\"value with \"\"escaped\"\" quotes\"\n".length();
        assertEquals(expected, boundary);
    }

    public void testFindNextRecordBoundaryEofNoNewline() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "field1,field2".getBytes(StandardCharsets.UTF_8);
        assertEquals(-1, reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextRecordBoundaryEmptyStream() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        assertEquals(-1, reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(new byte[0])));
    }

    public void testFindNextRecordBoundaryQuotedNewlineAcrossSplitBoundary() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        // Simulate a split boundary landing inside a quoted field with embedded newlines.
        // The boundary finder must skip past the quoted field to find the real record boundary.
        byte[] data = "partial,\"quoted\nfield\nwith\nnewlines\",end\nnext_record,b,c\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data));
        int expected = "partial,\"quoted\nfield\nwith\nnewlines\",end\n".length();
        assertEquals(expected, boundary);
    }

    public void testBracketPrefixedFieldStaysOneField() throws IOException {
        // Production hits CSV pattern: `,[37] Title text,...` — the `[37]` looks like an MVC cell, but the field
        // continues past the closing `]` until the next comma. Closing the cell on `]` (and starting a fresh entry)
        // turns the trailing text into a phantom extra column → "row has [N+1] columns" failures.
        String csv = "id:long,text:keyword,when:keyword\n1,[37] Title with text,2013-07-29\n2,plain,2013-07-30\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(3, page.getBlockCount());
            BytesRefBlock text = (BytesRefBlock) page.getBlock(1);
            assertEquals(new BytesRef("[37] Title with text"), text.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("plain"), text.getBytesRef(1, new BytesRef()));
            BytesRefBlock when = (BytesRefBlock) page.getBlock(2);
            assertEquals(new BytesRef("2013-07-29"), when.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("2013-07-30"), when.getBytesRef(1, new BytesRef()));
        }
    }

    public void testBracketCellWithStrayQuoteAndCommasIsOneField() throws IOException {
        // Reproduces the production hits CSV pattern where bracket MVC cells contain literal `"` characters
        // and embedded commas (e.g. `[some text",1,2013-...,38,-12345]`). The lookahead must mirror the splitter:
        // inside `[..]` only `[` and `]` matter — a stray `"` is a literal byte. Otherwise the lookahead reports
        // "no matching ]" and the splitter falls back to treating `[` as plain text, which turns inner commas into
        // delimiters and produces `row has [N+k] columns but schema defines [N]` failures.
        String csv = "id:long,tags:keyword,when:keyword\n"
            + "1,[some text\",1,2013-07-15 13:51:28,2013-07-15,38,177794517],ok\n"
            + "2,[plain],ok\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = mvcReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(3, page.getBlockCount());
            BytesRefBlock when = (BytesRefBlock) page.getBlock(2);
            assertEquals(new BytesRef("ok"), when.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("ok"), when.getBytesRef(1, new BytesRef()));
        }
    }

    public void testStrayQuoteInUnquotedFieldIsLiteralAndDoesNotMergeRows() throws IOException {
        // Two physical rows, each fully formed for a 3-column schema. The first row contains
        // a single literal `"` inside an unquoted text cell. RFC-4180 says quoting only opens at
        // field start; without the field-start check the parser would conclude the first row has an
        // open quote, glue the second row's bytes onto it, and report 6 columns instead of 3.
        String csv = "id:long,title:keyword,value:long\n1,Inch \" mark,42\n2,plain,7\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(3, page.getBlockCount());
            BytesRefBlock title = (BytesRefBlock) page.getBlock(1);
            assertEquals(new BytesRef("Inch \" mark"), title.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("plain"), title.getBytesRef(1, new BytesRef()));
            LongBlock value = (LongBlock) page.getBlock(2);
            assertEquals(42L, value.getLong(0));
            assertEquals(7L, value.getLong(1));
        }
    }

    public void testFindNextRecordBoundaryStrayQuoteIsNotAQuoteOpener() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        // Stray `"` inside an unquoted cell must not switch the scanner into quote mode; otherwise the
        // first `\n` is skipped and the segment boundary lands far past the next record.
        byte[] data = "1,Inch \" mark,42\n2,plain,7\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data));
        assertEquals("1,Inch \" mark,42\n".length(), boundary);
    }

    public void testFindNextRecordBoundaryNewlineInsideBracketMvc() throws IOException {
        CsvFormatReader reader = mvcReader(blockFactory);
        byte[] data = "before,[line1\nline2\nline3],after\nnext\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data));
        assertEquals("before,[line1\nline2\nline3],after\n".length(), boundary);
    }

    public void testFindNextRecordBoundaryNestedBracketMvcWithEmbeddedNewlines() throws IOException {
        CsvFormatReader reader = mvcReader(blockFactory);
        byte[] data = "a,[[cell\ninner]],b\nz\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data));
        assertEquals("a,[[cell\ninner]],b\n".length(), boundary);
    }

    public void testFindNextRecordBoundaryMultiValueSyntaxNoneDoesNotTreatBracketsAsMvc() throws IOException {
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "none"));
        byte[] data = "before,[not\nmvc],after\nnext\n".getBytes(StandardCharsets.UTF_8);
        long boundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data));
        assertEquals("before,[not\n".length(), boundary);
    }

    // --- findLastRecordBoundary tests ---

    public void testFindLastRecordBoundarySimpleTwoLines() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "a,b\nc,d\n".getBytes(StandardCharsets.UTF_8);
        int boundary = reader.recordSplitter().findLastRecordBoundary(data, data.length);
        assertEquals(data.length - 1, boundary);
    }

    public void testFindLastRecordBoundarySingleTrailingLineNoTerminator() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "a,b\nc,d".getBytes(StandardCharsets.UTF_8);
        int boundary = reader.recordSplitter().findLastRecordBoundary(data, data.length);
        assertEquals(3, boundary);
    }

    public void testFindLastRecordBoundaryEmpty() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        assertEquals(-1, reader.recordSplitter().findLastRecordBoundary(new byte[0], 0));
    }

    public void testFindLastRecordBoundaryAllInsideQuotedField() throws IOException {
        // Unterminated quoted cell: no boundary; caller must grow.
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "\"line one\nline two\nline three\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(-1, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastRecordBoundarySkipsEmbeddedNewlineInQuotedField() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "\"row1\nwith\nembedded\",a\nrow2,b\n".getBytes(StandardCharsets.UTF_8);
        int boundary = reader.recordSplitter().findLastRecordBoundary(data, data.length);
        assertEquals(data.length - 1, boundary);
    }

    public void testFindLastRecordBoundaryEmbeddedNewlineFollowedByUnterminatedTail() throws IOException {
        // Complete row, then an unterminated quoted field whose closing quote is in the next chunk.
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "row1,a\n\"row2 starts here\nstill inside quote".getBytes(StandardCharsets.UTF_8);
        int boundary = reader.recordSplitter().findLastRecordBoundary(data, data.length);
        assertEquals("row1,a\n".length() - 1, boundary);
    }

    public void testFindLastRecordBoundaryRespectsBracketMvc() throws IOException {
        // Embedded \n inside [..] must not be a boundary.
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "a,[v1\nv2\nv3],b\nrow2,plain,c\n".getBytes(StandardCharsets.UTF_8);
        int boundary = reader.recordSplitter().findLastRecordBoundary(data, data.length);
        assertEquals(data.length - 1, boundary);
    }

    public void testFindLastRecordBoundaryLengthSubsetOfBuffer() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] body = "a,b\nc,d\n".getBytes(StandardCharsets.UTF_8);
        byte[] padded = new byte[body.length + 64];
        System.arraycopy(body, 0, padded, 0, body.length);
        Arrays.fill(padded, body.length, padded.length, (byte) 0xff);
        int boundary = reader.recordSplitter().findLastRecordBoundary(padded, body.length);
        assertEquals(body.length - 1, boundary);
    }

    public void testFindLastRecordBoundaryCRLF() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "a,b\r\nc,d\r\n".getBytes(StandardCharsets.UTF_8);
        int boundary = reader.recordSplitter().findLastRecordBoundary(data, data.length);
        assertEquals(data.length - 1, boundary);
        assertEquals('\n', data[boundary]);
    }

    public void testFindLastRecordBoundaryDoubledQuoteEscape() throws IOException {
        // RFC 4180: "" is a literal quote inside a quoted field, not close-then-reopen.
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] data = "\"value with \"\"escaped\"\" quotes\"\nrest\n".getBytes(StandardCharsets.UTF_8);
        int boundary = reader.recordSplitter().findLastRecordBoundary(data, data.length);
        assertEquals(data.length - 1, boundary);
        assertEquals('\n', data[boundary]);
    }

    // --- findLastRecordBoundary tests for the QuotedFieldsOnly override path (TSV / non-bracket-MVC CSV) ---

    private static CsvFormatReader tsvReader(BlockFactory bf) {
        return (CsvFormatReader) new CsvFormatReader(bf).withConfig(Map.of("delimiter", "\t"));
    }

    private static CsvFormatReader noMvcReader(BlockFactory bf) {
        return (CsvFormatReader) new CsvFormatReader(bf).withConfig(Map.of("multi_value_syntax", "none"));
    }

    /** Comma-delimited reader with bracket multi-value parsing enabled (no longer the default). */
    private static CsvFormatReader mvcReader(BlockFactory bf) {
        return (CsvFormatReader) new CsvFormatReader(bf).withConfig(Map.of("multi_value_syntax", "brackets"));
    }

    public void testFindLastRecordBoundaryQuotedFieldsOnlyTsvSimpleTwoLines() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "a\tb\nc\td\n".getBytes(StandardCharsets.UTF_8);
        int boundary = reader.recordSplitter().findLastRecordBoundary(data, data.length);
        assertEquals(data.length - 1, boundary);
        assertEquals('\n', data[boundary]);
    }

    public void testFindLastRecordBoundaryQuotedFieldsOnlyTsvSingleTrailingLineNoTerminator() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "a\tb\nc\td".getBytes(StandardCharsets.UTF_8);
        int boundary = reader.recordSplitter().findLastRecordBoundary(data, data.length);
        assertEquals(3, boundary);
    }

    public void testFindLastRecordBoundaryQuotedFieldsOnlyTsvEmpty() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        assertEquals(-1, reader.recordSplitter().findLastRecordBoundary(new byte[0], 0));
    }

    public void testFindLastRecordBoundaryQuotedFieldsOnlyTsvNoNewline() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "a\tb\tc".getBytes(StandardCharsets.UTF_8);
        assertEquals(-1, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastRecordBoundaryQuotedFieldsOnlyEmbeddedNewlineInQuotedField() throws IOException {
        // multi_value_syntax=none routes through the QuotedFieldsOnly path; \n inside "..." is NOT a boundary.
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"row1\nwith\nembedded\",a\nrow2,b\n".getBytes(StandardCharsets.UTF_8);
        int boundary = reader.recordSplitter().findLastRecordBoundary(data, data.length);
        assertEquals(data.length - 1, boundary);
    }

    public void testFindLastRecordBoundaryQuotedFieldsOnlyUnterminatedQuotedTail() throws IOException {
        // One complete row, then an unterminated quoted field — open-tail contract: return the row's terminator.
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "row1,a\n\"row2 starts here\nstill inside quote".getBytes(StandardCharsets.UTF_8);
        int boundary = reader.recordSplitter().findLastRecordBoundary(data, data.length);
        assertEquals("row1,a\n".length() - 1, boundary);
    }

    public void testFindLastRecordBoundaryQuotedFieldsOnlyAllInsideQuotedField() throws IOException {
        // Unterminated quoted cell from byte 0: no boundary.
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"line one\nline two\nline three\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(-1, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastRecordBoundaryQuotedFieldsOnlyDoubledQuoteEscape() throws IOException {
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"value with \"\"escaped\"\" quotes\"\nrest\n".getBytes(StandardCharsets.UTF_8);
        int boundary = reader.recordSplitter().findLastRecordBoundary(data, data.length);
        assertEquals(data.length - 1, boundary);
        assertEquals('\n', data[boundary]);
    }

    public void testFindLastRecordBoundaryQuotedFieldsOnlyCRLF() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "a\tb\r\nc\td\r\n".getBytes(StandardCharsets.UTF_8);
        int boundary = reader.recordSplitter().findLastRecordBoundary(data, data.length);
        assertEquals(data.length - 1, boundary);
        assertEquals('\n', data[boundary]);
    }

    public void testFindLastRecordBoundaryQuotedFieldsOnlyLengthSubsetOfBuffer() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] body = "a\tb\nc\td\n".getBytes(StandardCharsets.UTF_8);
        byte[] padded = new byte[body.length + 64];
        System.arraycopy(body, 0, padded, 0, body.length);
        Arrays.fill(padded, body.length, padded.length, (byte) 0xff);
        int boundary = reader.recordSplitter().findLastRecordBoundary(padded, body.length);
        assertEquals(body.length - 1, boundary);
    }

    public void testFindLastRecordBoundaryQuotedFieldsOnlyMatchesDefault() throws IOException {
        // Equivalence: across randomized inputs, the override must produce the same answer as
        // driving findNextRecordBoundary forward through the buffer — i.e. what
        // SegmentableFormatReader.findLastRecordBoundary's default body computes.
        CsvFormatReader reader = noMvcReader(blockFactory);
        for (int trial = 0; trial < 50; trial++) {
            byte[] data = randomQuotedFieldsCsv(randomIntBetween(0, 4096));
            int override = reader.recordSplitter().findLastRecordBoundary(data, data.length);
            int oracle = referenceDefaultDriver(reader, data, data.length);
            assertEquals("trial " + trial + " input=" + new String(data, StandardCharsets.UTF_8), oracle, override);
        }
    }

    private byte[] randomQuotedFieldsCsv(int length) {
        // Random bytes drawn from a small alphabet that exercises every branch of the QuotedFieldsOnly
        // state machine: regular bytes, the quote char, the doubled-quote escape, the LF terminator,
        // the comma delimiter, and an occasional CR.
        byte[] alphabet = new byte[] { 'a', 'b', 'c', ',', '"', '\n', '\r' };
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = alphabet[randomIntBetween(0, alphabet.length - 1)];
        }
        return data;
    }

    /**
     * Replays the {@link RecordSplitter#findLastRecordBoundary} default body — drives
     * the current {@code findNextRecordBoundary} forward through the buffer. Used as the
     * equivalence oracle for the override.
     */
    private static int referenceDefaultDriver(CsvFormatReader reader, byte[] buf, int length) throws IOException {
        if (length <= 0) {
            return -1;
        }
        int lastBoundary = -1;
        int cumulative = 0;
        while (cumulative < length) {
            long consumed = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(buf, cumulative, length - cumulative));
            if (consumed < 0) {
                return lastBoundary;
            }
            cumulative += Math.toIntExact(consumed);
            lastBoundary = cumulative - 1;
        }
        return lastBoundary;
    }

    // --- Branch coverage for findNextRecordBoundaryQuotedFieldsOnly ---

    public void testFindNextBoundaryQfoEmptyStream() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        assertEquals(-1, reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(new byte[0])));
    }

    public void testFindNextBoundaryQfoNoTerminator() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "abc\tdef\tghi".getBytes(StandardCharsets.UTF_8);
        assertEquals(-1, reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextBoundaryQfoLeadingNewline() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "\nrest".getBytes(StandardCharsets.UTF_8);
        assertEquals(1, reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextBoundaryQfoSimpleRecord() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "a\tb\tc\nrest".getBytes(StandardCharsets.UTF_8);
        assertEquals("a\tb\tc\n".length(), reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextBoundaryQfoOpenQuoteThenEof() throws IOException {
        // Unpaired opening quote, stream ends — no boundary.
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"abc".getBytes(StandardCharsets.UTF_8);
        assertEquals(-1, reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextBoundaryQfoQuotedFieldThenNewline() throws IOException {
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"abc\"\nrest".getBytes(StandardCharsets.UTF_8);
        assertEquals("\"abc\"\n".length(), reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextBoundaryQfoEmbeddedNewlineInQuotes() throws IOException {
        // \n inside a quoted field is not a boundary.
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"line1\nline2\"\nrest".getBytes(StandardCharsets.UTF_8);
        assertEquals("\"line1\nline2\"\n".length(), reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextBoundaryQfoDoubledQuoteIsLiteral() throws IOException {
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"a\"\"b\"\nrest".getBytes(StandardCharsets.UTF_8);
        assertEquals("\"a\"\"b\"\n".length(), reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextBoundaryQfoDoubledQuoteThenNewline() throws IOException {
        // RFC quirk: doubled quote at the very end of a field, then \n.
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"a\"\"\"\nrest".getBytes(StandardCharsets.UTF_8);
        assertEquals("\"a\"\"\"\n".length(), reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextBoundaryQfoCloseQuoteThenOtherByte() throws IOException {
        // Close-quote followed by non-newline, non-quote byte: exit quotes, keep scanning.
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"a\"x\nrest".getBytes(StandardCharsets.UTF_8);
        assertEquals("\"a\"x\n".length(), reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextBoundaryQfoCloseQuoteThenEof() throws IOException {
        // Close-quote at the very end of stream — read returns -1 on peek.
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"abc\"".getBytes(StandardCharsets.UTF_8);
        assertEquals(-1, reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextBoundaryQfoCrLfReturnsAtLf() throws IOException {
        // CRLF returns after the LF byte.
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "abc\r\nrest".getBytes(StandardCharsets.UTF_8);
        assertEquals("abc\r\n".length(), reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextBoundaryQfoLoneCrIsBoundary() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "abc\rdef".getBytes(StandardCharsets.UTF_8);
        assertEquals("abc\r".length(), reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextBoundaryQfoReusesBufferedInputStream() throws IOException {
        // If the caller already passed a BufferedInputStream, the method should reuse it
        // (not wrap it again). Observable via the consumed byte count.
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "a\tb\nc\td\n".getBytes(StandardCharsets.UTF_8);
        BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(data));
        long consumed = reader.recordSplitter().findNextRecordBoundary(bis);
        assertEquals("a\tb\n".length(), consumed);
    }

    public void testFindNextBoundaryQfoMultipleQuotedFields() throws IOException {
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"a\",\"b\",\"c\"\nrest".getBytes(StandardCharsets.UTF_8);
        assertEquals("\"a\",\"b\",\"c\"\n".length(), reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    public void testFindNextBoundaryQfoTabInsideQuotedFieldIsLiteral() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "\"has\ttab\"\nrest".getBytes(StandardCharsets.UTF_8);
        assertEquals("\"has\ttab\"\n".length(), reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data)));
    }

    // --- Branch coverage for findLastRecordBoundaryQuotedFieldsOnly ---

    public void testFindLastBoundaryQfoEmptyBuffer() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        assertEquals(-1, reader.recordSplitter().findLastRecordBoundary(new byte[0], 0));
    }

    public void testFindLastBoundaryQfoNoNewline() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "abc\tdef\tghi".getBytes(StandardCharsets.UTF_8);
        assertEquals(-1, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastBoundaryQfoSingleNewlineAtStart() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(0, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastBoundaryQfoSingleNewlineAtEnd() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "abc\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(3, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastBoundaryQfoMultipleNewlinesReturnsLast() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "a\nb\nc\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length - 1, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastBoundaryQfoUnclosedQuoteSkipsLaterNewlines() throws IOException {
        CsvFormatReader reader = noMvcReader(blockFactory);
        // Two newlines outside quotes, then an open quote, then a newline inside the open region.
        byte[] data = "row1\nrow2\n\"open\nstill".getBytes(StandardCharsets.UTF_8);
        // The newline after the open quote is inside an unterminated region and must be ignored.
        assertEquals("row1\nrow2\n".length() - 1, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastBoundaryQfoQuoteOpenAndClosedSkipsEmbeddedNewline() throws IOException {
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"a\nb\",c\nrow2,x\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length - 1, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastBoundaryQfoDoubledQuoteIsLiteral() throws IOException {
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"a\"\"b\"\nrow2\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length - 1, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastBoundaryQfoDoubledQuoteContainsNewline() throws IOException {
        // "" inside a quoted field is literal — the field is still quoted, so \n is skipped.
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"a\"\"\nb\"\nrow2\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length - 1, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastBoundaryQfoQuoteAtVeryLastByte() throws IOException {
        // The quote-char is the last byte; no peek possible. Treated as closing if currently in
        // quotes (any trailing \n is impossible anyway since the buffer ends here).
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "row1\n\"abc\"".getBytes(StandardCharsets.UTF_8);
        assertEquals(4, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastBoundaryQfoCrLfReturnsAtLfIndex() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "a\r\nb\r\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length - 1, reader.recordSplitter().findLastRecordBoundary(data, data.length));
        assertEquals('\n', data[data.length - 1]);
    }

    public void testFindLastBoundaryQfoLoneCrIsBoundary() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "a\rb\rc".getBytes(StandardCharsets.UTF_8);
        assertEquals(3, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastBoundaryQfoLengthLessThanBuffer() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] body = "a\nb\n".getBytes(StandardCharsets.UTF_8);
        byte[] padded = new byte[body.length + 10];
        System.arraycopy(body, 0, padded, 0, body.length);
        Arrays.fill(padded, body.length, padded.length, (byte) '\n');
        // Use only the first body.length bytes — trailing \n bytes in the padded region must
        // not be visited.
        assertEquals(body.length - 1, reader.recordSplitter().findLastRecordBoundary(padded, body.length));
    }

    public void testFindLastBoundaryQfoNewlineInsideUnclosedQuotedFieldAtStart() throws IOException {
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"abc\ndef".getBytes(StandardCharsets.UTF_8);
        assertEquals(-1, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastBoundaryQfoNoQuotesPlainNewlines() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "abc\ndef\nghi\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(data.length - 1, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastBoundaryQfoQuotedNewlineThenPlainRow() throws IOException {
        // Embedded \n inside quotes then real terminator then another row.
        CsvFormatReader reader = noMvcReader(blockFactory);
        byte[] data = "\"a\nb\",c\nx,y\nz,w".getBytes(StandardCharsets.UTF_8);
        assertEquals("\"a\nb\",c\nx,y\n".length() - 1, reader.recordSplitter().findLastRecordBoundary(data, data.length));
    }

    public void testFindLastBoundaryQfoZeroLength() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        byte[] data = "anything\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(-1, reader.recordSplitter().findLastRecordBoundary(data, 0));
    }

    public void testFindLastBoundaryQfoNegativeLength() throws IOException {
        CsvFormatReader reader = tsvReader(blockFactory);
        assertEquals(-1, reader.recordSplitter().findLastRecordBoundary(new byte[0], -1));
    }

    // --- Cross-method equivalence on hand-picked inputs ---

    public void testQfoFindNextAndFindLastAgreeOnSimpleInputs() throws IOException {
        CsvFormatReader reader = noMvcReader(blockFactory);
        String[] inputs = {
            "",
            "\n",
            "row1\n",
            "row1\nrow2\n",
            "\"a\nb\",c\n",
            "\"a\"\"b\"\nrest\n",
            "\"unclosed",
            "row1\n\"unclosed",
            "a\r\nb\r\n",
            "\"a\"\nrest\n", };
        for (String s : inputs) {
            byte[] data = s.getBytes(StandardCharsets.UTF_8);
            int override = reader.recordSplitter().findLastRecordBoundary(data, data.length);
            int oracle = referenceDefaultDriver(reader, data, data.length);
            assertEquals("input=" + s, oracle, override);
        }
    }

    public void testBracketAwareLeadingWhitespaceBeforeBracketOpensMvc() throws IOException {
        String csv = "prefix:keyword,mid:keyword,suffix:keyword\nx,  [[37]],y\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = mvcReader(blockFactory);
        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(3, page.getBlockCount());
            BytesRefBlock midBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(1, midBlock.getValueCount(0));
            assertEquals(new BytesRef("[37]"), midBlock.getBytesRef(midBlock.getFirstValueIndex(0), new BytesRef()));
        }
    }

    public void testFindNextRecordBoundaryAtBufferBoundary() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        byte[] padding = new byte[8191];
        Arrays.fill(padding, (byte) 'x');
        byte[] suffix = "\nmore\n".getBytes(StandardCharsets.UTF_8);
        byte[] data = new byte[padding.length + suffix.length];
        System.arraycopy(padding, 0, data, 0, padding.length);
        System.arraycopy(suffix, 0, data, padding.length, suffix.length);
        long boundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data));
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
        long boundary = reader.recordSplitter().findNextRecordBoundary(new ByteArrayInputStream(data));
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
     * Pins that both the fused bracket-aware path and the non-fused (Jackson) path report the
     * same 1-based row index in error messages for a malformed row at a known position.
     */
    public void testTotalRowCountConsistentBetweenFusedAndNonFusedPaths() throws IOException {
        String csv = """
            id:long,name:keyword
            1,Alice
            2,Bob
            bad_id,Charlie
            4,Dan
            """;

        StorageObject object = createStorageObject(csv);
        ErrorPolicy skipRow = new ErrorPolicy(100, true);

        // Fused bracket-aware path (default: multi_value_syntax=BRACKETS, delimiter=',')
        CsvFormatReader fusedReader = new CsvFormatReader(blockFactory);
        try (
            CloseableIterator<Page> iterator = fusedReader.read(
                object,
                FormatReadContext.builder().batchSize(10).errorPolicy(skipRow).build()
            )
        ) {
            while (iterator.hasNext()) {
                iterator.next();
            }
        }
        List<String> fusedWarnings = drainWarnings();

        // Non-fused Jackson path (multi_value_syntax=NONE bypasses bracket-aware parsing)
        CsvFormatReader nonFusedReader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("multi_value_syntax", "NONE")
        );
        try (
            CloseableIterator<Page> iterator = nonFusedReader.read(
                object,
                FormatReadContext.builder().batchSize(10).errorPolicy(skipRow).build()
            )
        ) {
            while (iterator.hasNext()) {
                iterator.next();
            }
        }
        List<String> nonFusedWarnings = drainWarnings();

        // Both paths should report "Row [3]" for the bad row (1-based: header excluded, 3rd data row)
        String fusedDetail = fusedWarnings.stream().filter(w -> w.contains("Row [")).findFirst().orElse("");
        String nonFusedDetail = nonFusedWarnings.stream().filter(w -> w.contains("Row [")).findFirst().orElse("");
        assertTrue("Fused path should report Row [3], got: " + fusedDetail, fusedDetail.contains("Row [3]"));
        assertTrue("Non-fused path should report Row [3], got: " + nonFusedDetail, nonFusedDetail.contains("Row [3]"));
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

    // --- Datetime fast-path equivalence tests ---
    // tryParseSpaceSeparatedDatetimeMillis avoids the JDK DateTimeFormatter Parsed-HashMap
    // allocation that dominated ~16% of CPU on Q24 of the CSV ClickBench profile. These tests lock
    // in the equivalence contract: any input the fast path accepts must produce the same epoch
    // millis as the existing slow path (DateUtils.asDateTime), and any input it rejects must hit
    // the slow path unchanged (returning FAST_PATH_MISS).

    public void testFastPathSpaceSeparatedNoFraction() {
        long actual = CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("2024-05-12 14:30:45");
        assertEquals(Instant.parse("2024-05-12T14:30:45Z").toEpochMilli(), actual);
    }

    public void testFastPathSpaceSeparatedWithMillis() {
        long actual = CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("2024-05-12 14:30:45.123");
        assertEquals(Instant.parse("2024-05-12T14:30:45.123Z").toEpochMilli(), actual);
    }

    public void testFastPathSpaceSeparatedLeapDay() {
        long actual = CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("2020-02-29 00:00:00");
        assertEquals(Instant.parse("2020-02-29T00:00:00Z").toEpochMilli(), actual);
    }

    public void testFastPathSpaceSeparatedEpoch() {
        long actual = CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("1970-01-01 00:00:00");
        assertEquals(0L, actual);
    }

    public void testFastPathRejectsCalendarInvalidDates() {
        // 30 February — valid digits, invalid calendar date. Must fall through (FAST_PATH_MISS)
        // so the slow path can produce the usual "Failed to parse" error.
        assertEquals(CsvFormatReader.FAST_PATH_MISS, CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("2021-02-30 10:00:00"));
        // 13th month
        assertEquals(CsvFormatReader.FAST_PATH_MISS, CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("2021-13-01 10:00:00"));
        // Hour 24 — Iso8601Parser also rejects this and LocalDateTime.of throws.
        assertEquals(CsvFormatReader.FAST_PATH_MISS, CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("2021-01-01 24:00:00"));
    }

    public void testFastPathRejectsWrongShape() {
        // T-separated → not the space-separated fast path's job; ISO fast path handles it instead.
        assertEquals(CsvFormatReader.FAST_PATH_MISS, CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("2024-05-12T14:30:45"));
        // Date-only
        assertEquals(CsvFormatReader.FAST_PATH_MISS, CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("2024-05-12"));
        // Trailing Z — falls back to the general-purpose parser (preserves the existing semantics
        // that DateUtils.asDateTime's whitespace formatter would apply).
        assertEquals(CsvFormatReader.FAST_PATH_MISS, CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("2024-05-12 14:30:45Z"));
        // Microsecond precision (6-digit fraction) — only 3-digit ms is fast-pathed.
        assertEquals(CsvFormatReader.FAST_PATH_MISS, CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("2024-05-12 14:30:45.123456"));
        // Garbage
        assertEquals(CsvFormatReader.FAST_PATH_MISS, CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("not-a-date"));
        // Wrong separators
        assertEquals(CsvFormatReader.FAST_PATH_MISS, CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("2024/05/12 14:30:45"));
        // Non-ASCII digit (full-width '1') in the year — must be rejected without throwing
        assertEquals(CsvFormatReader.FAST_PATH_MISS, CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("\uFF12024-05-12 14:30:45"));
        // Negative year — `-2024-05-12 14:30:45` is 20 chars and fails the length guard
        // (19 or 23 only); `-999-05-12 14:30:45` is 19 chars but the leading '-' fails
        // parseFixedDigits at offset 0. Either way, BCE-style dates route to Stage 3.
        assertEquals(CsvFormatReader.FAST_PATH_MISS, CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("-2024-05-12 14:30:45"));
        assertEquals(CsvFormatReader.FAST_PATH_MISS, CsvFormatReader.tryParseSpaceSeparatedDatetimeMillis("-999-05-12 14:30:45"));
    }

    public void testDatetimeFastPathInvalidIsoFallsThroughCleanly() throws IOException {
        // Regression for the Stage 1 catch path: an ISO-shaped but calendar-invalid input like
        // 2021-02-30T10:00:00 succeeds in Iso8601Parser's lexical parse but fails inside
        // DateFormatters.from(...) with a generic DateTimeException (not DateTimeParseException).
        // Without the catch, the batch would abort with an uncaught exception. The catch routes
        // the row through Stage 3, whose JDK SMART resolver leniently maps Feb 30 to Feb 28 (same
        // behaviour as before this change). The second row is a sanity-check that the batch
        // continues normally.
        String csv = "ts:datetime\n2021-02-30T10:00:00\n2021-01-01T00:00:00Z\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            LongBlock block = (LongBlock) page.getBlock(0);
            assertEquals(Instant.parse("2021-02-28T10:00:00Z").toEpochMilli(), block.getLong(0));
            assertEquals(Instant.parse("2021-01-01T00:00:00Z").toEpochMilli(), block.getLong(1));
        }
    }

    public void testDatetimeFastPathRouting() throws IOException {
        // End-to-end smoke test exercising all three stages of tryParseDatetime in one batch:
        // * 2024-05-12 14:30:45 → Stage 2 (space-separated fast path)
        // * 2024-05-12T14:30:45Z → Stage 1 (ISO fast path)
        // * 2024-05-12T14:30:45+02:00 → Stage 1 (ISO fast path, with zone offset)
        // * 1715520645000 → looksNumeric → Long.parseLong
        String csv = "ts:datetime\n2024-05-12 14:30:45\n2024-05-12T14:30:45Z\n2024-05-12T14:30:45+02:00\n1715520645000\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(4, page.getPositionCount());
            LongBlock block = (LongBlock) page.getBlock(0);
            assertEquals(Instant.parse("2024-05-12T14:30:45Z").toEpochMilli(), block.getLong(0));
            assertEquals(Instant.parse("2024-05-12T14:30:45Z").toEpochMilli(), block.getLong(1));
            assertEquals(Instant.parse("2024-05-12T12:30:45Z").toEpochMilli(), block.getLong(2));
            assertEquals(1715520645000L, block.getLong(3));
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
        // splitLineBracketAware no longer throws on a stray `[` without a closing `]` (real data has things like
        // [6:42), so a literal `[` falls back to plain text). Use a structural error instead — too many fields for
        // the declared schema — which is the canonical "row malformed" case routed through the policy.
        String csv = """
            id:long,tags:keyword
            1,[a,b,c]
            2,extra,unexpected,fields
            3,[x,y]
            """;

        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = mvcReader(blockFactory);
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

    public void testNonRecordAlignedBracketAwareDropsPartialRecordWithCrInBrackets() throws IOException {
        // Byte-range macro-split where the leading partial record contains \r inside a bracket cell.
        // Without bracket-aware discard, readCsvRecord would split on the embedded \r, leaving
        // trailing bracket content as a phantom "record" that corrupts the next real row.
        String csv = "[a\rb],partial\n2,Bob\n3,Charlie\n";
        StorageObject object = createStorageObject(csv);
        List<Attribute> schema = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "id", DataType.LONG, Nullability.TRUE, null, false),
            new ReferenceAttribute(Source.EMPTY, null, "name", DataType.KEYWORD, Nullability.TRUE, null, false)
        );
        CsvFormatOptions opts = new CsvFormatOptions(
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
        CsvFormatReader reader = new CsvFormatReader(blockFactory).withOptions(opts).withSchema(schema);

        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).firstSplit(false).build();
        try (CloseableIterator<Page> iterator = reader.read(object, ctx)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals("bracket-aware discard must treat [a\\rb],partial as one record", 2, page.getPositionCount());
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

    public void testCsvErrorMessagesSummarizeAroundShortValuePassesThrough() {
        assertEquals("hello", CsvErrorMessages.summarizeAround("hello", 0));
        assertEquals("hello", CsvErrorMessages.summarizeAround("hello", 4));
        // Short values must pass through regardless of offset value (including negative and out-of-range).
        assertEquals("hello", CsvErrorMessages.summarizeAround("hello", -1));
        assertEquals("hello", CsvErrorMessages.summarizeAround("hello", 100));
        assertEquals("null", CsvErrorMessages.summarizeAround(null, 0));
        assertEquals("null", CsvErrorMessages.summarizeAround(null, -1));
    }

    public void testCsvErrorMessagesSummarizeAroundLongValueAnchorsOnOffset() {
        // Build a >MAX_EXCERPT_CHARS value with a unique marker at a known offset; the window
        // must include the marker (the actual fault) plus the offset annotation, and must start
        // with the elided-prefix indicator because the window does not begin at index 0.
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            huge.append('x');
        }
        int faultOffset = 500;
        String marker = "FAULTHERE";
        huge.replace(faultOffset, faultOffset + marker.length(), marker);
        String summarized = CsvErrorMessages.summarizeAround(huge.toString(), faultOffset);
        assertTrue(
            "expected length <= MAX_EXCERPT_CHARS, got " + summarized.length() + ": " + summarized,
            summarized.length() <= CsvErrorMessages.MAX_EXCERPT_CHARS
        );
        assertTrue(
            "expected offset annotation, got: " + summarized,
            summarized.contains("(offset " + faultOffset + " of " + huge.length() + " chars)")
        );
        assertTrue("expected fault bytes in window, got: " + summarized, summarized.contains(marker));
        // With start > 0 the excerpt must announce the elided prefix.
        assertTrue("expected leading ellipsis marker, got: " + summarized, summarized.startsWith("… "));
        // The window does not reach EOL (faultOffset + budget < length) so a trailing ellipsis is required.
        assertTrue("expected trailing ellipsis marker, got: " + summarized, summarized.endsWith("…"));
    }

    public void testCsvErrorMessagesSummarizeAroundOffsetNearStartHasNoLeadingEllipsis() {
        // When the fault is within OFFSET_LOOKBACK_CHARS of the start, the window begins at index 0
        // so the "… " leading marker is omitted (no characters elided before the window).
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            huge.append('a');
        }
        String summarized = CsvErrorMessages.summarizeAround(huge.toString(), 5);
        assertFalse("did not expect leading ellipsis, got: " + summarized, summarized.startsWith("… "));
        assertTrue("expected offset annotation, got: " + summarized, summarized.contains("(offset 5 of 1000 chars)"));
    }

    public void testCsvErrorMessagesSummarizeAroundLookbackBoundary() {
        // OFFSET_LOOKBACK_CHARS is 32: at offset 32 the window starts at index 0 (no leading marker);
        // at offset 33 the window starts at index 1 (leading marker emitted). Pin both sides of the
        // transition so an off-by-one in the lookback boundary is caught.
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            huge.append('b');
        }
        String atBoundary = CsvErrorMessages.summarizeAround(huge.toString(), CsvErrorMessages.OFFSET_LOOKBACK_CHARS);
        assertFalse("expected no leading ellipsis at offset == OFFSET_LOOKBACK_CHARS, got: " + atBoundary, atBoundary.startsWith("… "));
        String pastBoundary = CsvErrorMessages.summarizeAround(huge.toString(), CsvErrorMessages.OFFSET_LOOKBACK_CHARS + 1);
        assertTrue("expected leading ellipsis at offset == OFFSET_LOOKBACK_CHARS + 1, got: " + pastBoundary, pastBoundary.startsWith("… "));
    }

    public void testCsvErrorMessagesSummarizeAroundOffsetNearEndOmitsTrailingEllipsis() {
        // When the window reaches end-of-string, the trailing "…" marker is dropped because
        // nothing follows.
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            huge.append('z');
        }
        String summarized = CsvErrorMessages.summarizeAround(huge.toString(), 990);
        assertFalse("did not expect trailing ellipsis, got: " + summarized, summarized.endsWith("…"));
        // The final char of the underlying value must therefore be the final char of the excerpt.
        assertTrue("expected excerpt to end with the last char of value, got: " + summarized, summarized.endsWith("z"));
    }

    public void testCsvErrorMessagesSummarizeAroundUnknownOffsetFallsBackToHead() {
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 5000; i++) {
            huge.append('x');
        }
        String summarized = CsvErrorMessages.summarizeAround(huge.toString(), -1);
        assertTrue(
            "expected length <= MAX_EXCERPT_CHARS, got " + summarized.length(),
            summarized.length() <= CsvErrorMessages.MAX_EXCERPT_CHARS
        );
        assertTrue("expected truncated marker, got: " + summarized, summarized.contains("truncated"));
        assertTrue("expected total-length marker, got: " + summarized, summarized.contains("5000"));
        // The unknown-offset branch must not synthesize a fake offset annotation; assert the
        // anchored path was not taken.
        assertFalse("did not expect '(offset' in fallback, got: " + summarized, summarized.contains("(offset"));
        // Excerpt must come from the head of the value.
        assertTrue("expected excerpt to start at index 0, got: " + summarized, summarized.startsWith("x"));
    }

    public void testCsvErrorMessagesSummarizeAroundPathologicalInputStaysBounded() {
        // 100 KB input must not bloat the error message AND the offset annotation must survive,
        // since that is the operator's only handle into the source file.
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 100_000; i++) {
            huge.append('q');
        }
        String marker = "FAULTHERE";
        int faultOffset = 50_000;
        huge.replace(faultOffset, faultOffset + marker.length(), marker);
        String summarized = CsvErrorMessages.summarizeAround(huge.toString(), faultOffset);
        assertTrue(
            "expected length <= MAX_EXCERPT_CHARS, got " + summarized.length(),
            summarized.length() <= CsvErrorMessages.MAX_EXCERPT_CHARS
        );
        assertTrue("expected offset annotation, got: " + summarized, summarized.contains("(offset " + faultOffset + " of 100000 chars)"));
        assertTrue("expected fault bytes in window, got: " + summarized, summarized.contains(marker));
    }

    /**
     * End-to-end: an unclosed quoted field at end-of-file produces an error excerpt anchored on the
     * opening quote, not a head/tail-truncated view of the entire row. The row is sized so the
     * opening quote sits well inside the elided middle of the legacy head/tail summary, so a
     * regression that re-routes to {@link CsvErrorMessages#summarize} would hide the fault bytes.
     */
    public void testMalformedRowErrorAnchorsOnQuoteOffset() {
        // Pad both sides of the unmatched quote so the line is much longer than MAX_EXCERPT_CHARS
        // AND the fault sits in the middle of the line — where head/tail truncation hides it.
        StringBuilder padHead = new StringBuilder();
        for (int i = 0; i < 400; i++) {
            padHead.append('h');
        }
        // Trailing padding (still inside the unclosed quoted field) so the line is ~1000 chars and the
        // legacy head/tail summary would NOT include the opening quote in either window.
        StringBuilder padTail = new StringBuilder();
        for (int i = 0; i < 400; i++) {
            padTail.append('t');
        }
        String row = "1," + padHead + ",\"unterminated_field_here_" + padTail;
        int expectedOffset = "1,".length() + padHead.length() + ",".length();

        String csv = "id:long,name:keyword\n" + row;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = mvcReader(blockFactory);

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
        String msg = e.getMessage();
        assertTrue("expected Unclosed quoted field, got: " + msg, msg.contains("Unclosed quoted field"));
        assertTrue("expected offset annotation, got: " + msg, msg.contains("(offset " + expectedOffset + " of "));
        // The unique fault marker sits at the elided middle for legacy head/tail; with offset
        // anchoring it must survive in the excerpt.
        assertTrue("expected fault bytes in excerpt, got: " + msg, msg.contains("\"unterminated_field_here_"));
    }

    // --- FormatReadContext.readSchema() honor tests ---
    // These tests prove the runtime CSV reader uses the planner-resolved read schema (passed via
    // FormatReadContext.readSchema()) as the authoritative positional column layout, overriding
    // per-file inference. This closes the multi-file headerless CSV type-drift bug where two
    // files in the same glob would otherwise infer different types for the same column.

    public void testHeaderlessReadHonorsContextReadSchema() throws IOException {
        // Headerless CSV with two integer-looking columns. Per-file inference would say INTEGER,
        // INTEGER. With a bound schema of [col1:KEYWORD, col2:LONG], the emitted blocks must
        // reflect the bound types, not the inferred ones.
        String csv = "10,20\n30,40\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false));

        List<Attribute> boundSchema = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "col1", DataType.KEYWORD, Nullability.TRUE, null, false),
            new ReferenceAttribute(Source.EMPTY, null, "col2", DataType.LONG, Nullability.TRUE, null, false)
        );
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).readSchema(boundSchema).build();

        try (CloseableIterator<Page> iterator = reader.read(object, ctx)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount());
            // col1 was bound as KEYWORD — must emit BytesRefBlock with the raw token.
            BytesRefBlock col1 = (BytesRefBlock) page.getBlock(0);
            assertEquals(new BytesRef("10"), col1.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("30"), col1.getBytesRef(1, new BytesRef()));
            // col2 was bound as LONG — must emit LongBlock with the parsed value.
            LongBlock col2 = (LongBlock) page.getBlock(1);
            assertEquals(20L, col2.getLong(0));
            assertEquals(40L, col2.getLong(1));
        }
    }

    public void testHeaderlessReadFallsBackToInferenceWhenContextReadSchemaNull() throws IOException {
        // Same input as the previous test but with no bound schema. The reader falls back to
        // per-file inference (INTEGER, INTEGER). This is the negative control proving the new
        // slot is opt-in: existing call sites that don't set readSchema get the existing behavior.
        String csv = "10,20\n30,40\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false));

        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).build();
        assertNull("readSchema must default to null", ctx.readSchema());

        try (CloseableIterator<Page> iterator = reader.read(object, ctx)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount());
            // Both columns infer as INTEGER from the data sample.
            assertEquals(10, ((IntBlock) page.getBlock(0)).getInt(0));
            assertEquals(30, ((IntBlock) page.getBlock(0)).getInt(1));
            assertEquals(20, ((IntBlock) page.getBlock(1)).getInt(0));
            assertEquals(40, ((IntBlock) page.getBlock(1)).getInt(1));
        }
    }

    public void testHeaderlessReadSchemaResolvesCrossFileTypeDrift() throws IOException {
        // Cross-file type-drift case: two headerless files whose per-file inference would disagree
        // on col1 (file A has empty col1 → KEYWORD fallback; file B has integer col1 → INTEGER).
        // With a planner-resolved read schema [col1:KEYWORD, col2:LONG] passed via
        // context.readSchema(), file B emits BytesRefBlock for col1 instead of IntBlock —
        // the type-mismatch crash that TopN otherwise catches goes away.
        String fileB = "10,20\n30,40\n"; // would infer col1 as INTEGER without a bound read schema
        StorageObject fileBObject = createStorageObject(fileB);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("header_row", false));

        List<Attribute> readSchema = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "col1", DataType.KEYWORD, Nullability.TRUE, null, false),
            new ReferenceAttribute(Source.EMPTY, null, "col2", DataType.LONG, Nullability.TRUE, null, false)
        );
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).readSchema(readSchema).build();

        try (CloseableIterator<Page> iterator = reader.read(fileBObject, ctx)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertTrue(
                "col1 must be BytesRefBlock under bound read schema, got " + page.getBlock(0).getClass().getSimpleName(),
                page.getBlock(0) instanceof BytesRefBlock
            );
        }
    }

    // --- Phase 1A: isBlankOrComment ---

    public void testIsBlankOrCommentEmptyLine() {
        assertTrue(CsvFormatReader.isBlankOrComment("", "//"));
    }

    public void testIsBlankOrCommentWhitespaceOnly() {
        assertTrue(CsvFormatReader.isBlankOrComment("   \t  ", "//"));
    }

    public void testIsBlankOrCommentWithComment() {
        assertTrue(CsvFormatReader.isBlankOrComment("// this is a comment", "//"));
    }

    public void testIsBlankOrCommentWithLeadingWhitespaceComment() {
        assertTrue(CsvFormatReader.isBlankOrComment("   // indented comment", "//"));
    }

    public void testIsBlankOrCommentNormalLine() {
        assertFalse(CsvFormatReader.isBlankOrComment("hello,world", "//"));
    }

    public void testIsBlankOrCommentWithLeadingWhitespace() {
        assertFalse(CsvFormatReader.isBlankOrComment("  hello", "//"));
    }

    public void testIsBlankOrCommentNullCommentPrefix() {
        assertTrue(CsvFormatReader.isBlankOrComment("", null));
        assertFalse(CsvFormatReader.isBlankOrComment("data", null));
    }

    public void testIsBlankOrCommentEmptyCommentPrefix() {
        assertTrue(CsvFormatReader.isBlankOrComment("   ", ""));
        assertFalse(CsvFormatReader.isBlankOrComment("data", ""));
    }

    public void testIsBlankOrCommentPrefixLongerThanLine() {
        assertFalse(CsvFormatReader.isBlankOrComment("x", "//long-prefix"));
    }

    // --- Phase 1A: emitField ---

    public void testEmitFieldCleanValue() {
        assertEquals("hello", CsvFormatReader.emitField(new StringBuilder("hello")));
    }

    public void testEmitFieldEmpty() {
        assertEquals("", CsvFormatReader.emitField(new StringBuilder()));
    }

    public void testEmitFieldLeadingWhitespace() {
        assertEquals("hello", CsvFormatReader.emitField(new StringBuilder("  hello")));
    }

    public void testEmitFieldTrailingWhitespace() {
        assertEquals("hello", CsvFormatReader.emitField(new StringBuilder("hello  ")));
    }

    public void testEmitFieldBothWhitespace() {
        assertEquals("hello", CsvFormatReader.emitField(new StringBuilder("  hello  ")));
    }

    public void testEmitFieldWhitespaceOnly() {
        assertEquals("", CsvFormatReader.emitField(new StringBuilder("   ")));
    }

    public void testEmitFieldSingleChar() {
        assertEquals("x", CsvFormatReader.emitField(new StringBuilder("x")));
    }

    public void testEmitFieldSingleWhitespace() {
        assertEquals("", CsvFormatReader.emitField(new StringBuilder(" ")));
    }

    // --- Phase 2: Fused split+convert via bracket-aware path ---

    public void testFusedPathBasicIntegersProjected() throws IOException {
        String csv = """
            id:long,name:keyword,age:integer,active:boolean
            1,Alice,30,true
            2,Bob,25,false
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("id", "age"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(30, ((IntBlock) page.getBlock(1)).getInt(0));
            assertEquals(25, ((IntBlock) page.getBlock(1)).getInt(1));
        }
    }

    public void testFusedPathNegativeNumbers() throws IOException {
        String csv = """
            a:integer,b:long
            -42,-9999999999
            0,0
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("a", "b"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(-42, ((IntBlock) page.getBlock(0)).getInt(0));
            assertEquals(0, ((IntBlock) page.getBlock(0)).getInt(1));
            assertEquals(-9999999999L, ((LongBlock) page.getBlock(1)).getLong(0));
            assertEquals(0L, ((LongBlock) page.getBlock(1)).getLong(1));
        }
    }

    public void testFusedPathMixedTypes() throws IOException {
        String csv = """
            id:long,name:keyword,score:double
            1,Alice,95.5
            2,Bob,87.3
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("name", "score"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(1)).getDouble(0), 0.001);
        }
    }

    public void testFusedPathWithNullValues() throws IOException {
        String csv = """
            id:long,name:keyword,age:integer
            1,,30
            2,Bob,
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("id", "name", "age"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertTrue(page.getBlock(1).isNull(0));
            assertEquals(30, ((IntBlock) page.getBlock(2)).getInt(0));
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
            assertTrue(page.getBlock(2).isNull(1));
        }
    }

    public void testFusedPathSkipsNonProjectedFields() throws IOException {
        String csv = """
            a:keyword,b:keyword,c:keyword,d:keyword,e:keyword
            v1,v2,v3,v4,v5
            w1,w2,w3,w4,w5
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("c"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1, page.getBlockCount());
            assertEquals(new BytesRef("v3"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("w3"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(1, new BytesRef()));
        }
    }

    public void testFusedPathWithMultiValueBrackets() throws IOException {
        String csv = """
            id:long,tags:keyword
            1,[hello,world]
            2,[foo]
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("id", "tags"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            BytesRefBlock tagsBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(2, tagsBlock.getValueCount(0));
        }
    }

    public void testFusedPathWithQuotedFields() throws IOException {
        String csv = """
            id:long,name:keyword
            1,"Alice, Jr."
            2,"Bob ""B"" Smith"
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("name"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(new BytesRef("Alice, Jr."), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("Bob \"B\" Smith"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(1, new BytesRef()));
        }
    }

    public void testFusedPathInlineNumericOverflowFallsBack() throws IOException {
        String csv = """
            val:long
            9999999999999999999
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(
            Map.of("multi_value_syntax", "brackets", "error_mode", "null_field")
        );

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("val"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertTrue(page.getBlock(0).isNull(0));
        }
    }

    public void testFusedPathWithCommentLines() throws IOException {
        String csv = """
            id:long,name:keyword
            // this is a comment
            1,Alice
            // another comment
            2,Bob
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("id", "name"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
        }
    }

    public void testFusedPathColumnCountMismatchDetected() throws IOException {
        String csv = """
            id:long,name:keyword
            1,Alice,extra
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        ParsingException ex = expectThrows(ParsingException.class, () -> {
            try (CloseableIterator<Page> iterator = reader.read(object, List.of("id", "name"), 10)) {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }
        });
        assertThat(ex.getMessage(), Matchers.containsString("columns"));
    }

    public void testFusedPathFewerColumnsThanSchema() throws IOException {
        String csv = """
            a:integer,b:integer,c:integer
            1,2
            3,4,5
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("a", "c"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1, ((IntBlock) page.getBlock(0)).getInt(0));
            assertTrue(page.getBlock(1).isNull(0));
            assertEquals(3, ((IntBlock) page.getBlock(0)).getInt(1));
            assertEquals(5, ((IntBlock) page.getBlock(1)).getInt(1));
        }
    }

    /**
     * Regression guard for rowBuffer reuse: a full row followed by a shorter row that omits
     * a trailing projected column must produce null for the missing column, not echo the
     * value from the previous row.
     */
    public void testFusedPathRowBufferNotLeakedAcrossRows() throws IOException {
        String csv = """
            a:integer,b:integer,c:integer
            1,2,3
            4,5
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("a", "c"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            assertEquals(1, ((IntBlock) page.getBlock(0)).getInt(0));
            assertEquals(3, ((IntBlock) page.getBlock(1)).getInt(0));
            assertEquals(4, ((IntBlock) page.getBlock(0)).getInt(1));
            assertTrue("column c must be null for the shorter second row, not echoed from row 1", page.getBlock(1).isNull(1));
        }
    }

    /**
     * {@code Long.MIN_VALUE} (-9223372036854775808) has a magnitude one past
     * {@code Long.MAX_VALUE}, so the inline fast path's positive accumulator can't represent
     * it — the overflow guard bails and the string fallback via {@code Long.parseLong}
     * produces the correct value.
     */
    public void testFusedPathLongMinValueFallback() throws IOException {
        String csv = "val:long\n" + Long.MIN_VALUE + "\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("val"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(Long.MIN_VALUE, ((LongBlock) page.getBlock(0)).getLong(0));
        }
    }

    /**
     * Leading {@code +} is not a digit, so the inline fast path bails; the string fallback
     * via {@code Integer.parseInt} handles it.
     */
    public void testFusedPathLeadingPlusFallback() throws IOException {
        String csv = """
            val:integer
            +5
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("val"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(5, ((IntBlock) page.getBlock(0)).getInt(0));
        }
    }

    /**
     * Trailing whitespace after digits forces the inline fast path to bail; the string
     * fallback trims and parses correctly.
     */
    public void testFusedPathTrailingSpaceFallback() throws IOException {
        String csv = "val:long\n123 \n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("val"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(123L, ((LongBlock) page.getBlock(0)).getLong(0));
        }
    }

    /**
     * A quoted numeric field forces the fallback path (quotes suppress the inline numeric
     * accumulator); the string conversion strips quotes and parses correctly.
     */
    public void testFusedPathQuotedNumericFallback() throws IOException {
        String csv = """
            val:integer
            "123"
            """;
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("val"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(123, ((IntBlock) page.getBlock(0)).getInt(0));
        }
    }

    /**
     * A multi-line quoted field must be glued back together by the bracket-aware reader and
     * arrive intact (embedded newline included) through the fused path.
     */
    public void testFusedPathMultiLineQuotedField() throws IOException {
        String csv = "id:long,text:keyword\n1,\"hello\nworld\"\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("text"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(new BytesRef("hello\nworld"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
        }
    }

    // --- Stream drain prevention ---

    /**
     * Regression guard: {@code readSchema} must not drain the full stream body when closing
     * after reading a typed header. Storage providers that drain on close (e.g. S3) would
     * otherwise block the search thread for the full object transfer time.
     * The fix must abort the stream before the drain can occur.
     * <p>
     * This test uses a mock stream that simulates the drain-on-close behaviour. The test
     * fails against the unfixed code (because {@code close()} drains the entire body) and
     * passes once the fix ensures the stream is aborted after the header is read.
     */
    public void testReadSchemaDoesNotDrainStream_typedHeader() throws IOException {
        // Typed-header CSV: the schema is fully encoded in the header line so readSchema
        // needs only that one line. The rest of the file should never be consumed.
        StringBuilder csv = new StringBuilder("id:long,name:keyword,value:double\n");
        for (int i = 0; i < 200_000; i++) {
            csv.append(i).append(",name_").append(i).append(",").append(i * 1.5).append("\n");
        }
        byte[] bytes = csv.toString().getBytes(StandardCharsets.UTF_8);
        assertThat("test file must be >> reader buffer size to be meaningful", bytes.length, Matchers.greaterThan(1_000_000));

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(bytes, tracking);

        new CsvFormatReader(blockFactory).schema(object);

        // Only the header line was needed. The BufferedReader pre-fills up to 64 KB on
        // the first readLine(), so allow 2x that as a generous upper bound — but the vast
        // majority of the multi-MB file must not have been touched.
        assertThat(
            "readSchema must not drain the stream after reading a typed header; consumed "
                + tracking.bytesConsumed.get()
                + " of "
                + bytes.length
                + " bytes",
            tracking.bytesConsumed.get(),
            Matchers.lessThan((long) bytes.length / 2)
        );
    }

    /**
     * Regression guard: for plain (untyped) headers, type inference reads a bounded sample
     * of rows, but the remaining file body must not be drained on close.
     * <p>
     * This test uses a mock stream that simulates the drain-on-close behaviour. The test
     * fails against the unfixed code and passes once the fix aborts the stream after the
     * sample rows are consumed.
     */
    public void testReadSchemaDoesNotDrainStream_inferredSchema() throws IOException {
        // Plain headers trigger type inference from a sample (default 20 000 rows).
        // The file contains 200 000 rows so most of it should remain unread after schema().
        StringBuilder csv = new StringBuilder("id,name,value\n");
        for (int i = 0; i < 200_000; i++) {
            csv.append(i).append(",name_").append(i).append(",").append(i * 1.5).append("\n");
        }
        byte[] bytes = csv.toString().getBytes(StandardCharsets.UTF_8);
        assertThat("test file must be significantly larger than the schema sample", bytes.length, Matchers.greaterThan(2_000_000));

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(bytes, tracking);

        new CsvFormatReader(blockFactory).schema(object);

        // The sample covers at most DEFAULT_SAMPLE_SIZE rows, which is a small fraction
        // of the 200 000-row file. The remaining body must not be drained.
        assertThat(
            "readSchema must not drain beyond the schema sample; consumed "
                + tracking.bytesConsumed.get()
                + " of "
                + bytes.length
                + " bytes",
            tracking.bytesConsumed.get(),
            Matchers.lessThan((long) bytes.length / 2)
        );
    }

    /**
     * Regression for https://github.com/elastic/esql-planning/issues/894: the Jackson hot data path enforces
     * {@code max_record_size} via the upstream {@link CsvRecordCappingInputStream}. An oversized record
     * trips the cap during the {@link java.io.BufferedReader} bulk fill (potentially before any individual
     * row has been emitted), the {@link CsvRecordTooLargeException} propagates as an {@link IOException},
     * and the outer {@code CsvBatchIterator.hasNext()} wraps it in a {@link RuntimeException} whose cause
     * chain carries the original {@code "max_record_size [N]"} message.
     */
    public void testJacksonBulkPathPropagatesMaxRecordSizeError() {
        int maxRecordBytes = 32;
        String csv = "id:long,text:keyword\n1,ok\n100," + "x".repeat(maxRecordBytes) + "\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        FormatReadContext context = FormatReadContext.builder()
            .batchSize(10)
            .errorPolicy(ErrorPolicy.STRICT)
            .maxRecordBytes(maxRecordBytes)
            .build();

        RuntimeException ex = expectThrows(RuntimeException.class, () -> {
            try (CloseableIterator<Page> iterator = reader.read(object, context)) {
                while (iterator.hasNext()) {
                    Page page = iterator.next();
                    page.releaseBlocks();
                }
            }
        });
        Throwable rootCause = ex;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        assertTrue("expected a CsvRecordTooLargeException in the cause chain, got: " + ex, rootCause instanceof CsvRecordTooLargeException);
        assertThat(rootCause.getMessage(), Matchers.containsString("max_record_size [" + maxRecordBytes + "]"));
    }

    /**
     * Lenient policy still aborts the read once an oversized record trips the byte cap because the underlying
     * {@link CsvRecordCappingInputStream} cannot resume after a thrown {@link IOException}; the destructive
     * stream wrapper exists precisely to keep the byte-accounting monotonic with the parser's consumption.
     * This is a deliberate tradeoff documented on
     * {@link org.elasticsearch.xpack.esql.datasource.csv.CsvRecordCappingInputStream}: cap-too-large becomes
     * stream-fatal on the Jackson bulk path. The bracket-aware path retains row-level recovery via
     * {@link org.elasticsearch.xpack.esql.datasource.csv.CsvLogicalRecordReader}'s char-decoded accounting.
     */
    public void testJacksonBulkPathAbortsOnCapTooLargeEvenUnderLenientPolicy() {
        int maxRecordBytes = 32;
        String csv = "id:long,text:keyword\n1,ok\n100," + "x".repeat(maxRecordBytes) + "\n";
        StorageObject object = createStorageObject(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        FormatReadContext context = FormatReadContext.builder()
            .batchSize(10)
            .errorPolicy(ErrorPolicy.LENIENT)
            .maxRecordBytes(maxRecordBytes)
            .build();

        RuntimeException ex = expectThrows(RuntimeException.class, () -> {
            try (CloseableIterator<Page> iterator = reader.read(object, context)) {
                while (iterator.hasNext()) {
                    iterator.next().releaseBlocks();
                }
            }
        });
        Throwable rootCause = ex;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        assertTrue(
            "lenient policy must still abort with the cap exception in the cause chain, got: " + ex,
            rootCause instanceof CsvRecordTooLargeException
        );
        assertThat(rootCause.getMessage(), Matchers.containsString("max_record_size [" + maxRecordBytes + "]"));
    }

    /**
     * Regression for the inferred-schema bulk-path engagement (issue #894 review feedback): when the schema is
     * inferred at read time, the per-record sampling iterator must be torn down after sampling so the data path
     * picks up the Jackson bulk iterator. Without this, post-sample reads stay on the slow per-record
     * {@code CsvLogicalRecordReader} loop and the headline perf fix never engages for ad-hoc CSVs.
     *
     * <p>Behavioral assertion: the cap-stream wrap (active on non-bracket-aware reads) trips the cap as a
     * stream-fatal abort once an oversized data row is reached past the sampling window — which is only
     * possible if the bulk path is engaged after sampling. If the iterator stayed on
     * {@code CsvRecordIterator}, the cap would surface per-row and the read would either skip (lenient) or
     * fail with a different exception shape — neither matching the strict-mode contract asserted below.
     */
    public void testInferredSchemaSwitchesToJacksonBulkPathAfterSampling() {
        int maxRecordBytes = 64;
        StringBuilder csv = new StringBuilder("id,name\n");
        // A handful of small rows to feed schema inference, well below the cap.
        for (int i = 0; i < 10; i++) {
            csv.append(i).append(",row").append(i).append('\n');
        }
        // Oversized data row past the sampling prefix; only the Jackson-bulk-path cap stream sees this byte
        // span. Adding the row terminator makes the record length exceed maxRecordBytes.
        csv.append("99,").append("x".repeat(maxRecordBytes)).append('\n');
        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = new CsvFormatReader(blockFactory);

        FormatReadContext context = FormatReadContext.builder()
            .batchSize(10)
            .errorPolicy(ErrorPolicy.STRICT)
            .maxRecordBytes(maxRecordBytes)
            .build();

        RuntimeException ex = expectThrows(RuntimeException.class, () -> {
            try (CloseableIterator<Page> iterator = reader.read(object, context)) {
                while (iterator.hasNext()) {
                    iterator.next().releaseBlocks();
                }
            }
        });
        Throwable rootCause = ex;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        assertTrue(
            "inferred-schema reads must reach the cap-stream-protected bulk path after sampling, got: " + ex,
            rootCause instanceof CsvRecordTooLargeException
        );
        assertThat(rootCause.getMessage(), Matchers.containsString("max_record_size [" + maxRecordBytes + "]"));
    }

    /**
     * Regression for the bracket-aware row-recovery contract (issue #894 review feedback): when bracket-multi-value
     * parsing is enabled the cap-stream wrap is suppressed in {@link CsvFormatReader#read} so the cap stays an
     * exact, recoverable per-record check inside {@link CsvLogicalRecordReader#addBytes}. Under lenient policy an
     * oversized row must be skipped and the surrounding rows must still parse, rather than the cap-stream-wrapped
     * destructive abort that the non-bracket-aware path exhibits.
     */
    public void testBracketAwareLenientSkipsOversizedRecordAndKeepsReading() throws IOException {
        int maxRecordBytes = 32;
        StringBuilder csv = new StringBuilder("id:long,tags:keyword\n");
        csv.append("1,[a,b]\n");
        csv.append("2,[").append("x".repeat(maxRecordBytes)).append("]\n");
        csv.append("3,[c,d]\n");
        StorageObject object = createStorageObject(csv.toString());
        CsvFormatReader reader = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("multi_value_syntax", "brackets"));

        FormatReadContext context = FormatReadContext.builder()
            .batchSize(10)
            .errorPolicy(ErrorPolicy.LENIENT)
            .maxRecordBytes(maxRecordBytes)
            .build();

        long total = 0;
        try (CloseableIterator<Page> iterator = reader.read(object, context)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                total += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        assertEquals("bracket-aware lenient must drop the oversized row and keep the surrounding rows", 2L, total);
    }
}
