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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Golden behavior of the CSV/TSV read path, written to pin the exact, observable output of the
 * current (Jackson-backed) parser so that the upcoming direct-to-block parser (issue #774) can be
 * held to it. Every assertion here is a contract the new parser must reproduce byte-for-byte.
 *
 * <p>The read goes through {@link #read}, which is the single A/B seam: today it reads with the
 * current parser. Once {@code CsvFormatReader.withDirectBlockEnabled} lands, that helper will also
 * read with the direct-block arm and assert the two agree, so this whole suite becomes a parity
 * harness with no per-test changes.
 *
 * <p>Values are rendered per element type: numeric/boolean as boxed primitives, datetime/date_nanos
 * as their stored {@code long}, and any {@code BYTES_REF} column (keyword/text/ip/version) as the
 * raw {@link BytesRef} so keyword text and the encoded ip/version forms are both unambiguous.
 */
public class CsvDirectBlockParityTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /** Lenient reads emit response-header warnings; drop them so the parent {@code ensureNoWarnings} passes. */
    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            threadContext.stashContext();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Integer / long
    // ---------------------------------------------------------------------------------------------

    public void testLongAndIntegerBasic() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "a:long,b:integer\n1,2\n-3,-4\n0,0\n");
        assertEquals(List.of(row(1L, 2), row(-3L, -4), row(0L, 0)), rows);
    }

    public void testLongLeadingPlusAndZeros() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "a:long,b:integer\n+5,+6\n007,008\n");
        assertEquals(List.of(row(5L, 6), row(7L, 8)), rows);
    }

    public void testLongMaxMin() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "a:long\n" + Long.MAX_VALUE + "\n" + Long.MIN_VALUE + "\n");
        assertEquals(List.of(row(Long.MAX_VALUE), row(Long.MIN_VALUE)), rows);
    }

    public void testNumericWhitespaceTrimmed() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "a:long,b:integer\n  7 , 8 \n");
        assertEquals(List.of(row(7L, 8)), rows);
    }

    public void testLongOverflowNullFieldYieldsNull() throws IOException {
        List<List<Object>> rows = read(false, nullField(), "a:long\n99999999999999999999999\n");
        assertEquals(List.of(row((Object) null)), rows);
    }

    public void testIntegerOverflowNullFieldYieldsNull() throws IOException {
        List<List<Object>> rows = read(false, nullField(), "a:integer\n3000000000\n");
        assertEquals(List.of(row((Object) null)), rows);
    }

    public void testDecimalInLongColumnNullFieldYieldsNull() throws IOException {
        List<List<Object>> rows = read(false, nullField(), "a:long\n1.0\n");
        assertEquals(List.of(row((Object) null)), rows);
    }

    // ---------------------------------------------------------------------------------------------
    // Double
    // ---------------------------------------------------------------------------------------------

    public void testDoubleForms() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "d:double\n1.5\n-2.25\n3\n1e3\n-4.5E-2\n");
        assertEquals(List.of(row(1.5), row(-2.25), row(3.0), row(1000.0), row(-0.045)), rows);
    }

    // ---------------------------------------------------------------------------------------------
    // Boolean
    // ---------------------------------------------------------------------------------------------

    public void testBoolean() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "b:boolean\ntrue\nfalse\nTRUE\nFalse\n");
        assertEquals(List.of(row(true), row(false), row(true), row(false)), rows);
    }

    // ---------------------------------------------------------------------------------------------
    // Keyword / text
    // ---------------------------------------------------------------------------------------------

    public void testKeywordBasicAndUnicode() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\nhello\nna\u00efve caf\u00e9\n\u4f60\u597d\n");
        assertEquals(List.of(row(br("hello")), row(br("na\u00efve caf\u00e9")), row(br("\u4f60\u597d"))), rows);
    }

    public void testKeywordWhitespaceTrimmed() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\n  spaced  \n");
        assertEquals(List.of(row(br("spaced"))), rows);
    }

    public void testQuotedFieldWithDelimiterAndDoubledQuote() throws IOException {
        String csv = "a:keyword,b:keyword\n\"has,comma\",\"he said \"\"hi\"\"\"\n";
        List<List<Object>> rows = read(false, Map.of(), csv);
        assertEquals(List.of(row(br("has,comma"), br("he said \"hi\""))), rows);
    }

    public void testQuotedEmbeddedNewline() throws IOException {
        String csv = "a:keyword,b:keyword\n\"line1\nline2\",tail\n";
        List<List<Object>> rows = read(false, Map.of(), csv);
        assertEquals(List.of(row(br("line1\nline2"), br("tail"))), rows);
    }

    public void testEmptyQuotedFieldIsNull() throws IOException {
        // The current parser collapses an empty quoted field "" to null, exactly like an empty
        // unquoted cell; pin that so the direct-block parser must match.
        List<List<Object>> rows = read(false, Map.of(), "a:keyword,b:keyword\n\"\",x\n");
        assertEquals(List.of(row(null, br("x"))), rows);
    }

    // ---------------------------------------------------------------------------------------------
    // Null handling
    // ---------------------------------------------------------------------------------------------

    public void testEmptyUnquotedCellIsNull() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "a:keyword,b:long\n,5\nx,\n");
        assertEquals(List.of(row(null, 5L), row(br("x"), null)), rows);
    }

    public void testCustomNullValue() throws IOException {
        List<List<Object>> rows = read(false, Map.of("null_value", "NULL"), "a:keyword,b:long\nNULL,NULL\nx,5\n");
        assertEquals(List.of(row(null, null), row(br("x"), 5L)), rows);
    }

    // ---------------------------------------------------------------------------------------------
    // Datetime / date_nanos
    // ---------------------------------------------------------------------------------------------

    public void testDatetimeEpochAndIso() throws IOException {
        long epoch = 1609459200000L; // 2021-01-01T00:00:00Z
        List<List<Object>> rows = read(false, Map.of(), "ts:datetime\n" + epoch + "\n2021-01-01T00:00:00Z\n");
        assertEquals(List.of(row(epoch), row(epoch)), rows);
    }

    public void testDateNanosIso() throws IOException {
        String iso = "2024-01-15T12:34:56.123456789Z";
        List<List<Object>> rows = read(false, Map.of(), "ts:date_nanos\n" + iso + "\n");
        assertEquals(List.of(row(EsqlDataTypeConverter.dateNanosToLong(iso))), rows);
    }

    // ---------------------------------------------------------------------------------------------
    // ip / version (encoded BytesRef)
    // ---------------------------------------------------------------------------------------------

    public void testIp() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "addr:ip\n1.1.1.1\n8.8.8.8\n");
        assertEquals(List.of(row(ip("1.1.1.1")), row(ip("8.8.8.8"))), rows);
    }

    public void testVersion() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "v:version\n1.0.0\n2.3.1\n");
        assertEquals(
            List.of(row(EsqlDataTypeConverter.stringToVersion("1.0.0")), row(EsqlDataTypeConverter.stringToVersion("2.3.1"))),
            rows
        );
    }

    // ---------------------------------------------------------------------------------------------
    // TSV + modes
    // ---------------------------------------------------------------------------------------------

    public void testTsvBasic() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "a:long\tb:keyword\n1\thello\n2\tworld\n");
        assertEquals(List.of(row(1L, br("hello")), row(2L, br("world"))), rows);
    }

    public void testTsvPlainFieldLeadingQuoteIsLiteral() throws IOException {
        List<List<Object>> rows = read(true, Map.of("mode", "plain"), "a:keyword\tb:keyword\n\"quote\tnormal\n");
        assertEquals(List.of(row(br("\"quote"), br("normal"))), rows);
    }

    public void testQuotedEscapeNoneKeepsBackslashLiteral() throws IOException {
        List<List<Object>> rows = read(false, Map.of("escape", "none"), "p:keyword\n\"C:\\Users\"\n");
        assertEquals(List.of(row(br("C:\\Users"))), rows);
    }

    // ---------------------------------------------------------------------------------------------
    // Blank / comment lines and row counts
    // ---------------------------------------------------------------------------------------------

    public void testBlankLinesSkipped() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "a:long\n1\n\n2\n\n");
        assertEquals(List.of(row(1L), row(2L)), rows);
    }

    public void testCommentLinesSkipped() throws IOException {
        List<List<Object>> rows = read(false, Map.of("comment", "//"), "a:long\n1\n// a comment\n2\n");
        assertEquals(List.of(row(1L), row(2L)), rows);
    }

    public void testWhitespaceOnlyLine() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "a:keyword\nx\n   \ny\n");
        assertEquals(List.of(row(br("x")), row(br("y"))), rows);
    }

    public void testCommentWithLeadingWhitespaceSkipped() throws IOException {
        List<List<Object>> rows = read(false, Map.of("comment", "//"), "a:long\n1\n   // indented comment\n2\n");
        assertEquals(List.of(row(1L), row(2L)), rows);
    }

    public void testNonAsciiWhitespaceNotTrimmed() throws IOException {
        // Only ASCII whitespace is trimmed; a leading/trailing NBSP (U+00A0) is data and survives.
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\n\u00a0x\u00a0\n");
        assertEquals(List.of(row(br("\u00a0x\u00a0"))), rows);
    }

    // ---------------------------------------------------------------------------------------------
    // Structural edge cases
    // ---------------------------------------------------------------------------------------------

    public void testExtraColumnSkipRowDropsRow() throws IOException {
        List<List<Object>> rows = read(false, skipRow(), "a:long,b:keyword\n1,x,extra\n2,y\n");
        assertEquals(List.of(row(2L, br("y"))), rows);
    }

    public void testUnclosedQuoteAtEofStrictAborts() {
        assertThrows(Exception.class, () -> read(false, Map.of(), "a:keyword,b:keyword\n\"unterminated,x\n"));
    }

    public void testMultiplePagesPreserveRowOrder() throws IOException {
        StringBuilder csv = new StringBuilder("a:long\n");
        for (int i = 0; i < 7; i++) {
            csv.append(i).append('\n');
        }
        List<List<Object>> rows = read(false, Map.of(), null, null, 2, csv.toString());
        assertEquals(7, rows.size());
        for (int i = 0; i < 7; i++) {
            assertEquals(List.of((Object) (long) i), rows.get(i));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Ragged / extra columns
    // ---------------------------------------------------------------------------------------------

    public void testMissingTrailingColumnNullFilled() throws IOException {
        List<List<Object>> rows = read(false, nullField(), "a:long,b:keyword\n1\n");
        assertEquals(List.of(row(1L, null)), rows);
    }

    // ---------------------------------------------------------------------------------------------
    // Projection
    // ---------------------------------------------------------------------------------------------

    public void testProjectionSubset() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), null, List.of("c", "a"), "a:long,b:keyword,c:integer\n1,x,9\n");
        assertEquals(List.of(row(9, 1L)), rows);
    }

    // ---------------------------------------------------------------------------------------------
    // Error policy: STRICT aborts
    // ---------------------------------------------------------------------------------------------

    public void testStrictAbortsOnBadNumeric() {
        assertThrows(Exception.class, () -> read(false, Map.of(), ErrorPolicy.STRICT, null, "a:long\n1\nnotanumber\n3\n"));
    }

    public void testSkipRowDropsBadRow() throws IOException {
        List<List<Object>> rows = read(false, skipRow(), "a:long\n1\nnotanumber\n3\n");
        assertEquals(List.of(row(1L), row(3L)), rows);
    }

    public void testNullFieldNullsBadCell() throws IOException {
        List<List<Object>> rows = read(false, nullField(), "a:long,b:keyword\n1,x\nnotanumber,y\n");
        assertEquals(List.of(row(1L, br("x")), row(null, br("y"))), rows);
    }

    // ---------------------------------------------------------------------------------------------
    // Harness
    // ---------------------------------------------------------------------------------------------

    private static ErrorPolicy skipRow() {
        return new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 100, 0.0, true);
    }

    private static ErrorPolicy nullField() {
        return new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 100, 0.0, true);
    }

    private CsvFormatReader baseReader(boolean tsv) {
        CsvFormatReader reader = tsv
            ? new CsvFormatReader(blockFactory, CsvFormatOptions.TSV, "tsv", List.of(".tsv"))
            : new CsvFormatReader(blockFactory, "csv", List.of(".csv"));
        // TODO(directToBlock): once CsvFormatReader.withDirectBlockEnabled lands, read with BOTH the
        // Jackson arm and the direct-block arm here (or in read(...)) and assert they agree, turning
        // this golden suite into a parity harness.
        return reader;
    }

    private List<List<Object>> read(boolean tsv, Map<String, Object> config, String content) throws IOException {
        return read(tsv, config, null, null, content);
    }

    private List<List<Object>> read(boolean tsv, ErrorPolicy policy, String content) throws IOException {
        return read(tsv, Map.of(), policy, null, content);
    }

    private List<List<Object>> read(boolean tsv, Map<String, Object> config, ErrorPolicy policy, List<String> projection, String content)
        throws IOException {
        return read(tsv, config, policy, projection, 1024, content);
    }

    private List<List<Object>> read(
        boolean tsv,
        Map<String, Object> config,
        ErrorPolicy policy,
        List<String> projection,
        int batchSize,
        String content
    ) throws IOException {
        CsvFormatReader reader = config.isEmpty() ? baseReader(tsv) : (CsvFormatReader) baseReader(tsv).withConfig(config);
        StorageObject object = new InMemoryStorageObject(content.getBytes(StandardCharsets.UTF_8));
        FormatReadContext ctx = FormatReadContext.builder().projectedColumns(projection).batchSize(batchSize).errorPolicy(policy).build();
        List<List<Object>> rows = new ArrayList<>();
        try (CloseableIterator<Page> pages = reader.read(object, ctx)) {
            while (pages.hasNext()) {
                Page page = pages.next();
                try {
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        List<Object> r = new ArrayList<>(page.getBlockCount());
                        for (int b = 0; b < page.getBlockCount(); b++) {
                            r.add(valueAt(page.getBlock(b), p));
                        }
                        rows.add(r);
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        return rows;
    }

    private static Object valueAt(Block block, int p) {
        if (block.isNull(p)) {
            return null;
        }
        int first = block.getFirstValueIndex(p);
        return switch (block.elementType()) {
            case LONG -> ((LongBlock) block).getLong(first);
            case INT -> ((IntBlock) block).getInt(first);
            case DOUBLE -> ((DoubleBlock) block).getDouble(first);
            case BOOLEAN -> ((BooleanBlock) block).getBoolean(first);
            case BYTES_REF -> ((BytesRefBlock) block).getBytesRef(first, new BytesRef());
            default -> throw new AssertionError("unexpected element type: " + block.elementType());
        };
    }

    private static List<Object> row(Object... values) {
        List<Object> r = new ArrayList<>(values.length);
        for (Object v : values) {
            r.add(v);
        }
        return r;
    }

    private static BytesRef br(String s) {
        return new BytesRef(s);
    }

    private static BytesRef ip(String s) {
        return new BytesRef(InetAddressPoint.encode(InetAddresses.forString(s)));
    }

    private record InMemoryStorageObject(byte[] bytes) implements StorageObject {
        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(bytes);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(bytes, Math.toIntExact(position), Math.toIntExact(length));
        }

        @Override
        public long length() {
            return bytes.length;
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
            return StoragePath.of("mem://csv-direct-block-parity-tests");
        }
    }
}
