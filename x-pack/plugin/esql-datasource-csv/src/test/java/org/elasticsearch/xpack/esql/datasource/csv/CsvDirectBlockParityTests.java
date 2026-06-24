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
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Golden behavior of the CSV/TSV read path, pinning the exact, observable output of the
 * Jackson-backed parser so that the direct-to-block parser (issue #774) is held to it. Every
 * assertion here is a contract both parsers must reproduce byte-for-byte.
 *
 * <p>The read goes through {@link #read}, which is the single A/B seam: it reads with both the
 * direct-block arm ({@code CsvFormatReader.withDirectBlockEnabled(true)}) and the Jackson arm and
 * asserts the two agree, so every test in this suite is a parity check with no per-test changes.
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
    // max_field_size cap (parity with Jackson's StreamReadConstraints.maxStringLength)
    // ---------------------------------------------------------------------------------------------

    /** A plain unquoted field over the cap fails fast with byte-for-byte Jackson's maxStringLength message. */
    public void testMaxFieldSizePlainFailFast() throws IOException {
        assertFailFastParity(
            false,
            Map.of("max_field_size", 10),
            null,
            "k:keyword\nhelloworld12\n",
            "line -1:-1: CSV parse error at row [1]: CSV parse error: String value length (12) exceeds the maximum allowed "
                + "(10, from `StreamReadConstraints.getMaxStringLength()`); row: <unparsed>; set error_mode to skip_row "
                + "(or null_field) in WITH options to skip and warn instead of failing"
        );
    }

    /** The cap counts the decoded value of a quoted field, not its raw (quote-delimited) span. */
    public void testMaxFieldSizeQuotedFailFast() throws IOException {
        assertFailFastParity(
            false,
            Map.of("max_field_size", 5),
            null,
            "k:keyword\n\"helloworld\"\n",
            "line -1:-1: CSV parse error at row [1]: CSV parse error: String value length (10) exceeds the maximum allowed "
                + "(5, from `StreamReadConstraints.getMaxStringLength()`); row: <unparsed>; set error_mode to skip_row "
                + "(or null_field) in WITH options to skip and warn instead of failing"
        );
    }

    /** A non-projected field still trips the cap: Jackson tokenizes every column regardless of projection. */
    public void testMaxFieldSizeNonProjectedFailFast() throws IOException {
        assertFailFastParity(
            false,
            Map.of("max_field_size", 5),
            List.of("a"),
            "a:keyword,b:keyword\nshort,helloworld\n",
            "line -1:-1: CSV parse error at row [1]: CSV parse error: String value length (10) exceeds the maximum allowed "
                + "(5, from `StreamReadConstraints.getMaxStringLength()`); row: <unparsed>; set error_mode to skip_row "
                + "(or null_field) in WITH options to skip and warn instead of failing"
        );
    }

    /** Under skip_row an over-cap row is dropped and surrounding rows survive, matching Jackson. */
    public void testMaxFieldSizeSkipRow() throws IOException {
        List<List<Object>> rows = read(false, Map.of("max_field_size", 10), skipRow(), null, "k:keyword\nok\nhelloworld12\nfine\n");
        assertEquals(List.of(row(br("ok")), row(br("fine"))), rows);
    }

    /** The cap is measured on the trimmed value, so surrounding whitespace does not push a field over. */
    public void testMaxFieldSizeTrimmedWithinCap() throws IOException {
        List<List<Object>> rows = read(false, Map.of("max_field_size", 5), "k:keyword\n  abc  \n");
        assertEquals(List.of(row(br("abc"))), rows);
    }

    /** A doubled quote decodes to a single character, so {@code "a""b"} (decoded {@code a"b}) is within a cap of 5. */
    public void testMaxFieldSizeQuotedDoubledWithinCap() throws IOException {
        List<List<Object>> rows = read(false, Map.of("max_field_size", 5), "k:keyword\n\"a\"\"b\"\n");
        assertEquals(List.of(row(br("a\"b"))), rows);
    }

    /**
     * Runs both the direct-block and Jackson arms under FAIL_FAST and asserts each throws a
     * {@link ParsingException} whose message equals {@code expectedMessage}. Pinning the literal also
     * guards the Jackson baseline: a Jackson upgrade that reworded the constraint message trips this test.
     *
     * <p>Pinned under {@link Locale#ROOT}: Jackson formats the length numbers in this particular message
     * with the default {@code FORMAT} locale (so e.g. a Bengali locale yields Bengali numerals), whereas
     * the direct path emits locale-independent ASCII digits on purpose, the ES-idiomatic choice. Under a
     * non-ROOT locale the two would differ only in digit script; forcing ROOT pins the contract in the
     * production-relevant ASCII case without asserting that Jackson locale quirk.
     */
    private void assertFailFastParity(boolean tsv, Map<String, Object> config, List<String> projection, String content, String expected)
        throws IOException {
        Locale previous = Locale.getDefault();
        Locale.setDefault(Locale.ROOT);
        try {
            CsvFormatReader base = config.isEmpty() ? baseReader(tsv) : (CsvFormatReader) baseReader(tsv).withConfig(config);
            String jackson = captureFailFastMessage(base.withDirectBlockEnabled(false), projection, content);
            String direct = captureFailFastMessage(base.withDirectBlockEnabled(true), projection, content);
            assertEquals("Jackson baseline message changed", expected, jackson);
            assertEquals("direct-block message diverged from the Jackson baseline", jackson, direct);
        } finally {
            Locale.setDefault(previous);
        }
    }

    private String captureFailFastMessage(CsvFormatReader reader, List<String> projection, String content) throws IOException {
        try {
            drain(reader, projection, 1024, ErrorPolicy.STRICT, content);
            throw new AssertionError("expected a ParsingException but the read completed");
        } catch (ParsingException e) {
            return e.getMessage();
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

    /**
     * Probes the boundary of the direct path's char-range double parser (jackson-core's fast parser)
     * against the Jackson baseline's {@link Double#parseDouble}. The cross-check inside {@code read}
     * asserts the two paths agree; golden values pin {@code Double.parseDouble} semantics for the
     * trickier accepted forms: float type suffixes, hex floats, leading sign, and bare-dot literals.
     */
    public void testDoubleParserBoundaryForms() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "d:double\n1.5d\n2.5f\n3D\n4F\n0x1.8p1\n+1.5\n.5\n5.\n");
        assertEquals(List.of(row(1.5), row(2.5), row(3.0), row(4.0), row(3.0), row(1.5), row(0.5), row(5.0)), rows);
    }

    /** NaN / Infinity forms accepted by {@link Double#parseDouble} must parse identically on both paths. */
    public void testDoubleSpecialValues() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "d:double\nNaN\nInfinity\n-Infinity\n+Infinity\n");
        assertEquals(
            List.of(row(Double.NaN), row(Double.POSITIVE_INFINITY), row(Double.NEGATIVE_INFINITY), row(Double.POSITIVE_INFINITY)),
            rows
        );
    }

    /** Whitespace around a numeric cell is trimmed before parsing on both paths. */
    public void testDoubleSurroundingWhitespaceTrimmed() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "d:double\n  1.5  \n\t-2.25\t\n");
        assertEquals(List.of(row(1.5), row(-2.25)), rows);
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
    // TSV plain mode: these are eligible for the direct-to-block path, so the read(...) parity
    // harness compares the direct arm against the Jackson arm for every one of them.
    // ---------------------------------------------------------------------------------------------

    public void testTsvPlainLongAndIntegerBasic() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "a:long\tb:integer\n1\t2\n-3\t-4\n0\t0\n");
        assertEquals(List.of(row(1L, 2), row(-3L, -4), row(0L, 0)), rows);
    }

    public void testTsvPlainLeadingPlusAndZeros() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "a:long\tb:integer\n+5\t+6\n007\t008\n");
        assertEquals(List.of(row(5L, 6), row(7L, 8)), rows);
    }

    public void testTsvPlainLongMaxMin() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "a:long\n" + Long.MAX_VALUE + "\n" + Long.MIN_VALUE + "\n");
        assertEquals(List.of(row(Long.MAX_VALUE), row(Long.MIN_VALUE)), rows);
    }

    public void testTsvPlainNumericWhitespaceTrimmed() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "a:long\tb:integer\n  7 \t 8 \n");
        assertEquals(List.of(row(7L, 8)), rows);
    }

    public void testTsvPlainLongOverflowNullFieldYieldsNull() throws IOException {
        List<List<Object>> rows = read(true, nullField(), "a:long\n99999999999999999999999\n");
        assertEquals(List.of(row((Object) null)), rows);
    }

    public void testTsvPlainIntegerOverflowNullFieldYieldsNull() throws IOException {
        List<List<Object>> rows = read(true, nullField(), "a:integer\n3000000000\n");
        assertEquals(List.of(row((Object) null)), rows);
    }

    public void testTsvPlainDecimalInLongColumnNullFieldYieldsNull() throws IOException {
        List<List<Object>> rows = read(true, nullField(), "a:long\n1.0\n");
        assertEquals(List.of(row((Object) null)), rows);
    }

    public void testTsvPlainDoubleForms() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "d:double\n1.5\n-2.25\n3\n1e3\n-4.5E-2\n");
        assertEquals(List.of(row(1.5), row(-2.25), row(3.0), row(1000.0), row(-0.045)), rows);
    }

    public void testTsvPlainBoolean() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "b:boolean\ntrue\nfalse\nTRUE\nFalse\n");
        assertEquals(List.of(row(true), row(false), row(true), row(false)), rows);
    }

    public void testTsvPlainKeywordBasicAndUnicode() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "k:keyword\nhello\nna\u00efve caf\u00e9\n\u4f60\u597d\n");
        assertEquals(List.of(row(br("hello")), row(br("na\u00efve caf\u00e9")), row(br("\u4f60\u597d"))), rows);
    }

    public void testTsvPlainKeywordWhitespaceTrimmed() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "k:keyword\n  spaced  \n");
        assertEquals(List.of(row(br("spaced"))), rows);
    }

    public void testTsvPlainNonAsciiWhitespaceNotTrimmed() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "k:keyword\n\u00a0x\u00a0\n");
        assertEquals(List.of(row(br("\u00a0x\u00a0"))), rows);
    }

    public void testTsvPlainEmptyCellIsNull() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "a:keyword\tb:long\n\t5\nx\t\n");
        assertEquals(List.of(row(null, 5L), row(br("x"), null)), rows);
    }

    public void testTsvPlainCustomNullValue() throws IOException {
        List<List<Object>> rows = read(true, Map.of("null_value", "NULL"), "a:keyword\tb:long\nNULL\tNULL\nx\t5\n");
        assertEquals(List.of(row(null, null), row(br("x"), 5L)), rows);
    }

    public void testTsvPlainLiteralNullTextIsNull() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "k:keyword\nnull\nNULL\nx\n");
        assertEquals(List.of(row((Object) null), row((Object) null), row(br("x"))), rows);
    }

    public void testTsvPlainDatetimeEpochAndIso() throws IOException {
        long epoch = 1609459200000L;
        List<List<Object>> rows = read(true, Map.of(), "ts:datetime\n" + epoch + "\n2021-01-01T00:00:00Z\n");
        assertEquals(List.of(row(epoch), row(epoch)), rows);
    }

    public void testTsvPlainDateNanosIso() throws IOException {
        String iso = "2024-01-15T12:34:56.123456789Z";
        List<List<Object>> rows = read(true, Map.of(), "ts:date_nanos\n" + iso + "\n");
        assertEquals(List.of(row(EsqlDataTypeConverter.dateNanosToLong(iso))), rows);
    }

    public void testTsvPlainIp() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "addr:ip\n1.1.1.1\n8.8.8.8\n");
        assertEquals(List.of(row(ip("1.1.1.1")), row(ip("8.8.8.8"))), rows);
    }

    public void testTsvPlainVersion() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "v:version\n1.0.0\n2.3.1\n");
        assertEquals(
            List.of(row(EsqlDataTypeConverter.stringToVersion("1.0.0")), row(EsqlDataTypeConverter.stringToVersion("2.3.1"))),
            rows
        );
    }

    public void testTsvPlainMissingTrailingColumnNullFilled() throws IOException {
        List<List<Object>> rows = read(true, nullField(), "a:long\tb:keyword\n1\n");
        assertEquals(List.of(row(1L, null)), rows);
    }

    public void testTsvPlainExtraColumnSkipRowDropsRow() throws IOException {
        List<List<Object>> rows = read(true, skipRow(), "a:long\tb:keyword\n1\tx\textra\n2\ty\n");
        assertEquals(List.of(row(2L, br("y"))), rows);
    }

    public void testTsvPlainProjectionSubset() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), null, List.of("c", "a"), "a:long\tb:keyword\tc:integer\n1\tx\t9\n");
        assertEquals(List.of(row(9, 1L)), rows);
    }

    public void testTsvPlainStrictAbortsOnBadNumeric() {
        assertThrows(Exception.class, () -> read(true, Map.of(), ErrorPolicy.STRICT, null, "a:long\n1\nnotanumber\n3\n"));
    }

    public void testTsvPlainSkipRowDropsBadRow() throws IOException {
        List<List<Object>> rows = read(true, skipRow(), "a:long\n1\nnotanumber\n3\n");
        assertEquals(List.of(row(1L), row(3L)), rows);
    }

    public void testTsvPlainNullFieldNullsBadCell() throws IOException {
        List<List<Object>> rows = read(true, nullField(), "a:long\tb:keyword\n1\tx\nnotanumber\ty\n");
        assertEquals(List.of(row(1L, br("x")), row(null, br("y"))), rows);
    }

    public void testTsvPlainBlankLinesSkipped() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "a:long\n1\n\n2\n\n");
        assertEquals(List.of(row(1L), row(2L)), rows);
    }

    public void testTsvPlainCommentLinesSkipped() throws IOException {
        List<List<Object>> rows = read(true, Map.of("comment", "//"), "a:long\n1\n// a comment\n2\n");
        assertEquals(List.of(row(1L), row(2L)), rows);
    }

    public void testTsvPlainWhitespaceOnlyLine() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "a:keyword\nx\n   \ny\n");
        assertEquals(List.of(row(br("x")), row(br("y"))), rows);
    }

    public void testTsvPlainMultiplePagesPreserveRowOrder() throws IOException {
        StringBuilder tsv = new StringBuilder("a:long\n");
        for (int i = 0; i < 7; i++) {
            tsv.append(i).append('\n');
        }
        List<List<Object>> rows = read(true, Map.of(), null, null, 2, tsv.toString());
        assertEquals(7, rows.size());
        for (int i = 0; i < 7; i++) {
            assertEquals(List.of((Object) (long) i), rows.get(i));
        }
    }

    public void testTsvPlainInferredSchemaFromHeader() throws IOException {
        // Untyped header forces schema inference, which samples rows via Jackson; those prefetched
        // rows are drained through the String[] path before the direct-block loop takes over. Exercises
        // that hand-off boundary.
        List<List<Object>> rows = read(true, Map.of(), "a\tb\n1\thello\n2\tworld\n3\tagain\n");
        assertEquals(List.of(row(1, br("hello")), row(2, br("world")), row(3, br("again"))), rows);
    }

    public void testTsvPlainHeaderlessSynthesizedColumns() throws IOException {
        List<List<Object>> rows = read(true, Map.of("header_row", false), "1\thello\n2\tworld\n");
        assertEquals(List.of(row(1, br("hello")), row(2, br("world"))), rows);
    }

    public void testTsvPlainCountStar() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), null, List.of(), "a:long\tb:keyword\n1\tx\n2\ty\n3\tz\n");
        assertEquals(3, rows.size());
        for (List<Object> r : rows) {
            assertEquals(List.of(), r);
        }
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

    public void testTsvLeadingDelimiterBeforeCommentPrefixIsDataRow() throws IOException {
        // A TSV line whose first cell is empty (a leading TAB delimiter) is NOT a comment, even though
        // the comment prefix follows: Jackson classifies comments on the first parsed cell, so the
        // direct path must keep this as a two-column data row rather than dropping it.
        List<List<Object>> rows = read(true, Map.of("comment", "//"), "a:keyword\tb:keyword\nx\ty\n\t// c\n");
        assertEquals(List.of(row(br("x"), br("y")), row((Object) null, br("// c"))), rows);
    }

    public void testCommaLeadingDelimiterBeforeCommentPrefixIsDataRow() throws IOException {
        // Same first-cell rule for CSV: an empty first cell before the comment prefix is a data row.
        List<List<Object>> rows = read(false, Map.of("comment", "//"), "a:keyword,b:keyword\nx,y\n,// c\n");
        assertEquals(List.of(row(br("x"), br("y")), row((Object) null, br("// c"))), rows);
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
    // Quoted CSV escape / whitespace semantics (default QUOTED mode is eligible for the direct path,
    // so these run direct-vs-Jackson). Pinned from the current Jackson behavior.
    // ---------------------------------------------------------------------------------------------

    public void testQuotedInnerWhitespacePreserved() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\n\"  x  \"\n");
        assertEquals(List.of(row(br("  x  "))), rows);
    }

    public void testQuotedOuterWhitespaceTrimmed() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\n  \"x\"  \n");
        assertEquals(List.of(row(br("x"))), rows);
    }

    public void testQuotedEscapedQuote() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\n\"a\\\"b\"\n");
        assertEquals(List.of(row(br("a\"b"))), rows);
    }

    public void testQuotedEscapedBackslash() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\n\"a\\\\b\"\n");
        assertEquals(List.of(row(br("a\\b"))), rows);
    }

    public void testQuotedEscapedTabDecodesToTab() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\n\"a\\tb\"\n");
        assertEquals(List.of(row(br("a\tb"))), rows);
    }

    public void testUnquotedEscapedCommaIsLiteral() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\na\\,b\n");
        assertEquals(List.of(row(br("a,b"))), rows);
    }

    public void testQuotedEscapedQuoteThenComma() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\n\"a\\\",b\"\n");
        assertEquals(List.of(row(br("a\",b"))), rows);
    }

    public void testQuoteInMiddleOfUnquotedIsLiteral() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\nab\"cd\"\n");
        assertEquals(List.of(row(br("ab\"cd\""))), rows);
    }

    public void testQuotedNumericParses() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "n:long\n\"123\"\n");
        assertEquals(List.of(row(123L)), rows);
    }

    public void testTrailingContentAfterCloseQuoteStrictAborts() {
        assertThrows(Exception.class, () -> read(false, Map.of(), "k:keyword\n\"x\"y\n"));
    }

    public void testTrailingContentAfterCloseQuoteSkipRow() throws IOException {
        List<List<Object>> rows = read(false, skipRow(), "k:keyword\n\"x\"y\n\"ok\"\n");
        assertEquals(List.of(row(br("ok"))), rows);
    }

    public void testQuotedWhitespaceThenDelimiter() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "a:keyword,b:keyword\n\"x\"  ,y\n");
        assertEquals(List.of(row(br("x"), br("y"))), rows);
    }

    public void testEscapedNewlineUnquotedStaysInField() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\na\\\nb\n");
        assertEquals(List.of(row(br("a\nb"))), rows);
    }

    public void testEscapedNewlineInQuotesStaysInField() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\n\"a\\\nb\"\n");
        assertEquals(List.of(row(br("a\nb"))), rows);
    }

    public void testTrailingLoneEscapeDropped() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\nab\\\n");
        assertEquals(List.of(row(br("ab"))), rows);
    }

    public void testEscapeNValueIsLiteralN() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "k:keyword\n\"a\\Nb\"\n");
        assertEquals(List.of(row(br("aNb"))), rows);
    }

    public void testTwoQuotedFieldsWithEmbeddedDelimiter() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "a:keyword,b:keyword\n\"p,q\",\"r\"\n");
        assertEquals(List.of(row(br("p,q"), br("r"))), rows);
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
        return tsv
            ? new CsvFormatReader(blockFactory, CsvFormatOptions.TSV, "tsv", List.of(".tsv"))
            : new CsvFormatReader(blockFactory, "csv", List.of(".csv"));
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
        CsvFormatReader configured = config.isEmpty() ? baseReader(tsv) : (CsvFormatReader) baseReader(tsv).withConfig(config);
        // Parity harness: read once with the direct-to-block path (default) and once with it forced
        // off (Jackson), and assert the two agree row-for-row. For modes that are not eligible for the
        // direct path (e.g. bracket multi-values or escaped mode) both arms are Jackson and the
        // comparison is trivially true, but the golden assertEquals in each test still pins behavior.
        // The direct arm is read first so a test using assertThrows still observes the direct path's
        // exception.
        List<List<Object>> direct = drain(configured.withDirectBlockEnabled(true), projection, batchSize, policy, content);
        List<List<Object>> jackson = drain(configured.withDirectBlockEnabled(false), projection, batchSize, policy, content);
        assertEquals("direct-block output diverged from the Jackson baseline", jackson, direct);
        return direct;
    }

    private List<List<Object>> drain(CsvFormatReader reader, List<String> projection, int batchSize, ErrorPolicy policy, String content)
        throws IOException {
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
