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
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;
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

    /**
     * Both arms report the identical wrapped message for junk after a closing quote — the direct-block arm
     * previously emitted it without the {@code "CSV parse error: "} prefix that the fallback arm adds.
     */
    public void testContentAfterCloseQuoteErrorParity() throws IOException {
        assertFailFastParity(
            false,
            Map.of(),
            null,
            "k:keyword\n\"x\"y\n",
            "line -1:-1: CSV parse error at row [1]: CSV parse error: CSV row has unexpected content after a closing "
                + "quote; row: <unparsed>; set error_mode to skip_row (or null_field) in WITH options to skip and warn "
                + "instead of failing"
        );
    }

    /** The cap is measured on the trimmed value, so surrounding whitespace does not push a field over. */
    public void testMaxFieldSizeTrimmedWithinCap() throws IOException {
        List<List<Object>> rows = read(false, Map.of("max_field_size", 5, "trim_spaces", true), "k:keyword\n  abc  \n");
        assertEquals(List.of(row(br("abc"))), rows);
    }

    /**
     * Under no-trim the cap governs the RAW token, so a padded typed value whose raw length (9) exceeds the
     * cap (5) is dropped even though its trimmed value (3) would fit — and identically whether the padded
     * column is projected or not, so projection cannot change whether the row survives.
     */
    public void testMaxFieldSizePaddedTypedNoTrimDroppedRegardlessOfProjection() throws IOException {
        Map<String, Object> config = Map.of("max_field_size", 5);
        String content = "a:integer,b:integer\n   123   ,7\n";
        assertEquals(List.of(), read(false, config, skipRow(), null, content));
        assertEquals(List.of(), read(false, config, skipRow(), List.of("b"), content));
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

    public void testDecimalInLongColumnRoundsLikeCastEngine() throws IOException {
        // A decimal token in a long column now ROUNDS (declared read == ::long, which reuses the
        // cast engine), where the former Long.parseLong path rejected it as a null-field error.
        List<List<Object>> rows = read(false, nullField(), "a:long\n1.6\n");
        assertEquals(List.of(row(2L)), rows);
    }

    /**
     * A whitespace-bearing {@code null_value} nulls a typed value that equals the RAW marker, identically on
     * both arms — the direct arm previously trimmed the value before the marker check and missed it.
     */
    public void testPaddedNullMarkerOnTypedColumnParity() throws IOException {
        // Padded marker " 0 ", value " 0 " (== raw marker) → null on both arms.
        assertEquals(List.of(row((Object) null)), read(false, Map.of("null_value", " 0 "), "a:integer\n 0 \n"));
        // Unpadded marker "0", padded value " 0 " → null via the trimmed re-check (both arms already agree).
        assertEquals(List.of(row((Object) null)), read(false, Map.of("null_value", "0"), "a:integer\n 0 \n"));
        // A padded value that is NOT the marker still parses.
        assertEquals(List.of(row(5)), read(false, Map.of("null_value", " 0 "), "a:integer\n 5 \n"));
        // Escaped path: field "\ 5 " (escaped space + "5 ") decodes to " 5 " == the padded marker, and nulls
        // on both arms — emitUnquotedEscapedField now defers the typed trim to tryConvertValue like its house
        // peer, so the raw marker isn't stripped before the check.
        assertEquals(List.of(row((Object) null)), read(false, Map.of("null_value", " 5 "), "a:integer\n\\ 5 \n"));
    }

    /** Bracket mode drops trailing whitespace after a closing quote under no-trim, matching the quoted grammar. */
    public void testBracketTrailingWhitespaceAfterCloseQuoteDropped() throws IOException {
        // Unprojected read → the full-split bracket walker.
        assertEquals(
            List.of(row(br("x"), br("y"))),
            read(false, Map.of("multi_value_syntax", "brackets"), "a:keyword,b:keyword\nx,\"y\"  \n")
        );
        // Projected read → the fused bracket walker.
        assertEquals(
            List.of(row(br("y"))),
            read(false, Map.of("multi_value_syntax", "brackets"), null, List.of("b"), "a:keyword,b:keyword\nx,\"y\"  \n")
        );
        // Trailing whitespace then a delimiter (not EOL) — the other disjunct of the skip.
        assertEquals(
            List.of(row(br("x"), br("y"), br("z"))),
            read(false, Map.of("multi_value_syntax", "brackets"), "a:keyword,b:keyword,c:keyword\nx,\"y\" ,z\n")
        );
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
        // Trimming a string column is opt-in now (default is no-trim, RFC 4180); with trim_spaces both the
        // direct-block and Jackson paths trim to "spaced".
        List<List<Object>> rows = read(false, Map.of("trim_spaces", true), "k:keyword\n  spaced  \n");
        assertEquals(List.of(row(br("spaced"))), rows);
    }

    public void testKeywordWhitespacePreservedByDefault() throws IOException {
        // Default is no-trim: a string column keeps its surrounding whitespace, identically on both paths.
        // Uses a second column so the value under test is not at column 0; column-0 leading-whitespace
        // preservation is pinned separately by testColumnZeroLeadingWhitespaceCsv / ...TsvPlain, which now
        // agree on both the direct and house arms under no-trim (QUOTED / PLAIN). (Escaped mode is the only
        // dialect that still eats col-0 leading whitespace — it stays on Jackson; not exercised here.)
        List<List<Object>> rows = read(false, Map.of(), "a:keyword,b:keyword\nx,  spaced  \n");
        assertEquals(List.of(row(br("x"), br("  spaced  "))), rows);
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

    /** A declared KEYWORD column must be able to hold the literal string "null" (any case) — see #1098. */
    public void testLiteralNullTextOnKeywordIsPreserved() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "a:keyword,b:long\nnull,1\nNULL,2\n");
        assertEquals(List.of(row(br("null"), 1L), row(br("NULL"), 2L)), rows);
    }

    /** Non-string columns keep classifying the literal "null" token as a null marker. */
    public void testLiteralNullTextOnTypedColumnIsNull() throws IOException {
        List<List<Object>> rows = read(false, Map.of(), "a:long,b:keyword\nnull,null\n1,x\n");
        assertEquals(List.of(row(null, br("null")), row(1L, br("x"))), rows);
    }

    /**
     * B1 inference parity: an untyped header forces inference, which under no-trim samples rows through the
     * house record tokenizer. The configured {@code null_value} marker must map to null before the inferrer
     * sees it (it only knows empty / {@code "null"}), so {@code score} infers INTEGER and the marker row reads
     * back as null on both the direct-block and house arms.
     */
    public void testCustomNullValueInferredNumericUnderNoTrim() throws IOException {
        List<List<Object>> rows = read(false, Map.of("null_value", "NA"), "id,score\n1,NA\n2,7\n");
        assertEquals(List.of(row(1, null), row(2, 7)), rows);
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

    public void testTsvPlainDecimalInLongColumnRoundsLikeCastEngine() throws IOException {
        List<List<Object>> rows = read(true, nullField(), "a:long\n1.6\n");
        assertEquals(List.of(row(2L)), rows);
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
        List<List<Object>> rows = read(true, Map.of("trim_spaces", true), "k:keyword\n  spaced  \n");
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

    /** A declared KEYWORD column must be able to hold the literal string "null" (any case) — see #1098. */
    public void testTsvPlainLiteralNullTextOnKeywordIsPreserved() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "k:keyword\nnull\nNULL\nx\n");
        assertEquals(List.of(row(br("null")), row(br("NULL")), row(br("x"))), rows);
    }

    /** Non-string columns keep classifying the literal "null" token as a null marker. */
    public void testTsvPlainLiteralNullTextOnTypedColumnIsNull() throws IOException {
        List<List<Object>> rows = read(true, Map.of(), "a:long\tb:keyword\nnull\tnull\n1\tx\n");
        assertEquals(List.of(row(null, br("null")), row(1L, br("x"))), rows);
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

    public void testQuotedFirstCellThatDecodesToCommentIsSkipped() throws IOException {
        // The first cell is quoted, so its raw bytes start with a quote, but its DECODED value is
        // "//x", which Jackson classifies as a comment (it decides on the decoded, trimmed first cell)
        // and drops the whole row. The direct quoted path must decode the first cell and skip it too.
        List<List<Object>> rows = read(false, Map.of("comment", "//"), "a:keyword,b:keyword\n\"//x\",1\nkeep,2\n");
        assertEquals(List.of(row(br("keep"), br("2"))), rows);
    }

    public void testQuotedFirstCellWithInnerLeadingWhitespaceDecodesToComment() throws IOException {
        // Jackson trims the decoded first cell before the prefix test, so quoted inner leading
        // whitespace (" //x") still classifies as a comment. The direct path mirrors that trim.
        List<List<Object>> rows = read(false, Map.of("comment", "//"), "a:keyword,b:keyword\n\"  //x\",1\nkeep,2\n");
        assertEquals(List.of(row(br("keep"), br("2"))), rows);
    }

    public void testQuotedFirstCellThatIsNotACommentIsKept() throws IOException {
        // A field-leading quote whose decoded value is not a comment must NOT be dropped: the decode
        // re-check only skips genuine comments, so this row survives on both paths.
        List<List<Object>> rows = read(false, Map.of("comment", "//"), "a:keyword,b:keyword\n\"hello\",1\nkeep,2\n");
        assertEquals(List.of(row(br("hello"), br("1")), row(br("keep"), br("2"))), rows);
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

    /**
     * The QUOTED + escaping dialect follows Jackson's CSV escape, whose control set is exactly
     * {@code \t \n \r \0}; every other {@code \c} — including {@code \b} and {@code \f} — is the literal
     * {@code c} (the escape only protects the next character). The C-style {@link CsvFormatReader#decodeEscapeChar}
     * would map {@code \b}/{@code \f} to backspace/form-feed, which is the {@code mode: escaped} semantics and
     * would diverge from Jackson under trim (its fallback arm). Pinned in both trim polarities so the direct
     * walker, the house tokenizer, and Jackson all agree.
     */
    public void testQuotedEscapedBackslashBAndFAreLiteral() throws IOException {
        assertEquals(List.of(row(br("abb"))), read(false, Map.of(), "k:keyword\n\"a\\bb\"\n"));
        assertEquals(List.of(row(br("abb"))), read(false, Map.of("trim_spaces", true), "k:keyword\n\"a\\bb\"\n"));
        assertEquals(List.of(row(br("afb"))), read(false, Map.of(), "k:keyword\n\"a\\fb\"\n"));
        assertEquals(List.of(row(br("afb"))), read(false, Map.of("trim_spaces", true), "k:keyword\n\"a\\fb\"\n"));
        // \t \n \r \0 stay control escapes (unchanged, and agreeing with Jackson).
        assertEquals(List.of(row(br("a\tb"))), read(false, Map.of("trim_spaces", true), "k:keyword\n\"a\\tb\"\n"));
        assertEquals(List.of(row(br("a\nb"))), read(false, Map.of("trim_spaces", true), "k:keyword\n\"a\\nb\"\n"));
        // An unquoted escaped field in the same dialect follows the identical rule.
        assertEquals(List.of(row(br("abb"))), read(false, Map.of(), "k:keyword\na\\bb\n"));
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
        List<List<Object>> rows = read(false, Map.of("trim_spaces", true), "k:keyword\nab\\\n");
        assertEquals(List.of(row(br("ab"))), rows);
    }

    /**
     * Regression for the escaped-unquoted trim-order fix: an unquoted field whose raw bytes end with
     * {@code \ } (backslash + space) must be trimmed AFTER escape-decode: Jackson decodes {@code \ }
     * to a literal space and then trims trailing decoded whitespace, yielding {@code x}, not {@code x }.
     * The direct path must match that order.
     */
    public void testUnquotedEscapedTrailingSpaceTrimmedAfterDecode() throws IOException {
        // Raw field: x\ (x + backslash + space). With trim_spaces: skip-leading-ws, decode \ → ' ' giving
        // "x ", then trim the trailing decoded ws → "x". Without trim_spaces the decoded "x " is kept.
        assertEquals(List.of(row(br("x"))), read(false, Map.of("trim_spaces", true), "k:keyword\nx\\ \n"));
        assertEquals(List.of(row(br("x "))), read(false, Map.of(), "k:keyword\nx\\ \n"));
    }

    /**
     * Companion to {@link #testUnquotedEscapedTrailingSpaceTrimmedAfterDecode}: leading raw whitespace
     * before the value is also stripped before the escape loop, and trailing decoded whitespace is
     * stripped after it. Both paths must agree on the trim order for this composite case.
     */
    public void testUnquotedEscapedLeadingAndTrailingWhitespaceTrimOrder() throws IOException {
        // Raw field: " x\ " (two spaces + x + backslash + space).
        // Jackson: skip-leading-raw-ws → "x\ ", decode → "x ", trim-trailing-decoded → "x".
        List<List<Object>> rows = read(false, Map.of("trim_spaces", true), "k:keyword\n  x\\ \n");
        assertEquals(List.of(row(br("x"))), rows);
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
    // B1: padded-quoted fields and column-0 whitespace. Under no-trim the fallback arm is now the house
    // per-record tokenizer, so each read() below is a direct-vs-house differential; under trim both arms
    // agree with Jackson (the quirks are masked). Every case is asserted in both polarities.
    // ---------------------------------------------------------------------------------------------

    public void testPaddedQuoteBeforeQuotedField() throws IOException {
        assertEquals(List.of(row(br("x"), br("y"))), read(false, Map.of(), "a:keyword,b:keyword\nx,  \"y\"\n"));
        assertEquals(List.of(row(br("x"), br("y"))), read(false, Map.of("trim_spaces", true), "a:keyword,b:keyword\nx,  \"y\"\n"));
    }

    public void testPaddedQuoteBeforeQuotedFieldTsvQuotedMode() throws IOException {
        assertEquals(List.of(row(br("x"), br("y"))), read(true, Map.of("mode", "quoted"), "a:keyword\tb:keyword\nx\t  \"y\"\n"));
        assertEquals(
            List.of(row(br("x"), br("y"))),
            read(true, Map.of("mode", "quoted", "trim_spaces", true), "a:keyword\tb:keyword\nx\t  \"y\"\n")
        );
    }

    public void testPaddedQuoteWithEmbeddedDelimiterKeepsThreeColumns() throws IOException {
        String csv = "a:keyword,b:keyword,c:keyword\nx, \"a,b\",z\n";
        assertEquals(List.of(row(br("x"), br("a,b"), br("z"))), read(false, Map.of(), csv));
        assertEquals(List.of(row(br("x"), br("a,b"), br("z"))), read(false, Map.of("trim_spaces", true), csv));
    }

    public void testPaddedQuoteTypedHeadlineRepro() throws IOException {
        String csv = "id:integer,name:keyword\n1, \"Alice, PhD\"\n";
        assertEquals(List.of(row(1, br("Alice, PhD"))), read(false, Map.of(), csv));
        assertEquals(List.of(row(1, br("Alice, PhD"))), read(false, Map.of("trim_spaces", true), csv));
    }

    public void testColumnZeroLeadingWhitespaceCsv() throws IOException {
        assertEquals(List.of(row(br("  a"), br("b"))), read(false, Map.of(), "a:keyword,b:keyword\n  a,b\n"));
        assertEquals(List.of(row(br("a"), br("b"))), read(false, Map.of("trim_spaces", true), "a:keyword,b:keyword\n  a,b\n"));
    }

    public void testColumnZeroPaddedBeforeQuoteCsv() throws IOException {
        assertEquals(List.of(row(br("q"), br("b"))), read(false, Map.of(), "a:keyword,b:keyword\n  \"q\",b\n"));
        assertEquals(List.of(row(br("q"), br("b"))), read(false, Map.of("trim_spaces", true), "a:keyword,b:keyword\n  \"q\",b\n"));
    }

    public void testColumnZeroLeadingWhitespaceTsvPlain() throws IOException {
        assertEquals(List.of(row(br("  a"), br("b"))), read(true, Map.of(), "a:keyword\tb:keyword\n  a\tb\n"));
        assertEquals(List.of(row(br("a"), br("b"))), read(true, Map.of("trim_spaces", true), "a:keyword\tb:keyword\n  a\tb\n"));
    }

    public void testTrailingWhitespaceAfterCloseQuoteBothPolarities() throws IOException {
        assertEquals(List.of(row(br("x"), br("y"))), read(false, Map.of(), "a:keyword,b:keyword\n\"x\"  ,y\n"));
        assertEquals(List.of(row(br("x"), br("y"))), read(false, Map.of("trim_spaces", true), "a:keyword,b:keyword\n\"x\"  ,y\n"));
    }

    public void testWhitespaceOnlyLineBothPolarities() throws IOException {
        assertEquals(List.of(row(br("x")), row(br("y"))), read(false, Map.of(), "a:keyword\nx\n   \ny\n"));
        assertEquals(List.of(row(br("x")), row(br("y"))), read(false, Map.of("trim_spaces", true), "a:keyword\nx\n   \ny\n"));
    }

    /**
     * Prefetch replay: with a tiny {@code schema_sample_size}, the inferred-schema sample rows are read
     * via the house per-record path and replayed, while later rows flow through a fresh iterator. Padded
     * quotes inside AND beyond the sample window must tokenize identically (3 columns, {@code a,b} intact)
     * on both arms.
     */
    public void testPrefetchReplayPaddedQuotedAcrossSampleBoundary() throws IOException {
        String csv = "h1,h2,h3\nx, \"a,b\",z\np, \"c,d\",q\nr, \"e,f\",s\nt, \"g,h\",u\n";
        List<List<Object>> rows = read(false, Map.of("schema_sample_size", 2), null, null, csv);
        assertEquals(
            List.of(
                row(br("x"), br("a,b"), br("z")),
                row(br("p"), br("c,d"), br("q")),
                row(br("r"), br("e,f"), br("s")),
                row(br("t"), br("g,h"), br("u"))
            ),
            rows
        );
    }

    /**
     * {@code _rowPosition} misbind pin: projecting the synthetic offset column (which forces the
     * recordReader path on both arms) must not change the tokenized value of a padded-quoted data column
     * versus a read that does not project it.
     */
    public void testRowPositionProjectionDoesNotChangeValues() throws IOException {
        String csv = "id:integer,name:keyword\n1, \"Alice, PhD\"\n2, \"Bob, MD\"\n";
        assertEquals(List.of(row(br("Alice, PhD")), row(br("Bob, MD"))), read(false, Map.of(), null, List.of("name"), csv));
        List<List<Object>> withPos = read(false, Map.of(), null, List.of("_rowPosition", "name"), csv);
        List<Object> names = new ArrayList<>();
        for (List<Object> r : withPos) {
            names.add(r.get(1));
        }
        assertEquals(List.of(br("Alice, PhD"), br("Bob, MD")), names);
    }

    // ---------------------------------------------------------------------------------------------
    // Bracket-mode quote-prefix alignment (B-5): both bracket walkers must drop the outer whitespace
    // before a quote so ` "y"` yields `y`, matching the direct quoted grammar.
    // ---------------------------------------------------------------------------------------------

    /** Fused bracket path (splitAndConvertProjected, walker 2) — the default (non-ALL) scope. */
    public void testBracketModePaddedQuoteFusedPath() throws IOException {
        List<List<Object>> rows = read(false, Map.of("multi_value_syntax", "brackets"), "a:keyword,b:keyword\nx,  \"y\"\n");
        assertEquals(List.of(row(br("x"), br("y"))), rows);
    }

    /** Full-split bracket path (splitCommaDelimiterBracketAwareFields, walker 1) — reached via ALL scope. */
    public void testBracketModePaddedQuoteAllScopeFullSplitPath() throws IOException {
        List<List<Object>> rows = readAllScope(Map.of("multi_value_syntax", "brackets"), "a:keyword,b:keyword\nx,  \"y\"\n");
        assertEquals(List.of(row(br("x"), br("y"))), rows);
    }

    /**
     * Reads with an ALL stats scope bound to a throwaway sink, which routes bracket parsing through the
     * full-split walker rather than the fused one. Only the direct-block arm is exercised (bracket mode is
     * not direct-eligible, so both arms parse identically); the golden assertion pins the value.
     */
    private List<List<Object>> readAllScope(Map<String, Object> config, String content) throws IOException {
        CsvFormatReader reader = (CsvFormatReader) baseReader(false).withConfig(config);
        StorageObject object = new InMemoryStorageObject(content.getBytes(StandardCharsets.UTF_8));
        FormatReadContext ctx = FormatReadContext.builder()
            .batchSize(1024)
            .recordAligned(true)
            .firstSplit(true)
            .lastSplit(true)
            .splitStartByte(0)
            .stats(0, 1024, true)
            .statsColumnScope(StripeColumnScope.ALL)
            .build();
        List<List<Object>> rows = new ArrayList<>();
        try (
            var handle = ExternalStatsCapture.bind(ExternalStatsCapture.newSink());
            CloseableIterator<Page> pages = reader.read(object, ctx)
        ) {
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
