/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit + differential coverage for {@link CsvFormatReader#splitRecordFields}, the house record tokenizer
 * that replaces Jackson on the no-trim, non-escaped-mode ({@code QUOTED} / {@code PLAIN}) record paths.
 *
 * <p>The no-trim cases pin the exact grammar the direct-to-block walkers implement (leading-whitespace
 * preservation at column 0, outer-whitespace skip before a quote, padded-quoted column counts). The
 * trim=true block is a Jackson-equivalence differential: it confirms the splitter agrees with Jackson's
 * own tokenization under {@code TRIM_SPACES}, so widening {@link CsvFormatReader#jacksonGrammarApplies()}
 * to route trim=true through the splitter would stay behavior-preserving.
 */
public class CsvRecordTokenizerTests extends ESTestCase {

    private static final int NO_CAP = Integer.MAX_VALUE;

    // ---------------------------------------------------------------------------------------------
    // No-trim grammar (the production path): the B1 repros and the col-0 fix
    // ---------------------------------------------------------------------------------------------

    public void testQuotedPaddedBeforeQuoteSkipsWhitespace() {
        assertTokens(csv(), "x,  \"y\"", "x", "y");
    }

    public void testQuotedPaddedQuoteWithEmbeddedDelimiterKeepsColumnCount() {
        // The B1 headline: Jackson (no-trim) splits this into 4 fields; the house grammar keeps 3.
        assertTokens(csv(), "x, \"a,b\",z", "x", "a,b", "z");
    }

    public void testQuotedPaddedQuoteTwoColumns() {
        assertTokens(csv(), "1, \"Alice, PhD\"", "1", "Alice, PhD");
    }

    public void testCsvColumnZeroLeadingWhitespacePreserved() {
        assertTokens(csv(), "  a,b", "  a", "b");
    }

    public void testCsvColumnZeroPaddedBeforeQuote() {
        assertTokens(csv(), "  \"q\",b", "q", "b");
    }

    public void testPlainTsvColumnZeroLeadingWhitespacePreserved() {
        assertTokens(tsv(), "  a\tb", "  a", "b");
    }

    public void testTrailingWhitespaceAfterCloseQuote() {
        assertTokens(csv(), "x,\"y\"  ", "x", "y");
    }

    public void testEmptyQuotedFieldIsEmptyString() {
        // The tokenizer emits "" for an empty quoted field; downstream tryConvertValue maps "" to null.
        assertTokens(csv(), "\"\",x", "", "x");
    }

    public void testInnerWhitespacePreservedInQuotedField() {
        assertTokens(csv(), "\"  x  \"", "  x  ");
    }

    public void testDoubledQuoteDecodes() {
        assertTokens(csv(), "\"a\"\"b\"", "a\"b");
    }

    public void testUnquotedEscapedCommaIsLiteral() {
        assertTokens(csv(), "a\\,b", "a,b");
    }

    public void testUnquotedFieldVerbatimUnderNoTrim() {
        assertTokens(csv(), "x,  spaced  ", "x", "  spaced  ");
    }

    public void testTrailingDelimiterYieldsEmptyLastField() {
        assertTokens(csv(), "a,b,", "a", "b", "");
        assertTokens(tsv(), "a\tb\t", "a", "b", "");
    }

    public void testQuotedNewlinePreservedInsideField() {
        assertTokens(csv(), "\"line1\nline2\",tail", "line1\nline2", "tail");
    }

    public void testQuoteInMiddleOfUnquotedIsLiteral() {
        assertTokens(csv(), "ab\"cd\"", "ab\"cd\"");
    }

    // ---------------------------------------------------------------------------------------------
    // max_field_size cap: same message as the direct walker's rejectFieldTooLarge
    // ---------------------------------------------------------------------------------------------

    public void testPlainFieldOverCapThrowsWithJacksonMessage() {
        MalformedRowException e = expectThrows(
            MalformedRowException.class,
            () -> CsvFormatReader.splitRecordFields("helloworld12", csv(), 10)
        );
        assertEquals(CsvFormatReader.fieldSizeExceededDetail(12, 10), e.getMessage());
    }

    public void testQuotedFieldOverCapCountsDecodedLength() {
        MalformedRowException e = expectThrows(
            MalformedRowException.class,
            () -> CsvFormatReader.splitRecordFields("\"helloworld\"", csv(), 5)
        );
        assertEquals(CsvFormatReader.fieldSizeExceededDetail(10, 5), e.getMessage());
    }

    public void testDoubledQuoteWithinCap() {
        // "a""b" decodes to a"b (length 3), within a cap of 5.
        assertTokens(csv(), 5, "\"a\"\"b\"", "a\"b");
    }

    public void testNonFirstFieldOverCapStillThrows() {
        MalformedRowException e = expectThrows(
            MalformedRowException.class,
            () -> CsvFormatReader.splitRecordFields("short,helloworld", csv(), 5)
        );
        assertEquals(CsvFormatReader.fieldSizeExceededDetail(10, 5), e.getMessage());
    }

    // ---------------------------------------------------------------------------------------------
    // Unclosed quote
    // ---------------------------------------------------------------------------------------------

    public void testUnclosedQuoteThrows() {
        expectThrows(MalformedRowException.class, () -> CsvFormatReader.splitRecordFields("\"unterminated,x", csv(), NO_CAP));
    }

    // ---------------------------------------------------------------------------------------------
    // trim=true: Jackson-equivalence differential (splitter vs the production Jackson mapper shape)
    // ---------------------------------------------------------------------------------------------

    public void testTrimTrueMatchesJacksonQuoted() throws IOException {
        assertJacksonEquivalence(csvTrim(), quotedCorpus());
    }

    public void testTrimTrueMatchesJacksonPlain() throws IOException {
        assertJacksonEquivalence(tsvTrim(), plainCorpus());
    }

    // ---------------------------------------------------------------------------------------------
    // Harness
    // ---------------------------------------------------------------------------------------------

    private static void assertTokens(CsvFormatOptions options, String record, String... expected) {
        assertTokens(options, NO_CAP, record, expected);
    }

    private static void assertTokens(CsvFormatOptions options, int maxFieldChars, String record, String... expected) {
        assertEquals(
            "tokenization of [" + record + "]",
            Arrays.asList(expected),
            Arrays.asList(CsvFormatReader.splitRecordFields(record, options, maxFieldChars))
        );
    }

    /**
     * Parses every record in {@code corpus} with both the house splitter (trim on) and a Jackson mapper
     * built exactly as {@link CsvFormatReader#createMapper} / {@code newCsvSchema} do, and asserts the two
     * agree field-for-field after normalizing Jackson's {@code null} (its {@code withNullValue} substitution)
     * against the splitter's raw {@code ""}. Records Jackson skips entirely (blank under SKIP_EMPTY_LINES) are
     * skipped here — blank-line handling lives outside the tokenizer, in the per-record read loop.
     */
    private static void assertJacksonEquivalence(CsvFormatOptions options, List<String> corpus) throws IOException {
        CsvMapper mapper = jacksonMapper(options);
        CsvSchema schema = jacksonSchema(options);
        for (String record : corpus) {
            List<String> jackson = jacksonTokens(mapper, schema, record);
            if (jackson == null) {
                continue; // Jackson skipped it (empty line); the splitter's blank handling is elsewhere.
            }
            String[] house = CsvFormatReader.splitRecordFields(record, options, NO_CAP);
            assertEquals(
                "column count for [" + record + "] (jackson=" + jackson + " house=" + Arrays.asList(house) + ")",
                jackson.size(),
                house.length
            );
            for (int i = 0; i < jackson.size(); i++) {
                assertEquals(
                    "field [" + i + "] of [" + record + "]",
                    normalizeNull(jackson.get(i), options),
                    normalizeNull(house[i], options)
                );
            }
        }
    }

    /** Jackson's withNullValue substitutes null for a field equal to nullValue; fold both arms to one token. */
    private static String normalizeNull(String value, CsvFormatOptions options) {
        if (value == null || value.equals(options.nullValue())) {
            return " NULL ";
        }
        return value;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static List<String> jacksonTokens(CsvMapper mapper, CsvSchema schema, String record) throws IOException {
        try (MappingIterator<List> it = mapper.readerFor(List.class).with(schema).readValues(new StringReader(record))) {
            if (it.hasNext() == false) {
                return null;
            }
            List<?> row = it.next();
            List<String> out = new ArrayList<>(row.size());
            for (Object o : row) {
                out.add(o == null ? null : o.toString());
            }
            return out;
        }
    }

    private static CsvMapper jacksonMapper(CsvFormatOptions opts) {
        CsvMapper mapper = new CsvMapper();
        if (opts.trimSpaces()) {
            mapper.enable(CsvParser.Feature.TRIM_SPACES);
        }
        mapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES);
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
        return mapper;
    }

    private static CsvSchema jacksonSchema(CsvFormatOptions opts) {
        CsvSchema schema = CsvSchema.emptySchema().withColumnSeparator(opts.delimiter()).withNullValue(opts.nullValue());
        if (opts.quoting() == false) {
            return schema.withoutQuoteChar();
        }
        schema = schema.withQuoteChar(opts.quoteChar());
        return opts.escaping() ? schema.withEscapeChar(opts.escapeChar()) : schema;
    }

    private static List<String> quotedCorpus() {
        return List.of(
            "a,b,c",
            "x,  \"y\"",
            "x, \"a,b\",z",
            "1, \"Alice, PhD\"",
            "\"has,comma\",\"he said \"\"hi\"\"\"",
            "\"  x  \",tail",
            "  \"q\",b",
            "x,\"y\"  ,z",
            "a,b,",
            ",lead",
            "one",
            "\"quoted\"",
            "n1,n2,n3,n4"
        );
    }

    private static List<String> plainCorpus() {
        return List.of("a\tb\tc", "  a\tb", "x\t  spaced  ", "a\tb\t", "\tlead", "one", "n1\tn2\tn3");
    }

    private static CsvFormatOptions csv() {
        return CsvFormatOptions.DEFAULT;
    }

    private static CsvFormatOptions tsv() {
        return CsvFormatOptions.TSV;
    }

    private static CsvFormatOptions csvTrim() {
        return withTrim(CsvFormatOptions.DEFAULT, true);
    }

    private static CsvFormatOptions tsvTrim() {
        return withTrim(CsvFormatOptions.TSV, true);
    }

    private static CsvFormatOptions withTrim(CsvFormatOptions o, boolean trim) {
        return new CsvFormatOptions(
            o.delimiter(),
            o.quoteChar(),
            o.escapeChar(),
            o.commentPrefix(),
            o.nullValue(),
            o.encoding(),
            o.datetimeFormatter(),
            o.maxFieldSize(),
            o.multiValueSyntax(),
            o.headerRow(),
            o.columnPrefix(),
            o.quoting(),
            o.escaping(),
            trim
        );
    }
}
