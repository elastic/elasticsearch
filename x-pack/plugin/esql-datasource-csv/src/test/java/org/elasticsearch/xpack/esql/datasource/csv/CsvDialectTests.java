/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatOptions.Dialect;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * The dialect axis: {@code quoted} (RFC-4180 wrapping), {@code escaped} (backslash, no quoting —
 * ClickHouse/MySQL/Postgres), {@code plain} (no quoting, no escaping — Unix/bioinformatics). The
 * dialect is authoritative for the mechanism; {@code quote}/{@code escape} only carry the character
 * and are inert under a dialect that does not use them. Cross-validation: a user-supplied character
 * the dialect never consults is rejected (rule 1), and the dialect's ACTIVE characters must be
 * pairwise-distinct and not line terminators (rule 2).
 */
public class CsvDialectTests extends ESTestCase {

    // ---- Dialect.parse ----

    public void testDialectParse() {
        assertEquals(Dialect.QUOTED, Dialect.parse("quoted"));
        assertEquals(Dialect.ESCAPED, Dialect.parse("ESCAPED"));
        assertEquals(Dialect.PLAIN, Dialect.parse(" Plain "));
        assertNull(Dialect.parse(null));
        assertNull(Dialect.parse(""));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> Dialect.parse("clickhouse"));
        assertEquals("Invalid dialect [clickhouse]. Accepted values: \"quoted\", \"escaped\", \"plain\"", ex.getMessage());
    }

    public void testPreDialectConstructorDefaultsToQuoted() {
        assertEquals(Dialect.QUOTED, CsvFormatOptions.DEFAULT.dialect());
        // The .tsv baseline is PLAIN — bulk TSV at rest does not quote (see the TSV preset javadoc).
        assertEquals(Dialect.PLAIN, CsvFormatOptions.TSV.dialect());
        CsvFormatOptions legacy = new CsvFormatOptions(
            ',',
            '"',
            '\\',
            "//",
            "",
            StandardCharsets.UTF_8,
            null,
            CsvFormatOptions.DEFAULT_MAX_FIELD_SIZE,
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX
        );
        assertEquals(Dialect.QUOTED, legacy.dialect());
    }

    // ---- Rule 2: active characters pairwise-distinct, no line terminators ----

    public void testQuotedRejectsQuoteEqualsDelimiter() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> options(',', ',', '\\', Dialect.QUOTED));
        assertEquals("quote [,] must differ from delimiter [,]", ex.getMessage());
    }

    public void testQuotedRejectsQuoteEqualsEscape() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> options(',', '\'', '\'', Dialect.QUOTED));
        assertEquals("quote and escape must differ, both are [']", ex.getMessage());
    }

    public void testEscapedRejectsEscapeEqualsDelimiter() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> options('\t', '"', '\t', Dialect.ESCAPED));
        assertEquals("escape [\\t] must differ from delimiter [\\t]", ex.getMessage());
    }

    public void testLineTerminatorsRejectedForActiveCharacters() {
        assertEquals(
            "delimiter must not be a line terminator (\\n / \\r)",
            expectThrows(IllegalArgumentException.class, () -> options('\n', '"', '\\', Dialect.PLAIN)).getMessage()
        );
        assertEquals(
            "quote must not be a line terminator (\\n / \\r)",
            expectThrows(IllegalArgumentException.class, () -> options(',', '\n', '\\', Dialect.QUOTED)).getMessage()
        );
        assertEquals(
            "escape must not be a line terminator (\\n / \\r)",
            expectThrows(IllegalArgumentException.class, () -> options(',', '"', '\r', Dialect.ESCAPED)).getMessage()
        );
    }

    /** Inert characters are not constrained: only what the dialect consults must be coherent. */
    public void testInertCharactersAreUnconstrained() {
        // PLAIN never consults quote or escape — a colliding value is inert, not an error.
        assertEquals(Dialect.PLAIN, options(',', ',', ',', Dialect.PLAIN).dialect());
        // ESCAPED never consults the quote.
        assertEquals(Dialect.ESCAPED, options('\t', '\t', '\\', Dialect.ESCAPED).dialect());
        // QUOTED with a newline escape would be rejected; under PLAIN the same chars are fine.
        assertEquals(Dialect.PLAIN, options(',', '"', '\n', Dialect.PLAIN).dialect());
    }

    // ---- Rule 1: a user-supplied character the dialect never consults is rejected ----

    public void testPlainRejectsExplicitQuote() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> tsvReader().withConfigTrackingConsumedKeys(Map.of("dialect", "plain", "quote", "\""))
        );
        assertEquals("the [plain] dialect does not use a quote character; use dialect [quoted]", ex.getMessage());
    }

    public void testEscapedRejectsExplicitQuote() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> tsvReader().withConfigTrackingConsumedKeys(Map.of("dialect", "escaped", "quote", "'"))
        );
        assertEquals("the [escaped] dialect does not use a quote character; use dialect [quoted]", ex.getMessage());
    }

    public void testPlainRejectsExplicitEscape() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> csvReader().withConfigTrackingConsumedKeys(Map.of("dialect", "plain", "escape", "\\\\"))
        );
        assertEquals("the [plain] dialect does not use an escape character; use dialect [quoted] or [escaped]", ex.getMessage());
    }

    /** An empty string means "use the default", not an explicit character — no rule-1 rejection. */
    public void testEmptyCharacterValueIsNotExplicit() {
        assertNotNull(csvReader().withConfigTrackingConsumedKeys(Map.of("dialect", "plain", "quote", "")));
        assertNotNull(csvReader().withConfigTrackingConsumedKeys(Map.of("dialect", "plain", "escape", "")));
    }

    public void testBracketsRequireQuotedDialect() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> csvReader().withConfigTrackingConsumedKeys(Map.of("dialect", "plain", "multi_value_syntax", "brackets"))
        );
        assertEquals("multi_value_syntax [brackets] requires dialect [quoted]; the bracket scanner honors quoted fields", ex.getMessage());
    }

    public void testInvalidDialectValueRejected() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> csvReader().withConfigTrackingConsumedKeys(Map.of("dialect", "clickhouse"))
        );
        assertEquals("Invalid dialect [clickhouse]. Accepted values: \"quoted\", \"escaped\", \"plain\"", ex.getMessage());
    }

    // ---- Acceptance: coherent configurations resolve ----

    public void testCoherentDialectConfigsAccepted() {
        assertNotNull(tsvReader().withConfigTrackingConsumedKeys(Map.of("dialect", "plain")));
        assertNotNull(tsvReader().withConfigTrackingConsumedKeys(Map.of("dialect", "escaped")));
        assertNotNull(tsvReader().withConfigTrackingConsumedKeys(Map.of("dialect", "escaped", "escape", "\\\\")));
        assertNotNull(csvReader().withConfigTrackingConsumedKeys(Map.of("dialect", "quoted", "quote", "'")));
        assertNotNull(csvReader().withConfigTrackingConsumedKeys(Map.of("dialect", "plain", "delimiter", "|")));
    }

    // ---- helpers ----

    private static CsvFormatOptions options(char delimiter, char quote, char escape, Dialect dialect) {
        return new CsvFormatOptions(
            delimiter,
            quote,
            escape,
            "//",
            "",
            StandardCharsets.UTF_8,
            null,
            CsvFormatOptions.DEFAULT_MAX_FIELD_SIZE,
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX,
            dialect
        );
    }

    private static CsvFormatReader csvReader() {
        return new CsvFormatReader(blockFactory(), "csv", List.of(".csv"));
    }

    private static CsvFormatReader tsvReader() {
        return new CsvFormatReader(blockFactory(), CsvFormatOptions.TSV, "tsv", List.of(".tsv"));
    }

    private static BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
    }
}
