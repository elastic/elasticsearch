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
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatOptions.Mode;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * The {@code mode} preset over the (quoting, escaping) knobs: {@code quoted} (RFC-4180 wrapping),
 * {@code escaped} (backslash, no quoting — database exports), {@code plain} (no quoting, no
 * escaping — Unix/bioinformatics). {@code mode} only seeds the two knobs; explicit {@code quote}/
 * {@code escape} keys always override it (the literal {@code none} turns a knob off), so there is no
 * coherence rejection. The only validation that survives is structural: the ACTIVE characters must be
 * pairwise-distinct and not line terminators.
 */
public class CsvModeTests extends ESTestCase {

    // ---- Mode.parse ----

    public void testModeParse() {
        assertEquals(Mode.QUOTED, Mode.parse("quoted"));
        assertEquals(Mode.ESCAPED, Mode.parse("ESCAPED"));
        assertEquals(Mode.PLAIN, Mode.parse(" Plain "));
        assertNull(Mode.parse(null));
        assertNull(Mode.parse(""));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> Mode.parse("tabbed"));
        assertEquals("Invalid mode [tabbed]. Accepted values: \"quoted\", \"escaped\", \"plain\"", ex.getMessage());
    }

    public void testPreModeConstructorDefaultsToQuoted() {
        assertTrue(CsvFormatOptions.DEFAULT.quoting());
        assertTrue(CsvFormatOptions.DEFAULT.escaping());
        // The .tsv baseline is PLAIN — bulk TSV at rest does not quote (see the TSV preset javadoc).
        assertFalse(CsvFormatOptions.TSV.quoting());
        assertFalse(CsvFormatOptions.TSV.escaping());
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
        assertTrue(legacy.quoting());
        assertTrue(legacy.escaping());
    }

    // ---- Structural validation: active characters pairwise-distinct, no line terminators ----

    public void testQuotedRejectsQuoteEqualsDelimiter() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> options(',', ',', '\\', Mode.QUOTED));
        assertEquals("quote [,] must differ from delimiter [,]", ex.getMessage());
    }

    public void testQuotedRejectsQuoteEqualsEscape() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> options(',', '\'', '\'', Mode.QUOTED));
        assertEquals("quote and escape must differ, both are [']", ex.getMessage());
    }

    public void testEscapedRejectsEscapeEqualsDelimiter() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> options('\t', '"', '\t', Mode.ESCAPED));
        assertEquals("escape [\\t] must differ from delimiter [\\t]", ex.getMessage());
    }

    public void testLineTerminatorsRejectedForActiveCharacters() {
        assertEquals(
            "delimiter must not be a line terminator (\\n / \\r)",
            expectThrows(IllegalArgumentException.class, () -> options('\n', '"', '\\', Mode.PLAIN)).getMessage()
        );
        assertEquals(
            "quote must not be a line terminator (\\n / \\r)",
            expectThrows(IllegalArgumentException.class, () -> options(',', '\n', '\\', Mode.QUOTED)).getMessage()
        );
        assertEquals(
            "escape must not be a line terminator (\\n / \\r)",
            expectThrows(IllegalArgumentException.class, () -> options(',', '"', '\r', Mode.ESCAPED)).getMessage()
        );
    }

    /** Inactive characters are not constrained: only what the resolved knobs consult must be coherent. */
    public void testInactiveCharactersAreUnconstrained() {
        // PLAIN consults neither quote nor escape — a colliding value is inactive, not an error.
        assertFalse(options(',', ',', ',', Mode.PLAIN).quoting());
        // ESCAPED never consults the quote, so quote == delimiter is fine.
        CsvFormatOptions escaped = options('\t', '\t', '\\', Mode.ESCAPED);
        assertFalse(escaped.quoting());
        assertTrue(escaped.escaping());
        // QUOTED with a newline escape would be rejected; under PLAIN the same chars are fine.
        assertFalse(options(',', '"', '\n', Mode.PLAIN).escaping());
    }

    // ---- Permissive overrides: a character the preset wouldn't use is no longer rejected ----

    public void testPlainAcceptsExplicitQuoteOverride() {
        assertNotNull(tsvReader().withConfigTrackingConsumedKeys(Map.of("mode", "plain", "quote", "\"")));
    }

    public void testEscapedAcceptsExplicitQuoteOverride() {
        assertNotNull(tsvReader().withConfigTrackingConsumedKeys(Map.of("mode", "escaped", "quote", "'")));
        // escaped+quote is accepted, and emits the deterministic decode-disabled config warning; clear
        // it so it doesn't trip ESTestCase's unexpected-response-header check at teardown.
        threadContext.stashContext();
    }

    public void testPlainAcceptsExplicitEscapeOverride() {
        assertNotNull(csvReader().withConfigTrackingConsumedKeys(Map.of("mode", "plain", "escape", "\\\\")));
    }

    public void testQuoteNoneAndEscapeNoneAccepted() {
        assertNotNull(csvReader().withConfigTrackingConsumedKeys(Map.of("mode", "quoted", "quote", "none")));
        assertNotNull(csvReader().withConfigTrackingConsumedKeys(Map.of("mode", "quoted", "escape", "none")));
    }

    /** An empty string means "use the default", not an explicit character. */
    public void testEmptyCharacterValueIsNotExplicit() {
        assertNotNull(csvReader().withConfigTrackingConsumedKeys(Map.of("mode", "plain", "quote", "")));
        assertNotNull(csvReader().withConfigTrackingConsumedKeys(Map.of("mode", "plain", "escape", "")));
    }

    public void testBracketsRequireQuoting() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> csvReader().withConfigTrackingConsumedKeys(Map.of("mode", "plain", "multi_value_syntax", "brackets"))
        );
        assertEquals(
            "multi_value_syntax [brackets] requires quoting (mode [quoted] or an explicit quote character); "
                + "the bracket scanner honors quoted fields",
            ex.getMessage()
        );
        // ... but an explicit quote override re-enables quoting, so brackets are then accepted.
        assertNotNull(csvReader().withConfigTrackingConsumedKeys(Map.of("mode", "plain", "quote", "\"", "multi_value_syntax", "brackets")));
    }

    public void testInvalidModeValueRejected() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> csvReader().withConfigTrackingConsumedKeys(Map.of("mode", "tabbed"))
        );
        assertEquals("Invalid mode [tabbed]. Accepted values: \"quoted\", \"escaped\", \"plain\"", ex.getMessage());
    }

    // ---- Acceptance: configurations resolve ----

    public void testConfigsAccepted() {
        assertNotNull(tsvReader().withConfigTrackingConsumedKeys(Map.of("mode", "plain")));
        assertNotNull(tsvReader().withConfigTrackingConsumedKeys(Map.of("mode", "escaped")));
        assertNotNull(tsvReader().withConfigTrackingConsumedKeys(Map.of("mode", "escaped", "escape", "\\\\")));
        assertNotNull(csvReader().withConfigTrackingConsumedKeys(Map.of("mode", "quoted", "quote", "'")));
        assertNotNull(csvReader().withConfigTrackingConsumedKeys(Map.of("mode", "plain", "delimiter", "|")));
    }

    // ---- helpers ----

    private static CsvFormatOptions options(char delimiter, char quote, char escape, Mode mode) {
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
            mode.usesQuote(),
            mode.usesEscape(),
            false // trimSpaces: default no-trim
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
