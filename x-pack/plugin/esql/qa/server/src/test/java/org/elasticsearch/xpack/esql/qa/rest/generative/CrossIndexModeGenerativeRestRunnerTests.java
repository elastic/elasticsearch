/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.generator.Column;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.qa.rest.generative.CrossIndexModeGenerativeRestRunner.CANDIDATE_STRIPPED_MAPPING_PARAMS;
import static org.elasticsearch.xpack.esql.qa.rest.generative.CrossIndexModeGenerativeRestRunner.REFERENCE_STRIPPED_MAPPING_PARAMS;
import static org.elasticsearch.xpack.esql.qa.rest.generative.CrossIndexModeGenerativeRestRunner.SORTED_SET_BACKED_TYPES;
import static org.elasticsearch.xpack.esql.qa.rest.generative.CrossIndexModeGenerativeRestRunner.canonicalValue;
import static org.elasticsearch.xpack.esql.qa.rest.generative.CrossIndexModeGenerativeRestRunner.cellMatchesWithinRoundingTolerance;
import static org.elasticsearch.xpack.esql.qa.rest.generative.CrossIndexModeGenerativeRestRunner.isStrictAllFieldsNarrowingDifference;
import static org.elasticsearch.xpack.esql.qa.rest.generative.CrossIndexModeGenerativeRestRunner.rowsMatchWithinRoundingTolerance;
import static org.elasticsearch.xpack.esql.qa.rest.generative.CrossIndexModeGenerativeRestRunner.toCanonical;
import static org.elasticsearch.xpack.esql.qa.rest.generative.CrossIndexModeGenerativeRestRunner.toCanonicalCells;
import static org.elasticsearch.xpack.esql.qa.rest.generative.CrossIndexModeGenerativeRestRunner.withoutMappingParams;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for the comparison helpers in {@link CrossIndexModeGenerativeRestRunner}.
 *
 * <p>These cover the type-aware MV cell comparison ({@code toCanonical}), the near-zero
 * double snap ({@code canonicalValue}), the rounding-boundary fallback, the strict field-less
 * {@code qstr} failure allowance and the per-side mapping sanitisation. The helpers are package-private so this test can live
 * in the same package without requiring reflection or changes to production accessibility.
 */
public class CrossIndexModeGenerativeRestRunnerTests extends ESTestCase {

    // -----------------------------------------------------------------------
    // SORTED_SET_BACKED_TYPES coverage
    // -----------------------------------------------------------------------

    public void testSortedSetBackedTypesContainsExpected() {
        assertTrue(SORTED_SET_BACKED_TYPES.contains("keyword"));
        assertTrue(SORTED_SET_BACKED_TYPES.contains("text"));
        assertTrue(SORTED_SET_BACKED_TYPES.contains("ip"));
        assertTrue(SORTED_SET_BACKED_TYPES.contains("version"));
        assertTrue(SORTED_SET_BACKED_TYPES.contains("wildcard"));

        // Numeric and boolean types must NOT be in this set: they use SortedNumericDocValues
        // which sorts but does not deduplicate. Collapsing their duplicates would mask real bugs.
        assertFalse(SORTED_SET_BACKED_TYPES.contains("long"));
        assertFalse(SORTED_SET_BACKED_TYPES.contains("integer"));
        assertFalse(SORTED_SET_BACKED_TYPES.contains("double"));
        assertFalse(SORTED_SET_BACKED_TYPES.contains("boolean"));
        assertFalse(SORTED_SET_BACKED_TYPES.contains("date"));
    }

    // -----------------------------------------------------------------------
    // toCanonical — keyword MV: order + dedup absorbed
    // -----------------------------------------------------------------------

    /**
     * Standard mode returns keyword MVs sorted and deduplicated (SortedSetDocValues).
     * Columnar mode returns them in source-insertion order with duplicates.
     * After canonicalisation both should compare equal.
     */
    public void testKeywordMvDeduplicatedEqualsColumnarMvWithDuplicates() {
        // schema: one keyword column
        List<Column> schema = List.of(new Column("kw", "keyword", List.of()));

        // Standard: ["a","b"] (sorted + deduped from ["b","a","a"])
        List<List<Object>> stdRows = List.of(List.of(List.of("a", "b")));
        // Columnar: ["b","a","a"] (source order)
        List<List<Object>> colRows = List.of(List.of(List.of("b", "a", "a")));

        List<String> stdCanon = toCanonical(stdRows, schema);
        List<String> colCanon = toCanonical(colRows, schema);

        assertEquals("Standard and columnar keyword MV should canonicalise equal", stdCanon, colCanon);
    }

    /** Single-value keyword cells should also compare equal regardless of representation. */
    public void testKeywordSingleValueEqual() {
        List<Column> schema = List.of(new Column("kw", "keyword", List.of()));
        List<List<Object>> rows = List.of(List.of("hello"));
        List<String> canon = toCanonical(rows, schema);
        assertEquals(List.of("hello"), canon);
    }

    // -----------------------------------------------------------------------
    // toCanonical — long MV: order absorbed, duplicates retained
    // -----------------------------------------------------------------------

    /**
     * Standard mode returns long MVs sorted with duplicates kept (SortedNumericDocValues).
     * Columnar mode returns them in source-insertion order with duplicates.
     * Sorting on both sides should make them equal.
     */
    public void testLongMvOrderAbsorbed() {
        List<Column> schema = List.of(new Column("n", "long", List.of()));

        // Standard: [1,1,2] (sorted, dups kept)
        List<List<Object>> stdRows = List.of(List.of(List.of(1L, 1L, 2L)));
        // Columnar: [2,1,1] (source order)
        List<List<Object>> colRows = List.of(List.of(List.of(2L, 1L, 1L)));

        assertEquals(toCanonical(stdRows, schema), toCanonical(colRows, schema));
    }

    /**
     * Duplicate loss in long MV (one side has [1,1,2], other has [1,2]) must NOT be masked:
     * these should canonicalise differently because long does not dedup.
     */
    public void testLongMvDuplicateLossDetected() {
        List<Column> schema = List.of(new Column("n", "long", List.of()));

        List<List<Object>> withDups = List.of(List.of(List.of(1L, 1L, 2L)));
        List<List<Object>> withoutDup = List.of(List.of(List.of(1L, 2L)));

        assertNotEquals(toCanonical(withDups, schema), toCanonical(withoutDup, schema));
    }

    // -----------------------------------------------------------------------
    // toCanonical — boolean MV: order absorbed, duplicates retained
    // -----------------------------------------------------------------------

    /** Boolean MV: sorting absorbs order difference; duplicate retention is verified. */
    public void testBooleanMvOrderAbsorbed() {
        List<Column> schema = List.of(new Column("b", "boolean", List.of()));

        // Standard: [false,true,true] (sorted, SortedNumericDocValues — dups kept)
        List<List<Object>> stdRows = List.of(List.of(List.of(false, true, true)));
        // Columnar: [true,false,true] (source order)
        List<List<Object>> colRows = List.of(List.of(List.of(true, false, true)));

        assertEquals(toCanonical(stdRows, schema), toCanonical(colRows, schema));
    }

    /** Boolean MV duplicate loss must be detected (boolean does not dedup in standard mode). */
    public void testBooleanMvDuplicateLossDetected() {
        List<Column> schema = List.of(new Column("b", "boolean", List.of()));

        List<List<Object>> withDup = List.of(List.of(List.of(false, true, true)));
        List<List<Object>> withoutDup = List.of(List.of(List.of(false, true)));

        assertNotEquals(toCanonical(withDup, schema), toCanonical(withoutDup, schema));
    }

    // -----------------------------------------------------------------------
    // toCanonical — SKIP_VALUE_COLUMN_TYPES placeholder
    // -----------------------------------------------------------------------

    /** geo_point columns are replaced by "~" regardless of value. */
    public void testGeoPointColumnSkipped() {
        List<Column> schema = List.of(new Column("loc", "geo_point", List.of()));
        List<List<Object>> rows = List.of(List.of("POINT (1.0 2.0)"));
        List<String> canon = toCanonical(rows, schema);
        assertEquals(List.of("~"), canon);
    }

    // -----------------------------------------------------------------------
    // canonicalValue — near-zero double snap
    // -----------------------------------------------------------------------

    /** Welford residual ~1e-32 must snap to "0.0". */
    public void testNearZeroDoubleSnappedToZero() {
        assertEquals("0.0", canonicalValue(1e-32));
        assertEquals("0.0", canonicalValue(-1e-32));
        assertEquals("0.0", canonicalValue(1e-10));
        assertEquals("0.0", canonicalValue(0.0));
    }

    /** Values at or above 1e-9 must NOT be snapped. */
    public void testSmallButMeaningfulDoubleNotSnapped() {
        // 1e-9 is right at the boundary — we snap values strictly below 1e-9
        String canon = canonicalValue(1e-3);
        assertFalse("1e-3 should not snap to 0.0", canon.equals("0.0"));

        String canon2 = canonicalValue(0.001);
        assertFalse("0.001 should not snap to 0.0", canon2.equals("0.0"));
    }

    /** NaN and Infinity are returned as-is without snapping. */
    public void testSpecialDoublesNotSnapped() {
        assertEquals("NaN", canonicalValue(Double.NaN));
        assertEquals("Infinity", canonicalValue(Double.POSITIVE_INFINITY));
        assertEquals("-Infinity", canonicalValue(Double.NEGATIVE_INFINITY));
    }

    // -----------------------------------------------------------------------
    // canonicalValue — 5-significant-figure rounding
    // -----------------------------------------------------------------------

    /** Non-trivial double is rounded to 5 significant figures. */
    public void testDoubleRoundedToFiveSigFigs() {
        // 1.23456789 → 1.2346 (5 sig figs, HALF_DOWN) — also absorbs variance ULP noise
        // such as -1.43178 vs -1.43177.
        assertEquals(canonicalValue(-1.43178), canonicalValue(-1.43177));
        String canon = canonicalValue(1.23456789);
        assertFalse("Should be rounded, not full precision", canon.equals(String.valueOf(1.23456789)));
    }

    // -----------------------------------------------------------------------
    // canonicalValue — WKT geometry coordinate normalisation
    // -----------------------------------------------------------------------

    public void testWktCoordinatesNormalised() {
        String raw = "POINT (4.999999953433871 4.999999995343387)";
        String canon = canonicalValue(raw);
        // Both coordinates should round to something near 5.0
        assertTrue("Expected normalised WKT, got: " + canon, canon.startsWith("POINT (5.0 5.0)"));
    }

    // -----------------------------------------------------------------------
    // toCanonical — row ordering is multiset (rows sorted after canonicalisation)
    // -----------------------------------------------------------------------

    /**
     * Two result sets with the same rows in different order should produce the same canonical list
     * after the caller sorts them (toCanonical itself does not sort rows — the caller does).
     */
    public void testCanonicalRowsCanBeSortedToCompare() {
        List<Column> schema = List.of(new Column("x", "long", List.of()));

        List<List<Object>> rowsA = List.of(List.of(1L), List.of(2L));
        List<List<Object>> rowsB = List.of(List.of(2L), List.of(1L));

        List<String> canonA = toCanonical(rowsA, schema);
        List<String> canonB = toCanonical(rowsB, schema);

        java.util.Collections.sort(canonA);
        java.util.Collections.sort(canonB);

        assertEquals(canonA, canonB);
    }

    // -----------------------------------------------------------------------
    // rounding tolerance — fallback for values on a rounding boundary
    // -----------------------------------------------------------------------

    /** A stored coordinate and its doc-values reconstruction round to different last digits. */
    public void testWktCoordinateOnRoundingBoundaryMatches() {
        String ref = canonicalValue("POINT (-99.8825 16.8636)");
        String cand = canonicalValue("POINT (-99.88250002 16.86360001)");
        assertNotEquals("precondition: exact canonical forms differ", ref, cand);

        assertTrue(cellMatchesWithinRoundingTolerance(ref, cand, "keyword"));
    }

    public void testMultiValueWktCoordinateOnRoundingBoundaryMatches() {
        List<Column> schema = List.of(new Column("shape", "keyword", List.of()));
        List<List<String>> ref = toCanonicalCells(List.of(List.of(List.of("POINT (-99.8825 16.8636)", "POINT (1.0 2.0)"))), schema);
        List<List<String>> cand = toCanonicalCells(
            List.of(List.of(List.of("POINT (1.0 2.0)", "POINT (-99.88250002 16.86360001)"))),
            schema
        );
        assertNotEquals("precondition: exact canonical forms differ", ref, cand);

        assertTrue(cellMatchesWithinRoundingTolerance(ref.get(0).get(0), cand.get(0).get(0), "keyword"));
    }

    public void testDoubleOnRoundingBoundaryMatches() {
        assertTrue(cellMatchesWithinRoundingTolerance("-99.882", "-99.883", "double"));
    }

    /** One step is measured at the larger magnitude, so the step from 9.9999 up to 10.0 still counts as one. */
    public void testDoubleCrossingPowerOfTenMatches() {
        assertTrue(cellMatchesWithinRoundingTolerance("9.9999", "10.0", "double"));
    }

    public void testDoubleTwoRoundingStepsApartDoesNotMatch() {
        assertFalse(cellMatchesWithinRoundingTolerance("-99.882", "-99.884", "double"));
    }

    /** Near-zero values snap to exactly {@code 0.0}, so zero gets no tolerance at all. */
    public void testZeroIsNotRoundingTolerant() {
        assertFalse(cellMatchesWithinRoundingTolerance("0.0", "1.0E-9", "double"));
    }

    /** A long's canonical form is exact. A magnitude-based step would be 10^14 here. */
    public void testLongIsNotRoundingTolerant() {
        assertFalse(cellMatchesWithinRoundingTolerance("2706453028782618448", "2706453028782618449", "long"));
    }

    public void testKeywordIsNotRoundingTolerant() {
        assertFalse(cellMatchesWithinRoundingTolerance("v1.2345", "v1.2346", "keyword"));
    }

    public void testDifferentGeometryTypesDoNotMatch() {
        assertFalse(cellMatchesWithinRoundingTolerance("POINT (1.0 2.0)", "LINESTRING (1.0 2.0)", "keyword"));
    }

    public void testRowsPairedWithinToleranceRegardlessOfOrder() {
        List<Column> schema = List.of(new Column("k", "keyword", List.of()), new Column("d", "double", List.of()));
        List<List<String>> ref = List.of(List.of("a", "-99.882"), List.of("b", "1.0"));
        List<List<String>> cand = List.of(List.of("b", "1.0"), List.of("a", "-99.883"));

        assertTrue(rowsMatchWithinRoundingTolerance(ref, cand, schema));
    }

    public void testRowWithDifferentExactCellDoesNotMatch() {
        List<Column> schema = List.of(new Column("k", "keyword", List.of()), new Column("d", "double", List.of()));
        List<List<String>> ref = List.of(List.of("a", "-99.882"));
        List<List<String>> cand = List.of(List.of("b", "-99.883"));

        assertFalse(rowsMatchWithinRoundingTolerance(ref, cand, schema));
    }

    /** Both reference rows are within one step of the first candidate row, but it can only be paired once. */
    public void testCandidateRowIsNotPairedTwice() {
        List<Column> schema = List.of(new Column("d", "double", List.of()));
        List<List<String>> ref = List.of(List.of("-99.882"), List.of("-99.884"));
        List<List<String>> cand = List.of(List.of("-99.883"), List.of("5.0"));

        assertFalse(rowsMatchWithinRoundingTolerance(ref, cand, schema));
    }

    public void testToCanonicalEqualsJoinedCanonicalCells() {
        List<Column> schema = List.of(new Column("k", "keyword", List.of()), new Column("n", "long", List.of()));
        List<List<Object>> rows = List.of(List.of(List.of("b", "a", "a"), 1L), List.of("c", 2L));

        List<String> joined = toCanonicalCells(rows, schema).stream().map(cells -> String.join("\t", cells)).toList();

        assertEquals(joined, toCanonical(rows, schema));
    }

    // -----------------------------------------------------------------------
    // strict field-less qstr - all-fields narrowing
    // -----------------------------------------------------------------------

    private static final String NUMERIC_PARSE_FAILURE = "failed to create query: For input string: \"quick\"";

    public void testStrictFieldlessQstrParseFailureIsAllowed() {
        String query = "FROM ref_employees | WHERE qstr(\"quick\", {\"lenient\": false})";

        assertTrue(isStrictAllFieldsNarrowingDifference(query, NUMERIC_PARSE_FAILURE));
    }

    public void testStrictFieldlessQstrDateParseFailureIsAllowed() {
        String query = "FROM ref_employees | WHERE qstr(\"quick\", {\"lenient\": false})";
        String error = "failed to parse date field [quick] with format [strict_date_optional_time]";

        assertTrue(isStrictAllFieldsNarrowingDifference(query, error));
    }

    public void testStrictFieldlessQstrWithOtherOptionsIsAllowed() {
        String query = "FROM ref_employees | WHERE qstr(\"quick\", {\"phrase_slop\": 1, \"lenient\": false})";

        assertTrue(isStrictAllFieldsNarrowingDifference(query, NUMERIC_PARSE_FAILURE));
    }

    /** A field-prefixed query targets one field on both sides, so the all-fields list plays no part. */
    public void testStrictFieldPrefixedQstrIsNotAllowed() {
        String query = "FROM ref_employees | WHERE qstr(\"salary:quick\", {\"lenient\": false})";

        assertFalse(isStrictAllFieldsNarrowingDifference(query, NUMERIC_PARSE_FAILURE));
    }

    public void testLenientFieldlessQstrIsNotAllowed() {
        String query = "FROM ref_employees | WHERE qstr(\"quick\", {\"lenient\": true})";

        assertFalse(isStrictAllFieldsNarrowingDifference(query, NUMERIC_PARSE_FAILURE));
    }

    public void testNonParseFailureIsNotAllowed() {
        String query = "FROM ref_employees | WHERE qstr(\"quick\", {\"lenient\": false})";

        assertFalse(isStrictAllFieldsNarrowingDifference(query, "Unknown column [quick]"));
    }

    // -----------------------------------------------------------------------
    // mapping sanitisation
    // -----------------------------------------------------------------------

    private static final String MAPPING_WITH_IGNORE_ABOVE_AND_STORE = """
        {
          "properties": {
            "message": {
              "type": "text",
              "fields": { "raw": { "type": "keyword", "ignore_above": 10 } }
            },
            "host": { "type": "keyword", "store": true }
          }
        }""";

    public void testIgnoreAboveStrippedFromReferenceSide() throws IOException {
        String mapping = withoutMappingParams(MAPPING_WITH_IGNORE_ABOVE_AND_STORE, REFERENCE_STRIPPED_MAPPING_PARAMS);

        assertThat(mapping, not(containsString("ignore_above")));
    }

    public void testIgnoreAboveStrippedFromCandidateSide() throws IOException {
        String mapping = withoutMappingParams(MAPPING_WITH_IGNORE_ABOVE_AND_STORE, CANDIDATE_STRIPPED_MAPPING_PARAMS);

        assertThat(mapping, not(containsString("ignore_above")));
    }

    public void testStoreKeptOnReferenceSide() throws IOException {
        String mapping = withoutMappingParams(MAPPING_WITH_IGNORE_ABOVE_AND_STORE, REFERENCE_STRIPPED_MAPPING_PARAMS);

        assertThat(mapping, containsString("\"store\":true"));
    }

    public void testStoreStrippedFromCandidateSide() throws IOException {
        String mapping = withoutMappingParams(MAPPING_WITH_IGNORE_ABOVE_AND_STORE, CANDIDATE_STRIPPED_MAPPING_PARAMS);

        assertThat(mapping, not(containsString("store")));
    }

    /** Stripping only removes attributes. The fields and their types stay, so both sides still map the same columns. */
    public void testStrippingKeepsFieldDefinitions() throws IOException {
        String mapping = withoutMappingParams(MAPPING_WITH_IGNORE_ABOVE_AND_STORE, CANDIDATE_STRIPPED_MAPPING_PARAMS);

        assertThat(
            XContentHelper.convertToMap(JsonXContent.jsonXContent, mapping, false),
            equalTo(
                Map.of(
                    "properties",
                    Map.of(
                        "message",
                        Map.of("type", "text", "fields", Map.of("raw", Map.of("type", "keyword"))),
                        "host",
                        Map.of("type", "keyword")
                    )
                )
            )
        );
    }
}
