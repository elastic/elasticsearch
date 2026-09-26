/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.Column;

import java.util.List;

import static org.elasticsearch.xpack.esql.qa.rest.generative.CrossIndexModeGenerativeRestRunner.SORTED_SET_BACKED_TYPES;
import static org.elasticsearch.xpack.esql.qa.rest.generative.CrossIndexModeGenerativeRestRunner.canonicalValue;
import static org.elasticsearch.xpack.esql.qa.rest.generative.CrossIndexModeGenerativeRestRunner.toCanonical;

/**
 * Unit tests for the canonicalisation helpers in {@link CrossIndexModeGenerativeRestRunner}.
 *
 * <p>These cover the type-aware MV cell comparison ({@code toCanonical}) and the near-zero
 * double snap ({@code canonicalValue}). The helpers are package-private so this test can live
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
}
