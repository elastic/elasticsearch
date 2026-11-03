/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The per-column min/max/null-count fold the cache serves. It had no suite of its own -- it was reached only
 * incidentally through the two format suites, which between them never fed it a multivalue cell, a null inside one,
 * an untracked type, or a boolean, so the arms that decide those answers were never executed.
 * <p>
 * The three that would produce a wrong answer rather than a missing one: a multivalue cell must fold every element
 * and an empty one must count as null, or COUNT and the extrema disagree with the rows; an untracked type must
 * never serve extrema at all, because an UNSIGNED_LONG's stored signed representation orders wrongly; and a
 * kind-mismatched typed feed must contribute a count without touching the extrema. Each test below was checked by
 * breaking the arm it covers and confirming it goes red.
 */
public class ColumnStatsAccumulatorTests extends ESTestCase {

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type, Nullability.TRUE, null, false);
    }

    public void testMultivalueListFoldsEveryElement() {
        ColumnStatsAccumulator acc = ColumnStatsAccumulator.forProjectedAttributes(new Attribute[] { attr("v", DataType.LONG) });
        acc.acceptValueAt(0, List.of(5L, 1L, 9L));
        Map<String, ExternalStats.ColumnStats> snap = acc.snapshot();
        assertEquals(3L, snap.get("v").valueCount());
        assertEquals(0L, snap.get("v").nullCount());
        assertEquals(1L, snap.get("v").min());
        assertEquals(9L, snap.get("v").max());
    }

    public void testEmptyListCountsAsNull() {
        ColumnStatsAccumulator acc = ColumnStatsAccumulator.forProjectedAttributes(new Attribute[] { attr("v", DataType.LONG) });
        acc.acceptValueAt(0, List.of());
        Map<String, ExternalStats.ColumnStats> snap = acc.snapshot();
        assertEquals(0L, snap.get("v").valueCount());
        assertEquals(1L, snap.get("v").nullCount());
        assertNull(snap.get("v").min());
        assertNull(snap.get("v").max());
    }

    public void testNullListElementCountsAsNull() {
        ColumnStatsAccumulator acc = ColumnStatsAccumulator.forProjectedAttributes(new Attribute[] { attr("v", DataType.LONG) });
        acc.acceptValueAt(0, Arrays.asList(3L, null));
        Map<String, ExternalStats.ColumnStats> snap = acc.snapshot();
        assertEquals(1L, snap.get("v").valueCount());
        assertEquals(1L, snap.get("v").nullCount());
        assertEquals(3L, snap.get("v").min());
        assertEquals(3L, snap.get("v").max());
    }

    public void testUntrackedTypeCountsValuesButNeverMinMax() {
        ColumnStatsAccumulator acc = ColumnStatsAccumulator.forProjectedAttributes(new Attribute[] { attr("u", DataType.UNSIGNED_LONG) });
        acc.acceptValueAt(0, -1L); // 2^64-1 as stored signed long — signed min/max would be WRONG-ORDER
        acc.acceptValueAt(0, 1L);
        Map<String, ExternalStats.ColumnStats> snap = acc.snapshot();
        assertEquals(2L, snap.get("u").valueCount());
        assertNull("untracked type must never serve a min", snap.get("u").min());
        assertNull("untracked type must never serve a max", snap.get("u").max());
    }

    public void testBooleanScalarMinMax() {
        ColumnStatsAccumulator acc = ColumnStatsAccumulator.forProjectedAttributes(new Attribute[] { attr("b", DataType.BOOLEAN) });
        acc.acceptValueAt(0, Boolean.FALSE);
        acc.acceptValueAt(0, Boolean.TRUE);
        Map<String, ExternalStats.ColumnStats> snap = acc.snapshot();
        assertEquals(Boolean.FALSE, snap.get("b").min());
        assertEquals(Boolean.TRUE, snap.get("b").max());
        assertEquals(2L, snap.get("b").valueCount());
    }

    public void testTypedFeedToUntrackedColumnCountsButNoMinMax() {
        ColumnStatsAccumulator acc = ColumnStatsAccumulator.forProjectedAttributes(new Attribute[] { attr("u", DataType.UNSIGNED_LONG) });
        acc.acceptBooleanAt(0, true);
        acc.acceptIntAt(0, 3);
        acc.acceptDoubleAt(0, 3.5);
        acc.acceptBytesRefAt(0, new BytesRef("x"));
        acc.acceptLongAt(0, -1L);
        Map<String, ExternalStats.ColumnStats> snap = acc.snapshot();
        assertEquals(5L, snap.get("u").valueCount());
        assertNull(snap.get("u").min());
        assertNull(snap.get("u").max());
    }

    public void testOutOfRangeIndicesSilentlyIgnored() {
        ColumnStatsAccumulator acc = ColumnStatsAccumulator.forProjectedAttributes(new Attribute[] { attr("v", DataType.LONG) });
        acc.acceptNullAt(-1);
        acc.acceptNullAt(1);
        acc.acceptBooleanAt(-1, true);
        acc.acceptBooleanAt(1, true);
        acc.acceptIntAt(-1, 1);
        acc.acceptIntAt(1, 1);
        acc.acceptLongAt(-1, 1L);
        acc.acceptLongAt(1, 1L);
        acc.acceptDoubleAt(-1, 1.0);
        acc.acceptDoubleAt(1, 1.0);
        acc.acceptBytesRefAt(-1, new BytesRef("x"));
        acc.acceptBytesRefAt(1, new BytesRef("x"));
        acc.acceptValueAt(-1, 1L);
        acc.acceptValueAt(1, 1L);
        acc.acceptBlockAt(-1, null);
        acc.acceptBlockAt(1, null);
        Map<String, ExternalStats.ColumnStats> snap = acc.snapshot();
        assertEquals(0L, snap.get("v").valueCount());
        assertEquals(0L, snap.get("v").nullCount());
    }

    public void testEmptyFactoriesAreEmpty() {
        assertTrue(ColumnStatsAccumulator.forSchema(null).isEmpty());
        assertTrue(ColumnStatsAccumulator.forSchema(List.of()).isEmpty());
        assertTrue(ColumnStatsAccumulator.forProjectedAttributes(null).isEmpty());
        assertTrue(ColumnStatsAccumulator.forProjectedAttributes(new Attribute[0]).isEmpty());
        assertEquals(Map.of(), ColumnStatsAccumulator.forSchema(null).snapshot());
        assertFalse(ColumnStatsAccumulator.forProjectedAttributes(new Attribute[] { attr("v", DataType.LONG) }).isEmpty());
    }
}
