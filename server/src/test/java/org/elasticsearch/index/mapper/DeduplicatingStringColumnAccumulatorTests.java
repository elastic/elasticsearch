/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.escf.LuceneBinaryColumn;
import org.elasticsearch.sourcebatch.LuceneColumn;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link DeduplicatingStringColumnAccumulator}: dictionary interning,
 * multi-value, absent rows, drain into an ESCF array column, and duplicate detection.
 */
public class DeduplicatingStringColumnAccumulatorTests extends ESTestCase {

    private static BytesRef br(String s) {
        return new BytesRef(s);
    }

    /**
     * Drains the accumulator into a {@link LuceneBinaryColumn} via the
     * {@link FieldNamesFieldMapper#NAME} field name and a non-stored string field type. We use the
     * same wrapping that {@link FieldNamesFieldMapper#postColumnarParse} applies, so the assertions
     * exercise the exact same column.
     */
    private static LuceneBinaryColumn drain(DeduplicatingStringColumnAccumulator acc) {
        return LuceneBinaryColumn.of(
            acc.finish(BytesRefRecycler.NON_RECYCLING_INSTANCE),
            FieldNamesFieldMapper.NAME,
            StringField.TYPE_NOT_STORED
        );
    }

    /**
     * Drains the row cursor into a {@code docId → [values]} map. Mirrors the pattern used in
     * {@link org.elasticsearch.escf.LuceneBinaryColumnTests}.
     */
    private static Map<Integer, List<String>> drainRowCursor(LuceneColumn.RowFieldCursor cursor) {
        Map<Integer, List<String>> result = new LinkedHashMap<>();
        int doc = cursor.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            List<IndexableField> fields = new ArrayList<>();
            cursor.appendCurrentFields(fields);
            result.computeIfAbsent(doc, k -> new ArrayList<>()).add(fields.get(0).binaryValue().utf8ToString());
            doc = cursor.nextDoc();
        }
        return result;
    }

    /** An accumulator with no recorded entries reports isEmpty and produces all-absent rows. */
    public void testEmptyAllAbsent() {
        DeduplicatingStringColumnAccumulator acc = new DeduplicatingStringColumnAccumulator(3);
        assertTrue("isEmpty() before any record", acc.isEmpty());
        Map<Integer, List<String>> result = drainRowCursor(drain(acc).rowFieldCursor());
        assertTrue("no docs should appear in an empty column", result.isEmpty());
    }

    /** Single value recorded for one doc; the other docs remain absent. */
    public void testSingleValueSingleDoc() {
        DeduplicatingStringColumnAccumulator acc = new DeduplicatingStringColumnAccumulator(3);
        acc.record(1, br("alpha"));
        assertFalse(acc.isEmpty());
        Map<Integer, List<String>> result = drainRowCursor(drain(acc).rowFieldCursor());
        assertNull("doc 0 should be absent", result.get(0));
        assertEquals(List.of("alpha"), result.get(1));
        assertNull("doc 2 should be absent", result.get(2));
    }

    /** One distinct value per doc for all docs (dense, no dedup). */
    public void testDenseSingleValueAllDocs() {
        DeduplicatingStringColumnAccumulator acc = new DeduplicatingStringColumnAccumulator(3);
        acc.record(0, br("a"));
        acc.record(1, br("b"));
        acc.record(2, br("c"));
        Map<Integer, List<String>> result = drainRowCursor(drain(acc).rowFieldCursor());
        assertEquals(List.of("a"), result.get(0));
        assertEquals(List.of("b"), result.get(1));
        assertEquals(List.of("c"), result.get(2));
    }

    /**
     * Two documents share the same value string. The dictionary interns it to a single ordinal;
     * each document still gets its own element in the output.
     */
    public void testCrossDocInterning() {
        DeduplicatingStringColumnAccumulator acc = new DeduplicatingStringColumnAccumulator(2);
        acc.record(0, br("shared"));
        acc.record(1, br("shared")); // same content, distinct BytesRef instance
        Map<Integer, List<String>> result = drainRowCursor(drain(acc).rowFieldCursor());
        assertEquals(List.of("shared"), result.get(0));
        assertEquals(List.of("shared"), result.get(1));
    }

    /** Multiple distinct values per doc appear in insertion order. */
    public void testMultipleDistinctValuesPerDoc() {
        DeduplicatingStringColumnAccumulator acc = new DeduplicatingStringColumnAccumulator(1);
        acc.record(0, br("first"));
        acc.record(0, br("second"));
        acc.record(0, br("third"));
        Map<Integer, List<String>> result = drainRowCursor(drain(acc).rowFieldCursor());
        assertEquals(List.of("first", "second", "third"), result.get(0));
    }

    /** Duplicate {@code (doc, value)} pairs are a bug and trigger an assertion failure. */
    public void testDuplicateTriggersAssertion() {
        assumeTrue("assertions must be enabled", DeduplicatingStringColumnAccumulator.class.desiredAssertionStatus());
        DeduplicatingStringColumnAccumulator acc = new DeduplicatingStringColumnAccumulator(1);
        acc.record(0, br("x"));
        assertThrows(AssertionError.class, () -> acc.record(0, br("x")));
    }

    /**
     * Entries may arrive for docs in any order; the drain iterates docs ascending and produces
     * correct rows regardless.
     */
    public void testOutOfOrderDocRecording() {
        DeduplicatingStringColumnAccumulator acc = new DeduplicatingStringColumnAccumulator(3);
        acc.record(2, br("last"));
        acc.record(0, br("first"));
        acc.record(1, br("middle"));
        Map<Integer, List<String>> result = drainRowCursor(drain(acc).rowFieldCursor());
        assertEquals(List.of("first"), result.get(0));
        assertEquals(List.of("middle"), result.get(1));
        assertEquals(List.of("last"), result.get(2));
    }

    /** Absent docs at the leading, middle, and trailing positions produce empty ranges. */
    public void testAbsentDocs() {
        DeduplicatingStringColumnAccumulator acc = new DeduplicatingStringColumnAccumulator(5);
        acc.record(1, br("only-1"));
        acc.record(3, br("only-3"));
        Map<Integer, List<String>> result = drainRowCursor(drain(acc).rowFieldCursor());
        assertNull("doc 0 should be absent", result.get(0));
        assertEquals(List.of("only-1"), result.get(1));
        assertNull("doc 2 should be absent", result.get(2));
        assertEquals(List.of("only-3"), result.get(3));
        assertNull("doc 4 should be absent", result.get(4));
    }
}
