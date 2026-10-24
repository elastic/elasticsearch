/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.elasticsearch.columnar.ColumnarStringQueryTestUtils.found;
import static org.elasticsearch.columnar.ColumnarStringQueryTestUtils.indexOneSegment;
import static org.elasticsearch.columnar.ColumnarStringQueryTestUtils.oneSlotEach;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.elasticsearch.columnar.ColumnarTestUtils.stringPayload;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * A wildcard automaton answered by the column, over the shapes that answer it differently. Every pattern is
 * checked against what Lucene says the pattern means. Narrowing a pattern to a cheaper query is the caller's
 * business and is tested where it is done.
 */
public class ColumnarStringAutomatonQueryTests extends ESTestCase {

    private static final String FIELD = "kw";
    private static final String[] TERMS = { "alpha", "alpine", "bravo", "charlie", "delta" };

    /**
     * The patterns worth telling apart: three that name a shape a column answers without an automaton, and
     * four that do not and so have to be run as one.
     */
    private static final String[] PATTERNS = {
        "alpha",      // a whole value
        "al*",        // a start
        "*lph*",      // a run of bytes
        "*pha",       // an end, which is neither
        "al*a",       // bytes at both ends
        "alph?",      // a single unknown byte
        "*",          // every value
        "zzz*",       // nothing the column holds
        "" };

    /** Few distinct values, so the column carries a dictionary and a pattern is run once a term. */
    public void testLowCardinality() throws IOException {
        assertPatterns(values(between(300, 1500), d -> TERMS[d % TERMS.length]));
    }

    /** Every value distinct, so there is no dictionary and a pattern is run against the values. */
    public void testHighCardinality() throws IOException {
        assertPatterns(values(between(300, 1500), d -> "alpha-" + d));
    }

    /** Values in term order, the shape the narrowed term and prefix queries bisect rather than scan. */
    public void testSorted() throws IOException {
        final List<String> sorted = new ArrayList<>(values(between(300, 1500), d -> TERMS[d % TERMS.length]));
        sorted.sort(String::compareTo);
        assertPatterns(sorted);
    }

    /** Hot values over a long tail, so some values are named by the dictionary and the rest escape it. */
    public void testHotValuesWithTail() throws IOException {
        assertPatterns(values(between(600, 2000), d -> d % 40 == 7 ? "alpine-" + d : TERMS[d % TERMS.length]));
    }

    /** Documents without a value, which match nothing however the pattern is answered. */
    public void testSparse() throws IOException {
        assertPatterns(values(between(300, 1500), d -> d % 3 == 0 ? null : TERMS[d % TERMS.length]));
    }

    /** Values of no bytes, which a pattern accepts or does not like any other value. */
    public void testEmptyValues() throws IOException {
        assertPatterns(values(between(300, 1500), d -> d % 4 == 0 ? "" : TERMS[d % TERMS.length]));
    }

    /**
     * An automaton has no equality to cache on, so the query keys on what produced it. Two queries built from
     * the same pattern have to be the same query, and two built from different patterns have to differ, or a
     * cached filter would be handed to the wrong one.
     */
    public void testCacheIdentityFollowsThePattern() {
        final boolean singleValued = randomBoolean();
        assertEquals(automatonFor("al*a", singleValued), automatonFor("al*a", singleValued));
        assertEquals(automatonFor("al*a", singleValued).hashCode(), automatonFor("al*a", singleValued).hashCode());
        assertNotEquals(automatonFor("al*a", singleValued), automatonFor("al*b", singleValued));
        assertNotEquals(automatonFor("al*a", singleValued), automatonFor("other", "al*a", singleValued));
    }

    /**
     * An updated field, read as an overlay of its layers rather than as the column. The automaton has no
     * column to run over then, so it runs over the values a document at a time, and has to find the same
     * documents either way.
     */
    public void testMatchesThroughAnOverlaidColumn() throws IOException {
        final List<String> values = values(between(400, 1200), d -> d % 5 == 0 ? "alpine-" + d : TERMS[d % TERMS.length]);
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.STRING))
                .setMergePolicy(new LogDocMergePolicy());
            final FieldType type = columnarBinaryFieldType();
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (String value : values) {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, stringPayload(value), type));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(ColumnarTestUtils.hideTheColumn(reader));
                for (String pattern : PATTERNS) {
                    assertEquals(
                        "pattern [" + pattern + "] as an automaton through an overlay",
                        accepted(values, pattern),
                        found(searcher, automatonFor(pattern, false))
                    );
                }
            }
        }
    }

    /**
     * A single-valued field read as an overlay, whose blobs are each document's value rather than a payload, so the
     * automaton runs over the blob itself. Documents without the field and the empty string are among them.
     */
    public void testMatchesThroughAnOverlaidSingleValuedColumn() throws IOException {
        final List<String> values = values(
            between(400, 1200),
            d -> d % 7 == 3 ? null : d % 11 == 0 ? "" : d % 5 == 0 ? "alpine-" + d : TERMS[d % TERMS.length]
        );
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(ColumnarTestUtils.singleValuedStringCodec())
                .setMergePolicy(new LogDocMergePolicy());
            final FieldType type = columnarBinaryFieldType();
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (String value : values) {
                    final Document doc = new Document();
                    if (value != null) {
                        doc.add(new Field(FIELD, new BytesRef(value), type));
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(ColumnarTestUtils.hideTheColumn(reader));
                for (String pattern : PATTERNS) {
                    assertEquals(
                        "pattern [" + pattern + "] as an automaton through an overlay",
                        accepted(values, pattern),
                        found(searcher, automatonFor(pattern, true))
                    );
                }
            }
        }
    }

    public void testAColumnWithoutValuesMatchesNothing() throws IOException {
        final List<String> values = values(200, d -> TERMS[d % TERMS.length]);
        try (Directory dir = newDirectory()) {
            indexOneSegment(dir, columnarCodec(ColumnarFieldType.STRING), FIELD, oneSlotEach(values));
            try (DirectoryReader reader = DirectoryReader.open(dir); DirectoryReader dropped = ColumnarTestUtils.dropTheColumn(reader)) {
                ColumnarTestUtils.assertBinaryFieldWithoutValues(dropped, FIELD);
                final IndexSearcher searcher = new IndexSearcher(dropped);
                for (String pattern : new String[] { "alpha", "*" }) {
                    assertEquals("pattern [" + pattern + "]", List.of(), found(searcher, automatonFor(pattern, false)));
                }
            }
        }
    }

    public void testQuestionMarkMatchesOneSupplementaryCharacter() throws IOException {
        final List<String> values = List.of(new String(Character.toChars(0x1F600)), "ab", "a", "\u00e9");
        try (Directory dir = newDirectory()) {
            indexOneSegment(dir, columnarCodec(ColumnarFieldType.STRING), FIELD, oneSlotEach(values));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(List.of(0, 2, 3), found(new IndexSearcher(reader), automatonFor("?", false)));
            }
        }
    }

    public void testNullSlotsNeverMatchThroughAnOverlaidColumn() throws IOException {
        final BytesRef[][] docs = {
            { null },
            { new BytesRef("") },
            { new BytesRef("alpha"), null },
            null,
            { null, null },
            { null, new BytesRef("alpha") },
            { null, new BytesRef("") },
            { new BytesRef("miss"), null, new BytesRef("alpha") },
            {} };
        try (Directory dir = newDirectory()) {
            indexOneSegment(dir, columnarCodec(ColumnarFieldType.STRING), FIELD, docs);
            try (DirectoryReader reader = DirectoryReader.open(dir); DirectoryReader hidden = ColumnarTestUtils.hideTheColumn(reader)) {
                assertThat(hidden.leaves().get(0).reader().getBinaryDocValues(FIELD), not(instanceOf(StringColumnSource.class)));
                final IndexSearcher searcher = new IndexSearcher(hidden);
                assertEquals("every value, null slots aside", List.of(1, 2, 5, 6, 7), acceptedSlots(docs, "*"));
                final Map<String, Boolean> matchesSomething = new LinkedHashMap<>();
                matchesSomething.put("*", true);
                matchesSomething.put("alph?", true);
                matchesSomething.put("alpha", true);
                matchesSomething.put("zzz*", false);
                for (final Map.Entry<String, Boolean> entry : matchesSomething.entrySet()) {
                    final String pattern = entry.getKey();
                    final List<Integer> expected = acceptedSlots(docs, pattern);
                    assertEquals("pattern [" + pattern + "] accepts a value", entry.getValue(), expected.isEmpty() == false);
                    assertFalse(
                        "a null slot and an absent document never match",
                        expected.contains(0) || expected.contains(3) || expected.contains(4) || expected.contains(8)
                    );
                    assertEquals("pattern [" + pattern + "]", expected, found(searcher, automatonFor(pattern, false)));
                }
            }
        }
    }

    /** Every pattern, against the documents running Lucene's automaton for it over the values would find. */
    private void assertPatterns(List<String> values) throws IOException {
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.STRING))
                .setMergePolicy(new LogDocMergePolicy());
            final FieldType type = columnarBinaryFieldType();
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (String value : values) {
                    final Document doc = new Document();
                    if (value != null) {
                        doc.add(new Field(FIELD, stringPayload(value), type));
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                for (String pattern : PATTERNS) {
                    assertEquals(
                        "pattern [" + pattern + "] as an automaton",
                        accepted(values, pattern),
                        found(searcher, automatonFor(pattern, false))
                    );
                }
            }
        }
    }

    private static Query automatonFor(String pattern, boolean singleValued) {
        return automatonFor(FIELD, pattern, singleValued);
    }

    private static Query automatonFor(String field, String pattern, boolean singleValued) {
        return new ColumnarStringAutomatonQuery(
            field,
            Operations.determinize(
                WildcardQuery.toAutomaton(new Term(field, pattern), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
            ),
            "pattern=" + pattern,
            singleValued,
            ScanBudget.UNLIMITED
        );
    }

    /** The documents whose value Lucene's automaton for the pattern accepts. */
    private static List<Integer> accepted(List<String> values, String pattern) {
        final BytesRef[][] docs = new BytesRef[values.size()][];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = values.get(d) == null ? null : new BytesRef[] { new BytesRef(values.get(d)) };
        }
        return acceptedSlots(docs, pattern);
    }

    private static List<Integer> acceptedSlots(BytesRef[][] docs, String pattern) {
        final ByteRunAutomaton automaton = new ByteRunAutomaton(
            Operations.determinize(
                WildcardQuery.toAutomaton(new Term(FIELD, pattern), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
            )
        );
        final List<Integer> matching = new ArrayList<>();
        for (int d = 0; d < docs.length; d++) {
            if (docs[d] == null) {
                continue;
            }
            for (BytesRef slot : docs[d]) {
                if (slot != null && automaton.run(slot.bytes, slot.offset, slot.length)) {
                    matching.add(d);
                    break;
                }
            }
        }
        return matching;
    }

    private static List<String> values(int count, IntFunction<String> value) {
        final List<String> values = new ArrayList<>(count);
        for (int d = 0; d < count; d++) {
            values.add(value.apply(d));
        }
        return values;
    }

}
