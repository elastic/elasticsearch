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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Predicate;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Range and term-set queries over a keyword column. Every shape is checked against the documents the same test run
 * over the values by hand would find, and again with the column hidden, since a field read as an overlay has to
 * answer the same.
 */
public class ColumnarStringRangeAndAnyOfQueryTests extends ESTestCase {

    private static final String FIELD = "kw";

    /** A query and the predicate deciding, from the values themselves, which documents it must find. */
    private record Shape(String description, Query query, Predicate<BytesRef> matcher) {}

    private static List<Shape> shapes() {
        return List.of(
            new Shape("range[alpha,charlie]", range("alpha", true, "charlie", true), between("alpha", true, "charlie", true)),
            new Shape("range[alpha,charlie)", range("alpha", true, "charlie", false), between("alpha", true, "charlie", false)),
            new Shape("range(alpha,charlie]", range("alpha", false, "charlie", true), between("alpha", false, "charlie", true)),
            new Shape("range(alpha,alpha)", range("alpha", false, "alpha", false), between("alpha", false, "alpha", false)),
            new Shape("range-open-below", range(null, false, "bravo", true), between(null, false, "bravo", true)),
            new Shape("range-open-above", range("bravo", true, null, false), between("bravo", true, null, false)),
            new Shape("range-fully-open", range(null, false, null, false), value -> true),
            new Shape("range-empty-string-bound", range("", true, "alpha", false), between("", true, "alpha", false)),
            new Shape("range-above-everything", range("zzz", true, "zzzz", true), between("zzz", true, "zzzz", true)),
            new Shape("terms", anyOf("alpha", "charlie"), in("alpha", "charlie")),
            new Shape("terms-absent", anyOf("nothing-holds-this"), in("nothing-holds-this")),
            new Shape("terms-with-empty", anyOf("", "alpha"), in("", "alpha")),
            new Shape("terms-escaped", anyOf("escaped-3"), in("escaped-3"))
        );
    }

    private static Query range(String lower, boolean includeLower, String upper, boolean includeUpper) {
        return new ColumnarStringRangeQuery(
            FIELD,
            lower == null ? null : new BytesRef(lower),
            includeLower,
            upper == null ? null : new BytesRef(upper),
            includeUpper,
            ScanBudget.UNLIMITED
        );
    }

    private static Query anyOf(String... terms) {
        return new ColumnarStringAnyOfQuery(FIELD, termSet(terms), ScanBudget.UNLIMITED);
    }

    private static NavigableSet<BytesRef> termSet(String... terms) {
        final NavigableSet<BytesRef> set = new TreeSet<>();
        for (String term : terms) {
            set.add(new BytesRef(term));
        }
        return set;
    }

    private static Predicate<BytesRef> in(String... terms) {
        final NavigableSet<BytesRef> set = termSet(terms);
        return set::contains;
    }

    private static Predicate<BytesRef> between(String lower, boolean lowerInclusive, String upper, boolean upperInclusive) {
        final BytesRef low = lower == null ? null : new BytesRef(lower);
        final BytesRef high = upper == null ? null : new BytesRef(upper);
        return value -> {
            if (low != null) {
                final int cmp = value.compareTo(low);
                if (cmp < 0 || (cmp == 0 && lowerInclusive == false)) {
                    return false;
                }
            }
            if (high != null) {
                final int cmp = value.compareTo(high);
                if (cmp > 0 || (cmp == 0 && upperInclusive == false)) {
                    return false;
                }
            }
            return true;
        };
    }

    public void testDictionaryColumn() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie", "delta", "" };
        final List<List<String>> docs = new ArrayList<>();
        for (int d = 0; d < between(400, 1200); d++) {
            docs.add(List.of(terms[d % terms.length]));
        }
        assertShapes(docs);
    }

    public void testPlainColumn() throws IOException {
        final List<List<String>> docs = new ArrayList<>();
        for (int d = 0; d < between(400, 1200); d++) {
            docs.add(List.of("value-" + d));
        }
        assertShapes(docs);
    }

    public void testSortedColumn() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie", "delta" };
        final List<List<String>> docs = new ArrayList<>();
        final int count = between(400, 1200);
        for (int d = 0; d < count; d++) {
            docs.add(List.of(terms[(d * terms.length) / count]));
        }
        assertShapes(docs);
    }

    public void testColumnWithEscapes() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie" };
        final List<List<String>> docs = new ArrayList<>();
        for (int d = 0; d < between(400, 1200); d++) {
            docs.add(List.of(d % 25 == 3 ? "escaped-" + d : terms[d % terms.length]));
        }
        assertShapes(docs);
    }

    public void testMultiValuedDocuments() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie", "delta" };
        final List<List<String>> docs = new ArrayList<>();
        for (int d = 0; d < between(400, 1200); d++) {
            docs.add(switch (d % 4) {
                case 0 -> List.of(terms[d % terms.length]);
                case 1 -> List.of(terms[d % terms.length], terms[(d + 1) % terms.length]);
                case 2 -> List.of("alpha", "alpha");
                default -> List.of(terms[d % terms.length], "escaped-" + d, "");
            });
        }
        assertShapes(docs);
    }

    public void testNullsEmptyArraysAndAbsentFields() throws IOException {
        final String[] terms = { "alpha", "bravo", "" };
        final List<List<String>> docs = new ArrayList<>();
        for (int d = 0; d < between(400, 1200); d++) {
            docs.add(switch (d % 6) {
                case 0 -> List.of(terms[d % terms.length]);
                case 1 -> Collections.singletonList(null);
                case 2 -> Arrays.asList(null, terms[d % terms.length]);
                case 3 -> List.<String>of();
                case 4 -> null;
                case 5 -> Arrays.asList(terms[d % terms.length], null);
                default -> throw new AssertionError("remainder [" + d % 6 + "] names no shape");
            });
        }
        assertShapes(docs);
    }

    public void testSingleValuedColumn() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie", "" };
        final List<List<String>> docs = new ArrayList<>();
        for (int d = 0; d < between(400, 1200); d++) {
            docs.add(switch (d % 25) {
                case 3 -> List.of("escaped-" + d);
                case 7, 19 -> null;
                default -> List.of(terms[d % terms.length]);
            });
        }
        assertShapes(docs, true);
    }

    public void testTheBudgetIsSpentWhenTheColumnIsRead() throws IOException {
        final List<List<String>> docs = new ArrayList<>();
        for (int d = 0; d < 200; d++) {
            docs.add(List.of("term-" + (d % 4)));
        }
        final ScanBudget refuses = searcher -> { throw new IllegalStateException("no room to read a column"); };
        final List<Query> queries = List.of(
            new ColumnarStringRangeQuery(FIELD, new BytesRef("term-0"), true, new BytesRef("term-3"), true, refuses),
            new ColumnarStringAnyOfQuery(FIELD, termSet("term-0", "term-2"), refuses)
        );
        try (Directory dir = newDirectory()) {
            index(dir, docs, false);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                for (Query query : queries) {
                    final Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
                    final ScorerSupplier supplier = weight.scorerSupplier(reader.leaves().get(0));
                    assertNotNull("a supplier costs nothing to hand out", supplier);
                    expectThrows(IllegalStateException.class, () -> supplier.get(Long.MAX_VALUE));
                    expectThrows(IllegalStateException.class, () -> searcher.search(query, 1));
                }
            }
        }
    }

    public void testATermSetKeptInAnotherOrder() throws IOException {
        // NOTE: a column in term order bisects the query terms, so a caller's own comparator must not reach it.
        final String[] terms = { "alpha", "bravo", "charlie", "delta" };
        final List<List<String>> docs = new ArrayList<>();
        for (int d = 0; d < 600; d++) {
            docs.add(List.of(terms[d * terms.length / 600]));
        }
        final NavigableSet<BytesRef> reversed = new TreeSet<>(Comparator.reverseOrder());
        reversed.add(new BytesRef("alpha"));
        reversed.add(new BytesRef("charlie"));
        try (Directory dir = newDirectory()) {
            index(dir, docs, true);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertTrue(
                    "the values arrive in term order, which is the path that bisects",
                    ((StringColumnSource) reader.leaves().get(0).reader().getBinaryDocValues(FIELD)).reader().valuesSorted()
                );
                final List<Integer> expected = matching(docs, in("alpha", "charlie"));
                final IndexSearcher searcher = new IndexSearcher(reader);
                assertEquals(expected, found(searcher, new ColumnarStringAnyOfQuery(FIELD, reversed, ScanBudget.UNLIMITED)));
            }
        }
    }

    private void assertShapes(List<List<String>> docs) throws IOException {
        assertShapes(docs, false);
    }

    /** As {@link #assertShapes(List)}, written single-valued when {@code singleValued}: each document holds one value or none. */
    private void assertShapes(List<List<String>> docs, boolean singleValued) throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, docs, singleValued);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                // NOTE: the two searchers have to be reading the field two different ways, or the comparison below
                // holds for the wrong reason: one of them answering everything a document at a time and agreeing
                // with itself. So each is asked what it sees before either is asked a question.
                assertThat(
                    "the field is a column",
                    reader.leaves().get(0).reader().getBinaryDocValues(FIELD),
                    instanceOf(StringColumnSource.class)
                );
                final DirectoryReader hidden = ColumnarTestUtils.hideTheColumn(reader);
                assertThat(
                    "the column is hidden",
                    hidden.leaves().get(0).reader().getBinaryDocValues(FIELD),
                    not(instanceOf(StringColumnSource.class))
                );
                final IndexSearcher onTheColumn = new IndexSearcher(reader);
                final IndexSearcher onAnOverlay = new IndexSearcher(hidden);
                for (Shape shape : shapes()) {
                    final List<Integer> expected = matching(docs, shape.matcher());
                    assertEquals("[" + shape.description() + "] through the column", expected, found(onTheColumn, shape.query()));
                    assertEquals("[" + shape.description() + "] through an overlay", expected, found(onAnOverlay, shape.query()));
                }
            }
        }
    }

    private static void index(Directory dir, List<List<String>> docs, boolean singleValued) throws IOException {
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.STRING))
            .setMergePolicy(new LogDocMergePolicy());
        final FieldType type = singleValued ? ColumnarTestUtils.singleValuedBinaryFieldType() : columnarBinaryFieldType();
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (List<String> slots : docs) {
                final Document doc = new Document();
                if (slots != null) {
                    assert singleValued == false || slots.size() == 1 : "a single-valued document holds one value";
                    doc.add(new Field(FIELD, singleValued ? new BytesRef(slots.get(0)) : payload(slots), type));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static List<Integer> matching(List<List<String>> docs, Predicate<BytesRef> matcher) {
        final List<Integer> matched = new ArrayList<>();
        for (int d = 0; d < docs.size(); d++) {
            final List<String> slots = docs.get(d);
            if (slots == null) {
                continue;
            }
            for (String slot : slots) {
                // A null slot is no value, so it is never offered.
                if (slot != null && matcher.test(new BytesRef(slot))) {
                    matched.add(d);
                    break;
                }
            }
        }
        return matched;
    }

    private static BytesRef payload(List<String> slots) {
        final List<BytesRef> encoded = new ArrayList<>(slots.size());
        for (String slot : slots) {
            encoded.add(slot == null ? null : new BytesRef(slot));
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(encoded));
    }

    private static List<Integer> found(IndexSearcher searcher, Query query) throws IOException {
        final TopDocs hits = searcher.search(query, Integer.MAX_VALUE);
        final List<Integer> docs = new ArrayList<>();
        for (ScoreDoc hit : hits.scoreDocs) {
            docs.add(hit.doc);
        }
        Collections.sort(docs);
        return docs;
    }
}
