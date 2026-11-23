/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import static org.elasticsearch.columnar.ColumnarStringQueryTestUtils.found;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodecForField;
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

    private static List<Shape> shapes(boolean singleValued) {
        return List.of(
            new Shape("range[alpha,charlie]", range("alpha", true, "charlie", true, singleValued), between("alpha", true, "charlie", true)),
            new Shape(
                "range[alpha,charlie)",
                range("alpha", true, "charlie", false, singleValued),
                between("alpha", true, "charlie", false)
            ),
            new Shape(
                "range(alpha,charlie]",
                range("alpha", false, "charlie", true, singleValued),
                between("alpha", false, "charlie", true)
            ),
            new Shape("range(alpha,alpha)", range("alpha", false, "alpha", false, singleValued), between("alpha", false, "alpha", false)),
            new Shape("range-open-below", range(null, false, "bravo", true, singleValued), between(null, false, "bravo", true)),
            new Shape("range-open-above", range("bravo", true, null, false, singleValued), between("bravo", true, null, false)),
            new Shape("range-fully-open", range(null, false, null, false, singleValued), value -> true),
            new Shape("range-empty-string-bound", range("", true, "alpha", false, singleValued), between("", true, "alpha", false)),
            new Shape("range-above-everything", range("zzz", true, "zzzz", true, singleValued), between("zzz", true, "zzzz", true)),
            new Shape("terms", anyOf(singleValued, "alpha", "charlie"), in("alpha", "charlie")),
            new Shape("terms-absent", anyOf(singleValued, "nothing-holds-this"), in("nothing-holds-this")),
            new Shape("terms-with-empty", anyOf(singleValued, "", "alpha"), in("", "alpha")),
            new Shape("terms-escaped", anyOf(singleValued, "escaped-3"), in("escaped-3"))
        );
    }

    private static Query range(String lower, boolean includeLower, String upper, boolean includeUpper, boolean singleValued) {
        return new ColumnarStringRangeQuery(
            FIELD,
            lower == null ? null : new BytesRef(lower),
            includeLower,
            upper == null ? null : new BytesRef(upper),
            includeUpper,
            singleValued,
            ScanBudget.UNLIMITED
        );
    }

    private static Query anyOf(boolean singleValued, String... terms) {
        return new ColumnarStringAnyOfQuery(FIELD, termSet(terms), singleValued, ScanBudget.UNLIMITED);
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
        final boolean singleValued = randomBoolean();
        final List<Query> queries = List.of(
            new ColumnarStringRangeQuery(FIELD, new BytesRef("term-0"), true, new BytesRef("term-3"), true, singleValued, refuses),
            new ColumnarStringAnyOfQuery(FIELD, termSet("term-0", "term-2"), singleValued, refuses)
        );
        try (Directory dir = newDirectory()) {
            index(dir, docs, singleValued);
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
                assertEquals(expected, found(searcher, new ColumnarStringAnyOfQuery(FIELD, reversed, true, ScanBudget.UNLIMITED)));
            }
        }
    }

    public void testRangeQueryIdentityFollowsEveryField() {
        final boolean includeLower = randomBoolean();
        final boolean includeUpper = randomBoolean();
        final boolean singleValued = randomBoolean();
        final Query query = range("alpha", includeLower, "charlie", includeUpper, singleValued);
        final Query same = range("alpha", includeLower, "charlie", includeUpper, singleValued);
        assertEquals(query, same);
        assertEquals(query.hashCode(), same.hashCode());
        assertNotEquals(query, range("alpine", includeLower, "charlie", includeUpper, singleValued));
        assertNotEquals(query, range(null, includeLower, "charlie", includeUpper, singleValued));
        assertNotEquals(query, range("alpha", includeLower, "delta", includeUpper, singleValued));
        assertNotEquals(query, range("alpha", includeLower, null, includeUpper, singleValued));
        assertNotEquals(query, range("alpha", includeLower == false, "charlie", includeUpper, singleValued));
        assertNotEquals(query, range("alpha", includeLower, "charlie", includeUpper == false, singleValued));
        assertNotEquals(query, range("alpha", includeLower, "charlie", includeUpper, singleValued == false));
        assertNotEquals(
            query,
            new ColumnarStringRangeQuery(
                "other",
                new BytesRef("alpha"),
                includeLower,
                new BytesRef("charlie"),
                includeUpper,
                singleValued,
                ScanBudget.UNLIMITED
            )
        );
    }

    public void testAnyOfQueryIdentityFollowsEveryField() {
        final boolean singleValued = randomBoolean();
        final BytesRef alpha = new BytesRef("alpha");
        final BytesRef charlie = new BytesRef("charlie");
        final Query query = new ColumnarStringAnyOfQuery(FIELD, List.of(alpha, charlie), singleValued, ScanBudget.UNLIMITED);
        final Query sameTermsInAnotherOrder = new ColumnarStringAnyOfQuery(
            FIELD,
            List.of(charlie, alpha),
            singleValued,
            ScanBudget.UNLIMITED
        );
        assertEquals(query, sameTermsInAnotherOrder);
        assertEquals(query.hashCode(), sameTermsInAnotherOrder.hashCode());
        assertNotEquals(query, anyOf(singleValued, "alpha"));
        assertNotEquals(query, anyOf(singleValued, "alpha", "charlie", "delta"));
        assertNotEquals(query, anyOf(singleValued, "alpha", "delta"));
        assertNotEquals(query, anyOf(singleValued == false, "alpha", "charlie"));
        assertNotEquals(query, new ColumnarStringAnyOfQuery("other", termSet("alpha", "charlie"), singleValued, ScanBudget.UNLIMITED));
    }

    public void testAnyOfQueryKeepsItsOwnCopyOfTheTerms() throws IOException {
        final boolean singleValued = randomBoolean();
        final BytesRef callerTerm = new BytesRef("alpha");
        final Query query = new ColumnarStringAnyOfQuery(FIELD, List.of(callerTerm), singleValued, ScanBudget.UNLIMITED);
        final Query independent = anyOf(singleValued, "alpha");
        final BytesRef overwrite = new BytesRef("bravo");
        System.arraycopy(overwrite.bytes, overwrite.offset, callerTerm.bytes, callerTerm.offset, overwrite.length);
        assertEquals(query, independent);
        assertEquals(query.hashCode(), independent.hashCode());
        final List<Integer> alphaDocs = new ArrayList<>();
        for (int d = 0; d < 200; d += 4) {
            alphaDocs.add(d);
        }
        try (Directory dir = newDirectory()) {
            index(dir, oneTermADocument(200), singleValued);
            try (DirectoryReader reader = DirectoryReader.open(dir); DirectoryReader hidden = ColumnarTestUtils.hideTheColumn(reader)) {
                assertEquals("through the column", alphaDocs, found(new IndexSearcher(reader), query));
                assertEquals("through an overlay", alphaDocs, found(new IndexSearcher(hidden), query));
            }
        }
    }

    public void testAnyOfQueryIgnoresLaterChangesToItsInputs() throws IOException {
        final boolean singleValued = randomBoolean();
        final byte[] backing = "xxalphaYYbravoZZ".getBytes(StandardCharsets.UTF_8);
        final BytesRef alpha = new BytesRef(backing, 2, 5);
        final BytesRef bravo = new BytesRef(backing, 9, 5);
        final List<BytesRef> input = new ArrayList<>(List.of(alpha, bravo));
        final Query query = new ColumnarStringAnyOfQuery(FIELD, input, singleValued, ScanBudget.UNLIMITED);
        final Query independent = anyOf(singleValued, "alpha", "bravo");
        input.clear();
        alpha.offset += 1;
        alpha.length -= 1;
        bravo.length = 0;
        Arrays.fill(backing, (byte) 'z');
        assertEquals(query, independent);
        assertEquals(query.hashCode(), independent.hashCode());
        final List<Integer> alphaAndBravo = new ArrayList<>();
        for (int d = 0; d < 200; d++) {
            if (d % 4 <= 1) {
                alphaAndBravo.add(d);
            }
        }
        try (Directory dir = newDirectory()) {
            index(dir, oneTermADocument(200), singleValued);
            try (DirectoryReader reader = DirectoryReader.open(dir); DirectoryReader hidden = ColumnarTestUtils.hideTheColumn(reader)) {
                assertEquals("through the column", alphaAndBravo, found(new IndexSearcher(reader), query));
                assertEquals("through an overlay", alphaAndBravo, found(new IndexSearcher(hidden), query));
            }
        }
    }

    public void testAnyOfQueryIdentityIsByTermValue() {
        final boolean singleValued = randomBoolean();
        final Query query = anyOf(singleValued, "alpha", "charlie");
        final Query separatelyAllocated = new ColumnarStringAnyOfQuery(
            FIELD,
            List.of(new BytesRef("charlie"), new BytesRef("alpha")),
            singleValued,
            ScanBudget.UNLIMITED
        );
        final Query withDuplicates = new ColumnarStringAnyOfQuery(
            FIELD,
            List.of(new BytesRef("alpha"), new BytesRef("charlie"), new BytesRef("alpha"), new BytesRef("charlie")),
            singleValued,
            ScanBudget.UNLIMITED
        );
        assertEquals(query, separatelyAllocated);
        assertEquals(query.hashCode(), separatelyAllocated.hashCode());
        assertEquals(query, withDuplicates);
        assertEquals(query.hashCode(), withDuplicates.hashCode());
    }

    public void testRangeAndAnyOfFollowTheUtf8OrderOfSupplementaryCharacters() throws IOException {
        final boolean singleValued = randomBoolean();
        final String privateUse = "\uE000";
        final String supplementary = new String(Character.toChars(0x10000));
        final String[] cycle = { "a", privateUse, supplementary };
        final List<List<String>> docs = new ArrayList<>();
        for (int d = 0; d < 150; d++) {
            docs.add(List.of(cycle[d % cycle.length]));
        }
        try (Directory dir = newDirectory()) {
            index(dir, docs, singleValued);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                assertEquals(
                    "private use up to supplementary, both included",
                    docsWhere(150, r -> r != 0),
                    found(searcher, range(privateUse, true, supplementary, true, singleValued))
                );
                assertEquals(
                    "supplementary excluded",
                    docsWhere(150, r -> r == 1),
                    found(searcher, range(privateUse, true, supplementary, false, singleValued))
                );
                assertEquals(
                    "private use excluded",
                    docsWhere(150, r -> r == 2),
                    found(searcher, range(privateUse, false, supplementary, true, singleValued))
                );
                assertEquals(
                    "above the supplementary character",
                    docsWhere(150, r -> r == 2),
                    found(searcher, range(supplementary, true, null, true, singleValued))
                );
                assertEquals(
                    "below the private use character",
                    docsWhere(150, r -> r <= 1),
                    found(searcher, range(null, true, privateUse, true, singleValued))
                );
                assertEquals(
                    "bounds in UTF-16 order",
                    List.of(),
                    found(searcher, range(supplementary, true, privateUse, true, singleValued))
                );
                assertEquals("any of, in no order", docsWhere(150, r -> r != 1), found(searcher, anyOf(singleValued, supplementary, "a")));
            }
        }
    }

    public void testAFieldWithoutBinaryDocValuesIsNotScanned() throws IOException {
        final ScanBudget refusing = searcher -> { throw new AssertionError("a field without binary doc values has nothing to budget"); };
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodecForField("unrelated", ColumnarFieldType.STRING))
                .setMergePolicy(new LogDocMergePolicy());
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int d = 0; d < 100; d++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField(FIELD, d));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                final BytesRef lower = new BytesRef("a");
                final BytesRef upper = new BytesRef("z");
                final boolean singleValued = randomBoolean();
                assertEquals(
                    List.of(),
                    found(searcher, new ColumnarStringRangeQuery(FIELD, lower, true, upper, true, singleValued, refusing))
                );
                assertEquals(List.of(), found(searcher, new ColumnarStringAnyOfQuery(FIELD, List.of(lower), singleValued, refusing)));
            }
        }
    }

    private static List<Integer> docsWhere(final int count, final IntPredicate residueOfThree) {
        final List<Integer> docs = new ArrayList<>();
        for (int d = 0; d < count; d++) {
            if (residueOfThree.test(d % 3)) {
                docs.add(d);
            }
        }
        return docs;
    }

    public void testFieldAbsentFromTheSegment() throws IOException {
        final boolean singleValued = randomBoolean();
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.STRING))
                .setMergePolicy(new LogDocMergePolicy());
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int d = 0; d < 200; d++) {
                    final Document doc = new Document();
                    doc.add(new Field("other", payload(List.of("v" + d)), columnarBinaryFieldType()));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                assertEquals(List.of(), found(searcher, range("a", true, "z", true, singleValued)));
                assertEquals(List.of(), found(searcher, anyOf(singleValued, "alpha")));
                assertNoScorer(reader, range("a", true, "z", true, singleValued));
                assertNoScorer(reader, anyOf(singleValued, "alpha"));
            }
        }
    }

    public void testAColumnWithoutValuesMatchesNothing() throws IOException {
        final boolean singleValued = randomBoolean();
        try (Directory dir = newDirectory()) {
            index(dir, oneTermADocument(200), singleValued);
            try (DirectoryReader reader = DirectoryReader.open(dir); DirectoryReader dropped = ColumnarTestUtils.dropTheColumn(reader)) {
                ColumnarTestUtils.assertBinaryFieldWithoutValues(dropped, FIELD);
                final IndexSearcher searcher = new IndexSearcher(dropped);
                assertEquals(List.of(), found(searcher, range("a", true, "z", true, singleValued)));
                assertEquals(List.of(), found(searcher, anyOf(singleValued, "alpha")));
            }
        }
    }

    public void testAnEmptyTermSetMatchesNothing() throws IOException {
        final boolean singleValued = randomBoolean();
        try (Directory dir = newDirectory()) {
            index(dir, oneTermADocument(200), singleValued);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final Query noTerms = new ColumnarStringAnyOfQuery(FIELD, List.of(), singleValued, ScanBudget.UNLIMITED);
                assertEquals(List.of(), found(new IndexSearcher(reader), noTerms));
                assertNoScorer(reader, noTerms);
            }
        }
    }

    public void testAnInvertedOrEmptyRangeMatchesNothing() throws IOException {
        final boolean singleValued = randomBoolean();
        final Map<String, Query> ranges = Map.of(
            "lower above upper",
            range("charlie", true, "alpha", true, singleValued),
            "equal bounds, lower excluded",
            range("bravo", false, "bravo", true, singleValued),
            "equal bounds, upper excluded",
            range("bravo", true, "bravo", false, singleValued)
        );
        try (Directory dir = newDirectory()) {
            index(dir, oneTermADocument(200), singleValued);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                for (final Map.Entry<String, Query> range : ranges.entrySet()) {
                    assertEquals(range.getKey(), List.of(), found(searcher, range.getValue()));
                    assertNoScorer(reader, range.getValue());
                }
            }
        }
    }

    private static void assertNoScorer(final DirectoryReader reader, final Query query) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(reader);
        final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        for (final LeafReaderContext leaf : reader.leaves()) {
            assertNull("no scorer is supplied for " + query, weight.scorerSupplier(leaf));
        }
    }

    private static List<List<String>> oneTermADocument(int count) {
        final String[] terms = { "alpha", "bravo", "charlie", "delta" };
        final List<List<String>> docs = new ArrayList<>(count);
        for (int d = 0; d < count; d++) {
            docs.add(List.of(terms[d % terms.length]));
        }
        return docs;
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
                for (Shape shape : shapes(singleValued)) {
                    final List<Integer> expected = matching(docs, shape.matcher());
                    assertEquals("[" + shape.description() + "] through the column", expected, found(onTheColumn, shape.query()));
                    assertEquals("[" + shape.description() + "] through an overlay", expected, found(onAnOverlay, shape.query()));
                }
            }
        }
    }

    private static void index(Directory dir, List<List<String>> docs, boolean singleValued) throws IOException {
        final Codec codec = singleValued ? ColumnarTestUtils.singleValuedStringCodec() : columnarCodec(ColumnarFieldType.STRING);
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setMergePolicy(new LogDocMergePolicy());
        final FieldType type = columnarBinaryFieldType();
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

}
