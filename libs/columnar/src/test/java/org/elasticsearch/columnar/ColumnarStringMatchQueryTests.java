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
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.elasticsearch.columnar.ColumnarTestUtils.hideTheColumn;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * The filter shapes that are a test over values and nothing more - a set of terms, a range, a length - answered by the
 * column. Every one is checked against the documents the same test run over the values by hand would find, and against
 * what the same query finds with the column hidden, since a field read as an overlay has to answer the same.
 */
public class ColumnarStringMatchQueryTests extends ESTestCase {

    private static final String FIELD = "kw";

    /** The predicates worth telling apart, each with the description the query is compared by. */
    private record Shape(String description, Predicate<BytesRef> matcher) {}

    private static List<Shape> shapes() {
        return List.of(
            new Shape("terms", in("alpha", "charlie")),
            new Shape("terms-absent", in("nothing-holds-this")),
            new Shape("terms-with-empty", in("", "alpha")),
            new Shape("range[alpha,charlie)", between("alpha", true, "charlie", false)),
            new Shape("range(alpha,charlie]", between("alpha", false, "charlie", true)),
            new Shape("range-open-below", between(null, false, "bravo", true)),
            new Shape("range-open-above", between("bravo", true, null, false)),
            new Shape("length==5", value -> value.length == 5),
            new Shape("length>=6", value -> value.length >= 6),
            new Shape("length==0", value -> value.length == 0),
            // Accepts anything it is offered, which is what pins down what a null slot is: no value, so it is
            // offered to nothing and a document holding only nulls matches nothing.
            new Shape("everything", value -> true),
            new Shape("nothing", value -> false)
        );
    }

    private static Predicate<BytesRef> in(String... terms) {
        final Set<BytesRef> set = new java.util.HashSet<>();
        for (String term : terms) {
            set.add(new BytesRef(term));
        }
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

    /** Few terms over many documents, which is the shape that earns a dictionary. */
    public void testDictionaryColumn() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie", "delta", "" };
        final List<List<String>> docs = new ArrayList<>();
        for (int d = 0; d < between(400, 1200); d++) {
            docs.add(List.of(terms[d % terms.length]));
        }
        assertShapes(docs);
    }

    /** Values distinct enough that the column keeps no dictionary, so every value is tested on its own. */
    public void testPlainColumn() throws IOException {
        final List<List<String>> docs = new ArrayList<>();
        for (int d = 0; d < between(400, 1200); d++) {
            docs.add(List.of("value-" + d));
        }
        assertShapes(docs);
    }

    /**
     * A dictionary with values escaping it. An escaped value is named by nothing but its bytes, so it cannot inherit a
     * term's answer and has to be tested itself - which is the arm a column-wide test would otherwise miss.
     */
    public void testColumnWithEscapes() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie" };
        final List<List<String>> docs = new ArrayList<>();
        for (int d = 0; d < between(400, 1200); d++) {
            docs.add(List.of(d % 25 == 3 ? "escaped-" + d : terms[d % terms.length]));
        }
        assertShapes(docs);
    }

    /** A document matches when any slot it holds does, so the shapes are checked over documents holding several. */
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

    /**
     * Null slots, empty arrays and absent fields. A null is no value: it is offered to no matcher, not even one that
     * accepts everything, so a document holding only nulls matches nothing - while a document holding the empty string
     * holds a value a matcher may well accept.
     */
    public void testNullsEmptyArraysAndAbsentFields() throws IOException {
        final String[] terms = { "alpha", "bravo", "" };
        final List<List<String>> docs = new ArrayList<>();
        for (int d = 0; d < between(400, 1200); d++) {
            docs.add(switch (d % 6) {
                case 0 -> List.of(terms[d % terms.length]);
                // A lone null slot.
                case 1 -> java.util.Collections.singletonList(null);
                // A null beside a value.
                case 2 -> java.util.Arrays.asList(null, terms[d % terms.length]);
                // An empty array.
                case 3 -> List.<String>of();
                // The field absent altogether.
                case 4 -> null;
                default -> java.util.Arrays.asList(terms[d % terms.length], null);
            });
        }
        assertShapes(docs);
    }

    /**
     * The budget the search layer keeps for reading a column. It is consulted when the scorer's iterator is asked
     * for rather than when the supplier is built, since everything that allocates happens behind it and a supplier
     * Lucene decides not to use should cost nothing.
     */
    public void testTheBudgetIsSpentWhenTheColumnIsRead() throws IOException {
        final List<List<String>> docs = new ArrayList<>();
        for (int d = 0; d < 200; d++) {
            docs.add(List.of("term-" + (d % 4)));
        }
        final ScanBudget refuses = searcher -> { throw new IllegalStateException("no room to read a column"); };
        try (Directory dir = newDirectory()) {
            index(dir, docs);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                final Query query = new ColumnarStringMatchQuery(FIELD, value -> true, "everything", refuses);
                final Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
                final ScorerSupplier supplier = weight.scorerSupplier(reader.leaves().get(0));
                assertNotNull("a supplier costs nothing to hand out", supplier);
                expectThrows(IllegalStateException.class, () -> supplier.get(Long.MAX_VALUE));
                expectThrows(IllegalStateException.class, () -> searcher.search(query, 1));
            }
        }
    }

    /**
     * Every shape, against the documents the same predicate applied to the values by hand would find - through the
     * column, and again with the column hidden so the query has to read a document at a time.
     */
    private void assertShapes(List<List<String>> docs) throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, docs);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                // The two searchers have to be reading the field two different ways, or the comparison below holds
                // for the wrong reason: one of them answering everything a document at a time and agreeing with
                // itself. So each is asked what it sees before either is asked a question.
                assertThat(
                    "the field is a column",
                    reader.leaves().get(0).reader().getBinaryDocValues(FIELD),
                    instanceOf(StringColumnSource.class)
                );
                final DirectoryReader hidden = hideTheColumn(reader);
                assertThat(
                    "the column is hidden",
                    hidden.leaves().get(0).reader().getBinaryDocValues(FIELD),
                    not(instanceOf(StringColumnSource.class))
                );
                final IndexSearcher onTheColumn = new IndexSearcher(reader);
                final IndexSearcher onAnOverlay = new IndexSearcher(hidden);
                for (Shape shape : shapes()) {
                    final List<Integer> expected = matching(docs, shape.matcher());
                    assertEquals("[" + shape.description() + "] through the column", expected, found(onTheColumn, queryFor(shape)));
                    assertEquals("[" + shape.description() + "] through an overlay", expected, found(onAnOverlay, queryFor(shape)));
                }
            }
        }
    }

    private static void index(Directory dir, List<List<String>> docs) throws IOException {
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.STRING))
            .setMergePolicy(new LogDocMergePolicy());
        final FieldType type = columnarBinaryFieldType();
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (List<String> slots : docs) {
                final Document doc = new Document();
                if (slots != null) {
                    doc.add(new Field(FIELD, payload(slots), type));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static Query queryFor(Shape shape) {
        return new ColumnarStringMatchQuery(FIELD, shape.matcher(), shape.description(), ScanBudget.UNLIMITED);
    }

    /** The documents holding a slot the predicate accepts, worked out from the values themselves. */
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
        java.util.Collections.sort(docs);
        return docs;
    }
}
