/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The width-neutral scenarios run against both {@code integer} and {@code long} fields, since
 * {@link BitmapBKDQuery} serves both through {@link BitmapValues}. Scenarios that need values beyond
 * the 32-bit range are long-only and marked as such.
 */
public class BitmapBKDQueryTests extends ESTestCase {

    private static final String FIELD = "f";

    /** Beyond the 32-bit range, so a long bitmap spreads across several high-32-bit buckets. */
    private static final long BEYOND_INT = 1L << 40;

    private enum NumberType {
        INT {
            @Override
            BitmapValues bitmapOf(long... valuesIn) {
                int[] ints = new int[valuesIn.length];
                for (int i = 0; i < valuesIn.length; i++) {
                    ints[i] = (int) valuesIn[i];
                }
                return IntBitmap.bitmapOf(ints);
            }

            @Override
            void addField(Document doc, long value) {
                doc.add(new IntPoint(FIELD, (int) value));
            }

            @Override
            Query pointSetQuery(long... valuesIn) {
                int[] ints = new int[valuesIn.length];
                for (int i = 0; i < valuesIn.length; i++) {
                    ints[i] = (int) valuesIn[i];
                }
                return IntPoint.newSetQuery(FIELD, ints);
            }

            @Override
            SortField.Type sortType() {
                return SortField.Type.INT;
            }

            @Override
            Object missingValue(long value) {
                return (int) value;
            }
        },
        LONG {
            @Override
            BitmapValues bitmapOf(long... valuesIn) {
                return LongBitmap.bitmapOf(valuesIn);
            }

            @Override
            void addField(Document doc, long value) {
                doc.add(new LongPoint(FIELD, value));
            }

            @Override
            Query pointSetQuery(long... valuesIn) {
                return LongPoint.newSetQuery(FIELD, valuesIn);
            }

            @Override
            SortField.Type sortType() {
                return SortField.Type.LONG;
            }

            @Override
            Object missingValue(long value) {
                return value;
            }
        };

        abstract BitmapValues bitmapOf(long... valuesIn);

        abstract void addField(Document doc, long value);

        abstract Query pointSetQuery(long... valuesIn);

        abstract SortField.Type sortType();

        /** Boxed as the type {@link SortField#setMissingValue} demands for this type. */
        abstract Object missingValue(long value);

        /** Adds the point plus the doc values the index sort and the streaming scan's skip both read. */
        void addSortableField(Document doc, long value) {
            addField(doc, value);
            doc.add(new SortedNumericDocValuesField(FIELD, value));
        }

        BitmapValues empty() {
            return bitmapOf();
        }
    }

    private static Query query(NumberType type, long... valuesIn) {
        return new BitmapBKDQuery(FIELD, type.bitmapOf(valuesIn));
    }

    public void testEqualsAndHashCode() {
        for (NumberType type : NumberType.values()) {
            Query a = query(type, 1, 2, 3);
            Query b = query(type, 1, 2, 3);
            Query c = query(type, 1, 2);
            QueryUtils.check(a);
            QueryUtils.checkEqual(a, b);
            QueryUtils.checkUnequal(a, c);
            QueryUtils.checkUnequal(a, new BitmapBKDQuery("g", type.bitmapOf(1, 2, 3)));
        }
        // An int bitmap and a long bitmap holding the same values are different queries, because they
        // merge against differently encoded points.
        QueryUtils.checkUnequal(query(NumberType.INT, 1, 2, 3), query(NumberType.LONG, 1, 2, 3));
    }

    public void testToString() {
        for (NumberType type : NumberType.values()) {
            assertThat(new BitmapBKDQuery(FIELD, type.empty()).toString(FIELD), containsString("cardinality=0"));

            String description = query(type, 1, 100, 1000).toString(FIELD);
            assertThat(description, containsString("cardinality=3"));
            assertThat(description, containsString("first=1"));
            assertThat(description, containsString("last=1000"));
        }
    }

    public void testNoIndexedField() throws IOException {
        for (NumberType type : NumberType.values()) {
            try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                w.addDocument(new Document());
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertThat(searcher.count(query(type, 1)), equalTo(0));
                }
            }
        }
    }

    public void testSearch() throws IOException {
        for (NumberType type : NumberType.values()) {
            for (boolean splitIntoSegments : new boolean[] { false, true }) {
                try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                    for (int i = 0; i < 10; i++) {
                        Document doc = new Document();
                        type.addField(doc, i);
                        w.addDocument(doc);
                        if (splitIntoSegments && i < 9 && randomBoolean()) {
                            w.commit();
                        }
                    }
                    // One doc with no field value
                    w.addDocument(new Document());

                    try (IndexReader reader = w.getReader()) {
                        IndexSearcher searcher = newSearcher(reader);
                        String message = "type=" + type + " splitIntoSegments=" + splitIntoSegments;
                        assertThat(message, searcher.count(query(type, 1, 3, 5)), equalTo(3));
                        assertThat(message, searcher.count(query(type, 100)), equalTo(0));
                        assertThat(message, searcher.count(query(type, 0, 5, 9)), equalTo(3));
                        assertThat(message, searcher.count(query(type, 0)), equalTo(1));
                        assertThat(message, searcher.count(query(type, 9)), equalTo(1));
                        assertThat(message, searcher.count(query(type, 3, 100)), equalTo(1));
                    }
                }
            }
        }
    }

    /**
     * Many documents sharing one value. Past BKD's points-per-leaf limit this produces a leaf whose min
     * and max are both that value, which is the {@code CELL_INSIDE_QUERY} path in MergePointVisitor —
     * though the limit is randomised by the test framework, so that path is likely rather than
     * guaranteed. What is asserted either way is that heavy duplication matches every document once.
     */
    public void testManyDocsWithSameValue() throws IOException {
        for (NumberType type : NumberType.values()) {
            try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                for (int i = 0; i < 600; i++) {
                    Document doc = new Document();
                    type.addField(doc, 42);
                    w.addDocument(doc);
                }
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertThat(searcher.count(query(type, 42)), equalTo(600));
                    assertThat(searcher.count(query(type, 1)), equalTo(0));
                }
            }
        }
    }

    /**
     * A bitmap that cannot overlap a segment's value range is dropped in {@code scorerSupplier}, before
     * any traversal. Asserting no matches would pass even without that check, so this asserts the
     * supplier itself is {@code null} — which is what makes it a test of the pruning rather than of the
     * merge scan finding nothing.
     */
    public void testRangeSkipping() throws IOException {
        for (NumberType type : NumberType.values()) {
            try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                for (int i = 1; i <= 100; i++) {
                    Document doc = new Document();
                    type.addField(doc, i);
                    w.addDocument(doc);
                }
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    // Entirely above the indexed range, and entirely below it
                    for (Query nonOverlapping : List.of(query(type, 200, 300), query(type, 0))) {
                        assertThat(searcher.count(nonOverlapping), equalTo(0));

                        Weight weight = searcher.createWeight(searcher.rewrite(nonOverlapping), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
                        for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                            assertNull("type=" + type + " query=" + nonOverlapping, weight.scorerSupplier(leaf));
                        }
                    }
                }
            }
        }
    }

    /**
     * Documents with negative values must not confuse the merge scan. The bitmap iterates in unsigned
     * order while the encoded points are in signed order; the two only agree because the bitmap holds
     * no negatives, which the query builder enforces.
     */
    public void testNegativeDocumentValuesAreNotMatched() throws IOException {
        for (NumberType type : NumberType.values()) {
            try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                for (long value : new long[] { Integer.MIN_VALUE, -100, -1, 0, 1, 42 }) {
                    Document doc = new Document();
                    type.addField(doc, value);
                    w.addDocument(doc);
                }
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertThat(searcher.count(query(type, 0, 1, 42)), equalTo(3));
                    assertThat(searcher.count(query(type, 0)), equalTo(1));
                }
            }
        }
    }

    /**
     * Cross-checks the merge scan against {@link IntPoint#newSetQuery}/{@link LongPoint#newSetQuery} —
     * the traditional way to express this query — over random data, sweeping term counts from 1 to 100
     * where an off-by-one in a hand-written merge is most likely to show.
     * <p>
     * Compares the matched document ids rather than just how many matched: two queries can agree on a
     * count while disagreeing on which documents, and that would be a silent correctness bug.
     */
    public void testAgreesWithPointSetQuery() throws IOException {
        for (NumberType type : NumberType.values()) {
            long bound = type == NumberType.INT ? Integer.MAX_VALUE : Long.MAX_VALUE;
            try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                int docCount = randomIntBetween(50, 300);
                long[] indexed = new long[docCount];
                for (int i = 0; i < docCount; i++) {
                    // Draw some values from a deliberately narrow range so several documents share one
                    // value, exercising the bulk-add and CELL_INSIDE_QUERY paths.
                    indexed[i] = randomLongBetween(0, randomBoolean() ? 50 : bound);
                    Document doc = new Document();
                    type.addField(doc, indexed[i]);
                    w.addDocument(doc);
                }
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    for (int nTerms : TERM_COUNTS) {
                        long[] queried = randomQueryValues(nTerms, indexed, bound);
                        assertThat(
                            "type=" + type + " nTerms=" + nTerms + " values=" + Arrays.toString(queried),
                            matchingDocs(searcher, query(type, queried), docCount),
                            equalTo(matchingDocs(searcher, type.pointSetQuery(queried), docCount))
                        );
                    }
                }
            }
        }
    }

    /** Swept by the cross-check above; small counts first, since that is where edge cases hide. */
    private static final int[] TERM_COUNTS = { 1, 2, 3, 5, 10, 25, 50, 100 };

    /** Mixes values known to be indexed with values almost certainly not, so both outcomes are covered. */
    private long[] randomQueryValues(int nTerms, long[] indexed, long bound) {
        long[] queried = new long[nTerms];
        for (int i = 0; i < nTerms; i++) {
            queried[i] = randomBoolean() ? indexed[randomIntBetween(0, indexed.length - 1)] : randomLongBetween(0, bound);
        }
        return queried;
    }

    /** The matched doc ids, ascending, so two queries can be compared on identity rather than count. */
    private static List<Integer> matchingDocs(IndexSearcher searcher, Query query, int maxDoc) throws IOException {
        TopDocs topDocs = searcher.search(query, Math.max(1, maxDoc));
        List<Integer> docs = new ArrayList<>(topDocs.scoreDocs.length);
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            docs.add(scoreDoc.doc);
        }
        Collections.sort(docs);
        return docs;
    }

    /**
     * Long-only: values above 2^32 land in different high-32-bit buckets of the bitmap, so the iterator
     * must cross bucket boundaries while staying in step with the BKD tree's ordering.
     */
    public void testLongValuesBeyondIntRange() throws IOException {
        long[] indexed = { 0L, 1L, Integer.MAX_VALUE, 1L << 32, (1L << 32) + 1, 1L << 33, BEYOND_INT, Long.MAX_VALUE };
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : indexed) {
                Document doc = new Document();
                NumberType.LONG.addField(doc, value);
                w.addDocument(doc);
            }
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(query(NumberType.LONG, indexed)), equalTo(indexed.length));
                assertThat(searcher.count(query(NumberType.LONG, 1L << 32, Long.MAX_VALUE)), equalTo(2));
                assertThat(searcher.count(query(NumberType.LONG, 1L << 32, 1L << 34)), equalTo(1));
                // Shares a bucket with an indexed value but is not itself indexed
                assertThat(searcher.count(query(NumberType.LONG, (1L << 32) + 5)), equalTo(0));
            }
        }
    }

    public void testEmptyBitmapRewritesToMatchNoDocs() throws IOException {
        for (NumberType type : NumberType.values()) {
            try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                Document doc = new Document();
                type.addField(doc, 1);
                w.addDocument(doc);
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query rewritten = searcher.rewrite(new BitmapBKDQuery(FIELD, type.empty()));
                    assertThat(rewritten, instanceOf(MatchNoDocsQuery.class));
                }
            }
        }
    }

    private static RandomIndexWriter sortedWriter(NumberType type, Directory dir) throws IOException {
        return sortedWriter(type, dir, null);
    }

    /**
     * @param missingValue where the sort places a document carrying no value, or null for the default
     */
    private static RandomIndexWriter sortedWriter(NumberType type, Directory dir, Long missingValue) throws IOException {
        IndexWriterConfig config = newIndexWriterConfig();
        SortedNumericSortField sortField = new SortedNumericSortField(FIELD, type.sortType());
        if (missingValue != null) {
            sortField.setMissingValue(type.missingValue(missingValue));
        }
        config.setIndexSort(new Sort(sortField));
        return new RandomIndexWriter(random(), dir, config);
    }

    /**
     * Asserts that every leaf carrying the field qualifies for the sorted-index optimization, so a test written to
     * cover it cannot quietly fall back to collecting into a builder and still pass.
     * <p>
     * A leaf holding only documents without a value contributes no points at all, and the query skips such
     * a segment outright rather than streaming it. {@link RandomIndexWriter} flushes where it likes, so a
     * test that indexes valueless documents cannot pin down whether one exists.
     */
    private static void assertSortingOptimizationApplies(IndexReader reader) throws IOException {
        int streamed = 0;
        for (LeafReaderContext context : reader.leaves()) {
            PointValues points = context.reader().getPointValues(FIELD);
            if (points == null) {
                continue;
            }
            Sort sort = context.reader().getMetaData().sort();
            assertNotNull("index sort did not survive to the leaf", sort);
            assertThat(sort.getSort()[0].getField(), equalTo(FIELD));
            assertThat("field must be single-valued", points.size(), equalTo((long) points.getDocCount()));
            streamed++;
        }
        assertThat("no leaf carried the field, so nothing exercised the sorted-index optimization", streamed, greaterThan(0));
    }

    /**
     * Scattered singletons plus a few runs. The streaming walk treats both the same way, so the shape
     * that matters is only that some values are absent from the index and some are shared by several
     * documents.
     */
    private long[] randomQueriedValues(int maxValue) {
        SortedSet<Long> chosen = new TreeSet<>();
        for (int singletons = randomIntBetween(5, 60); singletons > 0; singletons--) {
            chosen.add((long) randomIntBetween(0, maxValue));
        }
        for (int runs = randomIntBetween(0, 3); runs > 0; runs--) {
            long start = randomIntBetween(0, maxValue);
            for (int i = 0, length = randomIntBetween(2, 50); i < length && start + i <= maxValue; i++) {
                chosen.add(start + i);
            }
        }
        // Above everything indexed, so the walk running off the end of the tree is covered.
        if (randomBoolean()) {
            chosen.add(maxValue + 1000L);
        }
        return chosen.stream().mapToLong(Long::longValue).toArray();
    }

    /**
     * The matched documents' field values, sorted. An index sort reorders documents, so doc ids cannot
     * be compared across a sorted and an unsorted copy of the same data, but the values they carry can.
     */
    private static List<Long> matchedValues(IndexSearcher searcher, Query query) throws IOException {
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        List<Long> matched = new ArrayList<>();
        for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
            ScorerSupplier supplier = weight.scorerSupplier(context);
            if (supplier == null) {
                continue;
            }
            SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), FIELD);
            DocIdSetIterator docs = supplier.get(Long.MAX_VALUE).iterator();
            for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                assertTrue("doc " + doc + " has no value", values.advanceExact(doc));
                for (int i = 0; i < values.docValueCount(); i++) {
                    matched.add(values.nextValue());
                }
            }
        }
        Collections.sort(matched);
        return matched;
    }

    /**
     * The sorted index must match the unsorted one over identical data, which pins correctness because
     * {@link #testAgreesWithPointSetQuery} already holds the unsorted path to {@code newSetQuery}. Then
     * {@link QueryUtils#check} must find {@code advance()} and {@code nextDoc()} in agreement &mdash; the only
     * check here that catches doc ids emitted out of order.
     * <p>
     * Layouts are randomised: all documents valued, some valueless, some valueless placed by the sort in the
     * middle of the range the rest span, and multi-valued, which must fall back to collecting.
     */
    public void testSortedAndUnsortedAgree() throws IOException {
        for (NumberType type : NumberType.values()) {
            int numDocs = atLeast(300);
            int maxValue = randomIntBetween(50, 300);
            // Index sort permits a multi-valued field (index.sort.mode picks min or max), so the fallback is
            // a configuration users can reach, not a theoretical one.
            boolean multiValued = randomBoolean();
            boolean someValueless = multiValued == false && randomBoolean();
            Long missingValue = someValueless && randomBoolean() ? (long) randomIntBetween(0, maxValue) : null;
            String layout = multiValued ? "multi-valued"
                : someValueless == false ? "all valued"
                : "valueless sorted at " + (missingValue == null ? "the default" : missingValue);

            long[] indexed = new long[numDocs];
            for (int i = 0; i < numDocs; i++) {
                // -1 marks a document indexed without a value at all.
                indexed[i] = someValueless && randomInt(2) == 0 ? -1 : randomIntBetween(0, maxValue);
            }
            try (
                Directory sortedDir = newDirectory();
                RandomIndexWriter sorted = sortedWriter(type, sortedDir, missingValue);
                Directory plainDir = newDirectory();
                RandomIndexWriter plain = new RandomIndexWriter(random(), plainDir)
            ) {
                for (long value : indexed) {
                    Document doc = new Document();
                    Document copy = new Document();
                    if (value >= 0) {
                        type.addSortableField(doc, value);
                        type.addSortableField(copy, value);
                        if (multiValued) {
                            // Far above the first, so value order and doc order disagree as widely as possible.
                            type.addSortableField(doc, value + 1000);
                            type.addSortableField(copy, value + 1000);
                        }
                    }
                    sorted.addDocument(doc);
                    plain.addDocument(copy);
                }
                try (IndexReader sortedReader = sorted.getReader(); IndexReader plainReader = plain.getReader()) {
                    IndexSearcher sortedSearcher = newSearcher(sortedReader);
                    IndexSearcher plainSearcher = newSearcher(plainReader);
                    if (multiValued == false) {
                        assertSortingOptimizationApplies(sortedSearcher.getIndexReader());
                    }
                    for (int iter = 0; iter < 10; iter++) {
                        Query query = query(type, randomQueriedValues(maxValue));
                        String message = "type=" + type + " layout=" + layout;
                        assertThat(message, matchedValues(sortedSearcher, query), equalTo(matchedValues(plainSearcher, query)));
                        QueryUtils.check(random(), query, sortedSearcher);
                    }
                }
            }
        }
    }

    /**
     * Enough documents per value that a whole cell holds one value, which is the {@code CELL_INSIDE_QUERY}
     * branch. On a sorted segment those documents are also a contiguous doc-id range, so the leaf's ids
     * reach the buffer through the bulk {@code visit} overloads rather than one at a time — the paths a
     * per-document test never touches.
     */
    public void testSortedIndexManyDocsPerValue() throws IOException {
        for (NumberType type : NumberType.values()) {
            try (Directory dir = newDirectory(); RandomIndexWriter w = sortedWriter(type, dir)) {
                for (int value = 0; value < 5; value++) {
                    for (int i = 0; i < 600; i++) {
                        Document doc = new Document();
                        type.addSortableField(doc, value);
                        w.addDocument(doc);
                    }
                }
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertSortingOptimizationApplies(searcher.getIndexReader());
                    assertThat(searcher.count(query(type, 2)), equalTo(600));
                    assertThat(searcher.count(query(type, 0, 4)), equalTo(1200));
                    assertThat(searcher.count(query(type, 7)), equalTo(0));
                    QueryUtils.check(random(), query(type, 1, 3), searcher);
                }
            }
        }
    }

    /**
     * Every other test draws values that fit in an {@code int}, even those running over a {@code long} field.
     * This is the one that uses genuinely 64-bit values, so the bitmap has to cross the high-32-bit bucket
     * boundaries the portable format splits on.
     */
    public void testSortedIndexLongValuesBeyondIntRange() throws IOException {
        long[] indexed = { 0L, 1L, Integer.MAX_VALUE, 1L << 32, (1L << 32) + 1, 1L << 33, BEYOND_INT, Long.MAX_VALUE };
        try (Directory dir = newDirectory(); RandomIndexWriter w = sortedWriter(NumberType.LONG, dir)) {
            for (long value : indexed) {
                Document doc = new Document();
                NumberType.LONG.addSortableField(doc, value);
                w.addDocument(doc);
            }
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertSortingOptimizationApplies(searcher.getIndexReader());
                assertThat(searcher.count(query(NumberType.LONG, indexed)), equalTo(indexed.length));
                assertThat(searcher.count(query(NumberType.LONG, 1L << 32, Long.MAX_VALUE)), equalTo(2));
                assertThat(searcher.count(query(NumberType.LONG, (1L << 32) + 5)), equalTo(0));
                QueryUtils.check(random(), query(NumberType.LONG, indexed), searcher);
            }
        }
    }
}
