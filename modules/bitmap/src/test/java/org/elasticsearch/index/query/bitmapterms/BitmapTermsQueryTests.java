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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
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
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The width-neutral scenarios run against both {@code integer} and {@code long} fields, since
 * {@link BitmapTermsQuery} serves both through {@link BitmapValues}. Scenarios that need values
 * beyond the 32-bit range are long-only and marked as such.
 */
public class BitmapTermsQueryTests extends ESTestCase {

    private static final String FIELD = "f";

    /** Beyond the 32-bit range, so a long bitmap spreads across several high-32-bit buckets. */
    private static final long BEYOND_INT = 1L << 40;

    private static final FieldType INDEX_TERMS_TYPE;

    static {
        INDEX_TERMS_TYPE = new FieldType();
        INDEX_TERMS_TYPE.setIndexOptions(IndexOptions.DOCS);
        INDEX_TERMS_TYPE.setOmitNorms(true);
        INDEX_TERMS_TYPE.setTokenized(false);
        INDEX_TERMS_TYPE.freeze();
    }

    /** Mirrors {@code NumberFieldMapper}'s {@code encodeIntIndexTerm}/{@code encodeLongIndexTerm}. */
    private enum Width {
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
            BytesRef encodeTerm(long value) {
                byte[] bytes = new byte[Integer.BYTES];
                NumericUtils.intToSortableBytes((int) value, bytes, 0);
                return new BytesRef(bytes);
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
            BytesRef encodeTerm(long value) {
                byte[] bytes = new byte[Long.BYTES];
                NumericUtils.longToSortableBytes(value, bytes, 0);
                return new BytesRef(bytes);
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

        abstract BytesRef encodeTerm(long value);

        abstract SortField.Type sortType();

        /** Boxed as the type {@link SortField#setMissingValue} demands for this width. */
        abstract Object missingValue(long value);

        void addField(Document doc, long value) {
            doc.add(new Field(FIELD, encodeTerm(value), INDEX_TERMS_TYPE));
        }

        /** Adds the term plus the doc values the index sort and the doc-range skip both read. */
        void addSortableField(Document doc, long value) {
            addField(doc, value);
            doc.add(new SortedNumericDocValuesField(FIELD, value));
        }

        Query termInSetQuery(long... valuesIn) {
            List<BytesRef> terms = new ArrayList<>(valuesIn.length);
            for (long value : valuesIn) {
                terms.add(encodeTerm(value));
            }
            return new TermInSetQuery(FIELD, terms);
        }

        BitmapValues empty() {
            return bitmapOf();
        }
    }

    private static Query query(Width width, long... valuesIn) {
        return new BitmapTermsQuery(FIELD, width.bitmapOf(valuesIn));
    }

    public void testEqualsAndHashCode() {
        for (Width width : Width.values()) {
            Query a = query(width, 1, 2, 3);
            Query b = query(width, 1, 2, 3);
            Query c = query(width, 1, 2);
            QueryUtils.check(a);
            QueryUtils.checkEqual(a, b);
            QueryUtils.checkUnequal(a, c);
            QueryUtils.checkUnequal(a, new BitmapTermsQuery("g", width.bitmapOf(1, 2, 3)));
        }
        // An int bitmap and a long bitmap holding the same values are different queries, because their
        // terms are encoded at different widths.
        QueryUtils.checkUnequal(query(Width.INT, 1, 2, 3), query(Width.LONG, 1, 2, 3));
    }

    public void testToString() {
        for (Width width : Width.values()) {
            assertThat(new BitmapTermsQuery(FIELD, width.empty()).toString(FIELD), containsString("cardinality=0"));

            String description = query(width, 1, 100, 1000).toString(FIELD);
            assertThat(description, containsString("cardinality=3"));
            assertThat(description, containsString("first=1"));
            assertThat(description, containsString("last=1000"));
        }
    }

    public void testNoIndexedField() throws IOException {
        for (Width width : Width.values()) {
            try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                w.addDocument(new Document());
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertThat(searcher.count(query(width, 1)), equalTo(0));
                }
            }
        }
    }

    public void testSearch() throws IOException {
        for (Width width : Width.values()) {
            for (boolean splitIntoSegments : new boolean[] { false, true }) {
                try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                    for (int i = 0; i < 10; i++) {
                        Document doc = new Document();
                        width.addField(doc, i);
                        w.addDocument(doc);
                        if (splitIntoSegments && i < 9 && randomBoolean()) {
                            w.commit();
                        }
                    }
                    // One doc with no field value
                    w.addDocument(new Document());

                    try (IndexReader reader = w.getReader()) {
                        IndexSearcher searcher = newSearcher(reader);
                        String message = "width=" + width + " splitIntoSegments=" + splitIntoSegments;
                        assertThat(message, searcher.count(query(width, 1, 3, 5)), equalTo(3));
                        assertThat(message, searcher.count(query(width, 100)), equalTo(0));
                        assertThat(message, searcher.count(query(width, 0, 5, 9)), equalTo(3));
                        assertThat(message, searcher.count(query(width, 0)), equalTo(1));
                        assertThat(message, searcher.count(query(width, 9)), equalTo(1));
                        assertThat(message, searcher.count(query(width, 3, 100)), equalTo(1));
                    }
                }
            }
        }
    }

    /**
     * The "bitmap is behind" branch under load: the terms dictionary jumps far ahead of the bitmap,
     * which then has to skip a long run of values to catch up.
     */
    public void testBitmapSkipsManyValuesBetweenTerms() throws IOException {
        for (Width width : Width.values()) {
            long[] indexedValues = width == Width.INT ? new long[] { 0L, 1L << 20, 1L << 28 } : new long[] { 0L, 1L << 32, BEYOND_INT };
            try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                for (long value : indexedValues) {
                    Document doc = new Document();
                    width.addField(doc, value);
                    w.addDocument(doc);
                }
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    // A dense run around each indexed value, so the scan repeatedly skips a stretch
                    long[] queried = new long[3000];
                    int at = 0;
                    for (long base : indexedValues) {
                        for (int offset = 0; offset < 1000; offset++) {
                            queried[at++] = base + offset;
                        }
                    }
                    assertThat(searcher.count(query(width, queried)), equalTo(indexedValues.length));
                }
            }
        }
    }

    /**
     * Documents with negative values must not confuse the merge scan. Their sortable-bytes terms sort
     * before every non-negative term, while the bitmap iterates in unsigned order; the two only agree
     * because the bitmap holds no negatives, which the query builder enforces.
     */
    public void testNegativeDocumentValuesAreNotMatched() throws IOException {
        for (Width width : Width.values()) {
            try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                for (long value : new long[] { Integer.MIN_VALUE, -100, -1, 0, 1, 42 }) {
                    Document doc = new Document();
                    width.addField(doc, value);
                    w.addDocument(doc);
                }
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertThat(searcher.count(query(width, 0, 1, 42)), equalTo(3));
                    assertThat(searcher.count(query(width, 0)), equalTo(1));
                }
            }
        }
    }

    /**
     * Cross-checks the merge scan against {@link TermInSetQuery} — how a regular {@code terms} query on
     * an {@code index_terms} field is executed — over random data, sweeping term counts from 1 to 100
     * where an off-by-one in a hand-written merge is most likely to show.
     * <p>
     * Compares the matched document ids rather than just how many matched: two queries can agree on a
     * count while disagreeing on which documents, and that would be a silent correctness bug.
     */
    public void testAgreesWithTermInSetQuery() throws IOException {
        for (Width width : Width.values()) {
            long bound = width == Width.INT ? Integer.MAX_VALUE : Long.MAX_VALUE;
            try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                int docCount = randomIntBetween(50, 300);
                long[] indexed = new long[docCount];
                for (int i = 0; i < docCount; i++) {
                    // Draw some values from a deliberately narrow range so several documents share one
                    // term, exercising the postings bulk-add path.
                    indexed[i] = randomLongBetween(0, randomBoolean() ? 50 : bound);
                    Document doc = new Document();
                    width.addField(doc, indexed[i]);
                    w.addDocument(doc);
                }
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    for (int nTerms : TERM_COUNTS) {
                        long[] queried = randomQueryValues(nTerms, indexed, bound);
                        assertThat(
                            "width=" + width + " nTerms=" + nTerms + " values=" + Arrays.toString(queried),
                            matchingDocs(searcher, query(width, queried), docCount),
                            equalTo(matchingDocs(searcher, width.termInSetQuery(queried), docCount))
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

    /** Long-only: the iterator must cross high-32-bit bucket boundaries in step with the terms order. */
    public void testLongValuesBeyondIntRange() throws IOException {
        long[] indexed = { 0L, 1L, Integer.MAX_VALUE, 1L << 32, (1L << 32) + 1, 1L << 33, BEYOND_INT, Long.MAX_VALUE };
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : indexed) {
                Document doc = new Document();
                Width.LONG.addField(doc, value);
                w.addDocument(doc);
            }
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(query(Width.LONG, indexed)), equalTo(indexed.length));
                assertThat(searcher.count(query(Width.LONG, 1L << 32, Long.MAX_VALUE)), equalTo(2));
                assertThat(searcher.count(query(Width.LONG, 1L << 32, 1L << 34)), equalTo(1));
                assertThat(searcher.count(query(Width.LONG, (1L << 32) + 5)), equalTo(0));
            }
        }
    }

    public void testEmptyBitmapRewritesToMatchNoDocs() throws IOException {
        for (Width width : Width.values()) {
            try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                Document doc = new Document();
                width.addField(doc, 1);
                w.addDocument(doc);
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query rewritten = searcher.rewrite(new BitmapTermsQuery(FIELD, width.empty()));
                    assertThat(rewritten, instanceOf(MatchNoDocsQuery.class));
                }
            }
        }
    }

    private static RandomIndexWriter sortedWriter(Width width, Directory dir) throws IOException {
        IndexWriterConfig config = newIndexWriterConfig();
        config.setIndexSort(new Sort(new SortedNumericSortField(FIELD, width.sortType())));
        return new RandomIndexWriter(random(), dir, config);
    }

    /**
     * Asserts every leaf qualifies for the streaming scan, so a test written to cover it cannot quietly
     * fall back to collecting into a builder and still pass.
     */
    private static void assertStreamingApplies(IndexReader reader) throws IOException {
        assertFalse("expected at least one leaf", reader.leaves().isEmpty());
        for (LeafReaderContext context : reader.leaves()) {
            Sort sort = context.reader().getMetaData().sort();
            assertNotNull("index sort did not survive to the leaf", sort);
            assertThat(sort.getSort()[0].getField(), equalTo(FIELD));
            Terms terms = context.reader().terms(FIELD);
            assertNotNull(terms);
            assertThat("field must be single-valued", terms.getSumDocFreq(), equalTo((long) terms.getDocCount()));
        }
    }

    /**
     * Scattered singletons plus a few runs. The streaming scan treats both the same way, so the shape
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
        // Above everything indexed, so the scan running off the end of the terms dictionary is covered.
        if (randomBoolean()) {
            chosen.add(maxValue + 1000L);
        }
        return chosen.stream().mapToLong(Long::longValue).toArray();
    }

    /**
     * Cross-checks the streaming scan against a brute-force count, and runs {@link QueryUtils#check} so
     * {@code advance()} is verified against {@code nextDoc()} rather than only exhaustive iteration.
     */
    public void testSortedIndexAgainstBruteForce() throws IOException {
        for (Width width : Width.values()) {
            int numDocs = atLeast(500);
            int maxValue = randomIntBetween(50, 500);
            long[] indexed = new long[numDocs];
            try (Directory dir = newDirectory(); RandomIndexWriter w = sortedWriter(width, dir)) {
                for (int i = 0; i < numDocs; i++) {
                    indexed[i] = randomIntBetween(0, maxValue);
                    Document doc = new Document();
                    width.addSortableField(doc, indexed[i]);
                    w.addDocument(doc);
                }
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertStreamingApplies(searcher.getIndexReader());
                    for (int iter = 0; iter < 15; iter++) {
                        long[] queried = randomQueriedValues(maxValue);
                        Set<Long> wanted = Arrays.stream(queried).boxed().collect(Collectors.toSet());
                        int expected = 0;
                        for (long value : indexed) {
                            if (wanted.contains(value)) {
                                expected++;
                            }
                        }
                        Query query = query(width, queried);
                        assertThat("width=" + width, searcher.count(query), equalTo(expected));
                        QueryUtils.check(random(), query, searcher);
                    }
                }
            }
        }
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
            NumericDocValues values = DocValues.unwrapSingleton(DocValues.getSortedNumeric(context.reader(), FIELD));
            DocIdSetIterator docs = supplier.get(Long.MAX_VALUE).iterator();
            for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                assertTrue("doc " + doc + " has no value", values.advanceExact(doc));
                matched.add(values.longValue());
            }
        }
        Collections.sort(matched);
        return matched;
    }

    /**
     * The streaming scan must match the same documents as collecting into a builder. Running both over
     * identical data isolates the strategy from the data, which a brute-force oracle alone cannot do.
     */
    public void testSortedAndUnsortedAgree() throws IOException {
        for (Width width : Width.values()) {
            int numDocs = atLeast(300);
            int maxValue = randomIntBetween(50, 300);
            long[] indexed = new long[numDocs];
            for (int i = 0; i < numDocs; i++) {
                indexed[i] = randomIntBetween(0, maxValue);
            }
            try (
                Directory sortedDir = newDirectory();
                RandomIndexWriter sorted = sortedWriter(width, sortedDir);
                Directory plainDir = newDirectory();
                RandomIndexWriter plain = new RandomIndexWriter(random(), plainDir)
            ) {
                for (long value : indexed) {
                    Document doc = new Document();
                    width.addSortableField(doc, value);
                    sorted.addDocument(doc);
                    Document copy = new Document();
                    width.addSortableField(copy, value);
                    plain.addDocument(copy);
                }
                try (IndexReader sortedReader = sorted.getReader(); IndexReader plainReader = plain.getReader()) {
                    IndexSearcher sortedSearcher = newSearcher(sortedReader);
                    IndexSearcher plainSearcher = newSearcher(plainReader);
                    assertStreamingApplies(sortedSearcher.getIndexReader());
                    for (int iter = 0; iter < 10; iter++) {
                        Query query = query(width, randomQueriedValues(maxValue));
                        assertThat("width=" + width, matchedValues(sortedSearcher, query), equalTo(matchedValues(plainSearcher, query)));
                    }
                }
            }
        }
    }

    /**
     * Documents missing a value sort to one end and appear in no term's postings, so they cannot disturb
     * the doc order the streaming scan relies on. This is the case the earlier doc-range strategy had to
     * exclude and this one does not.
     */
    public void testSortedIndexWithMissingValues() throws IOException {
        for (Width width : Width.values()) {
            try (Directory dir = newDirectory(); RandomIndexWriter w = sortedWriter(width, dir)) {
                int withValue = 0;
                for (int i = 0; i < 200; i++) {
                    Document doc = new Document();
                    // Roughly a third of the documents carry no value at all.
                    if (i % 3 != 0) {
                        width.addSortableField(doc, i);
                        withValue++;
                    }
                    w.addDocument(doc);
                }
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertStreamingApplies(searcher.getIndexReader());
                    long[] all = LongStream.range(0, 200).toArray();
                    assertThat(searcher.count(query(width, all)), equalTo(withValue));
                }
            }
        }
    }

    /**
     * Where documents missing a value are placed does not matter, because they carry no term: they can
     * only leave gaps in the doc ids a term's postings cover, never reorder them. Two documents with
     * values V1 &lt; V2 sort in that order whatever the missing value is, so {@code term(V1)}'s postings
     * still precede {@code term(V2)}'s.
     * <p>
     * The missing value here sits in the middle of the range the other documents span, so they
     * interleave rather than collecting at one end. {@code index.sort.missing} cannot express that — it
     * takes only {@code _first} and {@code _last} — but Lucene permits it, and it is the arrangement that
     * would expose the assumption if it were wrong.
     */
    public void testSortedIndexWithInterleavedMissingValues() throws IOException {
        for (Width width : Width.values()) {
            IndexWriterConfig config = newIndexWriterConfig();
            SortedNumericSortField sortField = new SortedNumericSortField(FIELD, width.sortType());
            sortField.setMissingValue(width.missingValue(100));
            config.setIndexSort(new Sort(sortField));
            try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir, config)) {
                int withValue = 0;
                for (int i = 0; i < 200; i++) {
                    Document doc = new Document();
                    if (i % 3 != 0) {
                        width.addSortableField(doc, i);
                        withValue++;
                    }
                    w.addDocument(doc);
                }
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertStreamingApplies(searcher.getIndexReader());
                    Query query = query(width, LongStream.range(0, 200).toArray());
                    assertThat("width=" + width, searcher.count(query), equalTo(withValue));
                    // Emitting doc ids out of order would fail the iterator contract here.
                    QueryUtils.check(random(), query, searcher);
                }
            }
        }
    }

    /**
     * Index sort places a multi-valued document by one of its values, so another of its values can belong
     * to a much later term while the document sits early. Streaming would then emit doc ids out of order,
     * so such a segment must collect instead. Asserted through {@link QueryUtils#check}, which verifies
     * the iterator contract that streaming out of order would break.
     */
    public void testMultiValuedFieldIsNotStreamed() throws IOException {
        for (Width width : Width.values()) {
            try (Directory dir = newDirectory(); RandomIndexWriter w = sortedWriter(width, dir)) {
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    width.addSortableField(doc, i);
                    // A second, far higher value on every document, so the terms order and the doc order
                    // disagree as widely as possible.
                    width.addSortableField(doc, 1000 + i);
                    w.addDocument(doc);
                }
                try (IndexReader reader = w.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    for (LeafReaderContext context : reader.leaves()) {
                        Terms terms = context.reader().terms(FIELD);
                        assertThat("expected multi-valued", terms.getSumDocFreq(), greaterThan((long) terms.getDocCount()));
                    }
                    Query query = query(width, 5, 1005, 40, 1040);
                    assertThat(searcher.count(query), equalTo(2));
                    QueryUtils.check(random(), query, searcher);
                }
            }
        }
    }

    /**
     * count() sums docFreq, which counts deleted documents too, so a segment with deletions must fall
     * back to counting by iteration.
     */
    public void testCountWithDeletions() throws IOException {
        for (Width width : Width.values()) {
            for (boolean sorted : new boolean[] { true, false }) {
                try (
                    Directory dir = newDirectory();
                    RandomIndexWriter w = sorted ? sortedWriter(width, dir) : new RandomIndexWriter(random(), dir)
                ) {
                    for (int i = 0; i < 200; i++) {
                        Document doc = new Document();
                        width.addSortableField(doc, i);
                        w.addDocument(doc);
                    }
                    w.deleteDocuments(new Term(FIELD, width.encodeTerm(100)));
                    try (IndexReader reader = w.getReader()) {
                        IndexSearcher searcher = newSearcher(reader);
                        // 150 values queried, one of them deleted.
                        assertThat(searcher.count(query(width, LongStream.range(0, 150).toArray())), equalTo(149));
                    }
                }
            }
        }
    }

    /** The docFreq count path needs no index sort, so it must agree with iteration on either layout. */
    public void testCountAgreesWithIteration() throws IOException {
        for (Width width : Width.values()) {
            for (boolean sorted : new boolean[] { true, false }) {
                int maxValue = 200;
                try (
                    Directory dir = newDirectory();
                    RandomIndexWriter w = sorted ? sortedWriter(width, dir) : new RandomIndexWriter(random(), dir)
                ) {
                    for (int i = 0; i < 400; i++) {
                        Document doc = new Document();
                        width.addSortableField(doc, randomIntBetween(0, maxValue));
                        w.addDocument(doc);
                    }
                    try (IndexReader reader = w.getReader()) {
                        IndexSearcher searcher = newSearcher(reader);
                        for (int iter = 0; iter < 10; iter++) {
                            Query query = query(width, randomQueriedValues(maxValue));
                            assertThat(
                                "width=" + width + " sorted=" + sorted,
                                searcher.count(query),
                                equalTo(matchedValues(searcher, query).size())
                            );
                        }
                    }
                }
            }
        }
    }

    /** Long-only: streaming must hold across high-32-bit bucket boundaries, not just within one. */
    public void testSortedIndexLongValuesBeyondIntRange() throws IOException {
        long[] indexed = { 0L, 1L, Integer.MAX_VALUE, 1L << 32, (1L << 32) + 1, 1L << 33, BEYOND_INT, Long.MAX_VALUE };
        try (Directory dir = newDirectory(); RandomIndexWriter w = sortedWriter(Width.LONG, dir)) {
            for (long value : indexed) {
                Document doc = new Document();
                Width.LONG.addSortableField(doc, value);
                w.addDocument(doc);
            }
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertStreamingApplies(searcher.getIndexReader());
                assertThat(searcher.count(query(Width.LONG, indexed)), equalTo(indexed.length));
                assertThat(searcher.count(query(Width.LONG, 1L << 32, Long.MAX_VALUE)), equalTo(2));
                assertThat(searcher.count(query(Width.LONG, (1L << 32) + 5)), equalTo(0));
                QueryUtils.check(random(), query(Width.LONG, indexed), searcher);
            }
        }
    }
}
