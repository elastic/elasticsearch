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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TopDocs;
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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
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
        };

        abstract BitmapValues bitmapOf(long... valuesIn);

        abstract BytesRef encodeTerm(long value);

        void addField(Document doc, long value) {
            doc.add(new Field(FIELD, encodeTerm(value), INDEX_TERMS_TYPE));
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
}
