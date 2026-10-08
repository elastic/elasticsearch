/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.lucene.search.cost.TermsQueryCostEstimator;
import org.elasticsearch.search.internal.BreakerWeightTestUtils.CountingCollectorManager;
import org.elasticsearch.search.internal.BreakerWeightTestUtils.TrackingCircuitBreaker;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.internal.BreakerWeightTestUtils.chargeAgainst;
import static org.elasticsearch.search.internal.BreakerWeightTestUtils.conjunction;
import static org.elasticsearch.search.internal.BreakerWeightTestUtils.newContextIndexSearcher;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class MultiTermBreakerWeightTests extends ESTestCase {

    private static final String FIELD = "f";
    private static final int NUM_DOCS = 2000;
    private static final int NUM_TERMS = 500;

    /** Mirrors Lucene's package-private {@code AbstractMultiTermQueryConstantScoreWrapper#BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD}. */
    private static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;

    private static final String LEAD_FIELD = "lead";
    private static final String RARE_LEAD = "rare";
    private static final String COMMON_LEAD = "common";
    private static final int RARE_DOCS = 5;

    private static final int DOCS_PER_SEGMENT = 500;
    private static final int FEW_TERMS = 5;

    private Directory directory;
    private DirectoryReader reader;

    @Before
    public void initDirectoryAndReader() throws Exception {
        directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
            for (int docId = 0; docId < NUM_DOCS; docId++) {
                Document doc = new Document();
                doc.add(new StringField(FIELD, term(docId % NUM_TERMS), Field.Store.NO));
                doc.add(new SortedSetDocValuesField(FIELD, new BytesRef(term(docId % NUM_TERMS))));
                doc.add(new StringField(LEAD_FIELD, docId < RARE_DOCS ? RARE_LEAD : COMMON_LEAD, Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
    }

    @After
    public void closeDirectoryAndReader() throws Exception {
        IOUtils.close(reader, directory);
    }

    private static String term(int i) {
        return String.format(java.util.Locale.ROOT, "term-%04d", i);
    }

    private static Query termInSetQuery() {
        return termInSetQuery(NUM_TERMS);
    }

    private static Query termInSetQuery(int numTerms) {
        return new TermInSetQuery(FIELD, firstTerms(numTerms));
    }

    private static Query indexOrDocValuesQuery() {
        return TermInSetQuery.newIndexOrDocValuesQuery(MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, FIELD, firstTerms(NUM_TERMS));
    }

    private static List<BytesRef> firstTerms(int numTerms) {
        List<BytesRef> terms = new ArrayList<>();
        for (int i = 0; i < numTerms; i++) {
            terms.add(new BytesRef(term(i)));
        }
        return terms;
    }

    /** More terms than the boolean-rewrite threshold, but only a few of them are actually indexed. */
    private static Query fewPresentTermsQuery() {
        List<BytesRef> terms = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            terms.add(new BytesRef(term(i)));
            terms.add(new BytesRef("missing-" + i));
        }
        return new TermInSetQuery(FIELD, terms);
    }

    private static Query termRangeQuery() {
        return new TermRangeQuery(FIELD, new BytesRef(term(0)), new BytesRef(term(NUM_TERMS - 1)), true, true);
    }

    public void testTermInSetQueryChargesAndReleasesAcrossSearch() throws IOException {
        assertChargesThenReleases(termInSetQuery());
    }

    public void testTermRangeQueryChargesAndReleasesAcrossSearch() throws IOException {
        assertChargesThenReleases(termRangeQuery());
    }

    public void testIndexOrDocValuesChargesAndReleasesAcrossSearch() throws IOException {
        assertChargesThenReleases(indexOrDocValuesQuery());
    }

    public void testIndexOrDocValuesInConjunctionChargesWhenIndexBranchSelected() throws IOException {
        long expectedPeak = expectedSearchPerLeafCharges(indexOrDocValuesQuery()).stream().mapToLong(Long::longValue).max().orElse(0L);
        assertThat("test setup: the index branch must have a positive per-leaf execution charge", expectedPeak, greaterThan(0L));

        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        Query commonLead = new TermQuery(new Term(LEAD_FIELD, COMMON_LEAD));
        int hits = runSearch(conjunction(commonLead, indexOrDocValuesQuery()), breaker);
        assertThat("the conjunction must match documents so its scorer actually runs", hits, greaterThan(0));
        assertThat("an unselective lead routes to the multi-term index branch, charged once", breaker.peak(), equalTo(expectedPeak));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testIndexOrDocValuesInConjunctionSkipsChargeWhenDocValuesBranchSelected() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        Query rareLead = new TermQuery(new Term(LEAD_FIELD, RARE_LEAD));
        int hits = runSearch(conjunction(rareLead, indexOrDocValuesQuery()), breaker);
        assertThat("the selective lead clause must still match documents so the scorer runs", hits, greaterThan(0));
        assertThat("the doc-values branch allocates no result set, so nothing is charged", breaker.peak(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testConstantScoreWrappedTermInSetChargedExactlyOnce() throws IOException {
        assertChargesThenReleases(new ConstantScoreQuery(termInSetQuery()));
    }

    public void testBoostQueryWrappedTermInSetChargedExactlyOnce() throws IOException {
        Query boosted = new BoostQuery(termInSetQuery(), 2.0f);
        long expectedTotal = expectedDrivenCharge(boosted, ScoreMode.COMPLETE);
        assertThat("test setup: the boosted query must have a positive execution charge", expectedTotal, greaterThan(0L));

        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        Weight weight = searcher.createWeight(searcher.rewrite(boosted), ScoreMode.COMPLETE, 1.0f);
        for (LeafReaderContext leaf : reader.leaves()) {
            chargeAgainst(weight, leaf);
        }
        assertThat("a boosted multi-term query must be charged exactly once (no double-charge)", breaker.getUsed(), equalTo(expectedTotal));
        searcher.close();
        assertThat("closing the searcher must release the residual charge", breaker.getUsed(), equalTo(0L));
    }

    public void testExpensiveMultiTermQueryTripsBreakerWithoutLeaking() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(100L);
        expectThrows(CircuitBreakingException.class, () -> runSearch(termInSetQuery(), breaker));
        assertThat("a tripped reservation must not leak onto the breaker", breaker.getUsed(), equalTo(0L));
    }

    public void testChargedOnlyAboveBooleanRewriteThreshold() throws IOException {
        assertNeverCharges(termInSetQuery(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD));
        assertChargesThenReleases(termInSetQuery(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD + 1));
    }

    public void testFewTermsPresentInLeafChargesNothing() throws IOException {
        assertNeverCharges(fewPresentTermsQuery());
    }

    public void testFewTermsPresentInLeafChargesNothingWhenProfiled() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        searcher.setProfiler(new QueryProfiler());
        int hits = searcher.search(fewPresentTermsQuery(), new CountingCollectorManager());
        assertThat("the query must match documents so its scorer actually runs", hits, greaterThan(0));
        assertThat("profiling must not hide the query Lucene runs as a plain disjunction", breaker.peak(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testTermMatchingEveryDocChargesNothing() throws IOException {
        try (Directory sharedDirectory = newDirectory()) {
            int numDocs = 50;
            try (IndexWriter writer = new IndexWriter(sharedDirectory, new IndexWriterConfig(null))) {
                for (int docId = 0; docId < numDocs; docId++) {
                    Document doc = new Document();
                    // "common" is on every doc and sorts before "unique-*", so Lucene sees it within its first few terms.
                    doc.add(new StringField(FIELD, "common", Field.Store.NO));
                    doc.add(new StringField(FIELD, "unique-" + docId, Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader sharedReader = DirectoryReader.open(sharedDirectory)) {
                List<BytesRef> terms = new ArrayList<>();
                terms.add(new BytesRef("common"));
                for (int i = 0; i < 2 * BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD; i++) {
                    terms.add(new BytesRef("unique-" + i));
                }
                TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
                ContextIndexSearcher searcher = newContextIndexSearcher(sharedReader);
                searcher.setCircuitBreaker(breaker);
                int hits = searcher.search(new TermInSetQuery(FIELD, terms), new CountingCollectorManager());
                assertThat(hits, equalTo(numDocs));
                assertThat("a term matching every doc is run as a single term query, with no doc-id set", breaker.peak(), equalTo(0L));
                assertThat(breaker.getUsed(), equalTo(0L));
            }
        }
    }

    public void testEachLeafChargedAndReleasedOnItsOwn() throws Exception {
        withMultiSegmentReader(new int[] { NUM_TERMS, NUM_TERMS, NUM_TERMS }, multiSegmentReader -> {
            List<Long> expectedCharges = expectedSearchPerLeafCharges(multiSegmentReader, termInSetQuery());
            assertThat("test setup: every leaf must have a scorer", expectedCharges.size(), equalTo(multiSegmentReader.leaves().size()));

            TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
            int hits = BreakerWeightTestUtils.runSearch(multiSegmentReader, termInSetQuery(), breaker);
            assertThat(hits, equalTo(multiSegmentReader.maxDoc()));
            assertThat("every leaf is charged once, for its own doc-id set", breaker.charges(), equalTo(expectedCharges));
            assertThat(
                "a leaf's charge is released before the next leaf charges",
                breaker.peak(),
                equalTo(expectedCharges.stream().mapToLong(Long::longValue).max().orElseThrow())
            );
            assertThat(breaker.getUsed(), equalTo(0L));
        });
    }

    public void testSkipDecisionIsMadePerLeaf() throws Exception {
        withMultiSegmentReader(new int[] { NUM_TERMS, FEW_TERMS, NUM_TERMS }, multiSegmentReader -> {
            List<Long> perLeafCharges = expectedSearchPerLeafCharges(multiSegmentReader, termInSetQuery());
            List<Long> expectedCharges = List.of(perLeafCharges.get(0), perLeafCharges.get(2));

            TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
            int hits = BreakerWeightTestUtils.runSearch(multiSegmentReader, termInSetQuery(), breaker);
            assertThat(hits, equalTo(multiSegmentReader.maxDoc()));
            assertThat("the leaf holding only a few of the terms is run as a disjunction", breaker.charges(), equalTo(expectedCharges));
            assertThat(breaker.getUsed(), equalTo(0L));
        });
    }

    public void testAccountingNotAllocatedWithoutMultiTermQuery() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        int hits = searcher.search(new TermQuery(new Term(FIELD, term(0))), new CountingCollectorManager());
        assertThat("the term query must match documents so the search actually runs", hits, greaterThan(0));
        assertFalse(
            "a search whose query tree contains no costly multi-term query must not allocate accounting",
            searcher.hasLeafExecutionAccounting()
        );
    }

    public void testAccountingAllocatedLazilyForMultiTermQuery() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        assertFalse("accounting must not be allocated before any query has run", searcher.hasLeafExecutionAccounting());
        searcher.search(termInSetQuery(), new CountingCollectorManager());
        assertTrue("wrapping a costly multi-term query must lazily allocate accounting", searcher.hasLeafExecutionAccounting());
    }

    public void testNoCircuitBreakerConfiguredIsNoop() throws IOException {
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        int hits = searcher.search(termInSetQuery(), new CountingCollectorManager());
        assertThat("the query must still match documents when no breaker is configured", hits, greaterThan(0));
        assertFalse("no breaker means no accounting is allocated", searcher.hasLeafExecutionAccounting());
    }

    public void testOutOfBandChargeReleasedOnClose() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        searcher.setCircuitBreaker(breaker);
        Weight weight = searcher.createWeight(searcher.rewrite(termInSetQuery()), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        for (LeafReaderContext leaf : reader.leaves()) {
            chargeAgainst(weight, leaf);
        }
        assertThat("the out-of-band scorer must charge execution RAM", breaker.getUsed(), greaterThan(0L));
        searcher.close();
        assertThat("closing the searcher must release the residual out-of-band charge", breaker.getUsed(), equalTo(0L));
    }

    private void assertChargesThenReleases(Query query) throws IOException {
        long expectedPeak = expectedSearchPerLeafCharges(query).stream().mapToLong(Long::longValue).max().orElse(0L);
        assertThat("test setup: the query must have a positive per-leaf execution charge", expectedPeak, greaterThan(0L));

        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        int hits = runSearch(query, breaker);
        assertThat("the query must match documents so its scorer actually runs", hits, greaterThan(0));
        assertThat(
            "the multi-term scorer must charge exactly the per-leaf execution RAM once (no double-charge)",
            breaker.peak(),
            equalTo(expectedPeak)
        );
        assertThat("the per-leaf execution charge must be released once the leaf is scored", breaker.getUsed(), equalTo(0L));
    }

    private void assertNeverCharges(Query query) throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
        int hits = runSearch(query, breaker);
        assertThat("the query must match documents so its scorer actually runs", hits, greaterThan(0));
        assertThat("a leaf Lucene runs as a plain disjunction allocates no doc-id set", breaker.peak(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private List<Long> expectedSearchPerLeafCharges(Query query) throws IOException {
        return expectedSearchPerLeafCharges(reader, query);
    }

    private static List<Long> expectedSearchPerLeafCharges(IndexReader indexReader, Query query) throws IOException {
        ContextIndexSearcher searcher = newContextIndexSearcher(indexReader);
        Weight weight = searcher.createWeight(searcher.rewrite(new ConstantScoreQuery(query)), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        List<Long> charges = new ArrayList<>();
        for (LeafReaderContext leaf : indexReader.leaves()) {
            ScorerSupplier scorerSupplier = weight.scorerSupplier(leaf);
            if (scorerSupplier != null) {
                charges.add(TermsQueryCostEstimator.executionBytesForLeaf(scorerSupplier.cost(), leaf.reader().maxDoc()));
            }
        }
        return charges;
    }

    private long expectedDrivenCharge(Query query, ScoreMode scoreMode) throws IOException {
        ContextIndexSearcher searcher = newContextIndexSearcher(reader);
        Weight weight = searcher.createWeight(searcher.rewrite(query), scoreMode, 1.0f);
        long total = 0L;
        for (LeafReaderContext leaf : reader.leaves()) {
            ScorerSupplier scorerSupplier = weight.scorerSupplier(leaf);
            if (scorerSupplier != null) {
                total += TermsQueryCostEstimator.executionBytesForLeaf(scorerSupplier.cost(), leaf.reader().maxDoc());
            }
        }
        return total;
    }

    private int runSearch(Query query, CircuitBreaker breaker) throws IOException {
        return BreakerWeightTestUtils.runSearch(reader, query, breaker);
    }

    /**
     * Opens an index with one segment per entry of {@code termsPerSegment}: segment {@code i} holds
     * {@link #DOCS_PER_SEGMENT} docs spread over the first {@code termsPerSegment[i]} terms.
     */
    private static void withMultiSegmentReader(int[] termsPerSegment, CheckedConsumer<DirectoryReader, Exception> body) throws Exception {
        try (Directory multiSegmentDirectory = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig(null).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(multiSegmentDirectory, config)) {
                for (int numTerms : termsPerSegment) {
                    for (int docId = 0; docId < DOCS_PER_SEGMENT; docId++) {
                        Document doc = new Document();
                        doc.add(new StringField(FIELD, term(docId % numTerms), Field.Store.NO));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (DirectoryReader multiSegmentReader = DirectoryReader.open(multiSegmentDirectory)) {
                assertThat(
                    "the fixture must produce one leaf per segment to exercise cross-leaf behaviour",
                    multiSegmentReader.leaves().size(),
                    equalTo(termsPerSegment.length)
                );
                body.accept(multiSegmentReader);
            }
        }
    }
}
