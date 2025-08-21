/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.FilterScorable;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreCachingWrappingScorer;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.DummyTotalHitCountCollector;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.search.query.QueryPhaseCollector.resolveTerminateAfterChecker;

public class QueryPhaseCollectorTests extends ESTestCase {
    private static Directory directory;
    private static IndexReader reader;
    private static IndexSearcher searcher;
    private static int numDocs;
    private static int numField2Docs;
    private static int numField3Docs;
    private static int numField2AndField3Docs;

    @BeforeClass
    public static void beforeClass() throws Exception {
        directory = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig());
        numDocs = randomIntBetween(900, 1000);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("field1", "value", Field.Store.NO));
            boolean field2 = randomBoolean();
            if (field2) {
                doc.add(new StringField("field2", "value", Field.Store.NO));
                numField2Docs++;
            }
            boolean field3 = randomBoolean();
            if (field3) {
                doc.add(new StringField("field3", "value", Field.Store.NO));
                numField3Docs++;
            }
            if (field2 && field3) {
                numField2AndField3Docs++;
            }
            writer.addDocument(doc);
        }
        writer.flush();
        reader = writer.getReader();
        searcher = newSearcher(reader);
        writer.close();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        IOUtils.close(reader, directory);
        searcher = null;
        reader = null;
        directory = null;
    }

    public void testNullTopDocsCollector() {
        expectThrows(NullPointerException.class, () -> new QueryPhaseCollector(null, null, resolveTerminateAfterChecker(0), null, null));
    }

    public void testNegativeTerminateAfter() {
        expectThrows(IllegalArgumentException.class, () -> resolveTerminateAfterChecker(randomIntBetween(Integer.MIN_VALUE, -1)));
    }

    public void testTopDocsOnly() throws IOException {
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topScoreDocManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Void>> manager = createCollectorManager(
                topScoreDocManager,
                null,
                0,
                null,
                null
            );
            Result<TopDocs, Void> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numDocs, result.topDocs.totalHits.value());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topScoreDocManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Void>> manager = createCollectorManager(
                topScoreDocManager,
                null,
                0,
                null,
                null
            );
            Result<TopDocs, Void> result = searcher.search(new TermQuery(new Term("field2", "value")), manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numField2Docs, result.topDocs.totalHits.value());
        }
    }

    public void testWithAggs() throws IOException {
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Integer>> manager = createCollectorManager(
                topDocsManager,
                null,
                0,
                aggsManager,
                null
            );
            Result<TopDocs, Integer> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numDocs, result.topDocs.totalHits.value());
            assertEquals(numDocs, result.aggs.intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Integer>> manager = createCollectorManager(
                topDocsManager,
                null,
                0,
                aggsManager,
                null
            );
            Result<TopDocs, Integer> result = searcher.search(new TermQuery(new Term("field2", "value")), manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numField2Docs, result.topDocs.totalHits.value());
            assertEquals(numField2Docs, result.aggs.intValue());
        }
    }

    public void testPostFilterTopDocsOnly() throws IOException {
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            TermQuery termQuery = new TermQuery(new Term("field2", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Void>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                0,
                null,
                null
            );
            Result<TopDocs, Void> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numField2Docs, result.topDocs.totalHits.value());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            TermQuery termQuery = new TermQuery(new Term("field1", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Void>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                0,
                null,
                null
            );
            Result<TopDocs, Void> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numDocs, result.topDocs.totalHits.value());
        }
    }

    public void testPostFilterWithAggs() throws IOException {
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            TermQuery termQuery = new TermQuery(new Term("field1", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Integer>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                0,
                aggsManager,
                null
            );
            Result<TopDocs, Integer> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numDocs, result.topDocs.totalHits.value());
            assertEquals(numDocs, result.aggs.intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            TermQuery termQuery = new TermQuery(new Term("field2", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Integer>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                0,
                aggsManager,
                null
            );
            Result<TopDocs, Integer> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numField2Docs, result.topDocs.totalHits.value());
            // post_filter is not applied to aggs
            assertEquals(reader.maxDoc(), result.aggs.intValue());
        }
    }

    public void testMinScoreTopDocsOnly() throws IOException {
        searcher.setSimilarity(new BM25Similarity());
        float maxScore;
        float thresholdScore;
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("field1", "value")), BooleanClause.Occur.MUST)
            .add(new BoostQuery(new TermQuery(new Term("field2", "value")), 200f), BooleanClause.Occur.SHOULD)
            .build();
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(numField2Docs + 1, null, 1000);
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value());
            maxScore = topDocs.scoreDocs[0].score;
            thresholdScore = topDocs.scoreDocs[numField2Docs].score;
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Void>> manager = createCollectorManager(
                topDocsManager,
                null,
                0,
                null,
                maxScore
            );
            Result<TopDocs, Void> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numField2Docs, result.topDocs.totalHits.value());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Void>> manager = createCollectorManager(
                topDocsManager,
                null,
                0,
                null,
                thresholdScore
            );
            Result<TopDocs, Void> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numDocs, result.topDocs.totalHits.value());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Void>> manager = createCollectorManager(
                topDocsManager,
                null,
                0,
                null,
                maxScore + 100f
            );
            Result<TopDocs, Void> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter);
            assertEquals(0, result.topDocs.totalHits.value());
        }
    }

    public void testMinScoreWithAggs() throws IOException {
        searcher.setSimilarity(new BM25Similarity());
        float maxScore;
        float thresholdScore;
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("field1", "value")), BooleanClause.Occur.MUST)
            .add(new BoostQuery(new TermQuery(new Term("field2", "value")), 200f), BooleanClause.Occur.SHOULD)
            .build();
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(numField2Docs + 1, null, 1000);
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value());
            maxScore = topDocs.scoreDocs[0].score;
            thresholdScore = topDocs.scoreDocs[numField2Docs].score;
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Integer>> manager = createCollectorManager(
                topDocsManager,
                null,
                0,
                aggsManager,
                maxScore
            );
            Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numField2Docs, result.topDocs.totalHits.value());
            // min_score is applied to aggs as well as top docs
            assertEquals(numField2Docs, result.aggs.intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Integer>> manager = createCollectorManager(
                topDocsManager,
                null,
                0,
                aggsManager,
                thresholdScore
            );
            Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numDocs, result.topDocs.totalHits.value());
            assertEquals(numDocs, result.aggs.intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Integer>> manager = createCollectorManager(
                topDocsManager,
                null,
                0,
                aggsManager,
                maxScore + 100f
            );
            Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter);
            assertEquals(0, result.topDocs.totalHits.value());
            assertEquals(0, result.aggs.intValue());
        }
    }

    public void testPostFilterAndMinScoreTopDocsOnly() throws IOException {
        searcher.setSimilarity(new BM25Similarity());
        float maxScore;
        float thresholdScore;
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("field1", "value")), BooleanClause.Occur.MUST)
            .add(new BoostQuery(new TermQuery(new Term("field3", "value")), 200f), BooleanClause.Occur.SHOULD)
            .build();
        TermQuery termQuery = new TermQuery(new Term("field2", "value"));
        Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(numField3Docs + 1, null, 1000);
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value());
            maxScore = topDocs.scoreDocs[0].score;
            thresholdScore = topDocs.scoreDocs[numField3Docs].score;
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Void>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                0,
                null,
                maxScore
            );
            Result<TopDocs, Void> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numField2AndField3Docs, result.topDocs.totalHits.value());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Void>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                0,
                null,
                thresholdScore
            );
            Result<TopDocs, Void> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numField2Docs, result.topDocs.totalHits.value());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Void>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                0,
                null,
                maxScore + 100f
            );
            Result<TopDocs, Void> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter);
            assertEquals(0, result.topDocs.totalHits.value());
        }
    }

    public void testPostFilterAndMinScoreWithAggs() throws IOException {
        searcher.setSimilarity(new BM25Similarity());
        float maxScore;
        float thresholdScore;
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("field1", "value")), BooleanClause.Occur.MUST)
            .add(new BoostQuery(new TermQuery(new Term("field3", "value")), 200f), BooleanClause.Occur.SHOULD)
            .build();
        TermQuery termQuery = new TermQuery(new Term("field2", "value"));
        Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(numField3Docs + 1, null, 1000);
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value());
            maxScore = topDocs.scoreDocs[0].score;
            thresholdScore = topDocs.scoreDocs[numField3Docs].score;
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Integer>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                0,
                aggsManager,
                maxScore
            );
            Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numField2AndField3Docs, result.topDocs.totalHits.value());
            assertEquals(numField3Docs, result.aggs.intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Integer>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                0,
                aggsManager,
                thresholdScore
            );
            Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numField2Docs, result.topDocs.totalHits.value());
            assertEquals(numDocs, result.aggs.intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Integer>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                0,
                aggsManager,
                maxScore + 100f
            );
            Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter);
            assertEquals(0, result.topDocs.totalHits.value());
            assertEquals(0, result.aggs.intValue());
        }
    }

    public void testTerminateAfterTopDocsOnly() throws IOException {
        {
            int terminateAfter = randomIntBetween(1, numDocs - 1);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<Integer, Void>> manager = createCollectorManager(
                topDocsManager,
                null,
                terminateAfter,
                null,
                null
            );
            Result<Integer, Void> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertTrue(result.terminatedAfter);
            assertEquals(terminateAfter, result.topDocs.intValue());
        }
        {
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<Integer, Void>> manager = createCollectorManager(
                topDocsManager,
                null,
                numDocs,
                null,
                null
            );
            Result<Integer, Void> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numDocs, result.topDocs.intValue());
        }
    }

    public void testTerminateAfterWithAggs() throws IOException {
        {
            int terminateAfter = randomIntBetween(1, numDocs - 1);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<Integer, Integer>> manager = createCollectorManager(
                topDocsManager,
                null,
                terminateAfter,
                aggsManager,
                null
            );
            Result<Integer, Integer> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertTrue(result.terminatedAfter);
            assertEquals(terminateAfter, result.topDocs.intValue());
            assertEquals(terminateAfter, result.aggs.intValue());
        }
        {
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<Integer, Integer>> manager = createCollectorManager(
                topDocsManager,
                null,
                numDocs,
                aggsManager,
                null
            );
            Result<Integer, Integer> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numDocs, result.topDocs.intValue());
            assertEquals(numDocs, result.aggs.intValue());
        }
    }

    public void testTerminateAfterTopDocsOnlyWithPostFilter() throws IOException {
        TermQuery termQuery = new TermQuery(new Term("field2", "value"));
        Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
        {
            int terminateAfter = randomIntBetween(1, numField2Docs - 1);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<Integer, Void>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                terminateAfter,
                null,
                null
            );
            Result<Integer, Void> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertTrue(result.terminatedAfter);
            assertEquals(terminateAfter, result.topDocs.intValue());
        }
        {
            int terminateAfter = randomIntBetween(numField2Docs, Integer.MAX_VALUE);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<Integer, Void>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                terminateAfter,
                null,
                null
            );
            Result<Integer, Void> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numField2Docs, result.topDocs.intValue());
        }
    }

    public void testTerminateAfterWithAggsAndPostFilter() throws IOException {
        TermQuery termQuery = new TermQuery(new Term("field2", "value"));
        Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
        {
            int terminateAfter = randomIntBetween(1, numField2Docs - 1);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<Integer, Integer>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                terminateAfter,
                aggsManager,
                null
            );
            Result<Integer, Integer> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertTrue(result.terminatedAfter);
            assertEquals(terminateAfter, result.topDocs.intValue());
            // aggs see more docs because they are not filtered
            assertThat(result.aggs, Matchers.greaterThanOrEqualTo(terminateAfter));
        }
        {
            int terminateAfter = randomIntBetween(numField2Docs, Integer.MAX_VALUE);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<Integer, Integer>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                terminateAfter,
                aggsManager,
                null
            );
            Result<Integer, Integer> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter);
            assertEquals(numField2Docs, result.topDocs.intValue());
            // aggs see more docs because they are not filtered
            assertThat(result.aggs, Matchers.greaterThanOrEqualTo(numField2Docs));
        }
    }

    public void testTerminateAfterTopDocsOnlyWithMinScore() throws IOException {
        searcher.setSimilarity(new BM25Similarity());
        float maxScore;
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("field1", "value")), BooleanClause.Occur.MUST)
            .add(new BoostQuery(new TermQuery(new Term("field2", "value")), 200f), BooleanClause.Occur.SHOULD)
            .build();
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(numField2Docs + 1, null, 1000);
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value());
            maxScore = topDocs.scoreDocs[0].score;
        }
        {
            int terminateAfter = randomIntBetween(1, numField2Docs - 1);
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Void>> manager = createCollectorManager(
                topDocsManager,
                null,
                terminateAfter,
                null,
                maxScore
            );
            Result<TopDocs, Void> result = searcher.search(booleanQuery, manager);
            assertTrue(result.terminatedAfter);
            assertEquals(terminateAfter, result.topDocs.totalHits.value());
        }
    }

    public void testTerminateAfterWithAggsAndMinScore() throws IOException {
        searcher.setSimilarity(new BM25Similarity());
        float maxScore;
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("field1", "value")), BooleanClause.Occur.MUST)
            .add(new BoostQuery(new TermQuery(new Term("field2", "value")), 200f), BooleanClause.Occur.SHOULD)
            .build();
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(numField2Docs + 1, null, 1000);
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value());
            maxScore = topDocs.scoreDocs[0].score;
        }
        {
            int terminateAfter = randomIntBetween(1, numField2Docs - 1);
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Integer>> manager = createCollectorManager(
                topDocsManager,
                null,
                terminateAfter,
                aggsManager,
                maxScore
            );
            Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertTrue(result.terminatedAfter);
            assertEquals(terminateAfter, result.topDocs.totalHits.value());
            assertEquals(terminateAfter, result.aggs.intValue());
        }
    }

    public void testTerminateAfterAndPostFilterAndMinScoreTopDocsOnly() throws IOException {
        searcher.setSimilarity(new BM25Similarity());
        float maxScore;
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("field1", "value")), BooleanClause.Occur.MUST)
            .add(new BoostQuery(new TermQuery(new Term("field3", "value")), 200f), BooleanClause.Occur.SHOULD)
            .build();
        TermQuery termQuery = new TermQuery(new Term("field2", "value"));
        Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(numField3Docs + 1, null, 1000);
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value());
            maxScore = topDocs.scoreDocs[0].score;
        }
        {
            int terminateAfter = randomIntBetween(1, numField2AndField3Docs - 1);
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Void>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                terminateAfter,
                null,
                maxScore
            );
            Result<TopDocs, Void> result = searcher.search(booleanQuery, manager);
            assertTrue(result.terminatedAfter);
            assertEquals(terminateAfter, result.topDocs.totalHits.value());
        }
    }

    public void testTerminateAfterAndPostFilterAndMinScoreWithAggs() throws IOException {
        searcher.setSimilarity(new BM25Similarity());
        float maxScore;
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("field1", "value")), BooleanClause.Occur.MUST)
            .add(new BoostQuery(new TermQuery(new Term("field3", "value")), 200f), BooleanClause.Occur.SHOULD)
            .build();
        TermQuery termQuery = new TermQuery(new Term("field2", "value"));
        Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(numField3Docs + 1, null, 1000);
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value());
            maxScore = topDocs.scoreDocs[0].score;
        }
        {
            int terminateAfter = randomIntBetween(1, numField2AndField3Docs - 1);
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = new TopScoreDocCollectorManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<QueryPhaseCollector, Result<TopDocs, Integer>> manager = createCollectorManager(
                topDocsManager,
                filterWeight,
                terminateAfter,
                aggsManager,
                maxScore
            );
            Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertTrue(result.terminatedAfter);
            assertEquals(terminateAfter, result.topDocs.totalHits.value());
            // aggs see more documents because the filter is not applied to them
            assertThat(result.aggs, Matchers.greaterThanOrEqualTo(terminateAfter));
        }
    }

    public void testScoreModeTopDocsOnly() throws IOException {
        ScoreMode scoreMode = randomFrom(ScoreMode.values());
        Weight weight = randomBoolean() ? searcher.createWeight(new MatchAllDocsQuery(), ScoreMode.COMPLETE, 1.0f) : null;
        int terminateAfter = randomBoolean() ? 0 : randomIntBetween(1, Integer.MAX_VALUE);
        QueryPhaseCollector qpc = new QueryPhaseCollector(
            new MockCollector(scoreMode),
            weight,
            resolveTerminateAfterChecker(terminateAfter),
            null,
            null
        );
        assertEquals(scoreMode, qpc.scoreMode());
    }

    public void testScoreModeTopDocsOnlyWithMinScore() throws IOException {
        Weight weight = randomBoolean() ? searcher.createWeight(new MatchAllDocsQuery(), ScoreMode.COMPLETE, 1.0f) : null;
        int terminateAfter = randomBoolean() ? 0 : randomIntBetween(1, Integer.MAX_VALUE);
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                new MockCollector(ScoreMode.TOP_SCORES),
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                null,
                100f
            );
            assertEquals(ScoreMode.TOP_SCORES, qpc.scoreMode());
        }
        {
            ScoreMode scoreMode = randomScoreModeExceptTopScores();
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                new MockCollector(scoreMode),
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                null,
                100f
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
    }

    public void testScoreModeWithAggsSameScoreMode() throws IOException {
        Weight weight = randomBoolean() ? searcher.createWeight(new MatchAllDocsQuery(), ScoreMode.COMPLETE, 1.0f) : null;
        int terminateAfter = randomBoolean() ? 0 : randomIntBetween(1, Integer.MAX_VALUE);
        ScoreMode scoreMode = randomScoreModeExceptTopScores();
        QueryPhaseCollector qpc = new QueryPhaseCollector(
            new MockCollector(scoreMode),
            weight,
            resolveTerminateAfterChecker(terminateAfter),
            new MockCollector(scoreMode),
            null
        );
        assertEquals(scoreMode, qpc.scoreMode());
    }

    public void testScoreModeWithAggsSameScoreModeWithMinScore() throws IOException {
        Weight weight = randomBoolean() ? searcher.createWeight(new MatchAllDocsQuery(), ScoreMode.COMPLETE, 1.0f) : null;
        int terminateAfter = randomBoolean() ? 0 : randomIntBetween(1, Integer.MAX_VALUE);
        ScoreMode scoreMode = randomScoreModeExceptTopScores();
        QueryPhaseCollector qpc = new QueryPhaseCollector(
            new MockCollector(scoreMode),
            weight,
            resolveTerminateAfterChecker(terminateAfter),
            new MockCollector(scoreMode),
            100f
        );
        assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
    }

    public void testScoreModeWithAggsExhaustive() throws IOException {
        Weight weight = randomBoolean() ? searcher.createWeight(new MatchAllDocsQuery(), ScoreMode.COMPLETE, 1.0f) : null;
        int terminateAfter = randomBoolean() ? 0 : randomIntBetween(1, Integer.MAX_VALUE);
        Float minScore = randomBoolean() ? 100f : null;
        Collector complete = new MockCollector(ScoreMode.COMPLETE);
        Collector completeNoScores = new MockCollector(ScoreMode.COMPLETE_NO_SCORES);
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                complete,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                completeNoScores,
                minScore
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                completeNoScores,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                complete,
                minScore
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                complete,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.TOP_DOCS),
                minScore
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                complete,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.TOP_DOCS_WITH_SCORES),
                minScore
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                completeNoScores,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.TOP_DOCS_WITH_SCORES),
                minScore
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                completeNoScores,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.TOP_DOCS),
                null
            );
            assertEquals(ScoreMode.COMPLETE_NO_SCORES, qpc.scoreMode());
        }
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                completeNoScores,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.TOP_DOCS),
                100f
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
    }

    public void testScoreModeWithAggsTopScores() throws IOException {
        Weight weight = randomBoolean() ? searcher.createWeight(new MatchAllDocsQuery(), ScoreMode.COMPLETE, 1.0f) : null;
        int terminateAfter = randomBoolean() ? 0 : randomIntBetween(1, Integer.MAX_VALUE);
        Float minScore = randomBoolean() ? 100f : null;
        Collector topScores = new MockCollector(ScoreMode.TOP_SCORES);
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                topScores,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.COMPLETE),
                minScore
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                topScores,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.COMPLETE_NO_SCORES),
                minScore
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                topScores,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.TOP_DOCS),
                minScore
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                topScores,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.TOP_DOCS_WITH_SCORES),
                minScore
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
    }

    public void testScoreModeWithAggsTopDocs() throws IOException {
        Weight weight = randomBoolean() ? searcher.createWeight(new MatchAllDocsQuery(), ScoreMode.COMPLETE, 1.0f) : null;
        int terminateAfter = randomBoolean() ? 0 : randomIntBetween(1, Integer.MAX_VALUE);
        Float minScore = randomBoolean() ? 100f : null;
        Collector topDocs = new MockCollector(ScoreMode.TOP_DOCS);
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                topDocs,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.COMPLETE),
                minScore
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                topDocs,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.TOP_DOCS_WITH_SCORES),
                minScore
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                topDocs,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.COMPLETE_NO_SCORES),
                null
            );
            assertEquals(ScoreMode.COMPLETE_NO_SCORES, qpc.scoreMode());
        }
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                topDocs,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.COMPLETE_NO_SCORES),
                100f
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
    }

    public void testScoreModeWithAggsTopDocsWithScores() throws IOException {
        Weight weight = randomBoolean() ? searcher.createWeight(new MatchAllDocsQuery(), ScoreMode.COMPLETE, 1.0f) : null;
        int terminateAfter = randomBoolean() ? 0 : randomIntBetween(1, Integer.MAX_VALUE);
        Float minScore = randomBoolean() ? 100f : null;
        Collector topDocsWithScores = new MockCollector(ScoreMode.TOP_DOCS_WITH_SCORES);
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                topDocsWithScores,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.COMPLETE),
                minScore
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                topDocsWithScores,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.COMPLETE_NO_SCORES),
                minScore
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
        {
            QueryPhaseCollector qpc = new QueryPhaseCollector(
                topDocsWithScores,
                weight,
                resolveTerminateAfterChecker(terminateAfter),
                new MockCollector(ScoreMode.TOP_DOCS),
                minScore
            );
            assertEquals(ScoreMode.COMPLETE, qpc.scoreMode());
        }
    }

    public void testWeightIsPropagatedTopDocsOnly() throws IOException {
        MockCollector topDocsCollector = new MockCollector(randomFrom(ScoreMode.values()));
        QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(
            topDocsCollector,
            null,
            resolveTerminateAfterChecker(0),
            null,
            null
        );
        searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
        assertTrue(topDocsCollector.setWeightCalled);
    }

    public void testWeightIsPropagatedWithAggs() throws IOException {
        MockCollector topDocsCollector = new MockCollector(randomFrom(ScoreMode.values()));
        MockCollector aggsCollector = new MockCollector(randomScoreModeExceptTopScores());
        QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(
            topDocsCollector,
            null,
            resolveTerminateAfterChecker(0),
            aggsCollector,
            null
        );
        searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
        assertTrue(topDocsCollector.setWeightCalled);
        assertTrue(aggsCollector.setWeightCalled);
    }

    public void testWeightPropagationWithPostFilterTopDocsOnly() throws IOException {
        // the weight is not propagated because docs collection is filtered
        MockCollector mockCollector = new MockCollector(randomFrom(ScoreMode.values()));
        TermQuery termQuery = new TermQuery(new Term("field2", "value"));
        Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
        QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(
            mockCollector,
            filterWeight,
            resolveTerminateAfterChecker(0),
            null,
            null
        );
        searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
        assertFalse(mockCollector.setWeightCalled);
    }

    public void testWeightPropagationWithPostFilterWithAggs() throws IOException {
        // the weight is propagated only to the aggs collector because docs collection is filtered
        MockCollector topDocsCollector = new MockCollector(randomFrom(ScoreMode.values()));
        MockCollector aggsCollector = new MockCollector(randomScoreModeExceptTopScores());
        TermQuery termQuery = new TermQuery(new Term("field2", "value"));
        Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
        QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(
            topDocsCollector,
            filterWeight,
            resolveTerminateAfterChecker(0),
            aggsCollector,
            null
        );
        searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
        assertFalse(topDocsCollector.setWeightCalled);
        assertTrue(aggsCollector.setWeightCalled);
    }

    public void testWeightPropagationWithMinScoreTopDocsOnly() throws IOException {
        // the weight is not propagated to the top docs collector
        MockCollector topDocsCollector = new MockCollector(randomFrom(ScoreMode.values()));
        QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(
            topDocsCollector,
            null,
            resolveTerminateAfterChecker(0),
            null,
            100f
        );
        searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
        assertFalse(queryPhaseCollector.isTerminatedAfter());
        assertFalse(topDocsCollector.setWeightCalled);
    }

    public void testWeightPropagationWithMinScoreWithAggs() throws IOException {
        // the weight is not propagated to either of the collectors
        MockCollector topDocsCollector = new MockCollector(randomFrom(ScoreMode.values()));
        MockCollector aggsCollector = new MockCollector(randomScoreModeExceptTopScores());
        QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(
            topDocsCollector,
            null,
            resolveTerminateAfterChecker(0),
            aggsCollector,
            100f
        );
        searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
        assertFalse(queryPhaseCollector.isTerminatedAfter());
        assertFalse(topDocsCollector.setWeightCalled);
        assertFalse(aggsCollector.setWeightCalled);
    }

    public void testCollectionTerminatedExceptionHandling() throws IOException {
        final int terminateAfter1 = random().nextInt(numDocs + 10);
        final int expectedCount1 = Math.min(terminateAfter1, numDocs);
        DummyTotalHitCountCollector collector1 = new DummyTotalHitCountCollector();

        final int terminateAfter2 = random().nextInt(numDocs + 10);
        final int expectedCount2 = Math.min(terminateAfter2, numDocs);
        DummyTotalHitCountCollector collector2 = new DummyTotalHitCountCollector();

        QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(
            new TerminateAfterCollector(collector1, terminateAfter1),
            null,
            resolveTerminateAfterChecker(0),
            new TerminateAfterCollector(collector2, terminateAfter2),
            null
        );
        searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
        assertEquals(expectedCount1, collector1.getTotalHits());
        assertEquals(expectedCount2, collector2.getTotalHits());
    }

    public void testSetScorerAfterCollectionTerminated() throws IOException {
        MockCollector mockCollector1 = new MockCollector(randomFrom(ScoreMode.values()));
        Collector collector1 = new TerminateAfterCollector(mockCollector1, 1);

        MockCollector mockCollector2 = new MockCollector(randomScoreModeExceptTopScores());
        Collector collector2 = new TerminateAfterCollector(mockCollector2, 2);

        Scorable scorer = new Scorable() {
            @Override
            public float score() {
                return 0;
            }
        };

        QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(
            collector1,
            null,
            resolveTerminateAfterChecker(0),
            collector2,
            null
        );

        LeafCollector leafCollector = queryPhaseCollector.getLeafCollector(null);
        leafCollector.setScorer(scorer);
        assertTrue(mockCollector1.setScorerCalled);
        assertTrue(mockCollector2.setScorerCalled);

        leafCollector.collect(0);
        leafCollector.collect(1);

        mockCollector1.setScorerCalled = false;
        mockCollector2.setScorerCalled = false;
        leafCollector.setScorer(scorer);
        assertFalse(mockCollector1.setScorerCalled);
        assertTrue(mockCollector2.setScorerCalled);

        expectThrows(CollectionTerminatedException.class, () -> leafCollector.collect(1));

        mockCollector1.setScorerCalled = false;
        mockCollector2.setScorerCalled = false;
        leafCollector.setScorer(scorer);
        assertFalse(mockCollector1.setScorerCalled);
        assertFalse(mockCollector2.setScorerCalled);
    }

    public void testSetMinCompetitiveScoreIsEnabledTopDocsOnly() throws IOException {
        // without aggs no need to disable set min competitive score
        Weight filterWeight = null;
        int terminateAfter = 0;
        Float minScore = null;
        if (randomBoolean()) {
            if (randomBoolean()) {
                filterWeight = new MatchAllDocsQuery().createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            }
            if (randomBoolean()) {
                terminateAfter = randomIntBetween(1, Integer.MAX_VALUE);
            }
            if (randomBoolean()) {
                minScore = 0f;
            }
        }
        TopScoresCollector topDocs = new TopScoresCollector();
        Collector queryPhaseCollector = new QueryPhaseCollector(
            topDocs,
            filterWeight,
            resolveTerminateAfterChecker(terminateAfter),
            null,
            minScore
        );
        LeafReaderContext leafReaderContext = searcher.getLeafContexts().get(0);
        LeafCollector leafCollector = queryPhaseCollector.getLeafCollector(leafReaderContext);
        MinCompetitiveScoreScorable scorer = new MinCompetitiveScoreScorable();
        leafCollector.setScorer(scorer);
        leafCollector.collect(0);
        assertTrue(scorer.setMinCompetitiveScoreCalled);
    }

    public void testSetMinCompetitiveScoreIsDisabledWithAggs() throws IOException {
        Weight filterWeight = null;
        int terminateAfter = 0;
        Float minScore = null;
        if (randomBoolean()) {
            if (randomBoolean()) {
                TermQuery termQuery = new TermQuery(new Term("field2", "value"));
                filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            }
            if (randomBoolean()) {
                terminateAfter = randomIntBetween(1, Integer.MAX_VALUE);
            }
            if (randomBoolean()) {
                minScore = randomFloat();
            }
        }
        TopScoresCollector topDocs = new TopScoresCollector();
        Collector aggs = new MockCollector(randomBoolean() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES);
        Collector queryPhaseCollector = new QueryPhaseCollector(
            topDocs,
            filterWeight,
            resolveTerminateAfterChecker(terminateAfter),
            aggs,
            minScore
        );
        LeafReaderContext leafReaderContext = searcher.getLeafContexts().get(0);
        LeafCollector leafCollector = queryPhaseCollector.getLeafCollector(leafReaderContext);
        MinCompetitiveScoreScorable scorer = new MinCompetitiveScoreScorable();
        leafCollector.setScorer(scorer);
        leafCollector.collect(0);
        assertFalse(scorer.setMinCompetitiveScoreCalled);
    }

    public void testSetMinCompetitiveScoreIsDisabledWithEarlyTerminatedAggs() throws IOException {
        // aggs don't support top_scores: even if their collection terminated, we can't skip low scoring hits despite the
        // top docs collector may support it, because the top-level score mode wasn't TOP_SCORES
        TopScoresCollector topDocs = new TopScoresCollector();
        Collector aggs = new TerminateAfterCollector(new DummyTotalHitCountCollector(), 0);
        Collector queryPhaseCollector = new QueryPhaseCollector(topDocs, null, resolveTerminateAfterChecker(0), aggs, null);
        LeafReaderContext leafReaderContext = searcher.getLeafContexts().get(0);
        LeafCollector leafCollector = queryPhaseCollector.getLeafCollector(leafReaderContext);
        MinCompetitiveScoreScorable scorer = new MinCompetitiveScoreScorable();
        leafCollector.setScorer(scorer);
        leafCollector.collect(0);
        assertFalse(scorer.setMinCompetitiveScoreCalled);
    }

    public void testCacheScoresIfNecessary() throws IOException {
        final LeafReaderContext ctx = searcher.getLeafContexts().get(0);
        {
            // single collector => no caching
            Collector c1 = new MockCollector(ScoreMode.COMPLETE, MinCompetitiveScoreScorable.class);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(c1, null, resolveTerminateAfterChecker(0), null, null);
            queryPhaseCollector.getLeafCollector(ctx).setScorer(new MinCompetitiveScoreScorable());
        }
        {
            // no collector needs scores => no caching
            Collector c1 = new MockCollector(ScoreMode.COMPLETE_NO_SCORES, MinCompetitiveScoreScorable.class);
            Collector c2 = new MockCollector(ScoreMode.COMPLETE_NO_SCORES, MinCompetitiveScoreScorable.class);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(c1, null, resolveTerminateAfterChecker(0), c2, null);
            queryPhaseCollector.getLeafCollector(ctx).setScorer(new MinCompetitiveScoreScorable());
        }
        {
            // only one collector needs scores => no caching
            Collector c1 = new MockCollector(ScoreMode.COMPLETE, MinCompetitiveScoreScorable.class);
            Collector c2 = new MockCollector(ScoreMode.COMPLETE_NO_SCORES, MinCompetitiveScoreScorable.class);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(c1, null, resolveTerminateAfterChecker(0), c2, null);
            queryPhaseCollector.getLeafCollector(ctx).setScorer(new MinCompetitiveScoreScorable());
        }
        {
            // both collectors need scores => caching
            Collector c1 = new MockCollector(ScoreMode.COMPLETE, ScoreCachingWrappingScorer.class);
            Collector c2 = new MockCollector(ScoreMode.COMPLETE, ScoreCachingWrappingScorer.class);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(c1, null, resolveTerminateAfterChecker(0), c2, null);
            queryPhaseCollector.getLeafCollector(ctx).setScorer(new MinCompetitiveScoreScorable());
        }
        {
            // both collectors need scores => caching, but one early terminates
            Collector c1 = new TerminateAfterCollector(new MockCollector(ScoreMode.COMPLETE, MinCompetitiveScoreScorable.class), 0);
            Collector c2 = new MockCollector(ScoreMode.COMPLETE, MinCompetitiveScoreScorable.class);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(c1, null, resolveTerminateAfterChecker(0), c2, null);
            queryPhaseCollector.getLeafCollector(ctx).setScorer(new MinCompetitiveScoreScorable());
            queryPhaseCollector = new QueryPhaseCollector(c2, null, resolveTerminateAfterChecker(0), c1, null);
            queryPhaseCollector.getLeafCollector(ctx).setScorer(new MinCompetitiveScoreScorable());
        }
    }

    public void testNoWrappingIfUnnecessaryTopDocsOnly() throws IOException {
        MockCollector mockCollector = new MockCollector(randomFrom(ScoreMode.values()));
        QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(mockCollector, null, resolveTerminateAfterChecker(0), null, null);
        LeafReaderContext context = searcher.getLeafContexts().get(0);
        LeafCollector leafCollector = queryPhaseCollector.getLeafCollector(context);
        assertThat(leafCollector, CoreMatchers.sameInstance(mockCollector));
    }

    public void testNoWrappingIfUnnecessaryTopDocsEarlyTerminated() throws IOException {
        TerminateAfterCollector topDocsCollector = new TerminateAfterCollector(new MockCollector(randomFrom(ScoreMode.values())), 0);
        MockCollector aggsCollector = new MockCollector(randomScoreModeExceptTopScores());
        QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(
            topDocsCollector,
            null,
            resolveTerminateAfterChecker(0),
            aggsCollector,
            null
        );
        LeafReaderContext context = searcher.getLeafContexts().get(0);
        LeafCollector leafCollector = queryPhaseCollector.getLeafCollector(context);
        assertThat(leafCollector, CoreMatchers.sameInstance(aggsCollector));
    }

    public void testCompetitiveIteratorNoAggs() throws IOException {
        // use a post_filter so that we wrap the top docs leaf collector, as this test verifies that
        // the wrapper calls competitiveIterator when appropriated
        Weight postFilterWeight = searcher.createWeight(new MatchAllDocsQuery(), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        MockCollector mockCollector = new MockCollector(randomFrom(ScoreMode.values()));
        QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(
            mockCollector,
            postFilterWeight,
            resolveTerminateAfterChecker(0),
            null,
            null
        );
        LeafReaderContext context = searcher.getLeafContexts().get(0);
        LeafCollector leafCollector = queryPhaseCollector.getLeafCollector(context);
        leafCollector.competitiveIterator();
        assertTrue(mockCollector.competitiveIteratorCalled);
    }

    public void testCompetitiveIteratorWithAggs() throws IOException {
        MockCollector topDocs = new MockCollector(randomFrom(ScoreMode.values()));
        MockCollector aggs = new MockCollector(randomScoreModeExceptTopScores());
        QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topDocs, null, resolveTerminateAfterChecker(0), aggs, null);
        LeafReaderContext context = searcher.getLeafContexts().get(0);
        LeafCollector leafCollector = queryPhaseCollector.getLeafCollector(context);
        leafCollector.competitiveIterator();
        assertFalse(topDocs.competitiveIteratorCalled);
        assertFalse(aggs.competitiveIteratorCalled);
    }

    public void testCompetitiveIteratorWithAggsCollectionTerminated() throws IOException {
        MockCollector topDocsMockCollector = new MockCollector(randomFrom(ScoreMode.values()));
        TerminateAfterCollector collector1 = new TerminateAfterCollector(topDocsMockCollector, 1);
        MockCollector aggsMockCollector = new MockCollector(randomScoreModeExceptTopScores());
        TerminateAfterCollector collector2 = new TerminateAfterCollector(aggsMockCollector, 2);
        QueryPhaseCollector queryPhaseCollector;
        if (randomBoolean()) {
            queryPhaseCollector = new QueryPhaseCollector(collector1, null, resolveTerminateAfterChecker(0), collector2, null);
        } else {
            queryPhaseCollector = new QueryPhaseCollector(collector2, null, resolveTerminateAfterChecker(0), collector1, null);
        }
        LeafReaderContext context = searcher.getLeafContexts().get(0);
        LeafCollector leafCollector = queryPhaseCollector.getLeafCollector(context);
        leafCollector.competitiveIterator();
        assertFalse(topDocsMockCollector.competitiveIteratorCalled);
        assertFalse(aggsMockCollector.competitiveIteratorCalled);
        leafCollector.collect(0);
        leafCollector.competitiveIterator();
        assertFalse(topDocsMockCollector.competitiveIteratorCalled);
        assertFalse(aggsMockCollector.competitiveIteratorCalled);
        leafCollector.collect(1);
        leafCollector.competitiveIterator();
        assertFalse(topDocsMockCollector.competitiveIteratorCalled);
        // when top docs collection has terminated, we forward competitive iterator to aggs collection
        assertTrue(aggsMockCollector.competitiveIteratorCalled);
        aggsMockCollector.competitiveIteratorCalled = false;
        expectThrows(CollectionTerminatedException.class, () -> leafCollector.collect(2));
        assertFalse(topDocsMockCollector.competitiveIteratorCalled);
        assertFalse(aggsMockCollector.competitiveIteratorCalled);
    }

    public void testLeafCollectorsAreNotPulledOnceTerminatedAfter() throws IOException {
        {
            MockCollector topDocsMockCollector = new MockCollector(randomFrom(ScoreMode.values()));
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(
                topDocsMockCollector,
                null,
                resolveTerminateAfterChecker(1),
                null,
                null
            );
            searcher.search(new NonCountingTermQuery(new Term("field1", "value")), queryPhaseCollector);
            assertEquals(1, topDocsMockCollector.leafCollectorsPulled);
        }
        {
            MockCollector topDocsMockCollector = new MockCollector(randomFrom(ScoreMode.values()));
            MockCollector aggsMockCollector = new MockCollector(randomScoreModeExceptTopScores());
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(
                topDocsMockCollector,
                null,
                resolveTerminateAfterChecker(1),
                aggsMockCollector,
                null
            );
            searcher.search(new NonCountingTermQuery(new Term("field1", "value")), queryPhaseCollector);
            assertEquals(1, topDocsMockCollector.leafCollectorsPulled);
        }
    }

    private static ScoreMode randomScoreModeExceptTopScores() {
        return randomFrom(Arrays.stream(ScoreMode.values()).filter(scoreMode -> scoreMode != ScoreMode.TOP_SCORES).toList());
    }

    private static class TerminateAfterCollector extends FilterCollector {
        private final int terminateAfter;
        private int count = 0;

        TerminateAfterCollector(Collector in, int terminateAfter) {
            super(in);
            this.terminateAfter = terminateAfter;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            if (count >= terminateAfter) {
                throw new CollectionTerminatedException();
            }
            final LeafCollector in = super.getLeafCollector(context);
            return new FilterLeafCollector(in) {
                @Override
                public void collect(int doc) throws IOException {
                    if (count >= terminateAfter) {
                        throw new CollectionTerminatedException();
                    }
                    super.collect(doc);
                    count++;
                }

                @Override
                public DocIdSetIterator competitiveIterator() throws IOException {
                    return in.competitiveIterator();
                }
            };
        }
    }

    private static class TopScoresCollector extends SimpleCollector {
        private Scorable scorer;
        float minScore = 0;

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.TOP_SCORES;
        }

        @Override
        public void setScorer(Scorable scorer) {
            this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            minScore = Math.nextUp(minScore);
            scorer.setMinCompetitiveScore(minScore);
        }
    }

    private static class MinCompetitiveScoreScorable extends Scorable {
        boolean setMinCompetitiveScoreCalled = false;

        @Override
        public float score() throws IOException {
            return 0;
        }

        @Override
        public void setMinCompetitiveScore(float minScore) {
            setMinCompetitiveScoreCalled = true;
        }
    }

    private static class MockCollector extends SimpleCollector {
        private final ScoreMode scoreMode;
        private final Class<?> expectedScorable;
        private boolean setScorerCalled = false;
        private boolean setWeightCalled = false;
        private boolean competitiveIteratorCalled = false;
        private int leafCollectorsPulled = 0;

        MockCollector(ScoreMode scoreMode) {
            this(scoreMode, null);
        }

        MockCollector(ScoreMode scoreMode, Class<?> expectedScorable) {
            this.scoreMode = scoreMode;
            this.expectedScorable = expectedScorable;
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) {
            leafCollectorsPulled++;
        }

        @Override
        public void setWeight(Weight weight) {
            setWeightCalled = true;
        }

        @Override
        public ScoreMode scoreMode() {
            return scoreMode;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            setScorerCalled = true;
            if (expectedScorable != null) {
                while (expectedScorable.equals(scorer.getClass()) == false && scorer instanceof FilterScorable) {
                    scorer = scorer.getChildren().iterator().next().child();
                }
                assertEquals(expectedScorable, scorer.getClass());
            }
        }

        @Override
        public DocIdSetIterator competitiveIterator() {
            competitiveIteratorCalled = true;
            return null;
        }

        @Override
        public void collect(int doc) throws IOException {}
    }

    /**
     * Returns a {@link CollectorManager} that creates {@link QueryPhaseCollector}s and reduces results obtained from the wrapped top docs
     * and (optional) aggs collector managers. Its purpose is to test QueryPhaseCollector's behaviour in concurrent scenarios.
     * We don't use {@link QueryPhaseCollectorManager} in this test as that would broaden the scope of this test excessively.
     *
     * @param <T> the type of the result obtained from top docs collection
     * @param <A> the type of the result obtained from the aggs collection
     */
    private static <T, A> CollectorManager<QueryPhaseCollector, Result<T, A>> createCollectorManager(
        CollectorManager<? extends Collector, T> topDocsCollectorManager,
        Weight postFilterWeight,
        int terminateAfter,
        CollectorManager<? extends Collector, A> aggsCollectorManager,
        Float minScore
    ) {
        QueryPhaseCollector.TerminateAfterChecker terminateAfterChecker = resolveTerminateAfterChecker(terminateAfter);

        return new CollectorManager<>() {
            @Override
            public QueryPhaseCollector newCollector() throws IOException {
                return new QueryPhaseCollector(
                    topDocsCollectorManager.newCollector(),
                    postFilterWeight,
                    terminateAfterChecker,
                    aggsCollectorManager == null ? null : aggsCollectorManager.newCollector(),
                    minScore
                );
            }

            @Override
            @SuppressWarnings("unchecked")
            public Result<T, A> reduce(Collection<QueryPhaseCollector> collectors) throws IOException {
                boolean terminatedAfter = collectors.stream().anyMatch(QueryPhaseCollector::isTerminatedAfter);
                T topDocs = ((CollectorManager<Collector, T>) topDocsCollectorManager).reduce(
                    collectors.stream().map(QueryPhaseCollector::getTopDocsCollector).toList()
                );
                if (aggsCollectorManager != null) {
                    A aggs = ((CollectorManager<Collector, A>) aggsCollectorManager).reduce(
                        collectors.stream().map(QueryPhaseCollector::getAggsCollector).toList()
                    );
                    return new Result<>(topDocs, aggs, terminatedAfter);
                }
                return new Result<>(topDocs, null, terminatedAfter);
            }
        };
    }

    private record Result<T, A>(T topDocs, A aggs, boolean terminatedAfter) {};
}
