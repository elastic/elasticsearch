/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.DummyTotalHitCountCollector;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;

public class QueryPhaseCollectorManagerTests extends ESTestCase {

    private Directory directory;
    private IndexReader reader;
    private IndexSearcher searcher;
    private int numDocs;
    private int numField2Docs;
    private int numField3Docs;
    private int numField2AndField3Docs;

    @Override
    public void setUp() throws Exception {
        super.setUp();
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

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(reader, directory);
    }

    public void testTopDocsOnly() throws IOException {
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topScoreDocManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            QueryPhaseCollectorManager<TopDocs, ?> manager = QueryPhaseCollector.createManager(topScoreDocManager, null, 0, null, null);
            QueryPhaseCollectorManager.Result<TopDocs, ?> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numDocs, result.topDocs().totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topScoreDocManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            QueryPhaseCollectorManager<TopDocs, ?> manager = QueryPhaseCollector.createManager(topScoreDocManager, null, 0, null, null);
            QueryPhaseCollectorManager.Result<TopDocs, ?> result = searcher.search(new TermQuery(new Term("field2", "value")), manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numField2Docs, result.topDocs().totalHits.value);
        }
    }

    public void testWithAggs() throws IOException {
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<TopDocs, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                null,
                0,
                aggsManager,
                null
            );
            QueryPhaseCollectorManager.Result<TopDocs, Integer> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numDocs, result.topDocs().totalHits.value);
            assertEquals(numDocs, result.aggs().intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<TopDocs, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                null,
                0,
                aggsManager,
                null
            );
            QueryPhaseCollectorManager.Result<TopDocs, Integer> result = searcher.search(
                new TermQuery(new Term("field2", "value")),
                manager
            );
            assertFalse(result.terminatedAfter());
            assertEquals(numField2Docs, result.topDocs().totalHits.value);
            assertEquals(numField2Docs, result.aggs().intValue());
        }
    }

    public void testPostFilterTopDocsOnly() throws IOException {
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            TermQuery termQuery = new TermQuery(new Term("field2", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollectorManager<TopDocs, ?> manager = QueryPhaseCollector.createManager(topDocsManager, filterWeight, 0, null, null);
            QueryPhaseCollectorManager.Result<TopDocs, ?> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numField2Docs, result.topDocs().totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            TermQuery termQuery = new TermQuery(new Term("field1", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollectorManager<TopDocs, ?> manager = QueryPhaseCollector.createManager(topDocsManager, filterWeight, 0, null, null);
            QueryPhaseCollectorManager.Result<TopDocs, ?> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numDocs, result.topDocs().totalHits.value);
        }
    }

    public void testPostFilterWithAggs() throws IOException {
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            TermQuery termQuery = new TermQuery(new Term("field1", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollectorManager<TopDocs, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                filterWeight,
                0,
                aggsManager,
                null
            );
            QueryPhaseCollectorManager.Result<TopDocs, Integer> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numDocs, result.topDocs().totalHits.value);
            assertEquals(numDocs, result.aggs().intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            TermQuery termQuery = new TermQuery(new Term("field2", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollectorManager<TopDocs, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                filterWeight,
                0,
                aggsManager,
                null
            );
            QueryPhaseCollectorManager.Result<TopDocs, Integer> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numField2Docs, result.topDocs().totalHits.value);
            // post_filter is not applied to aggs
            assertEquals(reader.maxDoc(), result.aggs().intValue());
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
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(
                numField2Docs + 1,
                null,
                1000
            );
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value);
            maxScore = topDocs.scoreDocs[0].score;
            thresholdScore = topDocs.scoreDocs[numField2Docs].score;
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            QueryPhaseCollectorManager<TopDocs, ?> manager = QueryPhaseCollector.createManager(topDocsManager, null, 0, null, maxScore);
            QueryPhaseCollectorManager.Result<TopDocs, ?> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numField2Docs, result.topDocs().totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            QueryPhaseCollectorManager<TopDocs, ?> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                null,
                0,
                null,
                thresholdScore
            );
            QueryPhaseCollectorManager.Result<TopDocs, ?> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numDocs, result.topDocs().totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            QueryPhaseCollectorManager<TopDocs, ?> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                null,
                0,
                null,
                maxScore + 100f
            );
            QueryPhaseCollectorManager.Result<TopDocs, ?> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter());
            assertEquals(0, result.topDocs().totalHits.value);
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
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(
                numField2Docs + 1,
                null,
                1000
            );
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value);
            maxScore = topDocs.scoreDocs[0].score;
            thresholdScore = topDocs.scoreDocs[numField2Docs].score;
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<TopDocs, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                null,
                0,
                aggsManager,
                maxScore
            );
            QueryPhaseCollectorManager.Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numField2Docs, result.topDocs().totalHits.value);
            // min_score is applied to aggs as well as top docs
            assertEquals(numField2Docs, result.aggs().intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<TopDocs, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                null,
                0,
                aggsManager,
                thresholdScore
            );
            QueryPhaseCollectorManager.Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numDocs, result.topDocs().totalHits.value);
            assertEquals(numDocs, result.aggs().intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<TopDocs, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                null,
                0,
                aggsManager,
                maxScore + 100f
            );
            QueryPhaseCollectorManager.Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter());
            assertEquals(0, result.topDocs().totalHits.value);
            assertEquals(0, result.aggs().intValue());
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
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(
                numField3Docs + 1,
                null,
                1000
            );
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value);
            maxScore = topDocs.scoreDocs[0].score;
            thresholdScore = topDocs.scoreDocs[numField3Docs].score;
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            QueryPhaseCollectorManager<TopDocs, Object> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                filterWeight,
                0,
                null,
                maxScore
            );
            QueryPhaseCollectorManager.Result<TopDocs, Object> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numField2AndField3Docs, result.topDocs().totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            QueryPhaseCollectorManager<TopDocs, ?> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                filterWeight,
                0,
                null,
                thresholdScore
            );
            QueryPhaseCollectorManager.Result<TopDocs, ?> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numField2Docs, result.topDocs().totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            QueryPhaseCollectorManager<TopDocs, ?> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                filterWeight,
                0,
                null,
                maxScore + 100f
            );
            QueryPhaseCollectorManager.Result<TopDocs, ?> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter());
            assertEquals(0, result.topDocs().totalHits.value);
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
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(
                numField3Docs + 1,
                null,
                1000
            );
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value);
            maxScore = topDocs.scoreDocs[0].score;
            thresholdScore = topDocs.scoreDocs[numField3Docs].score;
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<TopDocs, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                filterWeight,
                0,
                aggsManager,
                maxScore
            );
            QueryPhaseCollectorManager.Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numField2AndField3Docs, result.topDocs().totalHits.value);
            assertEquals(numField3Docs, result.aggs().intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<TopDocs, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                filterWeight,
                0,
                aggsManager,
                thresholdScore
            );
            QueryPhaseCollectorManager.Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numField2Docs, result.topDocs().totalHits.value);
            assertEquals(numDocs, result.aggs().intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<TopDocs, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                filterWeight,
                0,
                aggsManager,
                maxScore + 100f
            );
            QueryPhaseCollectorManager.Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertFalse(result.terminatedAfter());
            assertEquals(0, result.topDocs().totalHits.value);
            assertEquals(0, result.aggs().intValue());
        }
    }

    public void testTerminateAfterTopDocsOnly() throws IOException {
        {
            int terminateAfter = randomIntBetween(1, numDocs - 1);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<Integer, ?> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                null,
                terminateAfter,
                null,
                null
            );
            QueryPhaseCollectorManager.Result<Integer, ?> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertTrue(result.terminatedAfter());
            assertEquals(terminateAfter, result.topDocs().intValue());
        }
        {
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<Integer, ?> manager = QueryPhaseCollector.createManager(topDocsManager, null, numDocs, null, null);
            QueryPhaseCollectorManager.Result<Integer, ?> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numDocs, result.topDocs().intValue());
        }
    }

    public void testTerminateAfterWithAggs() throws IOException {
        {
            int terminateAfter = randomIntBetween(1, numDocs - 1);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<Integer, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                null,
                terminateAfter,
                aggsManager,
                null
            );
            QueryPhaseCollectorManager.Result<Integer, Integer> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertTrue(result.terminatedAfter());
            assertEquals(terminateAfter, result.topDocs().intValue());
            assertEquals(terminateAfter, result.aggs().intValue());
        }
        {
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<Integer, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                null,
                numDocs,
                aggsManager,
                null
            );
            QueryPhaseCollectorManager.Result<Integer, Integer> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numDocs, result.topDocs().intValue());
            assertEquals(numDocs, result.aggs().intValue());
        }
    }

    public void testTerminateAfterTopDocsOnlyWithPostFilter() throws IOException {
        TermQuery termQuery = new TermQuery(new Term("field2", "value"));
        Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
        {
            int terminateAfter = randomIntBetween(1, numField2Docs - 1);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<Integer, ?> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                filterWeight,
                terminateAfter,
                null,
                null
            );
            QueryPhaseCollectorManager.Result<Integer, ?> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertTrue(result.terminatedAfter());
            assertEquals(terminateAfter, result.topDocs().intValue());
        }
        {
            int terminateAfter = randomIntBetween(numField2Docs, Integer.MAX_VALUE);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<Integer, Object> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                filterWeight,
                terminateAfter,
                null,
                null
            );
            QueryPhaseCollectorManager.Result<Integer, Object> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numField2Docs, result.topDocs().intValue());
        }
    }

    public void testTerminateAfterWithAggsAndPostFilter() throws IOException {
        TermQuery termQuery = new TermQuery(new Term("field2", "value"));
        Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
        {
            int terminateAfter = randomIntBetween(1, numField2Docs - 1);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<Integer, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                filterWeight,
                terminateAfter,
                aggsManager,
                null
            );
            QueryPhaseCollectorManager.Result<Integer, Integer> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertTrue(result.terminatedAfter());
            assertEquals(terminateAfter, result.topDocs().intValue());
            // aggs see more docs because they are not filtered
            assertThat(result.aggs(), Matchers.greaterThanOrEqualTo(terminateAfter));
        }
        {
            int terminateAfter = randomIntBetween(numField2Docs, Integer.MAX_VALUE);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<Integer, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                filterWeight,
                terminateAfter,
                aggsManager,
                null
            );
            QueryPhaseCollectorManager.Result<Integer, Integer> result = searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(result.terminatedAfter());
            assertEquals(numField2Docs, result.topDocs().intValue());
            // aggs see more docs because they are not filtered
            assertThat(result.aggs(), Matchers.greaterThanOrEqualTo(numField2Docs));
        }
    }

    public void testTerminateAfterTopDocsOnlyWithMinScore() throws IOException {
        searcher.setSimilarity(new BM25Similarity());
        float maxScore;
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("field1", "value")), BooleanClause.Occur.MUST)
            .add(new BoostQuery(new TermQuery(new Term("field2", "value")), 200f), BooleanClause.Occur.SHOULD)
            .build();
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(
                numField2Docs + 1,
                null,
                1000
            );
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value);
            maxScore = topDocs.scoreDocs[0].score;
        }
        {
            int terminateAfter = randomIntBetween(1, numField2Docs - 1);
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            QueryPhaseCollectorManager<TopDocs, Object> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                null,
                terminateAfter,
                null,
                maxScore
            );
            QueryPhaseCollectorManager.Result<TopDocs, Object> result = searcher.search(booleanQuery, manager);
            assertTrue(result.terminatedAfter());
            assertEquals(terminateAfter, result.topDocs().totalHits.value);
        }
    }

    public void testTerminateAfterWithAggsAndMinScore() throws IOException {
        searcher.setSimilarity(new BM25Similarity());
        float maxScore;
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("field1", "value")), BooleanClause.Occur.MUST)
            .add(new BoostQuery(new TermQuery(new Term("field2", "value")), 200f), BooleanClause.Occur.SHOULD)
            .build();
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(
                numField2Docs + 1,
                null,
                1000
            );
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value);
            maxScore = topDocs.scoreDocs[0].score;
        }
        {
            int terminateAfter = randomIntBetween(1, numField2Docs - 1);
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<TopDocs, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                null,
                terminateAfter,
                aggsManager,
                maxScore
            );
            QueryPhaseCollectorManager.Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertTrue(result.terminatedAfter());
            assertEquals(terminateAfter, result.topDocs().totalHits.value);
            assertEquals(terminateAfter, result.aggs().intValue());
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
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(
                numField3Docs + 1,
                null,
                1000
            );
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value);
            maxScore = topDocs.scoreDocs[0].score;
        }
        {
            int terminateAfter = randomIntBetween(1, numField2AndField3Docs - 1);
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            QueryPhaseCollectorManager<TopDocs, Object> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                filterWeight,
                terminateAfter,
                null,
                maxScore
            );
            QueryPhaseCollectorManager.Result<TopDocs, ?> result = searcher.search(booleanQuery, manager);
            assertTrue(result.terminatedAfter());
            assertEquals(terminateAfter, result.topDocs().totalHits.value);
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
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(
                numField3Docs + 1,
                null,
                1000
            );
            TopDocs topDocs = searcher.search(booleanQuery, topDocsManager);
            assertEquals(numDocs, topDocs.totalHits.value);
            maxScore = topDocs.scoreDocs[0].score;
        }
        {
            int terminateAfter = randomIntBetween(1, numField2AndField3Docs - 1);
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            QueryPhaseCollectorManager<TopDocs, Integer> manager = QueryPhaseCollector.createManager(
                topDocsManager,
                filterWeight,
                terminateAfter,
                aggsManager,
                maxScore
            );
            QueryPhaseCollectorManager.Result<TopDocs, Integer> result = searcher.search(booleanQuery, manager);
            assertTrue(result.terminatedAfter());
            assertEquals(terminateAfter, result.topDocs().totalHits.value);
            // aggs see more documents because the filter is not applied to them
            assertThat(result.aggs(), Matchers.greaterThanOrEqualTo(terminateAfter));
        }
    }
}
