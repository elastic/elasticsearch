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
import org.apache.lucene.search.Collector;
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
import java.util.Collection;
import java.util.List;

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

    /**
     * Collector manager used temporarily to bridge tests and query phase as they have different expectations when it comes to their
     * return type and how results are retrieved from a collector manager.
     */
    private static class CollectorManagerAdapter<C extends Collector, T> implements CollectorManager<Collector, Void> {
        private final CollectorManager<C, T> wrapped;
        private T result;

        CollectorManagerAdapter(CollectorManager<C, T> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public Collector newCollector() throws IOException {
            return wrapped.newCollector();
        }

        @Override
        public Void reduce(Collection<Collector> collectors) throws IOException {
            @SuppressWarnings("unchecked")
            List<C> cs = collectors.stream().map(collector -> (C) collector).toList();
            result = wrapped.reduce(cs);
            return null;
        }

        public T getResult() {
            return result;
        }
    }

    public void testTopDocsOnly() throws IOException {
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topScoreDocManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topScoreDocManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topScoreDocAdapter, null, 0, null, null);
            searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numDocs, topScoreDocAdapter.getResult().totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topScoreDocManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topScoreDocManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topScoreDocAdapter, null, 0, null, null);
            searcher.search(new TermQuery(new Term("field2", "value")), manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numField2Docs, topScoreDocAdapter.getResult().totalHits.value);
        }
    }

    public void testWithAggs() throws IOException {
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topScoreDocAdapter, null, 0, aggsAdapter, null);
            searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numDocs, topScoreDocAdapter.getResult().totalHits.value);
            assertEquals(numDocs, aggsAdapter.getResult().intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topScoreDocAdapter, null, 0, aggsAdapter, null);
            searcher.search(new TermQuery(new Term("field2", "value")), manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numField2Docs, topScoreDocAdapter.getResult().totalHits.value);
            assertEquals(numField2Docs, aggsAdapter.getResult().intValue());
        }
    }

    public void testPostFilterTopDocsOnly() throws IOException {
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            TermQuery termQuery = new TermQuery(new Term("field2", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topScoreDocAdapter, filterWeight, 0, null, null);
            searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numField2Docs, topScoreDocAdapter.getResult().totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            TermQuery termQuery = new TermQuery(new Term("field1", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topScoreDocAdapter, filterWeight, 0, null, null);
            searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numDocs, topScoreDocAdapter.getResult().totalHits.value);
        }
    }

    public void testPostFilterWithAggs() throws IOException {
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            TermQuery termQuery = new TermQuery(new Term("field1", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topScoreDocAdapter, filterWeight, 0, aggsAdapter, null);
            searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numDocs, topScoreDocAdapter.getResult().totalHits.value);
            assertEquals(numDocs, aggsAdapter.getResult().intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            TermQuery termQuery = new TermQuery(new Term("field2", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topScoreDocAdapter, filterWeight, 0, aggsAdapter, null);
            searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numField2Docs, topScoreDocAdapter.getResult().totalHits.value);
            // post_filter is not applied to aggs
            assertEquals(reader.maxDoc(), aggsAdapter.getResult().intValue());
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
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topScoreDocAdapter, null, 0, null, maxScore);
            searcher.search(booleanQuery, manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numField2Docs, topScoreDocAdapter.getResult().totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topScoreDocAdapter, null, 0, null, thresholdScore);
            searcher.search(booleanQuery, manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numDocs, topScoreDocAdapter.getResult().totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topScoreDocAdapter, null, 0, null, maxScore + 100f);
            searcher.search(booleanQuery, manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(0, topScoreDocAdapter.getResult().totalHits.value);
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
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topScoreDocAdapter, null, 0, aggsAdapter, maxScore);
            searcher.search(booleanQuery, manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numField2Docs, topScoreDocAdapter.getResult().totalHits.value);
            // min_score is applied to aggs as well as top docs
            assertEquals(numField2Docs, aggsAdapter.getResult().intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(
                topScoreDocAdapter,
                null,
                0,
                aggsAdapter,
                thresholdScore
            );
            searcher.search(booleanQuery, manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numDocs, topScoreDocAdapter.getResult().totalHits.value);
            assertEquals(numDocs, aggsAdapter.getResult().intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(
                topScoreDocAdapter,
                null,
                0,
                aggsAdapter,
                maxScore + 100f
            );
            searcher.search(booleanQuery, manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(0, topScoreDocAdapter.getResult().totalHits.value);
            assertEquals(0, aggsAdapter.getResult().intValue());
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
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topScoreDocAdapter, filterWeight, 0, null, maxScore);
            searcher.search(booleanQuery, manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numField2AndField3Docs, topScoreDocAdapter.getResult().totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(
                topScoreDocAdapter,
                filterWeight,
                0,
                null,
                thresholdScore
            );
            searcher.search(booleanQuery, manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numField2Docs, topScoreDocAdapter.getResult().totalHits.value);
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(
                topScoreDocAdapter,
                filterWeight,
                0,
                null,
                maxScore + 100f
            );
            searcher.search(booleanQuery, manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(0, topScoreDocAdapter.getResult().totalHits.value);
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
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(
                topScoreDocAdapter,
                filterWeight,
                0,
                aggsAdapter,
                maxScore
            );
            searcher.search(booleanQuery, manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numField2AndField3Docs, topScoreDocAdapter.getResult().totalHits.value);
            assertEquals(numField3Docs, aggsAdapter.getResult().intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(
                topScoreDocAdapter,
                filterWeight,
                0,
                aggsAdapter,
                thresholdScore
            );
            searcher.search(booleanQuery, manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numField2Docs, topScoreDocAdapter.getResult().totalHits.value);
            assertEquals(numDocs, aggsAdapter.getResult().intValue());
        }
        {
            CollectorManager<TopScoreDocCollector, TopDocs> topDocsManager = TopScoreDocCollector.createSharedManager(1, null, 1000);
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topScoreDocAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(
                topScoreDocAdapter,
                filterWeight,
                0,
                aggsAdapter,
                maxScore + 100f
            );
            searcher.search(booleanQuery, manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(0, topScoreDocAdapter.getResult().totalHits.value);
            assertEquals(0, aggsAdapter.getResult().intValue());
        }
    }

    public void testTerminateAfterTopDocsOnly() throws IOException {
        {
            int terminateAfter = randomIntBetween(1, numDocs - 1);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> topDocsAdapter = new CollectorManagerAdapter<>(topDocsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topDocsAdapter, null, terminateAfter, null, null);
            searcher.search(new MatchAllDocsQuery(), manager);
            assertTrue(manager.isTerminatedEarly());
            assertEquals(terminateAfter, topDocsAdapter.getResult().intValue());
        }
        {
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> topDocsAdapter = new CollectorManagerAdapter<>(topDocsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topDocsAdapter, null, numDocs, null, null);
            searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numDocs, topDocsAdapter.getResult().intValue());
        }
    }

    public void testTerminateAfterWithAggs() throws IOException {
        {
            int terminateAfter = randomIntBetween(1, numDocs - 1);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> topDocsAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topDocsAdapter, null, terminateAfter, aggsAdapter, null);
            searcher.search(new MatchAllDocsQuery(), manager);
            assertTrue(manager.isTerminatedEarly());
            assertEquals(terminateAfter, topDocsAdapter.getResult().intValue());
            assertEquals(terminateAfter, aggsAdapter.getResult().intValue());
        }
        {
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> topDocsAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topDocsAdapter, null, numDocs, aggsAdapter, null);
            searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numDocs, topDocsAdapter.getResult().intValue());
            assertEquals(numDocs, aggsAdapter.getResult().intValue());
        }
    }

    public void testTerminateAfterTopDocsOnlyWithPostFilter() throws IOException {
        TermQuery termQuery = new TermQuery(new Term("field2", "value"));
        Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
        {
            int terminateAfter = randomIntBetween(1, numField2Docs - 1);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> topDocsAdapter = new CollectorManagerAdapter<>(topDocsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(
                topDocsAdapter,
                filterWeight,
                terminateAfter,
                null,
                null
            );
            searcher.search(new MatchAllDocsQuery(), manager);
            assertTrue(manager.isTerminatedEarly());
            assertEquals(terminateAfter, topDocsAdapter.getResult().intValue());
        }
        {
            int terminateAfter = randomIntBetween(numField2Docs, Integer.MAX_VALUE);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> topDocsAdapter = new CollectorManagerAdapter<>(topDocsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(
                topDocsAdapter,
                filterWeight,
                terminateAfter,
                null,
                null
            );
            searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numField2Docs, topDocsAdapter.getResult().intValue());
        }
    }

    public void testTerminateAfterWithAggsAndPostFilter() throws IOException {
        TermQuery termQuery = new TermQuery(new Term("field2", "value"));
        Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
        {
            int terminateAfter = randomIntBetween(1, numField2Docs - 1);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> topDocsAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(
                topDocsAdapter,
                filterWeight,
                terminateAfter,
                aggsAdapter,
                null
            );
            searcher.search(new MatchAllDocsQuery(), manager);
            assertTrue(manager.isTerminatedEarly());
            assertEquals(terminateAfter, topDocsAdapter.getResult().intValue());
            // aggs see more docs because they are not filtered
            assertThat(aggsAdapter.getResult(), Matchers.greaterThanOrEqualTo(terminateAfter));
        }
        {
            int terminateAfter = randomIntBetween(numField2Docs, Integer.MAX_VALUE);
            CollectorManager<DummyTotalHitCountCollector, Integer> topDocsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> topDocsAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(
                topDocsAdapter,
                filterWeight,
                terminateAfter,
                aggsAdapter,
                null
            );
            searcher.search(new MatchAllDocsQuery(), manager);
            assertFalse(manager.isTerminatedEarly());
            assertEquals(numField2Docs, topDocsAdapter.getResult().intValue());
            // aggs see more docs because they are not filtered
            assertThat(aggsAdapter.getResult(), Matchers.greaterThanOrEqualTo(numField2Docs));
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
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topDocsAdapter = new CollectorManagerAdapter<>(topDocsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(topDocsAdapter, null, terminateAfter, null, maxScore);
            searcher.search(booleanQuery, manager);
            assertTrue(manager.isTerminatedEarly());
            assertEquals(terminateAfter, topDocsAdapter.getResult().totalHits.value);
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
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topDocsAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(
                topDocsAdapter,
                null,
                terminateAfter,
                aggsAdapter,
                maxScore
            );
            searcher.search(booleanQuery, manager);
            assertTrue(manager.isTerminatedEarly());
            assertEquals(terminateAfter, topDocsAdapter.getResult().totalHits.value);
            assertEquals(terminateAfter, aggsAdapter.getResult().intValue());
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
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topDocsAdapter = new CollectorManagerAdapter<>(topDocsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(
                topDocsAdapter,
                filterWeight,
                terminateAfter,
                null,
                maxScore
            );
            searcher.search(booleanQuery, manager);
            assertTrue(manager.isTerminatedEarly());
            assertEquals(terminateAfter, topDocsAdapter.getResult().totalHits.value);
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
            CollectorManagerAdapter<TopScoreDocCollector, TopDocs> topDocsAdapter = new CollectorManagerAdapter<>(topDocsManager);
            CollectorManager<DummyTotalHitCountCollector, Integer> aggsManager = DummyTotalHitCountCollector.createManager();
            CollectorManagerAdapter<DummyTotalHitCountCollector, Integer> aggsAdapter = new CollectorManagerAdapter<>(aggsManager);
            QueryPhaseCollectorManager manager = QueryPhaseCollector.createManager(
                topDocsAdapter,
                filterWeight,
                terminateAfter,
                aggsAdapter,
                maxScore
            );
            searcher.search(booleanQuery, manager);
            assertTrue(manager.isTerminatedEarly());
            assertEquals(terminateAfter, topDocsAdapter.getResult().totalHits.value);
            // aggs see more documents because the filter is not applied to them
            assertThat(aggsAdapter.getResult(), Matchers.greaterThanOrEqualTo(terminateAfter));
        }
    }
}
