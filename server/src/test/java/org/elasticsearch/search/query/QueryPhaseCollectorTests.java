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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.FilterScorable;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.DummyTotalHitCountCollector;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class QueryPhaseCollectorTests extends ESTestCase {

    private Directory directory;
    private IndexReader reader;
    private IndexSearcher searcher;
    private int numDocs;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig());
        numDocs = randomIntBetween(900, 1000);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("field1", "value", Field.Store.NO));
            if (i == 0) {
                doc.add(new StringField("field2", "value", Field.Store.NO));
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
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, null, 0, null, null);
            searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(numDocs, topScoreDocCollector.topDocs().totalHits.value);
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, null, 0, null, null);
            searcher.search(new TermQuery(new Term("field2", "value")), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(1, topScoreDocCollector.topDocs().totalHits.value);
        }
    }

    public void testWithAggs() throws IOException {
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            DummyTotalHitCountCollector aggsCollector = new DummyTotalHitCountCollector();
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, null, 0, aggsCollector, null);
            searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(numDocs, topScoreDocCollector.topDocs().totalHits.value);
            assertEquals(numDocs, aggsCollector.getTotalHits());
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            DummyTotalHitCountCollector aggsCollector = new DummyTotalHitCountCollector();
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, null, 0, aggsCollector, null);
            searcher.search(new TermQuery(new Term("field2", "value")), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(1, topScoreDocCollector.topDocs().totalHits.value);
            assertEquals(1, aggsCollector.getTotalHits());
        }
    }

    public void testPostFilterTopDocsOnly() throws IOException {
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            TermQuery termQuery = new TermQuery(new Term("field2", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, filterWeight, 0, null, null);
            searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(1, topScoreDocCollector.topDocs().totalHits.value);
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            TermQuery termQuery = new TermQuery(new Term("field1", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, filterWeight, 0, null, null);
            searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(numDocs, topScoreDocCollector.topDocs().totalHits.value);
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            DummyTotalHitCountCollector aggsCollector = new DummyTotalHitCountCollector();
            TermQuery termQuery = new TermQuery(new Term("field2", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, filterWeight, 0, aggsCollector, null);
            searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(1, topScoreDocCollector.topDocs().totalHits.value);
            // post_filter is not applied to aggs
            assertEquals(reader.maxDoc(), aggsCollector.getTotalHits());
        }
        {
            // the weight is not propagated
            TotalHitCountCollector topDocsCollector = new TotalHitCountCollector();
            TermQuery termQuery = new TermQuery(new Term("field2", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topDocsCollector, filterWeight, 0, null, null);
            searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(1, topDocsCollector.getTotalHits());
        }
    }

    public void testPostFilterWithAggs() throws IOException {
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            DummyTotalHitCountCollector aggsCollector = new DummyTotalHitCountCollector();
            TermQuery termQuery = new TermQuery(new Term("field1", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, filterWeight, 0, aggsCollector, null);
            searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(numDocs, topScoreDocCollector.topDocs().totalHits.value);
            assertEquals(numDocs, aggsCollector.getTotalHits());
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            DummyTotalHitCountCollector aggsCollector = new DummyTotalHitCountCollector();
            TermQuery termQuery = new TermQuery(new Term("field2", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, filterWeight, 0, aggsCollector, null);
            searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(1, topScoreDocCollector.topDocs().totalHits.value);
            // post_filter is not applied to aggs
            assertEquals(reader.maxDoc(), aggsCollector.getTotalHits());
        }
        {
            // the weight is propagated only to the aggs collector
            TotalHitCountCollector topDocsCollector = new TotalHitCountCollector();
            TotalHitCountCollector aggsCollector = new TotalHitCountCollector();
            TermQuery termQuery = new TermQuery(new Term("field2", "value"));
            Weight filterWeight = termQuery.createWeight(searcher, ScoreMode.TOP_DOCS, 1f);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(
                topDocsCollector,
                filterWeight,
                0,
                assertNoCollection(aggsCollector),
                null
            );
            searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(1, topDocsCollector.getTotalHits());
            assertEquals(reader.maxDoc(), aggsCollector.getTotalHits());
        }
    }

    public void testMinScoreTopDocsOnly() throws IOException {
        float maxScore;
        float thresholdScore;
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("field1", "value")), BooleanClause.Occur.MUST)
            .add(new BoostQuery(new TermQuery(new Term("field2", "value")), 200f), BooleanClause.Occur.SHOULD)
            .build();
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(2, 1000);
            searcher.search(booleanQuery, topScoreDocCollector);
            TopDocs topDocs = topScoreDocCollector.topDocs();
            assertEquals(numDocs, topDocs.totalHits.value);
            maxScore = topDocs.scoreDocs[0].score;
            thresholdScore = topDocs.scoreDocs[1].score;
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, null, 0, null, maxScore);
            searcher.search(booleanQuery, new QueryPhaseCollector(topScoreDocCollector, null, 0, null, maxScore));
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(1, topScoreDocCollector.topDocs().totalHits.value);
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, null, 0, null, thresholdScore);
            searcher.search(booleanQuery, queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(numDocs, topScoreDocCollector.topDocs().totalHits.value);
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, null, 0, null, maxScore + 100f);
            searcher.search(booleanQuery, queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(0, topScoreDocCollector.topDocs().totalHits.value);
        }
        {
            // the weight is not propagated to the top docs collector
            TotalHitCountCollector topDocsCollector = new TotalHitCountCollector();
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(assertNoCollection(topDocsCollector), null, 0, null, 100f);
            searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(0, topDocsCollector.getTotalHits());
        }
    }

    public void testMinScoreWithAggs() throws IOException {
        float maxScore;
        float thresholdScore;
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(new TermQuery(new Term("field1", "value")), BooleanClause.Occur.MUST)
            .add(new BoostQuery(new TermQuery(new Term("field2", "value")), 200f), BooleanClause.Occur.SHOULD)
            .build();
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(2, 1000);
            searcher.search(booleanQuery, topScoreDocCollector);
            TopDocs topDocs = topScoreDocCollector.topDocs();
            assertEquals(numDocs, topDocs.totalHits.value);
            maxScore = topDocs.scoreDocs[0].score;
            thresholdScore = topDocs.scoreDocs[1].score;
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            DummyTotalHitCountCollector aggsCollector = new DummyTotalHitCountCollector();
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, null, 0, aggsCollector, maxScore);
            searcher.search(booleanQuery, queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(1, topScoreDocCollector.topDocs().totalHits.value);
            // min_score is applied to aggs as well as top docs
            assertEquals(1, aggsCollector.getTotalHits());
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            DummyTotalHitCountCollector aggsCollector = new DummyTotalHitCountCollector();
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, null, 0, aggsCollector, thresholdScore);
            searcher.search(booleanQuery, queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(numDocs, topScoreDocCollector.topDocs().totalHits.value);
            assertEquals(numDocs, aggsCollector.getTotalHits());
        }
        {
            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(1, 1000);
            DummyTotalHitCountCollector aggsCollector = new DummyTotalHitCountCollector();
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topScoreDocCollector, null, 0, null, maxScore + 100f);
            searcher.search(booleanQuery, queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(0, topScoreDocCollector.topDocs().totalHits.value);
            assertEquals(0, aggsCollector.getTotalHits());
        }
        {
            // the weight is not propagated to either of the collectors
            TotalHitCountCollector topDocsCollector = new TotalHitCountCollector();
            TotalHitCountCollector aggsCollector = new TotalHitCountCollector();
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(topDocsCollector, null, 0, aggsCollector, 100f);
            searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(0, topDocsCollector.getTotalHits());
            assertEquals(0, aggsCollector.getTotalHits());
        }
    }

    public void testWeightIsPropagatedTopDocsOnly() throws IOException {
        {
            TotalHitCountCollector topDocsCollector = new TotalHitCountCollector();
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(assertNoCollection(topDocsCollector), null, 0, null, null);
            searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(reader.maxDoc(), topDocsCollector.getTotalHits());
        }
    }

    public void testWeightIsPropagatedWithAggs() throws IOException {
        {
            TotalHitCountCollector topDocsCollector = new TotalHitCountCollector();
            TotalHitCountCollector aggsCollector = new TotalHitCountCollector();
            QueryPhaseCollector queryPhaseCollector = new QueryPhaseCollector(
                assertNoCollection(topDocsCollector),
                null,
                0,
                assertNoCollection(aggsCollector),
                null
            );
            searcher.search(new MatchAllDocsQuery(), queryPhaseCollector);
            assertFalse(queryPhaseCollector.isTerminatedAfter());
            assertEquals(reader.maxDoc(), topDocsCollector.getTotalHits());
            assertEquals(reader.maxDoc(), aggsCollector.getTotalHits());
        }
    }

    public void testDisablesSetMinCompetitiveScore() throws IOException {
        Scorable scorer = new Scorable() {
            @Override
            public int docID() {
                throw new UnsupportedOperationException();
            }

            @Override
            public float score() {
                return 0;
            }

            @Override
            public void setMinCompetitiveScore(float minScore) {
                throw new AssertionError();
            }
        };

        Collector collector = new SimpleCollector() {
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
        };
        {
            Collector queryPhaseCollector = new QueryPhaseCollector(collector, null, 0, new DummyTotalHitCountCollector(), null);
            LeafCollector leafCollector = queryPhaseCollector.getLeafCollector(reader.leaves().get(0));
            leafCollector.setScorer(scorer);
            leafCollector.collect(0); // no exception
        }
        {
            // without aggs no need to disable set min competitive score
            Collector queryPhaseCollector = new QueryPhaseCollector(collector, null, 0, null, null);
            LeafCollector leafCollector = queryPhaseCollector.getLeafCollector(reader.leaves().get(0));
            final boolean[] called = new boolean[] { false };
            leafCollector.setScorer(new FilterScorable(scorer) {
                @Override
                public void setMinCompetitiveScore(float minScore) {
                    called[0] = true;
                }
            });
            leafCollector.collect(0);
            assertTrue(called[0]);
        }
    }

    /*
        TODO Missing test coverage:
        - different combinations of features used together
        - different combinations of score mode between top docs and aggs
        - port tests from TestMultiCollector around min competitive score etc.
        - Deeper testing of terminate_after, without shortcut total hit count
     */

    private static Collector assertNoCollection(Collector collector) {
        return new FilterCollector(collector) {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                LeafCollector leafCollector = in.getLeafCollector(context);
                return new FilterLeafCollector(leafCollector) {
                    @Override
                    public void collect(int doc) {
                        throw new AssertionError("unexpected collection");
                    }
                };
            }
        };
    }

}
