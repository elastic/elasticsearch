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
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.TestSearchContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;

public class QueryPhaseTimeoutTests extends IndexShardTestCase {

    private static Directory dir;
    private static IndexReader reader;
    private static int numDocs;
    private IndexShard indexShard;

    @BeforeClass
    public static void init() throws Exception {
        dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        // the upper bound is higher than 2048 so that in some cases we time out after the first batch of bulk scoring, but before
        // getting to the end of the first segment
        numDocs = scaledRandomIntBetween(500, 2500);
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            doc.add(new StringField("field", Integer.toString(i), Field.Store.NO));
            doc.add(new LongPoint("long", i));
            doc.add(new KnnByteVectorField("byte_vector", new byte[] { 1, 2, 3 }));
            doc.add(new KnnFloatVectorField("float_vector", new float[] { 1, 2, 3 }));
            w.addDocument(doc);
        }
        w.close();
        reader = DirectoryReader.open(dir);
    }

    @AfterClass
    public static void destroy() throws Exception {
        if (reader != null) {
            reader.close();
        }
        dir.close();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexShard = newShard(true);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        closeShards(indexShard);
    }

    private static ContextIndexSearcher newContextSearcher(IndexReader reader) throws IOException {
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            LuceneTestCase.MAYBE_CACHE_POLICY,
            true
        );
    }

    public void testScorerTimeoutTerms() throws IOException {
        assumeTrue("Test requires more than one segment", reader.leaves().size() > 1);
        int size = randomBoolean() ? 0 : randomIntBetween(100, 500);
        scorerTimeoutTest(size, context -> {
            final TermsEnum termsEnum = context.reader().terms("field").iterator();
            termsEnum.next();
        });
    }

    public void testScorerTimeoutPoints() throws IOException {
        assumeTrue("Test requires more than one segment", reader.leaves().size() > 1);
        int size = randomBoolean() ? 0 : randomIntBetween(100, 500);
        scorerTimeoutTest(size, context -> {
            PointValues pointValues = context.reader().getPointValues("long");
            pointValues.size();
        });
    }

    private void scorerTimeoutTest(int size, CheckedConsumer<LeafReaderContext, IOException> timeoutTrigger) throws IOException {
        {
            TimeoutQuery query = newMatchAllScorerTimeoutQuery(timeoutTrigger, false);
            try (SearchContext context = createSearchContext(query, size)) {
                QueryPhase.executeQuery(context);
                assertFalse(context.queryResult().searchTimedOut());
                assertEquals(numDocs, context.queryResult().topDocs().topDocs.totalHits.value);
                assertEquals(size, context.queryResult().topDocs().topDocs.scoreDocs.length);
            }
        }
        {
            TimeoutQuery query = newMatchAllScorerTimeoutQuery(timeoutTrigger, true);
            try (SearchContext context = createSearchContextWithTimeout(query, size)) {
                QueryPhase.executeQuery(context);
                assertTrue(context.queryResult().searchTimedOut());
                int firstSegmentMaxDoc = reader.leaves().get(0).reader().maxDoc();
                assertEquals(Math.min(2048, firstSegmentMaxDoc), context.queryResult().topDocs().topDocs.totalHits.value);
                assertEquals(Math.min(size, firstSegmentMaxDoc), context.queryResult().topDocs().topDocs.scoreDocs.length);
            }
        }
    }

    private static TimeoutQuery newMatchAllScorerTimeoutQuery(
        CheckedConsumer<LeafReaderContext, IOException> timeoutTrigger,
        boolean isTimeoutExpected
    ) {
        return new TimeoutQuery() {
            @Override
            public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
                return new MatchAllWeight(this, boost, scoreMode) {
                    boolean firstSegment = true;

                    @Override
                    public Scorer scorer(LeafReaderContext context) throws IOException {
                        if (firstSegment == false && isTimeoutExpected) {
                            shouldTimeout = true;
                        }
                        timeoutTrigger.accept(context);
                        assert shouldTimeout == false : "should have already timed out";
                        firstSegment = false;
                        return super.scorer(context);
                    }
                };
            }
        };
    }

    public void testBulkScorerTimeout() throws IOException {
        int size = randomBoolean() ? 0 : randomIntBetween(100, 500);
        {
            TimeoutQuery query = newMatchAllBulkScorerTimeoutQuery(false);
            try (SearchContext context = createSearchContext(query, size)) {
                QueryPhase.executeQuery(context);
                assertFalse(context.queryResult().searchTimedOut());
                assertEquals(numDocs, context.queryResult().topDocs().topDocs.totalHits.value);
                assertEquals(size, context.queryResult().topDocs().topDocs.scoreDocs.length);
            }
        }
        {
            TimeoutQuery query = newMatchAllBulkScorerTimeoutQuery(true);
            try (SearchContext context = createSearchContextWithTimeout(query, size)) {
                QueryPhase.executeQuery(context);
                assertTrue(context.queryResult().searchTimedOut());
                int firstSegmentMaxDoc = reader.leaves().get(0).reader().maxDoc();
                assertEquals(Math.min(2048, firstSegmentMaxDoc), context.queryResult().topDocs().topDocs.totalHits.value);
                assertEquals(Math.min(size, firstSegmentMaxDoc), context.queryResult().topDocs().topDocs.scoreDocs.length);
            }
        }
    }

    private static TimeoutQuery newMatchAllBulkScorerTimeoutQuery(boolean timeoutExpected) {
        return new TimeoutQuery() {
            @Override
            public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
                return new MatchAllWeight(this, boost, scoreMode) {
                    @Override
                    public BulkScorer bulkScorer(LeafReaderContext context) {
                        final float score = score();
                        final int maxDoc = context.reader().maxDoc();
                        return new BulkScorer() {
                            @Override
                            public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                                assert shouldTimeout == false : "should have already timed out";
                                max = Math.min(max, maxDoc);
                                ScoreAndDoc scorer = new ScoreAndDoc();
                                scorer.score = score;
                                collector.setScorer(scorer);
                                for (int doc = min; doc < max; ++doc) {
                                    scorer.doc = doc;
                                    if (acceptDocs == null || acceptDocs.get(doc)) {
                                        collector.collect(doc);
                                    }
                                }
                                if (timeoutExpected) {
                                    // timeout after collecting the first batch of documents from the 1st segment, or the entire 1st segment
                                    shouldTimeout = true;
                                }
                                return max == maxDoc ? DocIdSetIterator.NO_MORE_DOCS : max;
                            }

                            @Override
                            public long cost() {
                                return 0;
                            }
                        };
                    }
                };
            }
        };
    }

    private TestSearchContext createSearchContextWithTimeout(TimeoutQuery query, int size) throws IOException {
        TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader)) {
            @Override
            public long getRelativeTimeInMillis() {
                return query.shouldTimeout ? 1L : 0L;
            }
        };
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        context.parsedQuery(new ParsedQuery(query));
        context.setSize(size);
        return context;
    }

    private TestSearchContext createSearchContext(Query query, int size) throws IOException {
        TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader));
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        context.parsedQuery(new ParsedQuery(query));
        context.setSize(size);
        return context;
    }

    private static class ScoreAndDoc extends Scorable {
        float score;
        int doc = -1;

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public float score() {
            return score;
        }
    }

    /**
     * Query that allows to artificially simulate a timeout error thrown at different stages during the execution of the query.
     * Used in combination with {@link MatchAllWeight}.
     */
    private abstract static class TimeoutQuery extends Query {
        boolean shouldTimeout = false;

        @Override
        public final String toString(String field) {
            return "timeout query";
        }

        @Override
        public final boolean equals(Object o) {
            return sameClassAs(o);
        }

        @Override
        public final int hashCode() {
            return classHash();
        }

        @Override
        public final void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }
    }

    /**
     * Weight that has similar behaviour to that exposed by {@link org.apache.lucene.search.MatchAllDocsQuery}, but it is not cacheable,
     * and it does not override {@link org.apache.lucene.search.Weight#count(LeafReaderContext)}, which is important to be able to
     * accurately simulate timeout errors
     */
    private abstract static class MatchAllWeight extends ConstantScoreWeight {
        private final ScoreMode scoreMode;

        protected MatchAllWeight(Query query, float score, ScoreMode scoreMode) {
            super(query, score);
            this.scoreMode = scoreMode;
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            return new ConstantScoreScorer(this, score(), scoreMode, DocIdSetIterator.all(context.reader().maxDoc()));
        }

        @Override
        public final boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }
    }
}
