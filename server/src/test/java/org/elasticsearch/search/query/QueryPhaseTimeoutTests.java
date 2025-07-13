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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.xcontent.Text;
import org.hamcrest.Matchers;
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
        // the upper bound is higher than 4096 so that in some cases we time out after the first batch of bulk scoring, but before
        // getting to the end of the first segment
        numDocs = scaledRandomIntBetween(500, 4500);
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
        // note that no executor is provided, as this test requires sequential execution
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            LuceneTestCase.MAYBE_CACHE_POLICY,
            true
        );
    }

    /**
     * Test that a timeout is appropriately handled when the (exitable) directory reader raises it while loading terms enum
     * as the scorer supplier is requested.
     */
    public void testScorerSupplierTimeoutTerms() throws IOException {
        assumeTrue("Test requires more than one segment", reader.leaves().size() > 1);
        int size = randomBoolean() ? 0 : randomIntBetween(100, 500);
        scorerSupplierTimeoutTest(size, context -> {
            final TermsEnum termsEnum = context.reader().terms("field").iterator();
            termsEnum.next();
        });
    }

    /**
     * Test that a timeout is appropriately handled when the (exitable) directory reader raises it while loading points
     * as the scorer supplier is requested.
     */
    public void testScorerSupplierTimeoutPoints() throws IOException {
        assumeTrue("Test requires more than one segment", reader.leaves().size() > 1);
        int size = randomBoolean() ? 0 : randomIntBetween(100, 500);
        scorerSupplierTimeoutTest(size, context -> {
            PointValues pointValues = context.reader().getPointValues("long");
            pointValues.size();
        });
    }

    private void scorerSupplierTimeoutTest(int size, CheckedConsumer<LeafReaderContext, IOException> timeoutTrigger) throws IOException {
        {
            TimeoutQuery query = newMatchAllScorerSupplierTimeoutQuery(timeoutTrigger, false);
            try (SearchContext context = createSearchContext(query, size)) {
                QueryPhase.executeQuery(context);
                assertFalse(context.queryResult().searchTimedOut());
                assertEquals(numDocs, context.queryResult().topDocs().topDocs.totalHits.value());
                assertEquals(size, context.queryResult().topDocs().topDocs.scoreDocs.length);
            }
        }
        {
            TimeoutQuery query = newMatchAllScorerSupplierTimeoutQuery(timeoutTrigger, true);
            try (SearchContext context = createSearchContextWithTimeout(query, size)) {
                QueryPhase.executeQuery(context);
                assertTrue(context.queryResult().searchTimedOut());
                int firstSegmentMaxDoc = reader.leaves().get(0).reader().maxDoc();
                // we are artificially raising the timeout when pulling the scorer supplier.
                // We score the entire first segment, then trigger timeout.
                assertEquals(firstSegmentMaxDoc, context.queryResult().topDocs().topDocs.totalHits.value());
                assertEquals(Math.min(size, firstSegmentMaxDoc), context.queryResult().topDocs().topDocs.scoreDocs.length);
            }
        }
    }

    private static TimeoutQuery newMatchAllScorerSupplierTimeoutQuery(
        CheckedConsumer<LeafReaderContext, IOException> timeoutTrigger,
        boolean isTimeoutExpected
    ) {
        return new TimeoutQuery() {
            @Override
            public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
                return new MatchAllWeight(this, boost, scoreMode) {
                    boolean firstSegment = true;

                    @Override
                    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                        // trigger the timeout as soon as the scorer supplier is request for the second segment
                        if (firstSegment == false && isTimeoutExpected) {
                            shouldTimeout = true;
                        }
                        timeoutTrigger.accept(context);
                        assert shouldTimeout == false : "should have already timed out";
                        firstSegment = false;
                        return super.scorerSupplier(context);
                    }
                };
            }
        };
    }

    /**
     * Test that a timeout is appropriately handled when the (exitable) directory reader raises it while loading terms enum
     * as the scorer is retrieved from the scorer supplier
     */
    public void testScorerGetTimeoutTerms() throws IOException {
        assumeTrue("Test requires more than one segment", reader.leaves().size() > 1);
        int size = randomBoolean() ? 0 : randomIntBetween(100, 500);
        scorerGetTimeoutTest(size, context -> {
            final TermsEnum termsEnum = context.reader().terms("field").iterator();
            termsEnum.next();
        });
    }

    /**
     * Test that a timeout is appropriately handled when the (exitable) directory reader raises it while loading points
     * as the scorer is retrieved from the scorer supplier
     */
    public void testScorerGetTimeoutPoints() throws IOException {
        assumeTrue("Test requires more than one segment", reader.leaves().size() > 1);
        int size = randomBoolean() ? 0 : randomIntBetween(100, 500);
        scorerGetTimeoutTest(size, context -> {
            PointValues pointValues = context.reader().getPointValues("long");
            pointValues.size();
        });
    }

    private void scorerGetTimeoutTest(int size, CheckedConsumer<LeafReaderContext, IOException> timeoutTrigger) throws IOException {
        {
            TimeoutQuery query = newMatchAllScorerGetTimeoutQuery(timeoutTrigger, false);
            try (SearchContext context = createSearchContext(query, size)) {
                QueryPhase.executeQuery(context);
                assertFalse(context.queryResult().searchTimedOut());
                assertEquals(numDocs, context.queryResult().topDocs().topDocs.totalHits.value());
                assertEquals(size, context.queryResult().topDocs().topDocs.scoreDocs.length);
            }
        }
        {
            TimeoutQuery query = newMatchAllScorerGetTimeoutQuery(timeoutTrigger, true);
            try (SearchContext context = createSearchContextWithTimeout(query, size)) {
                QueryPhase.executeQuery(context);
                assertTrue(context.queryResult().searchTimedOut());
                int firstSegmentMaxDoc = reader.leaves().get(0).reader().maxDoc();
                // we are artificially raising the timeout when pulling the scorer supplier.
                // We score the entire first segment, then trigger timeout.
                assertEquals(firstSegmentMaxDoc, context.queryResult().topDocs().topDocs.totalHits.value());
                assertEquals(Math.min(size, firstSegmentMaxDoc), context.queryResult().topDocs().topDocs.scoreDocs.length);
            }
        }
    }

    private static TimeoutQuery newMatchAllScorerGetTimeoutQuery(
        CheckedConsumer<LeafReaderContext, IOException> timeoutTrigger,
        boolean isTimeoutExpected
    ) {
        return new TimeoutQuery() {
            @Override
            public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
                return new MatchAllWeight(this, boost, scoreMode) {
                    boolean firstSegment = true;

                    @Override
                    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                        ScorerSupplier scorerSupplier = super.scorerSupplier(context);
                        return new ScorerSupplier() {
                            @Override
                            public Scorer get(long leadCost) throws IOException {
                                // trigger the timeout as soon as the scorer is requested for the second segment
                                if (firstSegment == false && isTimeoutExpected) {
                                    shouldTimeout = true;
                                }
                                timeoutTrigger.accept(context);
                                assert shouldTimeout == false : "should have already timed out";
                                firstSegment = false;
                                return scorerSupplier.get(leadCost);
                            }

                            @Override
                            public long cost() {
                                return scorerSupplier.cost();
                            }
                        };
                    }
                };
            }
        };
    }

    /**
     * Test that a timeout is appropriately handled while bulk scoring, via cancellable bulk scorer
     */
    public void testBulkScorerTimeout() throws IOException {
        int size = randomBoolean() ? 0 : randomIntBetween(100, 500);
        {
            TimeoutQuery query = newMatchAllBulkScorerTimeoutQuery(false);
            try (SearchContext context = createSearchContext(query, size)) {
                QueryPhase.executeQuery(context);
                assertFalse(context.queryResult().searchTimedOut());
                assertEquals(numDocs, context.queryResult().topDocs().topDocs.totalHits.value());
                assertEquals(size, context.queryResult().topDocs().topDocs.scoreDocs.length);
            }
        }
        {
            TimeoutQuery query = newMatchAllBulkScorerTimeoutQuery(true);
            try (SearchContext context = createSearchContextWithTimeout(query, size)) {
                QueryPhase.executeQuery(context);
                assertTrue(context.queryResult().searchTimedOut());
                int firstSegmentMaxDoc = reader.leaves().get(0).reader().maxDoc();
                // See CancellableBulkScorer#INITIAL_INTERVAL for the source of 4096: we always score the first
                // batch of up to 4096 docs, and only then raise the timeout
                assertEquals(Math.min(4096, firstSegmentMaxDoc), context.queryResult().topDocs().topDocs.totalHits.value());
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
                    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                        ScorerSupplier inScorerSupplier = super.scorerSupplier(context);
                        return new ScorerSupplier() {
                            @Override
                            public Scorer get(long leadCost) throws IOException {
                                return inScorerSupplier.get(leadCost);
                            }

                            @Override
                            public long cost() {
                                return inScorerSupplier.cost();
                            }

                            @Override
                            public BulkScorer bulkScorer() {
                                final float score = score();
                                final int maxDoc = context.reader().maxDoc();
                                return new BulkScorer() {
                                    @Override
                                    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                                        assert shouldTimeout == false : "should have already timed out";
                                        max = Math.min(max, maxDoc);
                                        Score scorer = new Score();
                                        scorer.score = score;
                                        collector.setScorer(scorer);
                                        for (int doc = min; doc < max; ++doc) {
                                            if (acceptDocs == null || acceptDocs.get(doc)) {
                                                collector.collect(doc);
                                            }
                                        }
                                        if (timeoutExpected) {
                                            // timeout after collecting the first batch of documents from the 1st segment, or the entire 1st
                                            // segment if max > firstSegment.maxDoc()
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
        };
    }

    private TestSearchContext createSearchContextWithTimeout(TimeoutQuery query, int size) throws IOException {
        TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader)) {
            @Override
            public long getRelativeTimeInMillis() {
                // this controls whether a timeout is raised or not. We abstract time away by pretending that the clock stops
                // when a timeout is not expected. The tiniest increment to relative time in millis triggers a timeout.
                // See QueryPhase#getTimeoutCheck
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

    public void testSuggestOnlyWithTimeout() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().suggest(new SuggestBuilder());
        try (SearchContext context = createSearchContextWithSuggestTimeout(searchSourceBuilder)) {
            assertTrue(context.hasOnlySuggest());
            QueryPhase.execute(context);
            assertTrue(context.queryResult().searchTimedOut());
            assertEquals(1, context.queryResult().suggest().size());
            assertEquals(0, context.queryResult().suggest().getSuggestion("suggestion").getEntries().size());
            assertNotNull(context.queryResult().topDocs());
            assertEquals(0, context.queryResult().topDocs().topDocs.totalHits.value());
        }
    }

    public void testSuggestAndQueryWithSuggestTimeout() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().suggest(new SuggestBuilder()).query(new MatchAllQueryBuilder());
        try (SearchContext context = createSearchContextWithSuggestTimeout(searchSourceBuilder)) {
            context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
            assertFalse(context.hasOnlySuggest());
            QueryPhase.execute(context);
            assertThat(context.queryResult().topDocs().topDocs.totalHits.value(), Matchers.greaterThan(0L));
            assertTrue(context.queryResult().searchTimedOut());
            assertEquals(1, context.queryResult().suggest().size());
            assertEquals(0, context.queryResult().suggest().getSuggestion("suggestion").getEntries().size());
        }
    }

    private TestSearchContext createSearchContextWithSuggestTimeout(SearchSourceBuilder searchSourceBuilder) throws IOException {
        ContextIndexSearcher contextIndexSearcher = newContextSearcher(reader);
        SuggestionSearchContext suggestionSearchContext = new SuggestionSearchContext();
        suggestionSearchContext.addSuggestion("suggestion", new TestSuggestionContext(new TestSuggester(contextIndexSearcher), null));
        TestSearchContext context = new TestSearchContext(null, indexShard, contextIndexSearcher) {
            @Override
            public SuggestionSearchContext suggest() {
                return suggestionSearchContext;
            }

            @Override
            public ShardSearchRequest request() {
                SearchRequest searchRequest = new SearchRequest();
                searchRequest.allowPartialSearchResults(true);
                searchRequest.source(searchSourceBuilder);
                return new ShardSearchRequest(
                    OriginalIndices.NONE,
                    searchRequest,
                    indexShard.shardId(),
                    0,
                    1,
                    AliasFilter.EMPTY,
                    1F,
                    0,
                    null
                );
            }
        };
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        return context;
    }

    private static final class TestSuggester extends Suggester<TestSuggestionContext> {
        private final ContextIndexSearcher contextIndexSearcher;

        TestSuggester(ContextIndexSearcher contextIndexSearcher) {
            this.contextIndexSearcher = contextIndexSearcher;
        }

        @Override
        protected TestSuggestion innerExecute(
            String name,
            TestSuggestionContext suggestion,
            IndexSearcher searcher,
            CharsRefBuilder spare
        ) {
            contextIndexSearcher.throwTimeExceededException();
            throw new AssertionError("should have thrown TimeExceededException");
        }

        @Override
        protected TestSuggestion emptySuggestion(String name, TestSuggestionContext suggestion, CharsRefBuilder spare) {
            return new TestSuggestion();
        }
    }

    private static final class TestSuggestionContext extends SuggestionSearchContext.SuggestionContext {
        TestSuggestionContext(Suggester<?> suggester, SearchExecutionContext searchExecutionContext) {
            super(suggester, searchExecutionContext);
        }
    }

    private static final class TestSuggestion extends Suggest.Suggestion<
        Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> {
        TestSuggestion() {
            super("suggestion", 10);
        }

        @Override
        protected Entry<? extends Entry.Option> newEntry(StreamInput in) {
            return new TestSuggestionEntry();
        }

        @Override
        public String getWriteableName() {
            return "suggestion";
        }
    }

    private static final class TestSuggestionEntry extends Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option> {
        @Override
        protected Option newOption(StreamInput in) {
            return new Option(new Text("text"), 1f) {
            };
        }
    }

    private static class Score extends Scorable {
        float score;

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
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            Scorer scorer = new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.all(context.reader().maxDoc()));
            return new DefaultScorerSupplier(scorer);
        }

        @Override
        public final boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }
    }
}
