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
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
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
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
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
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;

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
        TestSearchContext context = new TestSearchContext(createSearchExecutionContext(), indexShard, newContextSearcher(reader)) {
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
        TestSearchContext context = new TestSearchContext(createSearchExecutionContext(), indexShard, newContextSearcher(reader));
        context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        context.parsedQuery(new ParsedQuery(query));
        context.setSize(size);
        return context;
    }

    private SearchExecutionContext createSearchExecutionContext() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .creationDate(System.currentTimeMillis())
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        // final SimilarityService similarityService = new SimilarityService(indexSettings, null, Map.of());
        final long nowInMillis = randomNonNegativeLong();
        return new SearchExecutionContext(
            0,
            0,
            indexSettings,
            new BitsetFilterCache(indexSettings, BitsetFilterCache.Listener.NOOP),
            (ft, fdc) -> ft.fielddataBuilder(fdc).build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService()),
            null,
            MappingLookup.EMPTY,
            null,
            null,
            parserConfig(),
            writableRegistry(),
            null,
            new IndexSearcher(reader),
            () -> nowInMillis,
            null,
            null,
            () -> true,
            null,
            Collections.emptyMap(),
            MapperMetrics.NOOP
        );
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
            context.parsedQuery(new ParsedQuery(Queries.ALL_DOCS_INSTANCE));
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
        TestSearchContext context = new TestSearchContext(createSearchExecutionContext(), indexShard, contextIndexSearcher) {
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

    /**
     * Verifies that when a timeout occurs before search execution and no aggregations
     * are requested, QueryPhase returns an empty partial result with timed_out=true
     * and no aggregation container.
     */
    public void testTimeoutNoAggsReturnsEmptyResult() throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10);

        ContextIndexSearcher base = newContextSearcher(reader);
        ContextIndexSearcher throwing = new ContextIndexSearcher(
            base.getIndexReader(),
            base.getSimilarity(),
            base.getQueryCache(),
            base.getQueryCachingPolicy(),
            true
        ) {
            @Override
            public <C extends Collector, T> T search(Query query, CollectorManager<C, T> collectorManager) {
                this.throwTimeExceededException(); // simulate timeout right as search would begin
                throw new AssertionError("unreachable");
            }
        };

        try (SearchContext context = createSearchContext(source, throwing, null, null, true)) {
            context.parsedQuery(new ParsedQuery(Queries.ALL_DOCS_INSTANCE));

            QueryPhase.execute(context);

            assertTrue("search should be marked timed_out", context.queryResult().searchTimedOut());
            assertNotNull("topDocs must be present even on timeout", context.queryResult().topDocs());
            assertEquals("no hits returned on timeout", 0, context.queryResult().topDocs().topDocs.scoreDocs.length);
            assertNull("no aggs were requested so container should remain null", context.queryResult().aggregations());
        }
    }

    /**
     * Verifies that when both suggestions and aggregations are present in the SearchContext,
     * and a timeout occurs during aggregation setup, QueryPhase returns a well-formed partial
     * result: marked timed_out=true, with empty hits, an empty but non-null aggregation container,
     * and a safely accessible suggest container.
     */
    public void testTimeoutWithAggsAndSuggestsReturnsPartial() throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder().query(new MatchAllQueryBuilder())
            .aggregation(new ForceTimeoutAggregationBuilder("force_timeout"))
            .size(10);

        SuggestionSearchContext suggestCtx = new SuggestionSearchContext();
        suggestCtx.addSuggestion(
            "suggestion",
            new QueryPhaseTimeoutTests.TestSuggestionContext(new QueryPhaseTimeoutTests.TestSuggester(newContextSearcher(reader)), null)
        );

        try (SearchContext context = createSearchContext(source, newContextSearcher(reader), suggestCtx, true)) {
            context.parsedQuery(new ParsedQuery(Queries.ALL_DOCS_INSTANCE));

            QueryPhase.execute(context);

            assertTrue("search should be marked timed_out", context.queryResult().searchTimedOut());
            assertNotNull("topDocs must be present even on timeout", context.queryResult().topDocs());
            assertEquals("no hits returned on timeout", 0, context.queryResult().topDocs().topDocs.scoreDocs.length);
            assertNotNull("aggs container must be non-null on timeout when aggs were requested", context.queryResult().aggregations());
            assertTrue("aggregations list should be empty on timeout", context.queryResult().aggregations().expand().asList().isEmpty());
            context.queryResult().suggest().iterator().forEachRemaining(Assert::assertNotNull);
        }
    }

    /**
     * Verifies that when allowPartialSearchResults=false, a timeout is not converted
     * to a partial response but instead throws a SearchTimeoutException wrapped in
     * a QueryPhaseExecutionException.
     */
    public void testTimeoutDisallowPartialsThrowsException() throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder().query(new MatchAllQueryBuilder())
            .aggregation(new ForceTimeoutAggregationBuilder("force_timeout"))
            .size(10);

        try (SearchContext context = createSearchContext(source, newContextSearcher(reader), null, false)) {
            context.parsedQuery(new ParsedQuery(Queries.ALL_DOCS_INSTANCE));

            // expect QueryPhase to propagate a failure instead of marking timed_out=true
            QueryPhaseExecutionException ex = expectThrows(QueryPhaseExecutionException.class, () -> QueryPhase.execute(context));
            assertNotNull("expected a root cause", ex.getCause());
            assertTrue("expected the cause to be a SearchTimeoutException", ex.getCause() instanceof SearchTimeoutException);
        }
    }

    private TestSearchContext createSearchContext(
        SearchSourceBuilder source,
        ContextIndexSearcher cis,
        SuggestionSearchContext suggestCtx,
        boolean allowPartials
    ) throws IOException {
        AggregatorFactories factories;
        try (AggregationTestHelper aggHelper = new AggregationTestHelper()) {
            aggHelper.initPlugins();
            SearchExecutionContext sec = createSearchExecutionContext();
            AggregationContext aggCtx = aggHelper.createDefaultAggregationContext(sec.getIndexReader(), Queries.ALL_DOCS_INSTANCE);
            factories = source.aggregations().build(aggCtx, null);
        } catch (Exception e) {
            throw new IOException(e);
        }

        SearchContextAggregations scAggs = new SearchContextAggregations(factories, () -> {
            throw new AssertionError("reduce should not be called in this early-timeout test");
        });

        return createSearchContext(source, cis, scAggs, suggestCtx, allowPartials);
    }

    private TestSearchContext createSearchContext(
        SearchSourceBuilder source,
        ContextIndexSearcher cis,
        SearchContextAggregations aggsCtx,
        SuggestionSearchContext suggestCtx,
        boolean allowPartials
    ) throws IOException {
        TestSearchContext ctx = new TestSearchContext(createSearchExecutionContext(), indexShard, cis) {
            @Override
            public SearchContextAggregations aggregations() {
                return aggsCtx;
            }

            @Override
            public SuggestionSearchContext suggest() {
                return suggestCtx;
            }

            @Override
            public ShardSearchRequest request() {
                SearchRequest sr = new SearchRequest();
                sr.allowPartialSearchResults(allowPartials);
                sr.source(source);
                return new ShardSearchRequest(
                    OriginalIndices.NONE,
                    sr,
                    indexShard.shardId(),
                    0,  // slice id
                    1,  // total slices
                    AliasFilter.EMPTY,
                    1F,
                    0,
                    null
                );
            }
        };

        ctx.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
        return ctx;
    }

    /**
     * Helper extending {@link AggregatorTestCase} for creating and initializing
     * aggregation contexts in tests. Handles plugin setup and resource cleanup.
     */
    private static final class AggregationTestHelper extends AggregatorTestCase implements AutoCloseable {
        void intiPlugins() {
            super.initPlugins();
        }

        @Override
        public void close() {
            super.cleanupReleasables();
        }

        AggregationContext createDefaultAggregationContext(IndexReader reader, Query query) throws IOException {
            return createAggregationContext(
                reader,
                createIndexSettings(),
                query,
                new NoneCircuitBreakerService(),
                AggregationBuilder.DEFAULT_PREALLOCATION * 5,
                DEFAULT_MAX_BUCKETS,
                false,
                false
            );
        }
    }

    /**
     * Test aggregation builder that simulates a timeout during collector setup
     * to verify QueryPhase timeout handling behavior.
     */
    private static final class ForceTimeoutAggregationBuilder extends AbstractAggregationBuilder<ForceTimeoutAggregationBuilder> {

        ForceTimeoutAggregationBuilder(String name) {
            super(name);
        }

        @Override
        protected AggregatorFactory doBuild(
            AggregationContext ctx,
            AggregatorFactory parent,
            AggregatorFactories.Builder subfactoriesBuilder
        ) throws IOException {
            return new AggregatorFactory(getName(), ctx, parent, AggregatorFactories.builder(), getMetadata()) {
                @Override
                protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata) {
                    if (ctx.searcher() instanceof ContextIndexSearcher cis) {
                        cis.throwTimeExceededException();
                    }
                    throw new AssertionError("unreachable");
                }
            };
        }

        @Override
        protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
            ForceTimeoutAggregationBuilder copy = new ForceTimeoutAggregationBuilder(getName());
            copy.factoriesBuilder = factoriesBuilder;
            copy.setMetadata(metadata);
            return copy;
        }

        @Override
        public BucketCardinality bucketCardinality() {
            return BucketCardinality.ONE;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeString(getName());
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) {
            return builder;
        }

        @Override
        public String getType() {
            return GlobalAggregationBuilder.NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.zero();
        }

        @Override
        public boolean supportsVersion(TransportVersion version) {
            return super.supportsVersion(version);
        }
    }
}
