/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.search.suggest.SortBy;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
@ESIntegTestCase.SuiteScopeTestCase
public class SearchTimeoutIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(SearchTimeoutPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).build();
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        super.setupSuiteScopeCluster();
        indexRandom(true, "test", randomIntBetween(20, 50));
    }

    /**
     * Test the scenario where the query times out before starting to collect documents, verify that partial hits are not returned
     */
    public void testTopHitsTimeoutBeforeCollecting() {
        // setting the timeout is necessary only because we check that if a TimeExceededException is thrown, a timeout was set
        SearchRequestBuilder searchRequestBuilder = prepareSearch("test").setTimeout(new TimeValue(10, TimeUnit.SECONDS))
            .setQuery(new BulkScorerTimeoutQuery(false));
        ElasticsearchAssertions.assertResponse(searchRequestBuilder, searchResponse -> {
            assertThat(searchResponse.isTimedOut(), equalTo(true));
            assertEquals(0, searchResponse.getShardFailures().length);
            assertEquals(0, searchResponse.getFailedShards());
            assertThat(searchResponse.getSuccessfulShards(), greaterThan(0));
            assertEquals(searchResponse.getSuccessfulShards(), searchResponse.getTotalShards());
            // timeout happened before we could collect any doc, total hits is 0 and no hits are returned
            assertEquals(0, searchResponse.getHits().getTotalHits().value());
            assertEquals(0, searchResponse.getHits().getHits().length);
        });
    }

    /**
     * Test the scenario where the query times out while collecting documents, verify that partial hits results are returned
     */
    public void testTopHitsTimeoutWhileCollecting() {
        // setting the timeout is necessary only because we check that if a TimeExceededException is thrown, a timeout was set
        SearchRequestBuilder searchRequestBuilder = prepareSearch("test").setTimeout(new TimeValue(10, TimeUnit.SECONDS))
            .setQuery(new BulkScorerTimeoutQuery(true));
        ElasticsearchAssertions.assertResponse(searchRequestBuilder, searchResponse -> {
            assertThat(searchResponse.isTimedOut(), equalTo(true));
            assertEquals(0, searchResponse.getShardFailures().length);
            assertEquals(0, searchResponse.getFailedShards());
            assertThat(searchResponse.getSuccessfulShards(), greaterThan(0));
            assertEquals(searchResponse.getSuccessfulShards(), searchResponse.getTotalShards());
            assertThat(searchResponse.getHits().getTotalHits().value(), greaterThan(0L));
            assertThat(searchResponse.getHits().getHits().length, greaterThan(0));
        });
    }

    /**
     * Test the scenario where the query times out before starting to collect documents, verify that partial aggs results are not returned
     */
    public void testAggsTimeoutBeforeCollecting() {
        SearchRequestBuilder searchRequestBuilder = prepareSearch("test").setSize(0)
            // setting the timeout is necessary only because we check that if a TimeExceededException is thrown, a timeout was set
            .setTimeout(new TimeValue(10, TimeUnit.SECONDS))
            .setQuery(new BulkScorerTimeoutQuery(false))
            .addAggregation(new TermsAggregationBuilder("terms").field("field.keyword"));
        ElasticsearchAssertions.assertResponse(searchRequestBuilder, searchResponse -> {
            assertThat(searchResponse.isTimedOut(), equalTo(true));
            assertEquals(0, searchResponse.getShardFailures().length);
            assertEquals(0, searchResponse.getFailedShards());
            assertThat(searchResponse.getSuccessfulShards(), greaterThan(0));
            assertEquals(searchResponse.getSuccessfulShards(), searchResponse.getTotalShards());
            assertEquals(0, searchResponse.getHits().getTotalHits().value());
            assertEquals(0, searchResponse.getHits().getHits().length);
            StringTerms terms = searchResponse.getAggregations().get("terms");
            // timeout happened before we could collect any doc, total hits is 0 and no buckets are returned
            assertEquals(0, terms.getBuckets().size());
        });
    }

    /**
     * Test the scenario where the query times out while collecting documents, verify that partial aggs results are returned
     */
    public void testAggsTimeoutWhileCollecting() {
        SearchRequestBuilder searchRequestBuilder = prepareSearch("test").setSize(0)
            // setting the timeout is necessary only because we check that if a TimeExceededException is thrown, a timeout was set
            .setTimeout(new TimeValue(10, TimeUnit.SECONDS))
            .setQuery(new BulkScorerTimeoutQuery(true))
            .addAggregation(new TermsAggregationBuilder("terms").field("field.keyword"));
        ElasticsearchAssertions.assertResponse(searchRequestBuilder, searchResponse -> {
            assertThat(searchResponse.isTimedOut(), equalTo(true));
            assertEquals(0, searchResponse.getShardFailures().length);
            assertEquals(0, searchResponse.getFailedShards());
            assertThat(searchResponse.getSuccessfulShards(), greaterThan(0));
            assertEquals(searchResponse.getSuccessfulShards(), searchResponse.getTotalShards());
            assertThat(searchResponse.getHits().getTotalHits().value(), greaterThan(0L));
            assertEquals(0, searchResponse.getHits().getHits().length);
            StringTerms terms = searchResponse.getAggregations().get("terms");
            assertEquals(1, terms.getBuckets().size());
            StringTerms.Bucket bucket = terms.getBuckets().get(0);
            assertEquals("value", bucket.getKeyAsString());
            assertThat(bucket.getDocCount(), greaterThan(0L));
        });
    }

    /**
     * Test the scenario where the suggest phase (part of the query phase) times out, yet there are results
     * available coming from executing the query and aggs on each shard.
     */
    public void testSuggestTimeoutWithPartialResults() {
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.setGlobalText("text");
        TimeoutSuggestionBuilder timeoutSuggestionBuilder = new TimeoutSuggestionBuilder();
        suggestBuilder.addSuggestion("suggest", timeoutSuggestionBuilder);
        SearchRequestBuilder searchRequestBuilder = prepareSearch("test").suggest(suggestBuilder)
            .addAggregation(new TermsAggregationBuilder("terms").field("field.keyword"));
        ElasticsearchAssertions.assertResponse(searchRequestBuilder, searchResponse -> {
            assertThat(searchResponse.isTimedOut(), equalTo(true));
            assertEquals(0, searchResponse.getShardFailures().length);
            assertEquals(0, searchResponse.getFailedShards());
            assertThat(searchResponse.getSuccessfulShards(), greaterThan(0));
            assertEquals(searchResponse.getSuccessfulShards(), searchResponse.getTotalShards());
            assertThat(searchResponse.getHits().getTotalHits().value(), greaterThan(0L));
            assertThat(searchResponse.getHits().getHits().length, greaterThan(0));
            StringTerms terms = searchResponse.getAggregations().get("terms");
            assertEquals(1, terms.getBuckets().size());
            StringTerms.Bucket bucket = terms.getBuckets().get(0);
            assertEquals("value", bucket.getKeyAsString());
            assertThat(bucket.getDocCount(), greaterThan(0L));
        });
    }

    /**
     * Test the scenario where the rescore phase (part of the query phase) times out, yet there are results
     * available coming from executing the query and aggs on each shard.
     */
    public void testRescoreTimeoutWithPartialResults() {
        SearchRequestBuilder searchRequestBuilder = prepareSearch("test").setRescorer(new TimeoutRescorerBuilder())
            .addAggregation(new TermsAggregationBuilder("terms").field("field.keyword"));
        ElasticsearchAssertions.assertResponse(searchRequestBuilder, searchResponse -> {
            assertThat(searchResponse.isTimedOut(), equalTo(true));
            assertEquals(0, searchResponse.getShardFailures().length);
            assertEquals(0, searchResponse.getFailedShards());
            assertThat(searchResponse.getSuccessfulShards(), greaterThan(0));
            assertEquals(searchResponse.getSuccessfulShards(), searchResponse.getTotalShards());
            assertThat(searchResponse.getHits().getTotalHits().value(), greaterThan(0L));
            assertThat(searchResponse.getHits().getHits().length, greaterThan(0));
            StringTerms terms = searchResponse.getAggregations().get("terms");
            assertEquals(1, terms.getBuckets().size());
            StringTerms.Bucket bucket = terms.getBuckets().get(0);
            assertEquals("value", bucket.getKeyAsString());
            assertThat(bucket.getDocCount(), greaterThan(0L));
        });
    }

    public void testPartialResultsIntolerantTimeoutBeforeCollecting() {
        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            prepareSearch("test")
                // setting the timeout is necessary only because we check that if a TimeExceededException is thrown, a timeout was set
                .setTimeout(new TimeValue(10, TimeUnit.SECONDS))
                .setQuery(new BulkScorerTimeoutQuery(false))
                .setAllowPartialSearchResults(false) // this line causes timeouts to report failures
        );
        assertTrue(ex.toString().contains("Time exceeded"));
        assertEquals(429, ex.status().getStatus());
    }

    public void testPartialResultsIntolerantTimeoutWhileCollecting() {
        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            prepareSearch("test")
                // setting the timeout is necessary only because we check that if a TimeExceededException is thrown, a timeout was set
                .setTimeout(new TimeValue(10, TimeUnit.SECONDS))
                .setQuery(new BulkScorerTimeoutQuery(true))
                .setAllowPartialSearchResults(false) // this line causes timeouts to report failures
        );
        assertTrue(ex.toString().contains("Time exceeded"));
        assertEquals(429, ex.status().getStatus());
    }

    public void testPartialResultsIntolerantTimeoutWhileSuggestingOnly() {
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.setGlobalText("text");
        TimeoutSuggestionBuilder timeoutSuggestionBuilder = new TimeoutSuggestionBuilder();
        suggestBuilder.addSuggestion("suggest", timeoutSuggestionBuilder);
        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            prepareSearch("test").suggest(suggestBuilder).setAllowPartialSearchResults(false) // this line causes timeouts to report
                                                                                              // failures
        );
        assertTrue(ex.toString().contains("Time exceeded"));
        assertEquals(429, ex.status().getStatus());
    }

    public void testPartialResultsIntolerantTimeoutWhileSuggesting() {
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.setGlobalText("text");
        TimeoutSuggestionBuilder timeoutSuggestionBuilder = new TimeoutSuggestionBuilder();
        suggestBuilder.addSuggestion("suggest", timeoutSuggestionBuilder);
        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            prepareSearch("test").setQuery(new TermQueryBuilder("field", "value"))
                .suggest(suggestBuilder)
                .setAllowPartialSearchResults(false) // this line causes timeouts to report failures
        );
        assertTrue(ex.toString().contains("Time exceeded"));
        assertEquals(429, ex.status().getStatus());
    }

    public void testPartialResultsIntolerantTimeoutWhileRescoring() {
        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            prepareSearch("test").setQuery(new TermQueryBuilder("field", "value"))
                .setRescorer(new TimeoutRescorerBuilder())
                .setAllowPartialSearchResults(false) // this line causes timeouts to report failures
        );
        assertTrue(ex.toString().contains("Time exceeded"));
        assertEquals(429, ex.status().getStatus());
    }

    public static final class SearchTimeoutPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<QuerySpec<?>> getQueries() {
            return Collections.singletonList(new QuerySpec<QueryBuilder>("timeout", BulkScorerTimeoutQuery::new, parser -> {
                throw new UnsupportedOperationException();
            }));
        }

        @Override
        public List<SuggesterSpec<?>> getSuggesters() {
            return Collections.singletonList(new SuggesterSpec<>("timeout", TimeoutSuggestionBuilder::new, parser -> {
                throw new UnsupportedOperationException();
            }, TermSuggestion::new));
        }

        @Override
        public List<RescorerSpec<?>> getRescorers() {
            return Collections.singletonList(new RescorerSpec<>("timeout", TimeoutRescorerBuilder::new, parser -> {
                throw new UnsupportedOperationException();
            }));
        }
    }

    /**
     * Query builder that produces a Lucene Query which throws a
     * {@link org.elasticsearch.search.internal.ContextIndexSearcher.TimeExceededException} before or while scoring documents.
     * This helps make this test not time dependent, otherwise it would be unpredictable when exactly the timeout happens, which is
     * rather important if we want to test that we are able to return partial results on timeout.
     */
    public static final class BulkScorerTimeoutQuery extends AbstractQueryBuilder<BulkScorerTimeoutQuery> {

        private final boolean partialResults;

        BulkScorerTimeoutQuery(boolean partialResults) {
            this.partialResults = partialResults;
        }

        BulkScorerTimeoutQuery(StreamInput in) throws IOException {
            super(in);
            this.partialResults = in.readBoolean();
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeBoolean(partialResults);
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) {}

        @Override
        protected Query doToQuery(SearchExecutionContext context) {
            return new Query() {
                @Override
                public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
                    return new ConstantScoreWeight(this, boost) {
                        @Override
                        public boolean isCacheable(LeafReaderContext ctx) {
                            return false;
                        }

                        @Override
                        public ScorerSupplier scorerSupplier(LeafReaderContext context) {
                            return new ScorerSupplier() {
                                @Override
                                public BulkScorer bulkScorer() {
                                    if (partialResults == false) {
                                        ((ContextIndexSearcher) searcher).throwTimeExceededException();
                                    }
                                    final int maxDoc = context.reader().maxDoc();
                                    return new BulkScorer() {
                                        @Override
                                        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                                            max = Math.min(max, maxDoc);
                                            collector.setScorer(new Scorable() {
                                                @Override
                                                public float score() {
                                                    return 1f;
                                                }
                                            });
                                            for (int doc = min; doc < max; ++doc) {
                                                if (acceptDocs == null || acceptDocs.get(doc)) {
                                                    collector.collect(doc);
                                                    // collect one doc per segment, only then throw a timeout: this ensures partial
                                                    // results are returned
                                                    ((ContextIndexSearcher) searcher).throwTimeExceededException();
                                                }
                                            }
                                            // there is a slight chance that no docs are scored for a specific segment.
                                            // other shards / slices will throw the timeout anyway, one is enough.
                                            return max == maxDoc ? DocIdSetIterator.NO_MORE_DOCS : max;
                                        }

                                        @Override
                                        public long cost() {
                                            return maxDoc;
                                        }
                                    };
                                }

                                @Override
                                public Scorer get(long leadCost) {
                                    assert false;
                                    return new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.all(context.reader().maxDoc()));
                                }

                                @Override
                                public long cost() {
                                    assert false;
                                    return context.reader().maxDoc();
                                }
                            };
                        }
                    };
                }

                @Override
                public String toString(String field) {
                    return "timeout query";
                }

                @Override
                public void visit(QueryVisitor visitor) {
                    visitor.visitLeaf(this);
                }

                @Override
                public boolean equals(Object obj) {
                    return sameClassAs(obj);
                }

                @Override
                public int hashCode() {
                    return classHash();
                }
            };
        }

        @Override
        protected boolean doEquals(BulkScorerTimeoutQuery other) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }

        @Override
        public String getWriteableName() {
            return "timeout";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return null;
        }
    }

    /**
     * Suggestion builder that triggers a timeout as part of its execution
     */
    private static final class TimeoutSuggestionBuilder extends TermSuggestionBuilder {
        TimeoutSuggestionBuilder() {
            super("field");
        }

        TimeoutSuggestionBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return "timeout";
        }

        @Override
        public SuggestionSearchContext.SuggestionContext build(SearchExecutionContext context) {
            return new TimeoutSuggestionContext(new TimeoutSuggester((ContextIndexSearcher) context.searcher()), context);
        }
    }

    private static final class TimeoutSuggester extends Suggester<TimeoutSuggestionContext> {
        private final ContextIndexSearcher contextIndexSearcher;

        TimeoutSuggester(ContextIndexSearcher contextIndexSearcher) {
            this.contextIndexSearcher = contextIndexSearcher;
        }

        @Override
        protected TermSuggestion innerExecute(
            String name,
            TimeoutSuggestionContext suggestion,
            IndexSearcher searcher,
            CharsRefBuilder spare
        ) {
            contextIndexSearcher.throwTimeExceededException();
            throw new AssertionError("should have thrown TimeExceededException");
        }

        @Override
        protected TermSuggestion emptySuggestion(String name, TimeoutSuggestionContext suggestion, CharsRefBuilder spare) {
            return new TermSuggestion(name, suggestion.getSize(), SortBy.SCORE);
        }
    }

    private static final class TimeoutSuggestionContext extends SuggestionSearchContext.SuggestionContext {
        TimeoutSuggestionContext(Suggester<?> suggester, SearchExecutionContext searchExecutionContext) {
            super(suggester, searchExecutionContext);
        }
    }

    private static final class TimeoutRescorerBuilder extends RescorerBuilder<TimeoutRescorerBuilder> {
        TimeoutRescorerBuilder() {
            super();
        }

        TimeoutRescorerBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected void doWriteTo(StreamOutput out) {}

        @Override
        protected void doXContent(XContentBuilder builder, Params params) {}

        @Override
        protected RescoreContext innerBuildContext(int windowSize, SearchExecutionContext context) throws IOException {
            return new RescoreContext(10, new Rescorer() {
                @Override
                public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) {
                    ((ContextIndexSearcher) context.searcher()).throwTimeExceededException();
                    assert false;
                    return null;
                }

                @Override
                public Explanation explain(
                    int topLevelDocId,
                    IndexSearcher searcher,
                    RescoreContext rescoreContext,
                    Explanation sourceExplanation
                ) {
                    throw new UnsupportedOperationException();
                }
            });
        }

        @Override
        public String getWriteableName() {
            return "timeout";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return null;
        }

        @Override
        public RescorerBuilder<TimeoutRescorerBuilder> rewrite(QueryRewriteContext ctx) {
            return this;
        }
    }
}
