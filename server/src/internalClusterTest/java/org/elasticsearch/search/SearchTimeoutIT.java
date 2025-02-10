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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.internal.ContextIndexSearcher;
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
        return Collections.singleton(BulkScorerTimeoutQueryPlugin.class);
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

    public static final class BulkScorerTimeoutQueryPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<QuerySpec<?>> getQueries() {
            return Collections.singletonList(new QuerySpec<QueryBuilder>("timeout", BulkScorerTimeoutQuery::new, parser -> {
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
}
