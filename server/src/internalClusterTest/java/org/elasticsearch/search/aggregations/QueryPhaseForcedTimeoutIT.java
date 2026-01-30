/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.query.SearchTimeoutException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration test verifying that a TimeExceededException thrown during collector
 * preparation (before the actual search executes) is caught in QueryPhase and
 * correctly transformed into a partial response with `timed_out=true`, empty hits,
 * and empty aggregations rather than an exception.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class QueryPhaseForcedTimeoutIT extends ESIntegTestCase {

    private static final String INDEX = "index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ForceTimeoutAggPlugin.class);
    }

    @Before
    public void setupIndex() throws Exception {
        assertAcked(prepareCreate(INDEX).setMapping("""
            {
              "properties": {
                "kwd":   { "type": "keyword" },
                "txt": { "type": "text" }
              }
            }
            """));

        for (int i = 0; i < 10; i++) {
            IndexRequest ir = new IndexRequest(INDEX).source(
                jsonBuilder().startObject().field("kwd", "value" + i).field("txt", "text " + i).endObject()
            );
            client().index(ir).actionGet();
        }
        indicesAdmin().prepareRefresh(INDEX).get();
        ensureGreen(INDEX);
    }

    @After
    public void cleanup() {
        indicesAdmin().prepareDelete(INDEX).get();
    }

    /**
     * Executes a search using the ForceTimeoutAggPlugin aggregation which throws
     * TimeExceededException during collector preparation, and asserts that:
     * - the response is returned without failure,
     * - the `timed_out` flag is true,
     * - hits are empty, and
     * - aggregations are non-null but empty.
     */
    public void testTimeoutDuringCollectorPreparationReturnsTimedOutEmptyResult() {
        SearchResponse resp = null;
        try {
            resp = client().prepareSearch(INDEX)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(10)
                .setAllowPartialSearchResults(true)
                .addAggregation(new ForceTimeoutAggregationBuilder("force_timeout"))
                .get();

            assertThat(resp, notNullValue());
            assertThat("search should be marked timed_out", resp.isTimedOut(), is(true));
            assertThat("no hits returned", resp.getHits().getHits().length, equalTo(0));
            assertThat(resp.getAggregations(), notNullValue());
            assertThat("no aggr returned", resp.getAggregations().asList().isEmpty(), is(true));
            assertThat("no shard failures expected", resp.getShardFailures() == null || resp.getShardFailures().length == 0, is(true));
        } finally {
            if (resp != null) {
                resp.decRef();
            }
        }
    }

    /**
     * In this test we explicitly set allow_partial_search_results=false. Under this
     * setting, any shard-level failure in the query phase (including a timeout) is treated as
     * a hard failure for the whole search. The coordinating node does not return a response
     * with  timed_out=true, instead it fails the phase and throws a
     * {@link SearchPhaseExecutionException} whose cause is the underlying
     * {@link SearchTimeoutException}. This test asserts that behavior.
     */
    public void testTimeoutDuringCollectorPreparationDisallowPartialsThrowsException() {
        SearchPhaseExecutionException ex = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch(INDEX)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(10)
                .setAllowPartialSearchResults(false)
                .addAggregation(new ForceTimeoutAggregationBuilder("force_timeout"))
                .get()
        );

        assertNotNull("expected a cause on SearchPhaseExecutionException", ex.getCause());
        assertThat("expected inner cause to be SearchTimeoutException", ex.getCause(), instanceOf(SearchTimeoutException.class));
    }

    /**
     * A minimal plugin registering a custom aggregation (ForceTimeoutAggregationBuilder)
     * whose factory simulates a timeout during collector setup to test QueryPhase handling.
     */
    public static class ForceTimeoutAggPlugin extends Plugin implements SearchPlugin {
        public static final String NAME = "force_timeout_plugin";

        @Override
        public List<AggregationSpec> getAggregations() {
            return List.of(new AggregationSpec(NAME, ForceTimeoutAggregationBuilder::new, ForceTimeoutAggregationBuilder::parse));
        }
    }

    /**
     * Aggregation builder for the ForceTimeoutAggPlugin aggregation.
     * It has no parameters and its factory immediately triggers a timeout exception
     * when the search collectors are being prepared.
     */
    static class ForceTimeoutAggregationBuilder extends AbstractAggregationBuilder<ForceTimeoutAggregationBuilder> {

        public static final String TYPE = ForceTimeoutAggPlugin.NAME;

        private Map<String, Object> metadata;

        ForceTimeoutAggregationBuilder(String name) {
            super(name);
        }

        ForceTimeoutAggregationBuilder(StreamInput in) throws IOException {
            super(in);
        }

        static ForceTimeoutAggregationBuilder parse(XContentParser parser, String name) {
            return new ForceTimeoutAggregationBuilder(name);
        }

        @Override
        protected AggregatorFactory doBuild(
            AggregationContext context,
            AggregatorFactory parent,
            AggregatorFactories.Builder subfactoriesBuilder
        ) throws IOException {
            return new ForceTimeoutAggregatorFactory(getName(), context, parent, factoriesBuilder, getMetadata());
        }

        @Override
        protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
            ForceTimeoutAggregationBuilder copy = new ForceTimeoutAggregationBuilder(getName());
            copy.factoriesBuilder = factoriesBuilder;
            copy.setMetadata(metadata);
            return copy;
        }

        @Override
        public Map<String, Object> getMetadata() {
            return metadata;
        }

        @Override
        public BucketCardinality bucketCardinality() {
            return BucketCardinality.ONE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.zero();
        }

        @Override
        protected void doWriteTo(StreamOutput out) {
            // Empty
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) {
            return builder;
        }

        @Override
        public String getType() {
            return TYPE;
        }

        /**
         * Factory implementation for ForceTimeoutAggregationBuilder.
         * Its createInternal() method throws a TimeExceededException
         * before any actual collection occurs, simulating a timeout during setup.
         */
        static class ForceTimeoutAggregatorFactory extends AggregatorFactory {

            ForceTimeoutAggregatorFactory(
                String name,
                AggregationContext context,
                AggregatorFactory parent,
                AggregatorFactories.Builder subFactoriesBuilder,
                Map<String, Object> metadata
            ) throws IOException {
                super(name, context, parent, subFactoriesBuilder, metadata);
            }

            @Override
            protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata) {
                if (context.searcher() instanceof ContextIndexSearcher cis) {
                    cis.throwTimeExceededException();
                }
                throw new AssertionError("unreachable");
            }
        }
    }
}
