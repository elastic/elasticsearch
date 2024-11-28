/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorBase;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TransportSearchIT extends ESIntegTestCase {
    public static class TestPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<AggregationSpec> getAggregations() {
            return Collections.singletonList(
                new AggregationSpec(TestAggregationBuilder.NAME, TestAggregationBuilder::new, TestAggregationBuilder.PARSER)
                    .addResultReader(Max::new)
            );
        }

        @Override
        public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
            /**
             * Set up a fetch sub phase that throws an exception on indices whose name that start with "boom".
             */
            return Collections.singletonList(fetchContext -> new FetchSubPhaseProcessor() {
                @Override
                public void setNextReader(LeafReaderContext readerContext) {}

                @Override
                public StoredFieldsSpec storedFieldsSpec() {
                    return StoredFieldsSpec.NO_REQUIREMENTS;
                }

                @Override
                public void process(FetchSubPhase.HitContext hitContext) {
                    if (fetchContext.getIndexName().startsWith("boom")) {
                        throw new RuntimeException("boom");
                    }
                }
            });
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put("indices.breaker.request.type", "memory").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPlugin.class);
    }

    public void testLocalClusterAlias() throws ExecutionException, InterruptedException {
        long nowInMillis = randomLongBetween(0, Long.MAX_VALUE);
        IndexRequest indexRequest = new IndexRequest("test");
        indexRequest.id("1");
        indexRequest.source("field", "value");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        DocWriteResponse indexResponse = client().index(indexRequest).actionGet();
        assertEquals(RestStatus.CREATED, indexResponse.status());
        TaskId parentTaskId = new TaskId("node", randomNonNegativeLong());

        {
            SearchRequest searchRequest = SearchRequest.subSearchRequest(
                parentTaskId,
                new SearchRequest(),
                Strings.EMPTY_ARRAY,
                "local",
                nowInMillis,
                randomBoolean()
            );
            assertResponse(client().search(searchRequest), searchResponse -> {
                assertEquals(1, searchResponse.getHits().getTotalHits().value());
                SearchHit[] hits = searchResponse.getHits().getHits();
                assertEquals(1, hits.length);
                SearchHit hit = hits[0];
                assertEquals("local", hit.getClusterAlias());
                assertEquals("test", hit.getIndex());
                assertEquals("1", hit.getId());
            });
        }
        {
            SearchRequest searchRequest = SearchRequest.subSearchRequest(
                parentTaskId,
                new SearchRequest(),
                Strings.EMPTY_ARRAY,
                "",
                nowInMillis,
                randomBoolean()
            );
            assertResponse(client().search(searchRequest), searchResponse -> {
                assertEquals(1, searchResponse.getHits().getTotalHits().value());
                SearchHit[] hits = searchResponse.getHits().getHits();
                assertEquals(1, hits.length);
                SearchHit hit = hits[0];
                assertEquals("", hit.getClusterAlias());
                assertEquals("test", hit.getIndex());
                assertEquals("1", hit.getId());
            });
        }
    }

    public void testAbsoluteStartMillis() throws ExecutionException, InterruptedException {
        TaskId parentTaskId = new TaskId("node", randomNonNegativeLong());
        {
            IndexRequest indexRequest = new IndexRequest("test-1970.01.01");
            indexRequest.id("1");
            indexRequest.source("date", "1970-01-01");
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            DocWriteResponse indexResponse = client().index(indexRequest).actionGet();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }
        {
            IndexRequest indexRequest = new IndexRequest("test-1982.01.01");
            indexRequest.id("1");
            indexRequest.source("date", "1982-01-01");
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            DocWriteResponse indexResponse = client().index(indexRequest).actionGet();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }
        {
            assertHitCount(client().search(new SearchRequest()), 2);
        }
        {
            SearchRequest searchRequest = new SearchRequest("<test-{now/d}>");
            searchRequest.indicesOptions(IndicesOptions.fromOptions(true, true, true, true));
            assertResponse(client().search(searchRequest), searchResponse -> assertEquals(0, searchResponse.getTotalShards()));
        }
        {
            SearchRequest searchRequest = SearchRequest.subSearchRequest(
                parentTaskId,
                new SearchRequest(),
                Strings.EMPTY_ARRAY,
                "",
                0,
                randomBoolean()
            );
            assertHitCount(client().search(searchRequest), 2);
        }
        {
            SearchRequest searchRequest = SearchRequest.subSearchRequest(
                parentTaskId,
                new SearchRequest(),
                Strings.EMPTY_ARRAY,
                "",
                0,
                randomBoolean()
            );
            searchRequest.indices("<test-{now/d}>");
            assertResponse(client().search(searchRequest), searchResponse -> {
                assertEquals(1, searchResponse.getHits().getTotalHits().value());
                assertEquals("test-1970.01.01", searchResponse.getHits().getHits()[0].getIndex());
            });
        }
        {
            SearchRequest searchRequest = SearchRequest.subSearchRequest(
                parentTaskId,
                new SearchRequest(),
                Strings.EMPTY_ARRAY,
                "",
                0,
                randomBoolean()
            );
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            RangeQueryBuilder rangeQuery = new RangeQueryBuilder("date");
            rangeQuery.gte("1970-01-01");
            rangeQuery.lt("1982-01-01");
            sourceBuilder.query(rangeQuery);
            searchRequest.source(sourceBuilder);
            assertResponse(client().search(searchRequest), searchResponse -> {
                assertEquals(1, searchResponse.getHits().getTotalHits().value());
                assertEquals("test-1970.01.01", searchResponse.getHits().getHits()[0].getIndex());
            });
        }
    }

    public void testFinalReduce() throws ExecutionException, InterruptedException {
        long nowInMillis = randomLongBetween(0, Long.MAX_VALUE);
        TaskId taskId = new TaskId("node", randomNonNegativeLong());
        {
            IndexRequest indexRequest = new IndexRequest("test");
            indexRequest.id("1");
            indexRequest.source("price", 10);
            DocWriteResponse indexResponse = client().index(indexRequest).actionGet();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }
        {
            IndexRequest indexRequest = new IndexRequest("test");
            indexRequest.id("2");
            indexRequest.source("price", 100);
            DocWriteResponse indexResponse = client().index(indexRequest).actionGet();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }
        indicesAdmin().prepareRefresh("test").get();

        SearchRequest originalRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.size(0);
        originalRequest.source(source);
        TermsAggregationBuilder terms = new TermsAggregationBuilder("terms").userValueTypeHint(ValueType.NUMERIC);
        terms.field("price");
        terms.size(1);
        source.aggregation(terms);

        {
            SearchRequest searchRequest = randomBoolean()
                ? originalRequest
                : SearchRequest.subSearchRequest(taskId, originalRequest, Strings.EMPTY_ARRAY, "remote", nowInMillis, true);
            assertResponse(client().search(searchRequest), searchResponse -> {
                assertEquals(2, searchResponse.getHits().getTotalHits().value());
                InternalAggregations aggregations = searchResponse.getAggregations();
                LongTerms longTerms = aggregations.get("terms");
                assertEquals(1, longTerms.getBuckets().size());
            });
        }
        {
            SearchRequest searchRequest = SearchRequest.subSearchRequest(
                taskId,
                originalRequest,
                Strings.EMPTY_ARRAY,
                "remote",
                nowInMillis,
                false
            );
            assertResponse(client().search(searchRequest), searchResponse -> {
                assertEquals(2, searchResponse.getHits().getTotalHits().value());
                InternalAggregations aggregations = searchResponse.getAggregations();
                LongTerms longTerms = aggregations.get("terms");
                assertEquals(2, longTerms.getBuckets().size());
            });
        }
    }

    public void testWaitForRefreshIndexValidation() throws Exception {
        int numberOfShards = randomIntBetween(3, 10);
        assertAcked(prepareCreate("test1").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)));
        assertAcked(prepareCreate("test2").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)));
        assertAcked(prepareCreate("test3").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)));
        indicesAdmin().prepareAliases().addAlias("test1", "testAlias").get();
        indicesAdmin().prepareAliases().addAlias(new String[] { "test2", "test3" }, "testFailedAlias").get();

        long[] validCheckpoints = new long[numberOfShards];
        Arrays.fill(validCheckpoints, SequenceNumbers.UNASSIGNED_SEQ_NO);

        // no exception
        prepareSearch("testAlias").setWaitForCheckpoints(Collections.singletonMap("testAlias", validCheckpoints)).get().decRef();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            prepareSearch("testFailedAlias").setWaitForCheckpoints(Collections.singletonMap("testFailedAlias", validCheckpoints))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "Failed to resolve wait_for_checkpoints target [testFailedAlias]. Configured target "
                    + "must resolve to a single open index."
            )
        );

        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            prepareSearch("test1").setWaitForCheckpoints(Collections.singletonMap("test1", new long[2]))
        );
        assertThat(
            e2.getMessage(),
            containsString(
                "Target configured with wait_for_checkpoints must search the same number of shards as "
                    + "checkpoints provided. [2] checkpoints provided. Target [test1] which resolved to index [test1] has ["
                    + numberOfShards
                    + "] shards."
            )
        );

        IllegalArgumentException e3 = expectThrows(
            IllegalArgumentException.class,
            prepareSearch("testAlias").setWaitForCheckpoints(Collections.singletonMap("testAlias", new long[2]))
        );
        assertThat(
            e3.getMessage(),
            containsString(
                "Target configured with wait_for_checkpoints must search the same number of shards as "
                    + "checkpoints provided. [2] checkpoints provided. Target [testAlias] which resolved to index [test1] has ["
                    + numberOfShards
                    + "] shards."
            )
        );

        IllegalArgumentException e4 = expectThrows(
            IllegalArgumentException.class,
            prepareSearch("testAlias").setWaitForCheckpoints(Collections.singletonMap("test2", validCheckpoints))
        );
        assertThat(
            e4.getMessage(),
            containsString(
                "Target configured with wait_for_checkpoints must be a concrete index resolved in "
                    + "this search. Target [test2] is not a concrete index resolved in this search."
            )
        );
    }

    public void testShardCountLimit() throws Exception {
        try {
            final int numPrimaries1 = randomIntBetween(2, 10);
            final int numPrimaries2 = randomIntBetween(1, 10);
            assertAcked(prepareCreate("test1").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numPrimaries1)));
            assertAcked(prepareCreate("test2").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numPrimaries2)));

            // no exception
            prepareSearch("test1").get().decRef();

            updateClusterSettings(Settings.builder().put(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey(), numPrimaries1 - 1));

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, prepareSearch("test1"));
            assertThat(
                e.getMessage(),
                containsString("Trying to query " + numPrimaries1 + " shards, which is over the limit of " + (numPrimaries1 - 1))
            );

            updateClusterSettings(Settings.builder().put(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey(), numPrimaries1));

            // no exception
            prepareSearch("test1").get().decRef();

            e = expectThrows(IllegalArgumentException.class, prepareSearch("test1", "test2"));
            assertThat(
                e.getMessage(),
                containsString(
                    "Trying to query " + (numPrimaries1 + numPrimaries2) + " shards, which is over the limit of " + numPrimaries1
                )
            );

        } finally {
            updateClusterSettings(Settings.builder().putNull(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey()));
        }
    }

    public void testSearchIdle() throws Exception {
        int numOfReplicas = randomIntBetween(0, 1);
        internalCluster().ensureAtLeastNumDataNodes(numOfReplicas + 1);
        final Settings.Builder settings = indexSettings(randomIntBetween(1, 5), numOfReplicas).put(
            IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(),
            TimeValue.timeValueMillis(randomIntBetween(50, 500))
        );
        assertAcked(prepareCreate("test").setSettings(settings).setMapping("""
            {"properties":{"created_date":{"type": "date", "format": "yyyy-MM-dd"}}}"""));
        ensureGreen("test");
        assertBusy(() -> {
            for (String node : internalCluster().nodesInclude("test")) {
                final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
                for (IndexShard indexShard : indicesService.indexServiceSafe(resolveIndex("test"))) {
                    assertTrue(indexShard.isSearchIdle());
                }
            }
        });
        prepareIndex("test").setId("1").setSource("created_date", "2020-01-01").get();
        prepareIndex("test").setId("2").setSource("created_date", "2020-01-02").get();
        prepareIndex("test").setId("3").setSource("created_date", "2020-01-03").get();
        assertBusy(
            () -> assertResponse(
                prepareSearch("test").setQuery(new RangeQueryBuilder("created_date").gte("2020-01-02").lte("2020-01-03"))
                    .setPreFilterShardSize(randomIntBetween(1, 3)),
                resp -> assertThat(resp.getHits().getTotalHits().value(), equalTo(2L))
            )
        );
    }

    public void testCircuitBreakerReduceFail() throws Exception {
        int numShards = randomIntBetween(1, 10);
        indexSomeDocs("test", numShards, numShards * 3);

        {
            final AtomicArray<Boolean> responses = new AtomicArray<>(10);
            final CountDownLatch latch = new CountDownLatch(10);
            for (int i = 0; i < 10; i++) {
                int batchReduceSize = randomIntBetween(2, Math.max(numShards + 1, 3));
                SearchRequest request = prepareSearch("test").addAggregation(new TestAggregationBuilder("test"))
                    .setBatchedReduceSize(batchReduceSize)
                    .request();
                final int index = i;
                client().search(request, new ActionListener<>() {
                    @Override
                    public void onResponse(SearchResponse response) {
                        responses.set(index, true);
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        responses.set(index, false);
                        latch.countDown();
                    }
                });
            }
            latch.await();
            assertThat(responses.asList().size(), equalTo(10));
            for (boolean resp : responses.asList()) {
                assertTrue(resp);
            }
            assertBusy(() -> assertThat(requestBreakerUsed(), equalTo(0L)));
        }

        try {
            updateClusterSettings(Settings.builder().put("indices.breaker.request.limit", "1b"));
            final Client client = client();
            assertBusy(() -> {
                Exception exc = expectThrows(
                    Exception.class,
                    client.prepareSearch("test").addAggregation(new TestAggregationBuilder("test"))
                );
                assertThat(exc.getCause().getMessage(), containsString("<reduce_aggs>"));
            });

            final AtomicArray<Exception> exceptions = new AtomicArray<>(10);
            final CountDownLatch latch = new CountDownLatch(10);
            for (int i = 0; i < 10; i++) {
                int batchReduceSize = randomIntBetween(2, Math.max(numShards + 1, 3));
                SearchRequest request = prepareSearch("test").addAggregation(new TestAggregationBuilder("test"))
                    .setBatchedReduceSize(batchReduceSize)
                    .request();
                final int index = i;
                client().search(request, new ActionListener<>() {
                    @Override
                    public void onResponse(SearchResponse response) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception exc) {
                        exceptions.set(index, exc);
                        latch.countDown();
                    }
                });
            }
            latch.await();
            assertThat(exceptions.asList().size(), equalTo(10));
            for (Exception exc : exceptions.asList()) {
                assertThat(exc.getCause().getMessage(), containsString("<reduce_aggs>"));
            }
            assertBusy(() -> assertThat(requestBreakerUsed(), equalTo(0L)));
        } finally {
            updateClusterSettings(Settings.builder().putNull("indices.breaker.request.limit"));
        }
    }

    public void testCircuitBreakerFetchFail() throws Exception {
        int numShards = randomIntBetween(1, 10);
        int numDocs = numShards * 10;
        indexSomeDocs("boom", numShards, numDocs);

        final AtomicArray<Exception> exceptions = new AtomicArray<>(10);
        final CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            int batchReduceSize = randomIntBetween(2, Math.max(numShards + 1, 3));
            SearchRequest request = prepareSearch("boom").setBatchedReduceSize(batchReduceSize)
                .setAllowPartialSearchResults(false)
                .request();
            final int index = i;
            client().search(request, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse response) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception exc) {
                    exceptions.set(index, exc);
                    latch.countDown();
                }
            });
        }
        latch.await();
        assertThat(exceptions.asList().size(), equalTo(10));
        for (Exception exc : exceptions.asList()) {
            assertThat(exc.getCause().getMessage(), containsString("boom"));
        }
        assertBusy(() -> assertThat(requestBreakerUsed(), equalTo(0L)));
    }

    private void indexSomeDocs(String indexName, int numberOfShards, int numberOfDocs) {
        createIndex(indexName, Settings.builder().put("index.number_of_shards", numberOfShards).build());

        for (int i = 0; i < numberOfDocs; i++) {
            DocWriteResponse indexResponse = prepareIndex(indexName).setSource("number", randomInt()).get();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }
        indicesAdmin().prepareRefresh(indexName).get();
    }

    private long requestBreakerUsed() {
        NodesStatsResponse stats = clusterAdmin().prepareNodesStats().setBreaker(true).get();
        long estimated = 0;
        for (NodeStats nodeStats : stats.getNodes()) {
            estimated += nodeStats.getBreaker().getStats(CircuitBreaker.REQUEST).getEstimated();
        }
        return estimated;
    }

    /**
     * A test aggregation that doesn't consume circuit breaker memory when running on shards.
     * It is used to test the behavior of the circuit breaker when reducing multiple aggregations
     * together (coordinator node).
     */
    private static class TestAggregationBuilder extends AbstractAggregationBuilder<TestAggregationBuilder> {
        static final String NAME = "test";

        private static final ObjectParser<TestAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
            NAME,
            TestAggregationBuilder::new
        );

        TestAggregationBuilder(String name) {
            super(name);
        }

        TestAggregationBuilder(StreamInput input) throws IOException {
            super(input);
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            // noop
        }

        @Override
        protected AggregatorFactory doBuild(
            AggregationContext context,
            AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder
        ) throws IOException {
            return new AggregatorFactory(name, context, parent, subFactoriesBuilder, metadata) {
                @Override
                protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
                    throws IOException {
                    return new TestAggregator(name, parent);
                }
            };
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        @Override
        protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
            return new TestAggregationBuilder(name);
        }

        @Override
        public BucketCardinality bucketCardinality() {
            return BucketCardinality.NONE;
        }

        @Override
        public String getType() {
            return "test";
        }

        @Override
        public long bytesToPreallocate() {
            return 0;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.ZERO;
        }
    }

    /**
     * A test aggregator that extends {@link Aggregator} instead of {@link AggregatorBase}
     * to avoid tripping the circuit breaker when executing on a shard.
     */
    private static class TestAggregator extends Aggregator {
        private final String name;
        private final Aggregator parent;

        private TestAggregator(String name, Aggregator parent) {
            this.name = name;
            this.parent = parent;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Aggregator parent() {
            return parent;
        }

        @Override
        public Aggregator subAggregator(String aggregatorName) {
            return null;
        }

        @Override
        public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) {
            return new InternalAggregation[] { buildEmptyAggregation() };
        }

        @Override
        public void releaseAggregations() {}

        @Override
        public InternalAggregation buildEmptyAggregation() {
            return new Max(name(), Double.NaN, DocValueFormat.RAW, null);
        }

        @Override
        public void close() {}

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }

        @Override
        public void preCollection() throws IOException {}

        @Override
        public void postCollection() throws IOException {}

        @Override
        public Aggregator[] subAggregators() {
            throw new UnsupportedOperationException();
        }
    }
}
