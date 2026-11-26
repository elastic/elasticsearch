/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.SearchSortValues;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.eql.analysis.PostAnalyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.execution.assembler.BoxedQueryRequest;
import org.elasticsearch.xpack.eql.execution.assembler.SequenceCriterion;
import org.elasticsearch.xpack.eql.execution.search.extractor.ImplicitTiebreakerHitExtractor;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceMatcher;
import org.elasticsearch.xpack.eql.execution.sequence.TumblingWindow;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.optimizer.Optimizer;
import org.elasticsearch.xpack.eql.planner.Planner;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.session.Payload;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.xpack.eql.EqlTestUtils.booleanArrayOf;
import static org.hamcrest.Matchers.equalTo;

public class PITAwareQueryClientTests extends ESTestCase {

    private final List<HitExtractor> keyExtractors = emptyList();
    private static final QueryBuilder[] FILTERS = new QueryBuilder[] {
        rangeQuery("some_timestamp_field").gte("2023-12-07"),
        termQuery("tier", "hot"),
        idsQuery().addIds("1", "2", "3") };
    private static final String[] INDICES = new String[] { "test1", "test2", "test3" };
    private static final SearchResponse.Cluster.Status SUCCESSFUL = SearchResponse.Cluster.Status.SUCCESSFUL;

    public void testQueryFilterUsedInPitAndSearches() {
        runSequence(frequently() ? randomFrom(FILTERS) : null, SearchResponse.Clusters.EMPTY, SearchResponse.Clusters.EMPTY);
    }

    public void testAHitMissingAfterMatchingDropsTheSequence() {
        List<Payload> missed = new ArrayList<>();
        runSequence(null, SearchResponse.Clusters.EMPTY, SearchResponse.Clusters.EMPTY, true, missed::add);
        assertEquals(0, missed.get(0).values().size());
        assertEquals(1, missed.get(0).shardFailures().size());
        List<Payload> complete = new ArrayList<>();
        runSequence(null, SearchResponse.Clusters.EMPTY, SearchResponse.Clusters.EMPTY, false, complete::add);
        assertEquals(1, complete.get(0).values().size());
        assertEquals(0, complete.get(0).shardFailures().size());
    }

    public void testInnerSearchFollowsTheRequestRoundTripsFlag() {
        assertFalse(innerSearchMinimizesRoundTrips(false));
        assertTrue(innerSearchMinimizesRoundTrips(true));
    }

    private boolean innerSearchMinimizesRoundTrips(boolean ccsMinimizeRoundtrips) {
        try (var threadPool = createThreadPool()) {
            SearchRequest[] captured = new SearchRequest[1];
            NoOpClient esClient = new NoOpClient(threadPool) {
                @Override
                @SuppressWarnings("unchecked")
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    if (request instanceof MultiSearchRequest multi) {
                        captured[0] = multi.requests().get(0);
                        listener.onFailure(new IllegalStateException("stop"));
                        return;
                    }
                    captured[0] = (SearchRequest) request;
                    SearchResponse response = SearchResponse.emptyResponseBuilder().build();
                    try {
                        listener.onResponse((Response) response);
                    } finally {
                        response.decRef();
                    }
                }
            };
            EqlConfiguration eqlConfiguration = new EqlConfiguration(
                INDICES,
                INDICES,
                org.elasticsearch.xpack.ql.util.DateUtils.UTC,
                "nobody",
                "cluster",
                null,
                emptyMap(),
                null,
                TimeValue.timeValueSeconds(30),
                null,
                123,
                1,
                true,
                true,
                null,
                "",
                new TaskId("test", 123),
                new EqlSearchTask(
                    randomLong(),
                    "transport",
                    EqlSearchAction.NAME,
                    "",
                    null,
                    emptyMap(),
                    emptyMap(),
                    new AsyncExecutionId("", new TaskId(randomAlphaOfLength(10), 1)),
                    TimeValue.timeValueDays(5)
                ),
                false,
                null,
                null,
                ccsMinimizeRoundtrips
            );
            EqlSession eqlSession = new EqlSession(
                esClient,
                eqlConfiguration,
                new IndexResolver(esClient, "cluster", DefaultDataTypeRegistry.INSTANCE, () -> emptySet()),
                new PreAnalyzer(),
                new PostAnalyzer(),
                new EqlFunctionRegistry(),
                new Verifier(new Metrics()),
                new Optimizer(),
                new Planner(),
                NoopCircuitBreaker.INSTANCE
            );
            BasicQueryClient queryClient = new BasicQueryClient(eqlSession);
            queryClient.query(() -> SearchSourceBuilder.searchSource().query(matchAllQuery()), ActionListener.noop());
            boolean single = captured[0].isCcsMinimizeRoundtrips();
            SearchRequest inner = new SearchRequest("remote1:logs");
            inner.source(SearchSourceBuilder.searchSource().query(matchAllQuery()));
            queryClient.multiQuery(List.of(inner), ActionListener.noop());
            assertThat(captured[0].isCcsMinimizeRoundtrips(), equalTo(ccsMinimizeRoundtrips));
            return single;
        }
    }

    public void testRemoteClustersSkippedByThePitOrASearchAreRecorded() {
        ShardSearchFailure openFailure = new ShardSearchFailure(new IllegalStateException("remote1 lost"));
        ShardSearchFailure searchFailure = new ShardSearchFailure(new IllegalStateException("remote2 lost"));
        ShardSearchFailure shardFailure = new ShardSearchFailure(new IllegalStateException("remote3 shard lost"));
        ShardSearchFailure everyShardLost = new ShardSearchFailure(
            new IllegalStateException("remote4 shard lost"),
            new SearchShardTarget("n1", new ShardId("test1", "_na_", 0), "remote4")
        );
        EqlSession eqlSession = runSequence(
            null,
            clusters(cluster("remote1", SearchResponse.Cluster.Status.SKIPPED, openFailure), cluster("remote2", SUCCESSFUL)),
            clusters(
                cluster("remote2", SearchResponse.Cluster.Status.SKIPPED, searchFailure),
                cluster("remote3", SearchResponse.Cluster.Status.PARTIAL, shardFailure),
                cluster("remote4", SearchResponse.Cluster.Status.SKIPPED, everyShardLost)
            )
        );
        assertThat(eqlSession.skippedClusters(), equalTo(Map.of("remote1", List.of(openFailure), "remote2", List.of(searchFailure))));
    }

    private static SearchResponse.Clusters clusters(SearchResponse.Cluster... clusters) {
        Map<String, SearchResponse.Cluster> byAlias = new HashMap<>();
        for (SearchResponse.Cluster cluster : clusters) {
            byAlias.put(cluster.getClusterAlias(), cluster);
        }
        return new SearchResponse.Clusters(byAlias);
    }

    private static SearchResponse.Cluster cluster(String alias, SearchResponse.Cluster.Status status, ShardSearchFailure... failures) {
        return new SearchResponse.Cluster(alias, "test1", true, status, 1, 0, 0, 0, List.of(failures), null, false, null);
    }

    private EqlSession runSequence(QueryBuilder filter, SearchResponse.Clusters pitClusters, SearchResponse.Clusters searchClusters) {
        return runSequence(filter, pitClusters, searchClusters, false, payload -> {});
    }

    private EqlSession runSequence(
        QueryBuilder filter,
        SearchResponse.Clusters pitClusters,
        SearchResponse.Clusters searchClusters,
        boolean missFetchedHit,
        Consumer<Payload> payloadConsumer
    ) {
        try (var threadPool = createThreadPool()) {
            int stages = randomIntBetween(2, 5);
            final var esClient = new ESMockClient(threadPool, filter, stages, pitClusters, searchClusters);

            EqlConfiguration eqlConfiguration = new EqlConfiguration(
                INDICES,
                org.elasticsearch.xpack.ql.util.DateUtils.UTC,
                "nobody",
                "cluster",
                filter,
                emptyMap(),
                null,
                TimeValue.timeValueSeconds(30),
                null,
                123,
                1,
                randomBoolean(),
                randomBoolean(),
                null,
                "",
                new TaskId("test", 123),
                new EqlSearchTask(
                    randomLong(),
                    "transport",
                    EqlSearchAction.NAME,
                    "",
                    null,
                    emptyMap(),
                    emptyMap(),
                    new AsyncExecutionId("", new TaskId(randomAlphaOfLength(10), 1)),
                    TimeValue.timeValueDays(5)
                )
            );
            IndexResolver indexResolver = new IndexResolver(esClient, "cluster", DefaultDataTypeRegistry.INSTANCE, () -> emptySet());
            CircuitBreaker cb = NoopCircuitBreaker.INSTANCE;
            EqlSession eqlSession = new EqlSession(
                esClient,
                eqlConfiguration,
                indexResolver,
                new PreAnalyzer(),
                new PostAnalyzer(),
                new EqlFunctionRegistry(),
                new Verifier(new Metrics()),
                new Optimizer(),
                new Planner(),
                cb
            );
            QueryClient eqlClient = new PITAwareQueryClient(eqlSession) {
                @Override
                public void fetchHits(Iterable<List<HitReference>> refs, ActionListener<List<List<SearchHit>>> listener) {
                    List<List<SearchHit>> searchHits = new ArrayList<>();
                    boolean first = true;
                    for (List<HitReference> ref : refs) {
                        List<SearchHit> hits = new ArrayList<>(ref.size());
                        int index = 0;
                        for (HitReference hitRef : ref) {
                            if (missFetchedHit && first && index == ref.size() - 1) {
                                hits.add(null);
                            } else {
                                hits.add(new SearchHit(-1, hitRef.id()));
                            }
                            index++;
                        }
                        first = false;
                        searchHits.add(hits);
                    }
                    listener.onResponse(searchHits);
                }
            };

            List<SequenceCriterion> criteria = new ArrayList<>(stages);
            for (int i = 0; i < stages; i++) {
                final int j = i;
                criteria.add(
                    new SequenceCriterion(
                        i,
                        new BoxedQueryRequest(
                            () -> SearchSourceBuilder.searchSource().size(10).query(matchAllQuery()).terminateAfter(j),
                            "@timestamp",
                            emptyList(),
                            emptySet()
                        ),
                        keyExtractors,
                        TimestampExtractor.INSTANCE,
                        null,
                        ImplicitTiebreakerHitExtractor.INSTANCE,
                        false,
                        false
                    )
                );
            }

            SequenceMatcher matcher = new SequenceMatcher(stages, false, TimeValue.MINUS_ONE, null, booleanArrayOf(stages, false), cb);
            TumblingWindow window = new TumblingWindow(
                eqlClient,
                criteria,
                null,
                matcher,
                Collections.emptyList(),
                randomBoolean(),
                randomBoolean()
            );
            window.execute(wrap(response -> payloadConsumer.accept(response), ex -> { fail("Shouldn't have failed"); }));
            return eqlSession;
        }
    }

    /**
     *  This class is used by {@code PITFailureTests.testPitCloseOnFailure} method
     *  to test that PIT close is never (wrongly) invoked if PIT open failed.
     */
    private class ESMockClient extends NoOpClient {
        private final QueryBuilder filter;
        private final BytesReference pitId = new BytesArray("test_pit_id");
        private final SearchResponse.Clusters pitClusters;
        private final SearchResponse.Clusters searchClusters;
        private boolean openedPIT = false;
        private int searchRequestsRemainingCount;

        ESMockClient(
            ThreadPool threadPool,
            QueryBuilder filter,
            int stages,
            SearchResponse.Clusters pitClusters,
            SearchResponse.Clusters searchClusters
        ) {
            super(threadPool);
            this.filter = filter;
            this.searchRequestsRemainingCount = stages;
            this.pitClusters = pitClusters;
            this.searchClusters = searchClusters;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof OpenPointInTimeRequest openPIT) {
                assertFalse(openedPIT);
                assertEquals(filter, openPIT.indexFilter()); // check that the filter passed on to the eql query is used in opening the pit
                assertArrayEquals(INDICES, openPIT.indices()); // indices for opening pit should be the same as for the eql query itself

                openedPIT = true;
                OpenPointInTimeResponse response = new OpenPointInTimeResponse(pitId, 1, 1, 0, 0, pitClusters);
                listener.onResponse((Response) response);
            } else if (request instanceof ClosePointInTimeRequest closePIT) {
                assertTrue(openedPIT);
                assertThat(closePIT.getId(), equalBytes(pitId));

                openedPIT = false;
                ClosePointInTimeResponse response = new ClosePointInTimeResponse(true, 1);
                listener.onResponse((Response) response);
            } else if (request instanceof SearchRequest searchRequest) {
                assertTrue(openedPIT);
                searchRequestsRemainingCount--;
                assertTrue(searchRequestsRemainingCount >= 0);

                assertThat(searchRequest.source().pointInTimeBuilder().getEncodedId(), equalBytes(pitId));
                assertEquals(0, searchRequest.indices().length); // no indices set in the search request
                assertEquals(1, searchRequest.source().subSearches().size());

                BoolQueryBuilder actualQuery = (BoolQueryBuilder) searchRequest.source().subSearches().get(0).getQueryBuilder();
                assertEquals(3, actualQuery.filter().size());
                assertTrue(actualQuery.filter().get(0) instanceof MatchAllQueryBuilder); // the match_all we used when building the criteria
                assertTrue(actualQuery.filter().get(1) instanceof RangeQueryBuilder);
                QueryBuilder expectedQuery = termsQuery("_index", INDICES); // indices should be used as a filter further on
                assertEquals(expectedQuery, actualQuery.filter().get(2));

                handleSearchRequest(listener, searchRequest);
            } else {
                super.doExecute(action, request, listener);
            }
        }

        @SuppressWarnings("unchecked")
        <Response extends ActionResponse> void handleSearchRequest(ActionListener<Response> listener, SearchRequest searchRequest) {
            int ordinal = searchRequest.source().terminateAfter();
            SearchHit searchHit = new SearchHit(ordinal, String.valueOf(ordinal));
            searchHit.sortValues(
                new SearchSortValues(new Long[] { (long) ordinal, 1L }, new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW })
            );

            SearchHits searchHits = new SearchHits(new SearchHit[] { searchHit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 0.0f);
            SearchResponse response = new SearchResponse(
                searchHits,
                null,
                null,
                false,
                false,
                null,
                0,
                null,
                2,
                0,
                0,
                0,
                List.of(),
                List.of(),
                searchClusters,
                searchRequest.pointInTimeBuilder().getEncodedId(),
                null,
                null
            );

            try {
                ActionListener.respondAndRelease(listener, (Response) response);
            } finally {
                searchHits.decRef();
            }
        }
    }

    private static class TimestampExtractor implements HitExtractor {

        static final TimestampExtractor INSTANCE = new TimestampExtractor();

        @Override
        public String getWriteableName() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public String hitName() {
            return null;
        }

        @Override
        public Timestamp extract(SearchHit hit) {
            return Timestamp.of(String.valueOf(hit.docId()));
        }
    }
}
