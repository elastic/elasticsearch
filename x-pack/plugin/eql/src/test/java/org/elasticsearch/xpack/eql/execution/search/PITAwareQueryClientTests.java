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
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.PlainActionFuture;
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
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
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
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.xpack.eql.EqlTestUtils.booleanArrayOf;

public class PITAwareQueryClientTests extends ESTestCase {

    private final List<HitExtractor> keyExtractors = emptyList();
    private static final QueryBuilder[] FILTERS = new QueryBuilder[] {
        rangeQuery("some_timestamp_field").gte("2023-12-07"),
        termQuery("tier", "hot"),
        idsQuery().addIds("1", "2", "3") };
    private static final String[] INDICES = new String[] { "test1", "test2", "test3" };

    public void testQueryFilterUsedInPitAndSearches() {
        executeSequence(false, false, false);
    }

    public void testSkippedProjectDuringSecondaryStageSuppressesIncompleteSequence() {
        executeSequence(true, false, true);
        executeSequence(true, true, true);
    }

    /** An optional CCS remote must not suppress a sequence that completed on the reachable cluster. */
    public void testSkippedCcsClusterDuringSecondaryStageKeepsCompleteSequence() {
        executeSequence(true, true, false);
    }

    private void executeSequence(boolean boundedWindow, boolean skipDuringSecondaryStage, boolean crossProjectEnabled) {
        try (var threadPool = createThreadPool()) {
            final var filter = boundedWindow == false && frequently() ? randomFrom(FILTERS) : null;
            int stages = boundedWindow ? 2 : randomIntBetween(2, 5);
            final var esClient = new ESMockClient(threadPool, filter, stages);
            esClient.boundedWindow = boundedWindow;
            esClient.skipDuringSecondaryStage = skipDuringSecondaryStage;

            EqlConfiguration eqlConfiguration = new EqlConfiguration(
                INDICES,
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
                boundedWindow || randomBoolean(),
                boundedWindow == false && randomBoolean(),
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
                crossProjectEnabled,
                null,
                null
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
                    for (List<HitReference> ref : refs) {
                        List<SearchHit> hits = new ArrayList<>(ref.size());
                        for (HitReference hitRef : ref) {
                            SearchHit hit = new SearchHit(-1, hitRef.id());
                            hit.shard(new SearchShardTarget("node", new ShardId(hitRef.index(), "_na_", 0), null));
                            hits.add(hit);
                        }
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
                            () -> SearchSourceBuilder.searchSource().size(boundedWindow ? 2 : 10).query(matchAllQuery()).terminateAfter(j),
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

            SequenceMatcher matcher = new SequenceMatcher(
                stages,
                false,
                TimeValue.MINUS_ONE,
                boundedWindow ? new Limit(1, 0) : null,
                booleanArrayOf(stages, false),
                cb
            );
            TumblingWindow window = new TumblingWindow(
                eqlClient,
                criteria,
                null,
                matcher,
                Collections.emptyList(),
                boundedWindow || randomBoolean(),
                boundedWindow == false && randomBoolean()
            );
            PlainActionFuture<Payload> future = new PlainActionFuture<>();
            window.execute(future);
            Results results = Results.fromPayload(future.actionGet(TimeValue.timeValueSeconds(10)));
            EqlSearchResponse response = new EqlSearchResponse(
                new EqlSearchResponse.Hits(null, results.sequences(), results.totalHits()),
                results.tookTime().millis(),
                results.timedOut(),
                results.shardFailures()
            );
            try {
                if (boundedWindow) {
                    boolean suppressed = crossProjectEnabled && skipDuringSecondaryStage;
                    assertEquals(suppressed ? 0 : 1, response.hits().sequences().size());
                    assertEquals(suppressed ? 1 : 0, response.shardFailures().size());
                    if (suppressed) {
                        assertEquals("linked", response.shardFailures().getFirst().shard().getClusterAlias());
                    } else {
                        assertEquals(
                            List.of("0", "1"),
                            response.hits().sequences().getFirst().events().stream().map(EqlSearchResponse.Event::id).toList()
                        );
                    }
                }
                assertEquals(0, esClient.searchRequestsRemainingCount);
                assertFalse(esClient.openedPIT);
            } finally {
                response.decRef();
            }
        }
    }

    /**
     *  This class is used by {@code PITFailureTests.testPitCloseOnFailure} method
     *  to test that PIT close is never (wrongly) invoked if PIT open failed.
     */
    private class ESMockClient extends NoOpClient {
        private final QueryBuilder filter;
        private final BytesReference pitId = new BytesArray("test_pit_id");
        private boolean openedPIT = false;
        private int searchRequestsRemainingCount;
        private boolean skipDuringSecondaryStage;
        private boolean boundedWindow;

        ESMockClient(ThreadPool threadPool, QueryBuilder filter, int stages) {
            super(threadPool);
            this.filter = filter;
            this.searchRequestsRemainingCount = stages;
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
                OpenPointInTimeResponse response = new OpenPointInTimeResponse(pitId, 1, 1, 0, 0, SearchResponse.Clusters.EMPTY);
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
                if (boundedWindow && searchRequest.source().terminateAfter() == 1) {
                    assertNotNull(
                        "secondary stage must remain bounded by the base window",
                        ((RangeQueryBuilder) actualQuery.filter().get(1)).to()
                    );
                }
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
            SearchHit[] hits = boundedWindow && ordinal == 0
                ? new SearchHit[] { searchHit(0), searchHit(100) }
                : new SearchHit[] { searchHit(ordinal) };
            SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0.0f);
            SearchResponse.Clusters clusters = SearchResponse.Clusters.EMPTY;
            if (skipDuringSecondaryStage && ordinal == 1) {
                SearchResponse.Cluster skipped = new SearchResponse.Cluster.Builder(
                    new SearchResponse.Cluster("linked", "test*", true, null)
                ).setStatus(SearchResponse.Cluster.Status.SKIPPED)
                    .setFailures(List.of(new ShardSearchFailure(new IllegalStateException("linked stage unavailable"))))
                    .build();
                clusters = new SearchResponse.Clusters(Map.of("linked", skipped), false);
            }
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
                clusters,
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

    private static SearchHit searchHit(int ordinal) {
        SearchHit hit = new SearchHit(ordinal, String.valueOf(ordinal));
        hit.shard(new SearchShardTarget("node", new ShardId(INDICES[0], "_na_", 0), null));
        hit.sortValues(
            new SearchSortValues(new Long[] { (long) ordinal, 1L }, new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW })
        );
        return hit;
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
